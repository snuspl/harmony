/*
 * Copyright (C) 2017 Seoul National University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.snu.cay.jobserver.driver;

import edu.snu.cay.services.et.driver.api.ETMaster;
import edu.snu.cay.utils.CatchableExecutors;
import edu.snu.cay.utils.ConfigurationUtils;
import edu.snu.cay.utils.StateMachine;
import org.apache.reef.driver.client.JobMessageObserver;
import org.apache.reef.driver.context.FailedContext;
import org.apache.reef.driver.evaluator.FailedEvaluator;
import org.apache.reef.driver.task.FailedTask;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.annotations.Unit;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.time.event.StartTime;

import javax.inject.Inject;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.logging.Level;
import java.util.logging.Logger;

import static edu.snu.cay.jobserver.Parameters.*;

/**
 * Driver code for JobServer.
 * Upon request from client,
 * 1) it executes a job
 * 2) or finishes itself.
 */
@Unit
public final class JobServerDriver {
  private static final Logger LOG = Logger.getLogger(JobServerDriver.class.getName());

  private final JobMessageObserver jobMessageObserver;
  private final JobServerStatusManager jobServerStatusManager;
  private final JobScheduler jobScheduler;

  private final ResourcePool resourcePool;

  private final Injector jobBaseInjector;

  /**
   * It maintains {@link JobMaster}s of running jobs.
   */
  private final Map<String, JobMaster> jobMasterMap = new ConcurrentHashMap<>();

  private final StateMachine stateMachine;

  private final ExecutorService submitCommandHandler = CatchableExecutors.newFixedThreadPool(4);
  private final ExecutorService shutdownCommandHandler = CatchableExecutors.newFixedThreadPool(1);

  @Inject
  private JobServerDriver(final ETMaster etMaster,
                          final JobMessageObserver jobMessageObserver,
                          final Injector jobBaseInjector,
                          final JobServerStatusManager jobServerStatusManager,
                          final JobScheduler jobScheduler,
                          final ResourcePool resourcePool)
      throws IOException, InjectionException {
    this.jobMessageObserver = jobMessageObserver;
    this.jobBaseInjector = jobBaseInjector;
    this.jobServerStatusManager = jobServerStatusManager;
    this.jobScheduler = jobScheduler;
    this.resourcePool = resourcePool;
    this.stateMachine = initStateMachine();
  }

  private enum State {
    NOT_INIT,
    INIT,
    CLOSED
  }

  private static StateMachine initStateMachine() {
    return StateMachine.newBuilder()
        .addState(State.NOT_INIT, "JobServer is not initialized yet.")
        .addState(State.INIT, "JobServer has been initialized. It can handle commands from clients.")
        .addState(State.CLOSED, "JobServer has been closed. It cannot handle anymore commands.")
        .addTransition(State.NOT_INIT, State.INIT, "JobServer is initialized")
        .addTransition(State.INIT, State.CLOSED, "JobServer is closed.")
        .setInitialState(State.NOT_INIT)
        .build();
  }

  /**
   * Gets a {@link JobMaster} with {@code jobId}.
   * @param jobId a dolphin job identifier
   */
  JobMaster getJobMaster(final String jobId) {
    return jobMasterMap.get(jobId);
  }

  /**
   * Register a {@link JobMaster} upon job start.
   * @param jobId a job Id
   * @param jobMaster a {@link JobMaster}
   */
  void registerJobMaster(final String jobId, final JobMaster jobMaster) {
    if (jobMasterMap.put(jobId, jobMaster) != null) {
      throw new RuntimeException();
    }
  }

  /**
   * Deregisters a {@link JobMaster} upon job finish.
   * @param jobId a job Id
   */
  void deregisterJobMaster(final String jobId) {
    if (jobMasterMap.remove(jobId) == null) {
      throw new RuntimeException();
    }
  }

  /**
   * A driver start handler for showing network information to client.
   */
  public final class StartHandler implements EventHandler<StartTime> {
    @Override
    public void onNext(final StartTime startTime) {
      CatchableExecutors.newSingleThreadExecutor().submit(JobServerDriver.this::init);
    }
  }

  private synchronized void init() {
    stateMachine.checkState(State.NOT_INIT);

    resourcePool.init();
    sendMessageToClient("Now, Job Server is ready to receive commands");

    stateMachine.setState(State.INIT);
  }

  /**
   * Showdown JobServer immediately by forcibly closing all executors.
   */
  private synchronized void shutdown() {
    if (stateMachine.getCurrentState() != State.INIT) {
      return;
    }

    sendMessageToClient("Shutdown JobServer");

    stateMachine.setState(State.CLOSED);

    jobServerStatusManager.finishJobServer();
    resourcePool.close();
  }

  /**
   * Handles command message from client.
   * There are following commands:
   *    SUBMIT                    to submit a new job, providing a job configuration as an argument.
   *    SHUTDOWN                  to shutdown the job server.
   */
  public final class ClientMessageHandler implements EventHandler<byte[]> {

    @Override
    public synchronized void onNext(final byte[] bytes) {
      final String input = new String(bytes);
      final String[] result = input.split(COMMAND_DELIMITER, 2);
      final String command = result[0];

      // ignore commands
      if (stateMachine.getCurrentState() == State.NOT_INIT) {
        sendMessageToClient(String.format("Job Server is not initialized yet. Rejected command: %s", command));
        return;
      } else if (stateMachine.getCurrentState() == State.CLOSED) {
        sendMessageToClient(String.format("Job Server is being shut down. Rejected command: %s", command));
        return;
      }

      switch (command) {
      case SUBMIT_COMMAND:
        submitCommandHandler.submit(() -> {
          try {
            final String serializedConf = result[1];
            final Configuration jobConf = ConfigurationUtils.fromString(serializedConf);
            final JobEntity jobEntity = JobEntity.getJobEntity(jobBaseInjector, jobConf);

            final boolean isAccepted = jobScheduler.onJobArrival(jobEntity);

            sendMessageToClient(isAccepted ?
                String.format("Accept. JobId: %s", jobEntity.getJobId()) :
                String.format("Reject. JobId: %s", jobEntity.getJobId()));

          } catch (InjectionException | IOException e) {
            throw new RuntimeException("The given job configuration is incomplete", e);
          }
        });
        break;
      case SHUTDOWN_COMMAND:
        shutdownCommandHandler.submit(JobServerDriver.this::shutdown);
        break;
      default:
        throw new RuntimeException("There is unexpected command");
      }
    }
  }

  /**
   * Handler for FailedContext, which throws RuntimeException to shutdown the entire job.
   */
  public final class FailedContextHandler implements EventHandler<FailedContext> {
    @Override
    public void onNext(final FailedContext failedContext) {
      // TODO #677: Handle failure from Evaluators properly
      throw new RuntimeException(failedContext.asError());
    }
  }

  /**
   * Handler for FailedEvaluator, which throws RuntimeException to shutdown the entire job.
   */
  public final class FailedEvaluatorHandler implements EventHandler<FailedEvaluator> {
    @Override
    public void onNext(final FailedEvaluator failedEvaluator) {
      // TODO #677: Handle failure from Evaluators properly
      throw new RuntimeException(failedEvaluator.getEvaluatorException());
    }
  }

  /**
   * Handler for FailedTask, which throws RuntimeException to shutdown the entire job.
   */
  public final class FailedTaskHandler implements EventHandler<FailedTask> {
    @Override
    public void onNext(final FailedTask failedTask) {
      // TODO #677: Handle failure from Evaluators properly
      throw new RuntimeException(failedTask.asError());
    }
  }

  private void sendMessageToClient(final String message) {
    LOG.log(Level.INFO, message);
    jobMessageObserver.sendMessageToClient(message.getBytes());
  }
}
