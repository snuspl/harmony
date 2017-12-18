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
import edu.snu.cay.utils.ConfigurationUtils;
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
import java.util.concurrent.atomic.AtomicBoolean;
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

  private final Injector jobBaseInjector;

  /**
   * It maintains {@link JobMaster}s of running jobs.
   */
  private final Map<String, JobMaster> jobMasterMap = new ConcurrentHashMap<>();

  private final AtomicBoolean isClosed = new AtomicBoolean(false);

  @Inject
  private JobServerDriver(final ETMaster etMaster,
                          final JobMessageObserver jobMessageObserver,
                          final Injector jobBaseInjector,
                          final JobServerStatusManager jobServerStatusManager,
                          final JobScheduler jobScheduler)
      throws IOException, InjectionException {
    this.jobMessageObserver = jobMessageObserver;
    this.jobBaseInjector = jobBaseInjector;
    this.jobServerStatusManager = jobServerStatusManager;
    this.jobScheduler = jobScheduler;
  }

  /**
   * Gets a {@link JobMaster} with {@code jobId}.
   * @param jobId a dolphin job identifier
   */
  JobMaster getJobMaster(final String jobId) {
    return jobMasterMap.get(jobId);
  }

  public void putJobMaster(final String jobId, final JobMaster jobMaster) {
    if (jobMasterMap.put(jobId, jobMaster) != null) {
      throw new RuntimeException();
    }
  }

  public void removeJobMaster(final String jobId) {
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
      sendMessageToClient("Now, Job Server is ready to receive commands");
    }
  }

  /**
   * Initiates a shutdown in which previously submitted jobs are executed, but no new jobs will be accepted.
   * Invocation has no additional effect if already shut down.
   */
  private void shutdown() {
    final String shutdownMsg = "Initiates shutdown of JobServer";

    sendMessageToClient(shutdownMsg);
    LOG.log(Level.INFO, shutdownMsg);

    if (isClosed.compareAndSet(false, true)) {
      jobServerStatusManager.finishJobServer();
    }
  }

  /**
   * Handles command message from client.
   * There are following commands:
   *    SUBMIT                    to submit a new job.
   *    SHUTDOWN                  to shutdown the job server.
   */
  public final class ClientMessageHandler implements EventHandler<byte[]> {

    @Override
    public synchronized void onNext(final byte[] bytes) {
      final String input = new String(bytes);
      final String[] result = input.split(COMMAND_DELIMITER, 2);
      final String command = result[0];

      // ignore commands
      if (isClosed.get()) {
        final String rejectMsg = String.format("Job Server is being shut down. Rejected command: %s", command);
        LOG.log(Level.INFO, rejectMsg);
        sendMessageToClient(rejectMsg);
        return;
      }

      switch (command) {
      case SUBMIT_COMMAND:
        try {
          final String serializedConf = result[1];
          final Configuration jobConf = ConfigurationUtils.fromString(serializedConf);
          final JobEntity jobEntity = JobEntityBuilder.get(jobBaseInjector, jobConf).build();

          final boolean isAccepted = jobScheduler.onJobArrival(jobEntity);

          final String jobAcceptMsg = isAccepted ?
              String.format("Accept. JobId: %s", jobEntity.getJobId()) :
              String.format("Reject. JobId: %s", jobEntity.getJobId());
          sendMessageToClient(jobAcceptMsg);
          LOG.log(Level.INFO, jobAcceptMsg);

        } catch (InjectionException | IOException e) {
          throw new RuntimeException("The given job configuration is incomplete", e);
        }
        break;
      case SHUTDOWN_COMMAND:
        shutdown();
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
    jobMessageObserver.sendMessageToClient(message.getBytes());
  }
}
