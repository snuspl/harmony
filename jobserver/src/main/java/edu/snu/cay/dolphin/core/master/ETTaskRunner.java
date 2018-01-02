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
package edu.snu.cay.dolphin.core.master;

import edu.snu.cay.dolphin.core.server.ServerTasklet;
import edu.snu.cay.jobserver.JobLogger;
import edu.snu.cay.dolphin.core.worker.WorkerTasklet;
import edu.snu.cay.jobserver.Parameters;
import edu.snu.cay.services.et.driver.api.AllocatedExecutor;
import edu.snu.cay.services.et.driver.api.ETMaster;
import edu.snu.cay.services.et.driver.impl.RunningTasklet;
import edu.snu.cay.services.et.driver.impl.TaskletResult;
import edu.snu.cay.services.et.exceptions.ExecutorNotExistException;
import org.apache.reef.driver.client.JobMessageObserver;
import org.apache.reef.tang.InjectionFuture;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;

/**
 * A class for running tasks in server and worker executors.
 * It responds to the change of the entry of worker/server executors.
 * It also tracks active worker tasks for {@link #waitAndGetTaskResult()}.
 */
public final class ETTaskRunner {
  private final JobLogger jobLogger;

  private final ETMaster etMaster;

  private final JobMessageObserver jobMessageObserver;

  private final InjectionFuture<DolphinMaster> etDolphinMasterFuture;

  private final WorkerStateManager workerStateManager;

  private final String jobId;

  private final Map<String, AllocatedExecutor> workerExecutors = new ConcurrentHashMap<>();
  private final Map<String, AllocatedExecutor> serverExecutors = new ConcurrentHashMap<>();

  private final Map<String, RunningTasklet> workerExecutorIdToTasklet = new ConcurrentHashMap<>();
  private final Map<String, RunningTasklet> serverExecutorIdToTasklet = new ConcurrentHashMap<>();

  @Inject
  private ETTaskRunner(final JobLogger jobLogger,
                       final InjectionFuture<DolphinMaster> dolphinMasterFuture,
                       final JobMessageObserver jobMessageObserver,
                       final ETMaster etMaster,
                       final WorkerStateManager workerStateManager,
                       @Parameter(Parameters.JobId.class) final String jobId) {
    this.jobLogger = jobLogger;
    this.etMaster = etMaster;
    this.jobMessageObserver = jobMessageObserver;
    this.etDolphinMasterFuture = dolphinMasterFuture;
    this.workerStateManager = workerStateManager;
    this.jobId = jobId;
  }

  /**
   * Runs tasks on worker executors. It returns when all the worker task finish.
   * With optimization, the number of workers changes during runtime by {@link #updateExecutorEntry}.
   * @param workers a set of initial worker executors
   * @return a list of {@link TaskletResult}
   */
  public List<TaskletResult> run(final List<AllocatedExecutor> workers,
                                 final List<AllocatedExecutor> servers) {
    workers.forEach(worker -> workerExecutors.put(worker.getId(), worker));
    servers.forEach(server -> serverExecutors.put(server.getId(), server));

    // submit dummy tasks to servers
    servers.forEach(server -> server.submitTasklet(etDolphinMasterFuture.get().getServerTaskletConf())
        .addListener(runningTasklet -> serverExecutorIdToTasklet.put(server.getId(), runningTasklet)));
    workers.forEach(worker -> worker.submitTasklet(etDolphinMasterFuture.get().getWorkerTaskletConf())
        .addListener(runningTasklet -> workerExecutorIdToTasklet.put(worker.getId(), runningTasklet)));

    jobLogger.log(Level.INFO, "Wait for workers to finish run stage");

    workerStateManager.waitWorkersToFinishRunStage();

    workers.clear();
    workers.addAll(workerExecutors.values());

    servers.clear();
    servers.addAll(serverExecutors.values());

    jobLogger.log(Level.INFO, "Wait and get task results");
    // waiting for to complete
    return waitAndGetTaskResult();
  }

  /**
   * @param executorId executor identifier
   * @return a {@link RunningTasklet} running on the executor with {@code executorId}
   */
  public RunningTasklet getWorkerTasklet(final String executorId) {
    return workerExecutorIdToTasklet.get(executorId);
  }

  /**
   * Updates the entry of worker tasks, which is called by the Optimization orchestrator.
   * @param addedWorkers a set of added worker tasks
   * @param deletedWorkers a set of deleted worker tasks
   */
  public void updateExecutorEntry(final Set<String> addedWorkers,
                                  final Set<String> deletedWorkers,
                                  final Set<String> addedServers,
                                  final Set<String> deletedServers) {
    final int numPrevWorkers = workerExecutors.size();
    final int numPrevServers = serverExecutors.size();

    for (final String addedWorker : addedWorkers) {
      final RunningTasklet tasklet;
      final AllocatedExecutor executor;
      try {
        executor = etMaster.getExecutor(addedWorker);
        tasklet = executor.getRunningTasklet(jobId + "-" + WorkerTasklet.TASKLET_ID);
        if (tasklet == null) {
          throw new RuntimeException(String.format("Task is not running on the executor %s", addedWorker));
        }
      } catch (ExecutorNotExistException e) {
        throw new RuntimeException(e);
      }

      workerExecutors.put(executor.getId(), executor);
      workerExecutorIdToTasklet.put(addedWorker, tasklet);
    }

    for (final String addedServer : addedServers) {
      final RunningTasklet tasklet;
      final AllocatedExecutor executor;
      try {
        executor = etMaster.getExecutor(addedServer);
        tasklet = executor.getRunningTasklet(jobId + "-" + ServerTasklet.TASKLET_ID);
      } catch (ExecutorNotExistException e) {
        throw new RuntimeException(e);
      }
      serverExecutors.put(executor.getId(), executor);
      serverExecutorIdToTasklet.put(addedServer, tasklet);
    }

    workerExecutors.keySet().removeAll(deletedWorkers);
    serverExecutors.keySet().removeAll(deletedServers);
    workerExecutorIdToTasklet.keySet().removeAll(deletedWorkers);
    serverExecutorIdToTasklet.keySet().removeAll(deletedServers);

    final int numAfterWorkers = workerExecutors.size();
    final int numAfterServers = serverExecutors.size();

    // notify to the client when the number of worker/server changes
    if (numPrevWorkers != numAfterWorkers || numPrevServers != numAfterServers) {
      final String msgToClient = String.format("(S: %d, W: %d) -> (S: %d, W: %d)",
          numPrevServers, numPrevWorkers, numAfterServers, numAfterWorkers);
      jobMessageObserver.sendMessageToClient(msgToClient.getBytes(StandardCharsets.UTF_8));
    }
  }

  private List<TaskletResult> waitAndGetTaskResult() {
    final List<TaskletResult> taskletResultList = new ArrayList<>(workerExecutorIdToTasklet.size());

    workerExecutorIdToTasklet.values().forEach(task -> {
      try {
        taskletResultList.add(task.getTaskResult());
      } catch (InterruptedException e) {
        throw new RuntimeException("Exception while waiting for the task results", e);
      }
    });

    serverExecutorIdToTasklet.values().forEach(RunningTasklet::stop);

    jobLogger.log(Level.INFO, "Task finished");
    return taskletResultList;
  }
}
