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
package edu.snu.cay.dolphin.jobserver;

import edu.snu.cay.jobserver.driver.*;
import edu.snu.cay.services.et.driver.api.AllocatedExecutor;
import edu.snu.cay.services.et.driver.api.AllocatedTable;
import edu.snu.cay.services.et.driver.api.ETMaster;
import org.apache.reef.driver.client.JobMessageObserver;
import org.apache.reef.tang.InjectionFuture;

import javax.inject.Inject;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Dolphin's {@link JobDispatcher} implmentation.
 */
public final class DolphinJobDispatcher implements JobDispatcher {
  private static final Logger LOG = Logger.getLogger(DolphinJobDispatcher.class.getName());

  private final InjectionFuture<ETMaster> etMasterFuture;
  private final InjectionFuture<JobScheduler> jobSchedulerFuture;
  private final InjectionFuture<JobServerDriver> jobServerDriverFuture;

  private final JobMessageObserver jobMessageObserver;

  @Inject
  private DolphinJobDispatcher(final JobMessageObserver jobMessageObserver,
                               final InjectionFuture<ETMaster> etMasterFuture,
                               final InjectionFuture<JobScheduler> jobSchedulerFuture,
                               final InjectionFuture<JobServerDriver> jobServerDriverFuture) {
    this.etMasterFuture = etMasterFuture;
    this.jobSchedulerFuture = jobSchedulerFuture;
    this.jobServerDriverFuture = jobServerDriverFuture;
    this.jobMessageObserver = jobMessageObserver;
  }

  @Override
  public void executeJob(final JobEntity jobEntity) {
    final DolphinJobEntity dolphinJobEntity = (DolphinJobEntity) jobEntity;

    final String jobId = dolphinJobEntity.getJobId();

    final String jobStartMsg = String.format("Start executing a job. JobId: %s", jobId);
    sendMessageToClient(jobStartMsg);
    LOG.log(Level.INFO, jobStartMsg);

    LOG.log(Level.INFO, "Preparing executors and tables for job: {0}", jobId);

    final Future<List<AllocatedExecutor>> serversFuture = etMasterFuture.get()
        .addExecutors(dolphinJobEntity.getNumServers(), dolphinJobEntity.getServerExecutorConf());
    final Future<List<AllocatedExecutor>> workersFuture = etMasterFuture.get()
        .addExecutors(dolphinJobEntity.getNumWorkers(), dolphinJobEntity.getWorkerExecutorConf());

    new Thread(() -> {
      try {
        final List<AllocatedExecutor> servers = serversFuture.get();
        final List<AllocatedExecutor> workers = workersFuture.get();

        final Future<AllocatedTable> modelTableFuture =
            etMasterFuture.get().createTable(dolphinJobEntity.getServerTableConf(), servers);
        final Future<AllocatedTable> inputTableFuture =
            etMasterFuture.get().createTable(dolphinJobEntity.getWorkerTableConf(), workers);

        final AllocatedTable modelTable = modelTableFuture.get();
        final AllocatedTable inputTable = inputTableFuture.get();

        modelTable.subscribe(workers).get();
        inputTable.load(workers, dolphinJobEntity.getInputPath()).get();

        try {
          LOG.log(Level.FINE, "Spawn new jobMaster with ID: {0}", jobId);
          final JobMaster jobMaster = dolphinJobEntity.getJobMaster();
          jobServerDriverFuture.get().putJobMaster(jobId, jobMaster);

          final List<List<AllocatedExecutor>> executorGroups = Arrays.asList(servers, workers);
          final List<AllocatedTable> tables = Arrays.asList(modelTable, inputTable);

          jobMaster.start(executorGroups, tables);

          workers.forEach(AllocatedExecutor::close);
          servers.forEach(AllocatedExecutor::close);

        } finally {
          final String jobFinishMsg = String.format("Job execution has been finished. JobId: %s", jobId);
          LOG.log(Level.INFO, jobFinishMsg);
          sendMessageToClient(jobFinishMsg);
          jobServerDriverFuture.get().removeJobMaster(jobId);
          jobSchedulerFuture.get().onJobFinish(workers.size() + servers.size());
        }

      } catch (InterruptedException | ExecutionException e) {
        LOG.log(Level.SEVERE, "Exception while running a job");
        throw new RuntimeException(e);
      }
    }).start();
  }

  private void sendMessageToClient(final String message) {
    jobMessageObserver.sendMessageToClient(message.getBytes());
  }
}
