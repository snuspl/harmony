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
package edu.snu.cay.dolphin.pregel.jobserver;

import edu.snu.cay.dolphin.jobserver.driver.*;
import edu.snu.cay.services.et.driver.api.AllocatedExecutor;
import edu.snu.cay.services.et.driver.api.AllocatedTable;
import edu.snu.cay.services.et.driver.api.ETMaster;
import org.apache.reef.driver.client.JobMessageObserver;
import org.apache.reef.tang.InjectionFuture;
import org.apache.reef.tang.exceptions.InjectionException;

import javax.inject.Inject;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by xyzi on 06/12/2017.
 */
public final class PregelJobDispatcher implements JobDispatcher {
  private static final Logger LOG = Logger.getLogger(PregelJobDispatcher.class.getName());

  private final InjectionFuture<ETMaster> etMasterFuture;
  private final InjectionFuture<JobScheduler> jobSchedulerFuture;
  private final InjectionFuture<JobServerDriver> jobServerDriverFuture;

  private final JobMessageObserver jobMessageObserver;

  @Inject
  private PregelJobDispatcher(final JobMessageObserver jobMessageObserver,
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
    final PregelJobEntity pregelJobEntity = (PregelJobEntity) jobEntity;

    final String jobId = pregelJobEntity.getJobId();

    final String jobStartMsg = String.format("Start executing a job. JobId: %s", jobId);
    sendMessageToClient(jobStartMsg);
    LOG.log(Level.INFO, jobStartMsg);

    LOG.log(Level.INFO, "Preparing executors and tables for job: {0}", jobId);

    final Future<List<AllocatedExecutor>> workersFuture =
        etMasterFuture.get().addExecutors(pregelJobEntity.getNumWorkers(),
            pregelJobEntity.getWorkerExecutorConf());

    new Thread(() -> {
      try {
        final List<AllocatedExecutor> workers = workersFuture.get();

        final Future<AllocatedTable> vertexTableFuture =
            etMasterFuture.get().createTable(pregelJobEntity.getVertexTableConf(), workers);
        final Future<AllocatedTable> msgTable1Future =
            etMasterFuture.get().createTable(pregelJobEntity.getMsgTable1Conf(), workers);
        final Future<AllocatedTable> msgTable2Future =
            etMasterFuture.get().createTable(pregelJobEntity.getMsgTable2Conf(), workers);

        final AllocatedTable vertexTable = vertexTableFuture.get();
        final AllocatedTable msgTable1 = msgTable1Future.get();
        final AllocatedTable msgTable2 = msgTable2Future.get();

        vertexTable.load(workers, pregelJobEntity.getInputPath()).get();

        try {
          LOG.log(Level.FINE, "Spawn new jobMaster with ID: {0}", jobId);
          final JobMaster jobMaster = pregelJobEntity.getJobInjector().getInstance(JobMaster.class);
          jobServerDriverFuture.get().putJobMaster(jobId, jobMaster);

          final List<List<AllocatedExecutor>> executorGroups = Collections.singletonList(workers);
          final List<AllocatedTable> tables = Arrays.asList(vertexTable, msgTable1, msgTable2);

          jobMaster.start(executorGroups, tables);

          workers.forEach(AllocatedExecutor::close);

        } finally {
          final String jobFinishMsg = String.format("Job execution has been finished. JobId: %s", jobId);
          LOG.log(Level.INFO, jobFinishMsg);
          sendMessageToClient(jobFinishMsg);
          jobServerDriverFuture.get().removeJobMaster(jobId);
          jobSchedulerFuture.get().onJobFinish(workers.size());
        }

      } catch (InterruptedException | ExecutionException | InjectionException e) {
        LOG.log(Level.SEVERE, "Exception while running a job");
        throw new RuntimeException(e);
      }
    }).start();
  }

  private void sendMessageToClient(final String message) {
    jobMessageObserver.sendMessageToClient(message.getBytes());
  }
}
