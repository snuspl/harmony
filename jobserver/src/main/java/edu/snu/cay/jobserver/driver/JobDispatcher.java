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

import edu.snu.cay.services.et.driver.api.AllocatedExecutor;
import edu.snu.cay.services.et.driver.api.AllocatedTable;
import edu.snu.cay.services.et.driver.impl.GlobalTaskUnitScheduler;
import edu.snu.cay.utils.CatchableExecutors;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.reef.driver.client.JobMessageObserver;
import org.apache.reef.tang.InjectionFuture;

import javax.inject.Inject;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * An class for dispatching jobs with a given {@link JobEntity}.
 */
final class JobDispatcher {
  private static final Logger LOG = Logger.getLogger(JobDispatcher.class.getName());

  private final InjectionFuture<JobServerDriver> jobServerDriverFuture;
  private final InjectionFuture<JobScheduler> jobSchedulerFuture;
  private final InjectionFuture<GlobalTaskUnitScheduler> globalTaskUnitSchedulerFuture;
  private final JobMessageObserver jobMessageObserver;

  @Inject
  private JobDispatcher(final InjectionFuture<JobServerDriver> jobServerDriverFuture,
                        final InjectionFuture<JobScheduler> jobSchedulerFuture,
                        final InjectionFuture<GlobalTaskUnitScheduler> globalTaskUnitSchedulerFuture,
                        final JobMessageObserver jobMessageObserver) {
    this.jobServerDriverFuture = jobServerDriverFuture;
    this.jobSchedulerFuture = jobSchedulerFuture;
    this.globalTaskUnitSchedulerFuture = globalTaskUnitSchedulerFuture;
    this.jobMessageObserver = jobMessageObserver;
  }

  /**
   * Executes a job with given executors.
   * @param jobEntity a job entity
   * @param executors executors to use
   */
  void executeJob(final JobEntity jobEntity,
                  final List<AllocatedExecutor> executors) {
    CatchableExecutors.newSingleThreadExecutor().submit(() -> {
      final Pair<List<List<AllocatedExecutor>>, List<AllocatedTable>> executorGroupsToTables =
          jobEntity.setupExecutorsAndTables(executors);

      try {
        final JobMaster jobMaster = jobEntity.getJobMaster();
        jobServerDriverFuture.get().registerJobMaster(jobEntity.getJobId(), jobMaster);
        globalTaskUnitSchedulerFuture.get().onJobStart(jobEntity.getJobId(), executors.size());

        sendMessageToClient(String.format("Start executing a job. JobId: %s", jobEntity.getJobId()));
        jobMaster.start(executorGroupsToTables.getLeft(), executorGroupsToTables.getRight());

      } finally {
        sendMessageToClient(String.format("Job execution has been finished. JobId: %s", jobEntity.getJobId()));
        executorGroupsToTables.getRight().forEach(table -> {
          try {
            table.drop().get();
          } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
          }
        });
        globalTaskUnitSchedulerFuture.get().onJobFinish(jobEntity.getJobId());
        jobServerDriverFuture.get().deregisterJobMaster(jobEntity.getJobId());
        jobSchedulerFuture.get().onJobFinish(jobEntity);
      }
    });
  }

  private void sendMessageToClient(final String message) {
    LOG.log(Level.INFO, message);
    jobMessageObserver.sendMessageToClient(message.getBytes());
  }
}
