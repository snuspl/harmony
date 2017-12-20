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
import org.apache.reef.driver.client.JobMessageObserver;
import org.apache.reef.tang.InjectionFuture;

import javax.inject.Inject;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * An interface for frameworks to specify how to execute a job with given {@link JobEntity}.
 */
final class JobDispatcher {
  private static final Logger LOG = Logger.getLogger(JobDispatcher.class.getName());

  private final JobMessageObserver jobMessageObserver;
  private final InjectionFuture<JobServerDriver> jobServerDriverFuture;
  private final InjectionFuture<JobScheduler> jobSchedulerFuture;

  @Inject
  private JobDispatcher(final JobMessageObserver jobMessageObserver,
                        final InjectionFuture<JobServerDriver> jobServerDriverFuture,
                        final InjectionFuture<JobScheduler> jobSchedulerFuture) {
    this.jobMessageObserver = jobMessageObserver;
    this.jobServerDriverFuture = jobServerDriverFuture;
    this.jobSchedulerFuture = jobSchedulerFuture;
  }

  /**
   * Executes a job.
   */
  void executeJob(final JobEntity jobEntity,
                         final List<AllocatedExecutor> executorList,
                         final List<AllocatedTable> tableList) {
    final String jobId = jobEntity.getJobId();

    final String jobStartMsg = String.format("Start executing a job. JobId: %s", jobId);
    sendMessageToClient(jobStartMsg);
    LOG.log(Level.INFO, jobStartMsg);

    new Thread(() -> {
      try {
        LOG.log(Level.FINE, "Spawn new jobMaster with ID: {0}", jobId);
        final JobMaster jobMaster = jobEntity.getJobMaster();
        jobServerDriverFuture.get().putJobMaster(jobId, jobMaster);

        jobMaster.start(executorList, tableList);

      } finally {
        final String jobFinishMsg = String.format("Job execution has been finished. JobId: %s", jobId);
        LOG.log(Level.INFO, jobFinishMsg);
        sendMessageToClient(jobFinishMsg);
        jobServerDriverFuture.get().removeJobMaster(jobId);
        jobSchedulerFuture.get().onJobFinish(executorList.size());
      }
    }).start();
  }

  private void sendMessageToClient(final String message) {
    jobMessageObserver.sendMessageToClient(message.getBytes());
  }
}
