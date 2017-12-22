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

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * A simple implementation of job scheduler that immediately launches job on arrival using all executors for each job.
 */
public final class SchedulerImpl implements JobScheduler {
  private final ResourcePool resourcePool;
  private final JobDispatcher jobDispatcher;

  @Inject
  private SchedulerImpl(final ResourcePool resourcePool,
                        final JobDispatcher jobDispatcher) {
    this.resourcePool = resourcePool;
    this.jobDispatcher = jobDispatcher;
  }

  /**
   * Execute a new job immediately.
   */
  @Override
  public synchronized boolean onJobArrival(final JobEntity jobEntity) {
    final List<AllocatedExecutor> executorsToUse = pickExecutorsToUse(resourcePool.getExecutors(), jobEntity);

    jobDispatcher.executeJob(jobEntity, executorsToUse);
    return true;
  }

  /**
   * Simply pick all executors.
   */
  private List<AllocatedExecutor> pickExecutorsToUse(
      final Map<String, AllocatedExecutor> executorMap, final JobEntity jobEntity) {
    return new ArrayList<>(executorMap.values());
  }

  @Override
  public synchronized void onJobFinish(final JobEntity jobEntity) {
    // do nothing
  }

  @Override
  public synchronized void onResourceChange(final int delta) {
    throw new UnsupportedOperationException("Changes in Resource availability are not supported for now");
  }
}
