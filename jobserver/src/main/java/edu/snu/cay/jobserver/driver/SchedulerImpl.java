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
import edu.snu.cay.services.et.driver.api.ETMaster;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

/**
 * A basic implementation of job scheduler based on FIFO policy.
 * It submits jobs in order, whenever resources are available.
 */
public final class SchedulerImpl implements JobScheduler {
  private static final Logger LOG = Logger.getLogger(SchedulerImpl.class.getName());

  private final ResourcePool resourcePool;

  private final ETMaster etMaster;
  private final JobDispatcher jobDispatcher;

  @Inject
  private SchedulerImpl(final ETMaster etMaster,
                        final ResourcePool resourcePool,
                        final JobDispatcher jobDispatcher) {
    this.etMaster = etMaster;
    this.resourcePool = resourcePool;
    this.jobDispatcher = jobDispatcher;
  }

  /**
   * Execute a new job immediately, if there're enough free resources.
   * Otherwise, put it into a queue so it can be executed when resources become available.
   */
  @Override
  public synchronized boolean onJobArrival(final JobEntity jobEntity) {
    // pick executors to use, considering resource utilization status and data locality
    final List<AllocatedExecutor> executorsToUse = pickExecutorsToUse(resourcePool.getExecutors(), jobEntity);

    final List<AllocatedTable> tablesToUse = jobEntity.setupTables(etMaster, executorsToUse);

    jobDispatcher.executeJob(jobEntity, executorsToUse, tablesToUse);
    return true;
  }

  private List<AllocatedExecutor> pickExecutorsToUse(
      final Map<String, AllocatedExecutor> executorMap, final JobEntity jobEntity) {
    // TODO #00: simply use all executors
    return new ArrayList<>(executorMap.values());
  }

  /**
   * Executes waiting jobs if the enough amount of resources become available for them.
   */
  @Override
  public synchronized void onJobFinish(final int numReleasedResources) {

  }

  @Override
  public synchronized void onResourceChange(final int delta) {
    throw new UnsupportedOperationException("Resource availability is not supported for now");
  }
}
