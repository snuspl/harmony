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
import java.util.*;

/**
 * Created by xyzi on 29/03/2018.
 */
public final class PartitioningScheduler implements JobScheduler {
  private final ResourcePool resourcePool;
  private final JobDispatcher jobDispatcher;

  // allocated resources
  private final Set<AllocatedExecutor> allocatedExecutors = new HashSet<>();
  private final Map<String, List<AllocatedExecutor>> jobIdToExecutors = new HashMap<>();

  @Inject
  private PartitioningScheduler(final ResourcePool resourcePool,
                                final JobDispatcher jobDispatcher) {
    this.resourcePool = resourcePool;
    this.jobDispatcher = jobDispatcher;
  }

  @Override
  public synchronized boolean onJobArrival(final JobEntity jobEntity) {
    final Set<AllocatedExecutor> availableExecutors = new HashSet<>(resourcePool.getExecutors().values());
    availableExecutors.removeAll(allocatedExecutors);
    final int numExecutorsToUse = jobEntity.getNumExecutorsToUse();
    if (availableExecutors.size() < numExecutorsToUse) {
      return false;
    }

    final Iterator<AllocatedExecutor> executorIterator = availableExecutors.iterator();

    final List<AllocatedExecutor> executorsToUse = new ArrayList<>(numExecutorsToUse);
    for (int i = 0; i < numExecutorsToUse; i++) {
      executorsToUse.add(executorIterator.next());
    }

    allocatedExecutors.addAll(executorsToUse);
    jobIdToExecutors.put(jobEntity.getJobId(), executorsToUse);

    jobDispatcher.executeJob(jobEntity, executorsToUse);
    return true;
  }

  @Override
  public synchronized void onJobFinish(final JobEntity jobEntity) {
    final List<AllocatedExecutor> releasedExecutors = jobIdToExecutors.remove(jobEntity.getJobId());
    allocatedExecutors.removeAll(releasedExecutors);
  }

  @Override
  public void onResourceChange(final int delta) {
    throw new UnsupportedOperationException("Changes in Resource availability are not supported for now");
  }
}
