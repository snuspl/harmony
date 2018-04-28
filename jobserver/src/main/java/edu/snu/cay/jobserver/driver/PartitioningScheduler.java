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

import com.google.common.collect.Lists;
import edu.snu.cay.jobserver.Parameters;
import edu.snu.cay.services.et.driver.api.AllocatedExecutor;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by xyzi on 29/03/2018.
 */
public final class PartitioningScheduler implements JobScheduler {
  private static final Logger LOG = Logger.getLogger(PartitioningScheduler.class.getName());

  private final ResourcePool resourcePool;
  private final JobDispatcher jobDispatcher;

  private final AtomicBoolean initialized = new AtomicBoolean(false);

  private final int numPartitions;
  private final int numConcurrentJobs;

  private final Map<String, Partition> jobIdToPartition = new HashMap<>();

  private final Queue<Partition> partitions;

  private final Queue<JobEntity> waitingJobs = new LinkedList<>();

  private class Partition {
    private final List<AllocatedExecutor> executors;

    private final int numMaxConcurrentJobs;

    private final Set<String> runningJobs;

    Partition(final List<AllocatedExecutor> executors,
                     final int numMaxConcurrentJobs) {
      this.executors = executors;
      this.numMaxConcurrentJobs = numMaxConcurrentJobs;
      this.runningJobs = Collections.newSetFromMap(new ConcurrentHashMap<>());
    }

    synchronized void executeJob(final JobEntity jobEntity) {
      this.runningJobs.add(jobEntity.getJobId());
      jobDispatcher.executeJob(jobEntity, executors);
    }

    synchronized boolean isAvailable() {
      return runningJobs.size() < numMaxConcurrentJobs;
    }
  }

  @Inject
  private PartitioningScheduler(@Parameter(Parameters.NumPartitions.class) final int numPartitions,
                                @Parameter(Parameters.NumConcurrentJobs.class) final int numConcurrentJobs,
                                final ResourcePool resourcePool,
                                final JobDispatcher jobDispatcher) {
    this.numPartitions = numPartitions;
    this.numConcurrentJobs = numConcurrentJobs;
    this.partitions = new ConcurrentLinkedQueue<>();
    this.resourcePool = resourcePool;
    this.jobDispatcher = jobDispatcher;
  }

  @Override
  public synchronized boolean onJobArrival(final JobEntity jobEntity) {
    if (initialized.compareAndSet(false, true)) {
      initPartitions();
    }

    scheduleJob(jobEntity);
    return true;
  }

  private void initPartitions() {
    final List<AllocatedExecutor> executorList = new ArrayList<>(resourcePool.getExecutors().values());

    if (executorList.size() < numPartitions) {
      throw new RuntimeException();
    }

    LOG.log(Level.INFO, "Number of partitions: {0}", numPartitions);
    final int partitionSize = executorList.size() / numPartitions;
    for (final List<AllocatedExecutor> executors : Lists.partition(executorList, partitionSize)) {
      partitions.add(new Partition(executors, numConcurrentJobs));
    }
  }

  private void scheduleJob(final JobEntity jobEntity) {
    if (partitions.isEmpty()) {
      waitingJobs.add(jobEntity);
      LOG.log(Level.INFO, "Wait for schedule. JobId: {0}", jobEntity.getJobId());
      return;
    }

    final Partition partition = partitions.poll();
    jobIdToPartition.put(jobEntity.getJobId(), partition);

    LOG.log(Level.INFO, "Schedule job. JobId: {0}", jobEntity.getJobId());
    partition.executeJob(jobEntity);
    if (partition.isAvailable()) {
      partitions.add(partition);
    }
  }

  @Override
  public synchronized void onJobFinish(final JobEntity jobEntity) {
    final Partition releasedPartition = jobIdToPartition.remove(jobEntity.getJobId());
    partitions.add(releasedPartition);

    if (!waitingJobs.isEmpty()) {
      final JobEntity nextJob = waitingJobs.poll();
      scheduleJob(nextJob);
    }
  }

  @Override
  public void onResourceChange(final int delta) {
    throw new UnsupportedOperationException("Changes in Resource availability are not supported for now");
  }
}
