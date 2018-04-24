/*
 * Copyright (C) 2018 Seoul National University
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
package edu.snu.cay.services.et.driver.impl;

import edu.snu.cay.services.et.driver.api.MessageSender;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.reef.tang.InjectionFuture;

import javax.inject.Inject;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A class that globally schedules TaskUnits in executors.
 * 1) Executors send wait messsages for a specific TaskUnit and
 * 2) Master broadcasts ready messages when the TaskUnit in all executors are in WAIT state.
 * It makes executors schedule TaskUnits in same order, when there're concurrent TaskUnits.
 * This global schedule eliminates unnecessary synchronization overhead of independent local schedules.
 */
public final class GlobalTaskUnitScheduler {
  private final Map<String, Integer> jobIdToNumExecutors = new ConcurrentHashMap<>();
  private final Map<String, Pair<AtomicInteger, Set<String>>> jobIdToCounter = new ConcurrentHashMap<>();

  private final InjectionFuture<MessageSender> msgSenderFuture;

  @Inject
  private GlobalTaskUnitScheduler(final InjectionFuture<MessageSender> msgSenderFuture) {
    this.msgSenderFuture = msgSenderFuture;
  }

  /**
   * A method to be called upon job start.
   * @param jobId a job ID
   * @param numExecutors the number of executor participating a job
   */
  public void onJobStart(final String jobId, final int numExecutors) {
    jobIdToNumExecutors.put(jobId, numExecutors);
  }

  /**
   * A method to be called upon job finish.
   * @param jobId a job ID
   */
  public void onJobFinish(final String jobId) {
    jobIdToNumExecutors.remove(jobId);
  }

  /**
   * A method to be called upon a TaskUnit wait msg from an executor.
   * @param taskletId a tasklet ID
   * @param executorId an executor ID
   */
  void onTaskUnitWaitMsg(final String taskletId, final String executorId) {
    final String[] splits = taskletId.split("-"); // ad-hoc way to get a job identifier
    final String jobId = splits[0] + "-" + splits[1]; // [app ID]-[job count]

    jobIdToCounter.putIfAbsent(jobId,
        Pair.of(new AtomicInteger(0), Collections.newSetFromMap(new ConcurrentHashMap<>())));
    final Pair<AtomicInteger, Set<String>> pair = jobIdToCounter.get(jobId);
    pair.getValue().add(executorId);
    final int count = pair.getKey().incrementAndGet();

    if (count == jobIdToNumExecutors.get(jobId)) {
      jobIdToCounter.remove(jobId);
      sendTaskUnitReadyMsg(taskletId, pair.getValue());
    }
  }

  /**
   * Send TaskUnit ready msgs of a job with {@code taskletId} to the executors.
   * @param taskletId a tasklet ID
   * @param executorIds a list of executor IDs
   */
  private void sendTaskUnitReadyMsg(final String taskletId, final Set<String> executorIds) {
    msgSenderFuture.get().sendTaskUnitReadyMsg(executorIds, taskletId);
  }
}
