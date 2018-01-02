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

import edu.snu.cay.dolphin.DolphinParameters;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A class for controlling and synchronizing mini-batch progress at workers.
 * It synchronizes workers within {@link DolphinParameters.ClockSlack}.
 */
public final class MiniBatchController {
  private final MasterSideMsgSender masterSideMsgSender;

  private final int totalMiniBatchesToRun;
  private final AtomicInteger miniBatchCounter = new AtomicInteger(0);

  private final int slack;
  private final AtomicBoolean closedFlag = new AtomicBoolean(false);

  private final Map<String, Integer> workerIdToProgress = new ConcurrentHashMap<>();
  private final SortedMap<Integer, Set<String>> progressToWorkerIds = new ConcurrentSkipListMap<>();

  private AtomicInteger minProgress = new AtomicInteger(0);
  private Set<String> blockedWorkers = Collections.newSetFromMap(new ConcurrentHashMap<>());

  @Inject
  private MiniBatchController(@Parameter(DolphinParameters.MaxNumEpochs.class) final int numEpochs,
                              @Parameter(DolphinParameters.NumTotalMiniBatches.class) final int numMiniBatchesInEpoch,
                              @Parameter(DolphinParameters.ClockSlack.class) final int slack,
                              final MasterSideMsgSender masterSideMsgSender) {
    this.slack = slack;
    this.masterSideMsgSender = masterSideMsgSender;
    this.totalMiniBatchesToRun = numEpochs * numMiniBatchesInEpoch;
  }

  /**
   * On sync msgs from workers at every starts of mini-batches.
   */
  synchronized void onSync(final String workerId) {
    if (closedFlag.get()) {
      masterSideMsgSender.sendMiniBatchControlMsg(workerId, true);
      return;
    }

    final int miniBatchCount = miniBatchCounter.incrementAndGet();

    // update workerIdToProgress
    final int progress = workerIdToProgress.compute(workerId,
        (s, prevProgress) -> prevProgress == null ? 1 : prevProgress + 1);

    // update progressToWorkerIds
    if (progress != 1) {
      progressToWorkerIds.compute(progress - 1, (k, v) -> {
        v.remove(workerId);
        return v.isEmpty() ? null : v;
      });
    }
    progressToWorkerIds.compute(progress, (k, v) -> {
      final Set<String> workers = v == null ? Collections.newSetFromMap(new ConcurrentHashMap<>()) : v;
      workers.add(workerId);
      return workers;
    });

    // update min progress
    final int newMinProgress = progressToWorkerIds.firstKey();
    if (newMinProgress > minProgress.get()) {
      minProgress.set(newMinProgress);

      // release the fastest blocked workers
      blockedWorkers.forEach(blockedWorkerId -> masterSideMsgSender.sendMiniBatchControlMsg(blockedWorkerId, false));
      blockedWorkers.clear();
    }

    // finish job
    final boolean stop = miniBatchCount + 1 > totalMiniBatchesToRun;
    if (stop) { // stop workers if they have finished all mini-batches
      closedFlag.set(true);

      // release all workers to let them do the last mini-batches
      masterSideMsgSender.sendMiniBatchControlMsg(workerId, false);
      blockedWorkers.forEach(blockedWorkerId -> masterSideMsgSender.sendMiniBatchControlMsg(blockedWorkerId, false));
      blockedWorkers.clear();

    } else {
      if (progress > minProgress.get() + slack) {
        // block
        blockedWorkers.add(workerId);
      } else {
        // release
        masterSideMsgSender.sendMiniBatchControlMsg(workerId, false);
      }
    }
  }
}
