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
import edu.snu.cay.jobserver.JobLogger;
import org.apache.reef.tang.InjectionFuture;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;

/**
 * A class for controlling and synchronizing mini-batch progress at workers.
 */
public final class MiniBatchController {
  private final JobLogger jobLogger;

  private final int numWorkers;

  private final AtomicInteger numBlockedWorkers = new AtomicInteger(0);

  private final MasterSideMsgSender masterSideMsgSender;

  private final InjectionFuture<ETTaskRunner> etTaskRunnerFuture;

  private final int totalMiniBatchesToRun;
  private final AtomicInteger miniBatchCounter = new AtomicInteger(0);

  @Inject
  private MiniBatchController(@Parameter(DolphinParameters.NumWorkers.class) final int numWorkers,
                              @Parameter(DolphinParameters.MaxNumEpochs.class) final int numEpochs,
                              @Parameter(DolphinParameters.NumTotalMiniBatches.class)
                              final int numMiniBatchesInEpoch,
                              final JobLogger jobLogger,
                              final MasterSideMsgSender masterSideMsgSender,
                              final InjectionFuture<ETTaskRunner> etTaskRunnerFuture) {
    this.jobLogger = jobLogger;
    this.numWorkers = numWorkers;
    this.masterSideMsgSender = masterSideMsgSender;
    this.etTaskRunnerFuture = etTaskRunnerFuture;
    this.totalMiniBatchesToRun = numEpochs * numMiniBatchesInEpoch;
  }

  /**
   * On sync msgs from workers at every starts of mini-batches.
   */
  synchronized void onSync() {
    final int miniBatchIdx = miniBatchCounter.incrementAndGet();
    jobLogger.log(Level.INFO, "Batch progress: {0} / {1}.",
        new Object[]{miniBatchIdx, totalMiniBatchesToRun});

    if (numBlockedWorkers.incrementAndGet() == numWorkers) {
      final boolean stop = miniBatchIdx - numWorkers + 1 > totalMiniBatchesToRun;

      etTaskRunnerFuture.get().getAllWorkerExecutors().forEach(workerId ->
          masterSideMsgSender.sendMiniBatchControlMsg(workerId, stop)
      );
      numBlockedWorkers.set(0);
    }
  }
}
