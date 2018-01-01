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
import edu.snu.cay.dolphin.ProgressMsg;
import edu.snu.cay.jobserver.Parameters;
import org.apache.reef.driver.ProgressProvider;
import org.apache.reef.driver.client.JobMessageObserver;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;

/**
 * A component to track mini-batch progress by workers.
 * It receives a progress message for every batch by workers.
 * Upon every {@link DolphinParameters.NumTotalMiniBatches} batches,
 * it checkpoints a model table for offline model evaluation.
 */
public final class BatchProgressTracker implements ProgressProvider {
  private final JobLogger jobLogger;

  private final ModelChkpManager modelChkpManager;
  private final JobMessageObserver jobMessageObserver;

  private final String jobId;

  private final int numMaxEpochs;
  private final int totalMiniBatchesToRun;
  private final int numMiniBatchesInEpoch;

  private final boolean offlineModelEval;

  private final Map<String, Integer> workerIdToBatchProgress = new ConcurrentHashMap<>();

  private final AtomicInteger miniBatchCounter = new AtomicInteger(0);

  @Inject
  private BatchProgressTracker(final JobLogger jobLogger,
                               final JobMessageObserver jobMessageObserver,
                               @Parameter(Parameters.JobId.class) final String jobId,
                               @Parameter(DolphinParameters.MaxNumEpochs.class) final int numEpochs,
                               @Parameter(DolphinParameters.NumTotalMiniBatches.class)
                                 final int numMiniBatchesInEpoch,
                               @Parameter(DolphinParameters.OfflineModelEvaluation.class)
                                 final boolean offlineModelEval,
                               final ModelChkpManager modelChkpManager) {
    this.jobLogger = jobLogger;
    this.jobMessageObserver = jobMessageObserver;
    this.modelChkpManager = modelChkpManager;
    this.jobId = jobId;
    this.numMaxEpochs = numEpochs;
    this.totalMiniBatchesToRun = numEpochs * numMiniBatchesInEpoch;
    this.numMiniBatchesInEpoch = numMiniBatchesInEpoch;
    this.offlineModelEval = offlineModelEval;
  }

  /**
   * @return a global minimum epoch progress
   */
  public int getGlobalMinEpochIdx() {
    return miniBatchCounter.get() / numMiniBatchesInEpoch;
  }

  synchronized void onProgressMsg(final ProgressMsg msg) {
    final String workerId = msg.getExecutorId().toString();
    final int miniBatchIdx = miniBatchCounter.incrementAndGet();
    jobLogger.log(Level.INFO, "Batch progress: {0} / {1}.",
        new Object[]{miniBatchIdx, totalMiniBatchesToRun});

    if ((miniBatchIdx - 1) % numMiniBatchesInEpoch == 0) {
      final int epochIdx = miniBatchIdx / numMiniBatchesInEpoch;
      final String msgToClient = String.format("Epoch progress: [%d / %d], JobId: %s",
          epochIdx, numMaxEpochs, jobId);
      jobMessageObserver.sendMessageToClient(msgToClient.getBytes(StandardCharsets.UTF_8));
    }

    if (offlineModelEval) {
      if (miniBatchIdx % numMiniBatchesInEpoch == 0) {
        jobLogger.log(Level.INFO, "Checkpoint model table. EpochIdx: {0}", miniBatchIdx / numMiniBatchesInEpoch);
        modelChkpManager.createCheckpoint();
      }
    }

    workerIdToBatchProgress.compute(workerId, (id, batchCount) -> batchCount == null ? 1 : batchCount + 1);
    jobLogger.log(Level.INFO, "Committed Batches per workers: {0}", workerIdToBatchProgress);
  }

  @Override
  public float getProgress() {
    return miniBatchCounter.get() / totalMiniBatchesToRun;
  }
}
