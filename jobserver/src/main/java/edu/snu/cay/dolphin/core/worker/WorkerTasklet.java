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
package edu.snu.cay.dolphin.core.worker;

import edu.snu.cay.dolphin.DolphinParameters;
import edu.snu.cay.dolphin.metric.avro.BatchMetrics;
import edu.snu.cay.dolphin.metric.avro.DolphinWorkerMetrics;
import edu.snu.cay.dolphin.metric.avro.EpochMetrics;
import edu.snu.cay.dolphin.metric.avro.WorkerMetricsType;
import edu.snu.cay.services.et.configuration.parameters.TaskletIdentifier;
import edu.snu.cay.services.et.evaluator.api.Tasklet;
import edu.snu.cay.services.et.evaluator.impl.TaskUnitInfo;
import edu.snu.cay.services.et.evaluator.impl.TaskUnitScheduler;
import edu.snu.cay.services.et.metric.MetricCollector;
import org.apache.reef.exception.evaluator.NetworkException;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Tasklet for running Dolphin trainers on ET.
 */
public final class WorkerTasklet<K, V> implements Tasklet {
  private static final Logger LOG = Logger.getLogger(WorkerTasklet.class.getName());
  public static final String TASKLET_ID = "WorkerTasklet";

  private final String taskletId;
  private final int startingEpoch;

  private final ProgressReporter progressReporter;
  private final WorkerGlobalBarrier workerGlobalBarrier;
  private final MiniBatchBarrier miniBatchBarrier;
  private final TrainingDataProvider<K, V> trainingDataProvider;
  private final ModelAccessor modelAccessor;
  private final TestDataProvider<V> testDataProvider;
  private final Trainer<K, V> trainer;
  private final MetricCollector metricCollector;

  private final TaskUnitScheduler taskUnitScheduler;
  private final TaskUnitInfo commTaskUnitInfo;
  private final TaskUnitInfo compTaskUnitInfo;

  /**
   * A boolean flag that becomes true when {@link #close()} is called,
   * which consequently stops the task from training and terminates it.
   */
  private final AtomicBoolean abortFlag = new AtomicBoolean(false);

  @Inject
  private WorkerTasklet(@Parameter(TaskletIdentifier.class) final String taskletId,
                        @Parameter(DolphinParameters.StartingEpochIdx.class) final int startingEpoch,
                        final TaskUnitScheduler taskUnitScheduler,
                        final ProgressReporter progressReporter,
                        final WorkerGlobalBarrier workerGlobalBarrier,
                        final MiniBatchBarrier miniBatchBarrier,
                        final TrainingDataProvider<K, V> trainingDataProvider,
                        final ModelAccessor modelAccessor,
                        final TestDataProvider<V> testDataProvider,
                        final Trainer<K, V> trainer,
                        final MetricCollector metricCollector) {
    this.taskletId = taskletId;
    this.startingEpoch = startingEpoch;
    this.taskUnitScheduler = taskUnitScheduler;
    this.progressReporter = progressReporter;
    this.workerGlobalBarrier = workerGlobalBarrier;
    this.miniBatchBarrier = miniBatchBarrier;
    this.trainingDataProvider = trainingDataProvider;
    this.modelAccessor = modelAccessor;
    this.testDataProvider = testDataProvider;
    this.trainer = trainer;
    this.metricCollector = metricCollector;

    this.commTaskUnitInfo = new TaskUnitInfo(taskletId, "COMM", TaskUnitInfo.ResourceType.NET);
    this.compTaskUnitInfo = new TaskUnitInfo(taskletId, "COMP", TaskUnitInfo.ResourceType.CPU);
  }

  @Override
  public void run() throws Exception {
    LOG.log(Level.INFO, "{0} starting from epoch {1}", new Object[]{taskletId, startingEpoch});

    final List<V> testData = testDataProvider.getTestData();
    LOG.log(Level.INFO, "Test data set size: {0}", testData.size());

    trainer.initGlobalSettings();

    // synchronize all workers before starting the main iterations
    // to avoid meaningless computation by the workers who started earlier
    workerGlobalBarrier.await();

    int epochIdx = startingEpoch;
    while (true) {
      LOG.log(Level.INFO, "Starting epoch {0}", epochIdx);

      final long epochStartTime = System.currentTimeMillis();
      final PerOpTimeInEpoch perOpTimeInEpoch = new PerOpTimeInEpoch();
      trainingDataProvider.prepareDataForEpoch();

      int numProcessedDataInEpoch = 0;
      int miniBatchIdx = 0;
      while (true) {
        final Collection<Map.Entry<K, V>> miniBatchData = trainingDataProvider.getNextBatchData();
        if (miniBatchData.isEmpty()) {
          break; // Finish the epoch when there are no more data to process
        }

        LOG.log(Level.INFO, "Starting batch {0} in epoch {1}", new Object[] {miniBatchIdx, epochIdx});
        if (miniBatchBarrier.await()) {
          taskUnitScheduler.onTaskUnitFinished(commTaskUnitInfo);
          cleanup();
          return;
        }

        modelAccessor.getAndResetMetrics();
        final long miniBatchStartTime = System.currentTimeMillis();

        if (epochIdx == 0 && miniBatchIdx == 0) {
          taskUnitScheduler.waitSchedule(commTaskUnitInfo);
        }

        trainer.setMiniBatchData(miniBatchData);
        trainer.pullModel();
        taskUnitScheduler.onTaskUnitFinished(commTaskUnitInfo);

        taskUnitScheduler.waitSchedule(compTaskUnitInfo);
        trainer.localCompute();
        taskUnitScheduler.onTaskUnitFinished(compTaskUnitInfo);

        taskUnitScheduler.waitSchedule(commTaskUnitInfo);
        trainer.pushUpdate();

        final double miniBatchElapsedTime = (System.currentTimeMillis() - miniBatchStartTime) / 1000.0D;

        progressReporter.reportBatchFinish(miniBatchIdx);

        sendMiniBatchMetricsAndUpdateEpochOpTime(perOpTimeInEpoch, epochIdx, miniBatchIdx, miniBatchData.size(),
            miniBatchElapsedTime, trainingDataProvider.getNumBatchesPerEpoch());
        
        numProcessedDataInEpoch += miniBatchData.size();
        miniBatchIdx++;

        if (abortFlag.get()) {
          LOG.log(Level.INFO, "The tasklet {0} is getting closed.", taskletId);
          taskUnitScheduler.onTaskUnitFinished(commTaskUnitInfo);
          return;
        }
      }

      final double epochElapsedTimeSec = (System.currentTimeMillis() - epochStartTime) / 1000.0D;
      trainer.onEpochFinished(epochIdx);
      sendEpochMetrics(epochIdx, miniBatchIdx, numProcessedDataInEpoch, epochElapsedTimeSec, perOpTimeInEpoch);
      epochIdx++;
    }
  }

  /**
   * Cleanup worker state before finish.
   */
  private void cleanup() throws NetworkException {
    // Synchronize all workers before cleanup for workers
    // to finish with the globally equivalent view of trained model
    workerGlobalBarrier.await();

    trainer.cleanup();

    if (modelAccessor instanceof CachedModelAccessor) {
      ((CachedModelAccessor) modelAccessor).stopRefreshingCache();
    }
  }
  
  /**
   * Update {@code perOpTimeInEpoch} and send batch metrics.
   * @param perOpTimeInEpoch Update with metrics collected from this mini-batch round
   * @param epochIdx Index of the epoch
   * @param miniBatchIdx Index of the mini-batch
   * @param processedDataItemCount The number of items processed in the epoch
   * @param miniBatchElapsedTime Total elapsed time in this mini-batch round
   * @param numBatchesPerEpoch Total number of batches per epoch
   */
  private void sendMiniBatchMetricsAndUpdateEpochOpTime(final PerOpTimeInEpoch perOpTimeInEpoch, final int epochIdx,
                                                        final int miniBatchIdx, final int processedDataItemCount,
                                                        final double miniBatchElapsedTime,
                                                        final int numBatchesPerEpoch) {
    // Calculate mini-batch computation time by using metrics collected from ModelAccessor
    final Map<String, Double> modelAccessorMetrics = modelAccessor.getAndResetMetrics();
    final double batchPullTime = modelAccessorMetrics.get(ModelAccessor.METRIC_TOTAL_PULL_TIME_SEC);
    final double batchPushTime = modelAccessorMetrics.get(ModelAccessor.METRIC_TOTAL_PUSH_TIME_SEC);
    final double batchCompTime = miniBatchElapsedTime - batchPullTime - batchPushTime;
    final double dataProcessingRate = processedDataItemCount / miniBatchElapsedTime;

    // Update epoch operation time with metrics collected from this mini-batch round
    perOpTimeInEpoch.accumulate(batchCompTime, batchPullTime, batchPushTime);
    
    // Build metrics in the batch
    final BatchMetrics batchMetrics = BatchMetrics.newBuilder()
                .setBatchTimeSec(miniBatchElapsedTime)
                .setDataProcessingRate(dataProcessingRate)
                .setNumBatchDataInstances(processedDataItemCount)
                .setBatchIdx(miniBatchIdx)
                .setEpochIdx(epochIdx)
                .setBatchPushTimeSec(batchPushTime)
                .setBatchPullTimeSec(batchPullTime)
                .setBatchCompTimeSec(batchCompTime)
                .setNumBatchesPerEpoch(numBatchesPerEpoch)
                .build();

    // Encapsulate the metrics for ET
    final DolphinWorkerMetrics encapsulatedMetrics = DolphinWorkerMetrics.newBuilder()
        .setType(WorkerMetricsType.BatchMetrics)
        .setBatchMetrics(batchMetrics)
        .build();

    metricCollector.addCustomMetric(encapsulatedMetrics);
    metricCollector.flush();
  }

  /**
   * @param epochIdx Index of the epoch
   * @param numBatchesPerEpoch Index of the mini-batch
   * @param processedDataItemCount The number of items processed in the epoch
   * @param epochElapsedTime The elapsed time in the epoch in total, including time for computing the objective value.
   * @param perOpTimeInEpoch The elapsed time per operation in the epoch (i.e., computation, pull and push)
   */
  private void sendEpochMetrics(final int epochIdx, final int numBatchesPerEpoch,
                                final int processedDataItemCount,
                                final double epochElapsedTime,
                                final PerOpTimeInEpoch perOpTimeInEpoch) {
    // Build metrics in the epoch
    final EpochMetrics epochMetrics = EpochMetrics.newBuilder()
        .setEpochCompTimeSec(perOpTimeInEpoch.getTotalCompTime())
        .setEpochIdx(epochIdx)
        .setEpochPullTimeSec(perOpTimeInEpoch.getTotalPullTime())
        .setEpochPushTimeSec(perOpTimeInEpoch.getTotalPushTime())
        .setEpochTimeSec(epochElapsedTime)
        .setNumBatchesPerEpoch(numBatchesPerEpoch)
        .setNumEpochDataInstances(processedDataItemCount)
        .build();

    // Encapsulate the metrics for ET
    final DolphinWorkerMetrics encapsulatedMetrics = DolphinWorkerMetrics.newBuilder()
        .setType(WorkerMetricsType.EpochMetrics)
        .setEpochMetrics(epochMetrics)
        .build();

    metricCollector.addCustomMetric(encapsulatedMetrics);
    metricCollector.flush();
  }

  /**
   * Called when the Task is requested to close.
   * The {@link #abortFlag} is set true, so the task terminates execution.
   */
  @Override
  public void close() {
    LOG.log(Level.INFO, "Requested to close!");
    abortFlag.set(true);
  }

  /**
   * Encapsulates the elapsed time per operation (i.e., compute, push, pull) in an epoch.
   */
  private class PerOpTimeInEpoch {
    private double totalCompTime;
    private double totalPullTime;
    private double totalPushTime;

    PerOpTimeInEpoch() {
      this.totalCompTime = 0d;
      this.totalPullTime = 0d;
      this.totalPushTime = 0d;
    }

    /**
     * Accumulate the batch time to compute the total elapsed time per operation in the epoch.
     */
    void accumulate(final double compTime, final double pullTime, final double pushTime) {
      totalCompTime += compTime;
      totalPullTime += pullTime;
      totalPushTime += pushTime;
    }

    double getTotalCompTime() {
      return totalCompTime;
    }

    double getTotalPullTime() {
      return totalPullTime;
    }

    double getTotalPushTime() {
      return totalPushTime;
    }
  }
}
