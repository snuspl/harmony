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
import edu.snu.cay.services.et.configuration.parameters.TaskletIdentifier;
import edu.snu.cay.services.et.evaluator.TaskUnitScheduler;
import edu.snu.cay.services.et.evaluator.api.Tasklet;
import edu.snu.cay.services.et.evaluator.impl.TaskUnitInfo;
import edu.snu.cay.services.et.metric.MetricCollector;
import org.apache.reef.exception.evaluator.NetworkException;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.*;
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
    this.progressReporter = progressReporter;
    this.workerGlobalBarrier = workerGlobalBarrier;
    this.miniBatchBarrier = miniBatchBarrier;
    this.trainingDataProvider = trainingDataProvider;
    this.modelAccessor = modelAccessor;
    this.testDataProvider = testDataProvider;
    this.trainer = trainer;
    this.metricCollector = metricCollector;
    this.taskUnitScheduler = taskUnitScheduler;
  }

  @Override
  public void run() throws Exception {
    LOG.log(Level.INFO, "{0} starting from epoch {1}", new Object[]{taskletId, startingEpoch});

    final TaskUnitInfo pullTaskUnitInfo = new TaskUnitInfo(taskletId, "PULL", TaskUnitInfo.ResourceType.NET);
    final TaskUnitInfo compTaskUnitInfo = new TaskUnitInfo(taskletId, "COMP", TaskUnitInfo.ResourceType.CPU);
    final TaskUnitInfo pushTaskUnitInfo = new TaskUnitInfo(taskletId, "PUSH", TaskUnitInfo.ResourceType.NET);

    final List<V> testData = testDataProvider.getTestData();
    LOG.log(Level.INFO, "Test data set size: {0}", testData.size());

    trainer.initGlobalSettings();

    // synchronize all workers before starting the main iterations
    // to avoid meaningless computation by the workers who started earlier
    workerGlobalBarrier.await();

    int epochIdx = startingEpoch;
    while (true) {
      LOG.log(Level.INFO, "Starting epoch {0}", epochIdx);

      trainingDataProvider.prepareDataForEpoch();

      int miniBatchIdx = 0;
      while (true) {
        final Collection<Map.Entry<K, V>> miniBatchData = trainingDataProvider.getNextBatchData();
        if (miniBatchData.isEmpty()) {
          break; // Finish the epoch when there are no more data to process
        }

        LOG.log(Level.INFO, "Starting batch {0} in epoch {1}", new Object[] {miniBatchIdx, epochIdx});
        if (miniBatchBarrier.await()) {
          cleanup();
          return;
        }

        modelAccessor.getAndResetMetrics();

        trainer.setMiniBatchData(miniBatchData);

        taskUnitScheduler.waitSchedule(pullTaskUnitInfo);
        final long pullStartTime = System.currentTimeMillis();
        trainer.pullModel();
        final double pullTime = (System.currentTimeMillis() - pullStartTime) / 1000D;
        taskUnitScheduler.onTaskUnitFinished(pullTaskUnitInfo);

        taskUnitScheduler.waitSchedule(compTaskUnitInfo);
        final long compStartTime = System.currentTimeMillis();
        trainer.localCompute();
        final double compTime = (System.currentTimeMillis() - compStartTime) / 1000D;
        taskUnitScheduler.onTaskUnitFinished(compTaskUnitInfo);

        taskUnitScheduler.waitSchedule(pushTaskUnitInfo);
        final long pushStartTime = System.currentTimeMillis();
        trainer.pushUpdate();
        final double pushTime = (System.currentTimeMillis() - pushStartTime) / 1000D;
        taskUnitScheduler.onTaskUnitFinished(pushTaskUnitInfo);

        progressReporter.reportBatchFinish(miniBatchIdx);

        LOG.log(Level.INFO, "TaskUnitTime. pullTime: {0}, compTime: {1}, pushTime: {2}",
            new Object[]{pullTime, compTime, pushTime});

        miniBatchIdx++;

        if (abortFlag.get()) {
          LOG.log(Level.INFO, "The tasklet {0} is getting closed.", taskletId);
          return;
        }
      }

      trainer.onEpochFinished(epochIdx);
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
   * Called when the Task is requested to close.
   * The {@link #abortFlag} is set true, so the task terminates execution.
   */
  @Override
  public void close() {
    LOG.log(Level.INFO, "Requested to close!");
    abortFlag.set(true);
  }
}
