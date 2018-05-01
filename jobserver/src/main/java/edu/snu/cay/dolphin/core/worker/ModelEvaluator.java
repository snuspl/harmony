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

import edu.snu.cay.common.metric.avro.Metrics;
import edu.snu.cay.dolphin.ModelEvalAnsMsg;
import edu.snu.cay.dolphin.metric.avro.DolphinWorkerMetrics;
import edu.snu.cay.dolphin.metric.avro.WorkerMetricsType;
import edu.snu.cay.jobserver.JobLogger;
import edu.snu.cay.services.et.metric.MetricCollector;
import org.apache.reef.exception.evaluator.NetworkException;
import org.apache.reef.io.network.group.impl.utils.ResettingCountDownLatch;
import org.apache.reef.tang.InjectionFuture;

import javax.inject.Inject;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A component for evaluating a trained model.
 */
public final class ModelEvaluator {
  private static final Logger LOG = Logger.getLogger(ModelEvaluator.class.getName());

  private final JobLogger jobLogger;

  private final InjectionFuture<MetricCollector> metricCollectorFuture;
  private final InjectionFuture<WorkerSideMsgSender> msgSenderFuture;

  private final TrainingDataProvider trainingDataProvider;
  private final TestDataProvider testDataProvider;
  private final Trainer trainer;

  private final ResettingCountDownLatch latch = new ResettingCountDownLatch(1);
  private volatile boolean doNext = true;

  @Inject
  private ModelEvaluator(final InjectionFuture<WorkerSideMsgSender> msgSenderFuture,
                         final Trainer trainer,
                         final TestDataProvider testDataProvider,
                         final TrainingDataProvider trainingDataProvider,
                         final InjectionFuture<MetricCollector> metricCollectorFuture,
                         final JobLogger jobLogger) {
    this.jobLogger = jobLogger;
    this.metricCollectorFuture = metricCollectorFuture;
    this.msgSenderFuture = msgSenderFuture;
    this.trainer = trainer;
    this.trainingDataProvider = trainingDataProvider;
    this.testDataProvider = testDataProvider;
  }

  void evaluateCurrModel() {
    final Collection trainingData = trainingDataProvider.getEpochData();
    final List testData;
    try {
      testData = testDataProvider.getTestData();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    LOG.log(Level.INFO, "Evaluate the current model");

    final Map<CharSequence, Double> objValue = trainer.evaluateModel(trainingData, testData);
    jobLogger.log(Level.INFO, "ObjValue: {0}", objValue);
  }

  /**
   * Evaluate all checkpointed models.
   */
  void evaluatePrevModels() {
    askMasterToPrepare(); // prepare input table
    final Collection trainingData = trainingDataProvider.getEpochData();
    final List testData;
    try {
      testData = testDataProvider.getTestData();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    int modelCount = 0;
    while (askMasterToPrepare()) {
      LOG.log(Level.INFO, "Evaluate a {0}th model", modelCount++);

      final Map<CharSequence, Double> objValue = trainer.evaluateModel(trainingData, testData);

      jobLogger.log(Level.INFO, "ObjValue: {0}", objValue);

      final DolphinWorkerMetrics metrics = DolphinWorkerMetrics.newBuilder()
          .setType(WorkerMetricsType.ModelEvalMetrics)
          .setObjValue(Metrics.newBuilder().setData(objValue).build())
          .build();

      // send metric to master
      metricCollectorFuture.get().addCustomMetric(metrics);
      metricCollectorFuture.get().flush();
    }

    LOG.log(Level.INFO, "Finish evaluating models");
  }

  /**
   * Tell master that it's ready to evaluate the next model.
   * And wait master's response.
   * At first try, master loads an input table.
   * @return True if there exists a next model table to evaluate
   */
  private boolean askMasterToPrepare() {
    LOG.log(Level.INFO, "Ask master.");
    // send message to master
    try {
      msgSenderFuture.get().sendModelEvalAskMsg();
    } catch (NetworkException e) {
      throw new RuntimeException(e);
    }

    // wait on latch
    latch.awaitAndReset(1);

    return doNext;
  }

  /**
   * A response from master about {@link #askMasterToPrepare()}.
   */
  void onMasterMsg(final ModelEvalAnsMsg msg) {
    this.doNext = msg.getDoNext();

    // release latch
    latch.countDown();
  }
}
