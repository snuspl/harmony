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

import edu.snu.cay.dolphin.core.client.ETDolphinLauncher;
import edu.snu.cay.dolphin.core.server.ServerTasklet;
import edu.snu.cay.dolphin.core.worker.WorkerSideMsgHandler;
import edu.snu.cay.dolphin.core.worker.ModelEvaluationTasklet;
import edu.snu.cay.dolphin.core.worker.WorkerTasklet;
import edu.snu.cay.dolphin.metric.ETDolphinMetricMsgCodec;
import edu.snu.cay.dolphin.metric.parameters.ServerMetricFlushPeriodMs;
import edu.snu.cay.jobserver.JobLogger;
import edu.snu.cay.jobserver.Parameters;
import edu.snu.cay.dolphin.DolphinMsg;
import edu.snu.cay.dolphin.DolphinParameters.*;
import edu.snu.cay.services.et.configuration.TaskletConfiguration;
import edu.snu.cay.services.et.driver.api.AllocatedExecutor;
import edu.snu.cay.services.et.driver.api.AllocatedTable;
import edu.snu.cay.services.et.driver.impl.RunningTasklet;
import edu.snu.cay.services.et.driver.impl.TaskletResult;
import edu.snu.cay.services.et.evaluator.api.Tasklet;
import edu.snu.cay.services.et.metric.MetricManager;
import edu.snu.cay.services.et.metric.configuration.MetricServiceExecutorConf;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Configurations;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.tang.formats.ConfigurationSerializer;

import javax.inject.Inject;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.logging.Level;

/**
 * A Dolphin master, which runs a dolphin job with given executors and tables.
 */
public final class DolphinMaster {
  private final JobLogger jobLogger;

  private final ModelChkpManager modelChkpManager;
  private final MetricManager metricManager;
  private final ETTaskRunner taskRunner;
  private final BatchProgressTracker progressTracker;
  private final MasterSideMsgHandler msgHandler;

  private final long serverMetricFlushPeriodMs;

  private final String jobId;

  private final boolean offlineModelEval; // whether to perform model evaluation offline or online
  private final boolean modelEvaluation;
  private final String modelTableId;
  private final String inputTableId;
  private final String localModelTableId;
  private final Configuration workerConf;

  @Inject
  private DolphinMaster(final JobLogger jobLogger,
                        final MetricManager metricManager,
                        final ModelChkpManager modelChkpManager,
                        final ETTaskRunner taskRunner,
                        final BatchProgressTracker progressTracker,
                        final ConfigurationSerializer confSerializer,
                        final MasterSideMsgHandler masterSideMsgHandler,
                        @Parameter(Parameters.JobId.class) final String jobId,
                        @Parameter(ModelTableId.class) final String modelTableId,
                        @Parameter(InputTableId.class) final String inputTableId,
                        @Parameter(LocalModelTableId.class) final String localModelTableId,
                        @Parameter(OfflineModelEvaluation.class) final boolean offlineModelEval,
                        @Parameter(ModelEvaluation.class) final boolean modelEvaluation,
                        @Parameter(ServerMetricFlushPeriodMs.class) final long serverMetricFlushPeriodMs,
                        @Parameter(ETDolphinLauncher.SerializedWorkerConf.class) final String serializedWorkerConf)
      throws IOException, InjectionException {
    this.jobLogger = jobLogger;
    this.modelChkpManager = modelChkpManager;
    this.metricManager = metricManager;
    this.taskRunner = taskRunner;
    this.progressTracker = progressTracker;
    this.msgHandler = masterSideMsgHandler;
    this.serverMetricFlushPeriodMs = serverMetricFlushPeriodMs;
    this.jobId = jobId;
    this.modelTableId = modelTableId;
    this.inputTableId = inputTableId;
    this.localModelTableId = localModelTableId;
    this.workerConf = confSerializer.fromString(serializedWorkerConf);
    this.offlineModelEval = offlineModelEval;
    this.modelEvaluation = modelEvaluation;
  }

  public TaskletConfiguration getWorkerTaskletConf() {
    return TaskletConfiguration.newBuilder()
        .setId(jobId + "-" + WorkerTasklet.class.getSimpleName())
        .setTaskletClass(WorkerTasklet.class)
        .setTaskletMsgHandlerClass(WorkerSideMsgHandler.class)
        .setUserParamConf(Configurations.merge(
            Tang.Factory.getTang().newConfigurationBuilder()
                .bindNamedParameter(Parameters.JobId.class, jobId)
                .bindNamedParameter(StartingEpochIdx.class, Integer.toString(progressTracker.getGlobalMinEpochIdx()))
                .bindNamedParameter(ModelTableId.class, modelTableId)
                .bindNamedParameter(InputTableId.class, inputTableId)
                .bindNamedParameter(LocalModelTableId.class, localModelTableId)
                .bindNamedParameter(OfflineModelEvaluation.class, Boolean.toString(offlineModelEval))
                .bindNamedParameter(ModelEvaluation.class, Boolean.toString(modelEvaluation))
                .build(),
            workerConf)).build();
  }

  public TaskletConfiguration getWorkerTaskletConf(final Class<? extends Tasklet> taskletClass) {
    return TaskletConfiguration.newBuilder()
        .setId(jobId + "-" + taskletClass.getSimpleName())
        .setTaskletClass(taskletClass)
        .setTaskletMsgHandlerClass(WorkerSideMsgHandler.class)
        .setUserParamConf(Configurations.merge(
            Tang.Factory.getTang().newConfigurationBuilder()
                .bindNamedParameter(Parameters.JobId.class, jobId)
                .bindNamedParameter(StartingEpochIdx.class, Integer.toString(progressTracker.getGlobalMinEpochIdx()))
                .bindNamedParameter(ModelTableId.class, modelTableId)
                .bindNamedParameter(InputTableId.class, inputTableId)
                .bindNamedParameter(LocalModelTableId.class, localModelTableId)
                .bindNamedParameter(OfflineModelEvaluation.class, Boolean.toString(offlineModelEval))
                .bindNamedParameter(ModelEvaluation.class, Boolean.toString(modelEvaluation))
                .build(),
            workerConf))
        .build();
  }

  public TaskletConfiguration getServerTaskletConf() {
    return TaskletConfiguration.newBuilder()
        .setId(jobId + "-" + ServerTasklet.TASKLET_ID)
        .setTaskletClass(ServerTasklet.class)
        .build();
  }

  public MetricServiceExecutorConf getWorkerMetricConf() {
    return MetricServiceExecutorConf.newBuilder()
        .setCustomMetricCodec(ETDolphinMetricMsgCodec.class)
        .build();
  }

  public MetricServiceExecutorConf getServerMetricConf() {
    return MetricServiceExecutorConf.newBuilder()
        .setMetricFlushPeriodMs(serverMetricFlushPeriodMs)
        .build();
  }

  /**
   * Returns a msg handler, which handles {@link DolphinMsg}.
   * It should be called when driver-side msg handler has been called.
   * @return a master
   */
  public MasterSideMsgHandler getMsgHandler() {
    return msgHandler;
  }

  /**
   * Start running a job with given executors and tables.
   * It returns after checking the result of tasks.
   */
  public void start(final List<AllocatedExecutor> servers, final List<AllocatedExecutor> workers,
                    final AllocatedTable modelTable, final AllocatedTable trainingDataTable) {
    try {
      // TODO #5: tasklet-level metric collection
      workers.forEach(worker -> metricManager.startMetricCollection(worker.getId(), getWorkerMetricConf()));

      final List<TaskletResult> taskletResults = taskRunner.run(workers, servers);
      checkTaskResults(taskletResults);

      workers.forEach(worker -> metricManager.stopMetricCollection(worker.getId()));
      if (offlineModelEval) {
        modelChkpManager.waitChkpsToBeDone();
      }
    } catch (Exception e) {
      throw new RuntimeException("Dolphin job has been failed", e);
    }
  }

  /**
   * Start evaluating a model with given server and worker executors.
   * It loads and evaluate all checkpoints of a model table.
   */
  public void evaluate(final List<AllocatedExecutor> servers, final List<AllocatedExecutor> workers) {
    workers.forEach(worker -> metricManager.startMetricCollection(worker.getId(), getWorkerMetricConf()));

    modelChkpManager.setExecutors(servers, workers);

    final List<Future<RunningTasklet>> taskFutures = new ArrayList<>(workers.size());
    workers.forEach(worker ->
        taskFutures.add(worker.submitTasklet(getWorkerTaskletConf(ModelEvaluationTasklet.class))));

    final List<TaskletResult> taskResults = new ArrayList<>(workers.size());
    taskFutures.forEach(taskFuture -> {
      try {
        taskResults.add(taskFuture.get().getTaskResult());
      } catch (InterruptedException | ExecutionException e) {
        throw new RuntimeException(e);
      }
    });

    checkTaskResults(taskResults);

    workers.forEach(worker -> metricManager.stopMetricCollection(worker.getId()));
  }

  private void checkTaskResults(final List<TaskletResult> taskletResultList) {
    taskletResultList.forEach(taskResult -> {
      if (!taskResult.isSuccess()) {
        final String taskId = taskResult.getTaskletId();
        throw new RuntimeException(String.format("Task %s has been failed", taskId));
      }
    });
    jobLogger.log(Level.INFO, "Worker tasks completes successfully");
  }
}
