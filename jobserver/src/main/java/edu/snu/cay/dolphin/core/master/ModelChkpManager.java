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
import edu.snu.cay.services.et.common.util.concurrent.ListenableFuture;
import edu.snu.cay.services.et.driver.api.AllocatedExecutor;
import edu.snu.cay.services.et.driver.api.AllocatedTable;
import edu.snu.cay.services.et.driver.api.ETMaster;
import edu.snu.cay.services.et.exceptions.TableNotExistException;
import edu.snu.cay.utils.CatchableExecutors;
import org.apache.reef.driver.client.JobMessageObserver;
import org.apache.reef.tang.InjectionFuture;
import org.apache.reef.tang.annotations.Parameter;

import javax.annotation.concurrent.NotThreadSafe;
import javax.inject.Inject;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;

/**
 * Tracks and maintains checkpoints of model table(s) during training.
 * It's for restoring model snapshots from checkpoints and evaluating it after training.
 * Since this class is not thread-safe, be careful to access it concurrently (okay for the current use case).
 */
@NotThreadSafe
final class ModelChkpManager {
  private final JobLogger jobLogger;

  private final LinkedList<List<Future<String>>> checkpointIdFutures = new LinkedList<>();

  private final boolean hasLocalModel;
  private final String localModelTableId;
  private final String modelTableId;
  private final String inputTableId;

  private final InjectionFuture<ETMaster> etMasterFuture;
  private final InjectionFuture<JobMessageObserver> jobMessageObserverFuture;

  private final InjectionFuture<MasterSideMsgSender> msgSender;

  private final AtomicInteger workerCount = new AtomicInteger(0);
  private final AtomicInteger chkpCounter = new AtomicInteger(0);
  private final AtomicInteger restoreCounter = new AtomicInteger(0);
  private final AtomicBoolean inputTableRestored = new AtomicBoolean(false);

  private List<AllocatedExecutor> runningServers;
  private List<AllocatedExecutor> runningWorkers;
  private List<AllocatedExecutor> remoteWorkers;

  private final ExecutorService executor = CatchableExecutors.newSingleThreadExecutor();

  @Inject
  private ModelChkpManager(final JobLogger jobLogger,
                           final InjectionFuture<ETMaster> etMasterFuture,
                           final InjectionFuture<MasterSideMsgSender> msgSender,
                           final InjectionFuture<JobMessageObserver> jobMessageObserverFuture,
                           @Parameter(DolphinParameters.HasLocalModelTable.class) final boolean hasLocalTable,
                           @Parameter(DolphinParameters.LocalModelTableId.class) final String localModelTableId,
                           @Parameter(DolphinParameters.ModelTableId.class) final String modelTableId,
                           @Parameter(DolphinParameters.InputTableId.class) final String inputTableId) {
    this.jobLogger = jobLogger;
    this.etMasterFuture = etMasterFuture;
    this.jobMessageObserverFuture = jobMessageObserverFuture;
    this.msgSender = msgSender;

    this.hasLocalModel = hasLocalTable;
    this.localModelTableId = localModelTableId;
    this.modelTableId = modelTableId;
    this.inputTableId = inputTableId;
  }

  /**
   * Set executors for evaluating a model.
   * Should be called before starting model evaluation.
   * It assumes that these entries does not change in model evaluation phase.
   * @param serverExecutors server executors
   * @param workerExecutors worker executors
   */
  void setExecutors(final List<AllocatedExecutor> serverExecutors,
                    final List<AllocatedExecutor> workerExecutors) {
    this.runningServers = serverExecutors;
    this.runningWorkers = workerExecutors;
    this.remoteWorkers = new ArrayList<>(runningWorkers);
    remoteWorkers.removeAll(runningServers);
  }

  /**
   * On a message from worker.
   * Workers send messages that they are ready to evaluate next model.
   * Evaluation of one model table is started when all runningWorkers are synchronized.
   * Manager restores a model table from the oldest checkpoint and lets runningWorkers evaluate the table.
   * When there's no more checkpoints, manager stops worker from evaluation.
   */
  void onWorkerMsg() {
    final int numWorkersSentMsg = workerCount.incrementAndGet();

    jobLogger.log(Level.INFO, "Msg from a worker. [{0} / {1}]", new Object[]{numWorkersSentMsg, runningWorkers.size()});

    if (numWorkersSentMsg == runningWorkers.size()) {
      workerCount.set(0); // reset
      executor.submit(() -> {
        final boolean doNext;

        // load input table at first time
        if (inputTableRestored.compareAndSet(false, true)) {
          doNext = true;
          restoreInputTable();
        } else {
          dropPrevModelTables();
          doNext = restoreOldestCheckpoint();
          if (!doNext) {
            dropInputTable();
          }
        }
        runningWorkers.forEach(worker -> msgSender.get().sendModelEvalAnsMsg(worker.getId(), doNext));
      });
    }
  }

  private void restoreInputTable() {
    try {
      final List<Future<String>> chkpIdFutures = checkpointIdFutures.pop();
      final String inputChkpId = chkpIdFutures.get(0).get();
      final Future<AllocatedTable> inputTableFuture = etMasterFuture.get().createTable(inputChkpId, runningWorkers);
      final AllocatedTable restoredInputTable = inputTableFuture.get();

      jobLogger.log(Level.INFO, "Input table [{0}] has been restored from checkpoint.", restoredInputTable.getId());
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException(e);
    }
  }

  private void dropInputTable() {
    try {
      etMasterFuture.get().getTable(inputTableId).drop().get();
    } catch (InterruptedException | ExecutionException | TableNotExistException e) {
      throw new RuntimeException(e);
    }
  }

  private void dropPrevModelTables() {
    // Need to drop the previous model tables first
    try {
      final Future future = etMasterFuture.get().getTable(modelTableId).drop();
      if (hasLocalModel) {
        etMasterFuture.get().getTable(localModelTableId).drop().get();
      }
      future.get();
    } catch (TableNotExistException e) {
      // skip if table does not exist
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Restores tables with the oldest checkpoint.
   * It waits until the restoration finishes.
   */
  private boolean restoreOldestCheckpoint() {
    if (checkpointIdFutures.isEmpty()) {
      jobLogger.log(Level.INFO, "No more checkpoints.");
      return false;
    }

    jobMessageObserverFuture.get().sendMessageToClient(String.format("Model eval progress: [%d / %d]",
            restoreCounter.incrementAndGet(), chkpCounter.get()).getBytes());

    // restore a model to evaluate from a checkpoint
    try {
      final List<Future<String>> chkpIdFutures = checkpointIdFutures.pop();

      final String modelChkpId = chkpIdFutures.get(0).get();
      final Future<AllocatedTable> modelTableFuture = etMasterFuture.get().createTable(modelChkpId, runningServers);

      if (hasLocalModel) {
        final String localModelChkpId = chkpIdFutures.get(1).get();
        final Future<AllocatedTable> localModelTableFuture = etMasterFuture.get()
            .createTable(localModelChkpId, runningWorkers);
        final AllocatedTable restoredTable = localModelTableFuture.get();

        jobLogger.log(Level.INFO, "Local model table [{0}] has been restored from checkpoint.",
            restoredTable.getId());
      }

      final AllocatedTable restoredModelTable = modelTableFuture.get();
      restoredModelTable.subscribe(remoteWorkers).get();

      jobLogger.log(Level.INFO, "Model table [{0}] has been restored from checkpoint.", restoredModelTable.getId());

    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException(e);
    }

    return true;
  }

  /**
   * Create a checkpoint of a current model table.
   */
  void createCheckpoint() {
    final int idx = chkpCounter.getAndIncrement();
    final List<Future<String>> chkpFutures;

    try {
      final ListenableFuture<String> modelChkpIdFuture = etMasterFuture.get().getTable(modelTableId).checkpoint();

      modelChkpIdFuture.addListener(chkpId ->
          jobLogger.log(Level.INFO, "{0}-th model checkpoint is created. Checkpoint Id: {1}",
              new Object[]{idx, chkpId}));

      if (hasLocalModel) {
        final ListenableFuture<String> localModelChkpIdFuture = etMasterFuture.get()
            .getTable(localModelTableId).checkpoint();
        localModelChkpIdFuture.addListener(chkpId ->
            jobLogger.log(Level.INFO, "{0}-th local model checkpoint is created. Checkpoint Id: {1}",
                new Object[]{idx, chkpId}));
        chkpFutures = Arrays.asList(modelChkpIdFuture, localModelChkpIdFuture);
      } else {
        chkpFutures = Collections.singletonList(modelChkpIdFuture);
      }

      checkpointIdFutures.add(chkpFutures);

    } catch (TableNotExistException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Waits all checkpoints requested by {@link #createCheckpoint()} to be done.
   */
  void waitChkpsToBeDone() {
    try {
      final Future<String> future = etMasterFuture.get().getTable(inputTableId).checkpoint();
      checkpointIdFutures.addFirst(Collections.singletonList(future));
    } catch (TableNotExistException e) {
      throw new RuntimeException(e);
    }

    checkpointIdFutures.forEach(futures -> {
      try {
        for (final Future<String> future : futures) {
          future.get();
        }
      } catch (InterruptedException | ExecutionException e) {
        throw new RuntimeException(e);
      }
    });
    jobLogger.log(Level.INFO, "All checkpoints completed. The number of checkpointed models: {0}",
        checkpointIdFutures.size() - 1);
  }
}
