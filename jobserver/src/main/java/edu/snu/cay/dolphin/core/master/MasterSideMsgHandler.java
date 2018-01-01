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

import edu.snu.cay.dolphin.DolphinMsg;
import edu.snu.cay.dolphin.ProgressMsg;
import edu.snu.cay.utils.CatchableExecutors;
import org.apache.reef.tang.InjectionFuture;

import javax.inject.Inject;
import java.util.concurrent.ExecutorService;

/**
 * A master-side message handler that routes messages to an appropriate component corresponding to the msg type.
 */
public final class MasterSideMsgHandler {
  private final InjectionFuture<WorkerStateManager> workerStateManagerFuture;
  private final InjectionFuture<MiniBatchController> miniBatchControllerFuture;
  private final InjectionFuture<BatchProgressTracker> batchProgressTrackerFuture;
  private final InjectionFuture<ModelChkpManager> modelChkpManagerFuture;

  private static final int NUM_PROGRESS_MSG_THREADS = 8;
  private static final int NUM_SYNC_MSG_THREADS = 8;
  private static final int NUM_MODEL_EV_MSG_THREADS = 8;

  private final ExecutorService progressMsgExecutor = CatchableExecutors.newFixedThreadPool(NUM_PROGRESS_MSG_THREADS);
  private final ExecutorService syncMsgExecutor = CatchableExecutors.newFixedThreadPool(NUM_SYNC_MSG_THREADS);
  private final ExecutorService modelEvalMsgExecutor = CatchableExecutors.newFixedThreadPool(NUM_MODEL_EV_MSG_THREADS);

  @Inject
  private MasterSideMsgHandler(final InjectionFuture<WorkerStateManager> workerStateManagerFuture,
                               final InjectionFuture<MiniBatchController> miniBatchControllerFuture,
                               final InjectionFuture<BatchProgressTracker> batchProgressTrackerFuture,
                               final InjectionFuture<ModelChkpManager> modelChkpManagerFuture) {
    this.workerStateManagerFuture = workerStateManagerFuture;
    this.miniBatchControllerFuture = miniBatchControllerFuture;
    this.batchProgressTrackerFuture = batchProgressTrackerFuture;
    this.modelChkpManagerFuture = modelChkpManagerFuture;
  }

  /**
   * Handles dolphin msgs from workers.
   */
  public void onDolphinMsg(final DolphinMsg dolphinMsg) {
    switch (dolphinMsg.getType()) {
    case ProgressMsg:
      final ProgressMsg progressMsg = dolphinMsg.getProgressMsg();
      progressMsgExecutor.submit(() -> batchProgressTrackerFuture.get().onProgressMsg(progressMsg));
      break;
    case SyncMsg:
      syncMsgExecutor.submit(() -> workerStateManagerFuture.get().onSyncMsg(dolphinMsg.getSyncMsg()));
      break;
    case MiniBatchSyncMsg:
      miniBatchControllerFuture.get().onSync(dolphinMsg.getMiniBatchSyncMsg().getWorkerId().toString());
      break;
    case ModelEvalAskMsg:
      modelEvalMsgExecutor.submit(() -> modelChkpManagerFuture.get().onWorkerMsg());
      break;
    default:
      throw new RuntimeException("Unexpected msg type" + dolphinMsg.getType());
    }
  }
}
