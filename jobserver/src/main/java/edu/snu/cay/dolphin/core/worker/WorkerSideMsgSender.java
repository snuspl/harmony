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

import edu.snu.cay.dolphin.*;
import edu.snu.cay.jobserver.TaskletJobMsg;
import edu.snu.cay.jobserver.Parameters;
import edu.snu.cay.services.et.configuration.parameters.ExecutorIdentifier;
import edu.snu.cay.services.et.configuration.parameters.TaskletIdentifier;
import edu.snu.cay.services.et.evaluator.impl.TaskletCustomMsgSender;
import edu.snu.cay.utils.AvroUtils;
import org.apache.reef.annotations.audience.EvaluatorSide;
import org.apache.reef.exception.evaluator.NetworkException;
import org.apache.reef.io.serialization.SerializableCodec;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;

import java.nio.ByteBuffer;

/**
 * Worker-side message sender.
 */
@EvaluatorSide
public final class WorkerSideMsgSender {
  private final String jobId;
  private final String taskletId;

  private final String executorId;
  private final TaskletCustomMsgSender taskletCustomMsgSender;

  private final SerializableCodec<WorkerGlobalBarrier.State> codec;

  @Inject
  private WorkerSideMsgSender(final TaskletCustomMsgSender taskletCustomMsgSender,
                              final SerializableCodec<WorkerGlobalBarrier.State> codec,
                              @Parameter(Parameters.JobId.class) final String jobId,
                              @Parameter(TaskletIdentifier.class) final String taskletId,
                              @Parameter(ExecutorIdentifier.class) final String executorId) {
    this.taskletCustomMsgSender = taskletCustomMsgSender;
    this.jobId = jobId;
    this.taskletId = taskletId;
    this.executorId = executorId;
    this.codec = codec;
  }

  /**
   * Send {@link ProgressMsg} to master-side for every epoch.
   * @param epochIdx a current processing epoch index
   */
  void sendEpochProgressMsg(final int epochIdx) throws NetworkException {
    final ProgressMsg progressMsg = ProgressMsg.newBuilder()
        .setExecutorId(executorId)
        .setType(ProgressMsgType.Epoch)
        .setProgress(epochIdx)
        .build();

    final DolphinMsg dolphinMsg = DolphinMsg.newBuilder()
        .setType(dolphinMsgType.ProgressMsg)
        .setProgressMsg(progressMsg)
        .build();

    sendDolphinMsg(dolphinMsg);
  }

  /**
   * Send {@link ProgressMsg} to master-side for every mini-batch.
   * @param batchIdx a current processing mini-batch index
   */
  void sendBatchProgressMsg(final int batchIdx) throws NetworkException {
    final ProgressMsg progressMsg = ProgressMsg.newBuilder()
        .setExecutorId(executorId)
        .setType(ProgressMsgType.Batch)
        .setProgress(batchIdx)
        .build();

    final DolphinMsg dolphinMsg = DolphinMsg.newBuilder()
        .setType(dolphinMsgType.ProgressMsg)
        .setProgressMsg(progressMsg)
        .build();

    sendDolphinMsg(dolphinMsg);
  }

  /**
   * Send {@link SyncMsg} to master-side.
   * @param state a current state
   */
  public void sendSyncMsg(final WorkerGlobalBarrier.State state) throws NetworkException {
    final byte[] serializedState = codec.encode(state);

    final SyncMsg syncMsg = SyncMsg.newBuilder()
        .setExecutorId(executorId)
        .setSerializedState(ByteBuffer.wrap(serializedState))
        .build();

    final DolphinMsg dolphinMsg = DolphinMsg.newBuilder()
        .setType(dolphinMsgType.SyncMsg)
        .setSyncMsg(syncMsg)
        .build();

    sendDolphinMsg(dolphinMsg);
  }

  public void sendMiniBatchSyncMsg() {
    final DolphinMsg dolphinMsg = DolphinMsg.newBuilder()
        .setType(dolphinMsgType.MiniBatchSyncMsg)
        .setMiniBatchSyncMsg(MiniBatchSyncMsg.newBuilder()
        .setWorkerId(executorId).build())
        .build();

    sendDolphinMsg(dolphinMsg);
  }

  /**
   * Send a msg to master for asking model evaluation.
   */
  void sendModelEvalAskMsg() throws NetworkException {
    final DolphinMsg dolphinMsg = DolphinMsg.newBuilder()
        .setType(dolphinMsgType.ModelEvalAskMsg)
        .build();

    sendDolphinMsg(dolphinMsg);
  }

  private void sendDolphinMsg(final DolphinMsg dolphinMsg) {
    final TaskletJobMsg taskletJobMsg = TaskletJobMsg.newBuilder()
        .setJobId(jobId)
        .setTaskletId(taskletId)
        .setJobMsg(ByteBuffer.wrap(AvroUtils.toBytes(dolphinMsg, DolphinMsg.class)))
        .build();

    taskletCustomMsgSender.send(AvroUtils.toBytes(taskletJobMsg, TaskletJobMsg.class));
  }
}
