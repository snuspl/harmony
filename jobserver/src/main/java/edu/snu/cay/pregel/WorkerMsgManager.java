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
package edu.snu.cay.pregel;

import edu.snu.cay.jobserver.JobServerMsg;
import edu.snu.cay.jobserver.Parameters;
import edu.snu.cay.services.et.configuration.parameters.TaskletIdentifier;
import edu.snu.cay.services.et.evaluator.api.TaskletCustomMsgHandler;
import edu.snu.cay.services.et.evaluator.impl.TaskletCustomMsgSender;
import edu.snu.cay.utils.AvroUtils;
import org.apache.reef.annotations.audience.EvaluatorSide;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.nio.ByteBuffer;
import java.util.concurrent.CountDownLatch;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A component for an executor to synchronize with other executors.
 * By calling {@link #waitForTryNextSuperstepMsg}, it sends a message to {@link PregelMaster} and waits a response.
 * Master will decide whether the worker continues or not.
 */
@EvaluatorSide
final class WorkerMsgManager implements TaskletCustomMsgHandler {
  private static final Logger LOG = Logger.getLogger(WorkerMsgManager.class.getName());

  private final TaskletCustomMsgSender taskletCustomMsgSender;

  /**
   * This value is updated by the response from master at the end of each superstep.
   * If it's true worker stops running supersteps.
   */
  private volatile boolean goNextSuperstep;

  private volatile CountDownLatch syncLatch;

  private final String jobId;
  private final String taskletId;

  @Inject
  private WorkerMsgManager(@Parameter(Parameters.JobId.class) final String jobId,
                           @Parameter(TaskletIdentifier.class) final String taskletId,
                           final TaskletCustomMsgSender taskletCustomMsgSender) {
    this.jobId = jobId;
    this.taskletId = taskletId;
    this.taskletCustomMsgSender = taskletCustomMsgSender;
  }

  @Override
  public void onNext(final byte[] bytes) {
    final SuperstepControlMsg controlMsg = AvroUtils.fromBytes(bytes, SuperstepControlMsg.class);
    LOG.log(Level.INFO, "Received superstep control message: {0}", controlMsg);
    onControlMsg(controlMsg);
  }

  private void onControlMsg(final SuperstepControlMsg msg) {
    switch (msg.getType()) {
    case ControlMsgType.Start:
      goNextSuperstep = true;
      break;
    case ControlMsgType.Stop:
      goNextSuperstep = false;
      break;
    default:
      throw new RuntimeException("unexpected type");
    }
    syncLatch.countDown();
  }

  /**
   * Synchronize with other executors.
   * It sends a message to master and waits a response message.
   *
   * @param numActiveVertices the number of active vertices after superstep
   * @param numSentMsgs the number of sent messages in this superstep
   */
  boolean waitForTryNextSuperstepMsg(final int numActiveVertices, final int numSentMsgs) {

    // 1. reset state
    this.goNextSuperstep = false;
    this.syncLatch = new CountDownLatch(1);

    // 2. send a message
    final boolean isAllVerticesHalt = numActiveVertices == 0;
    final boolean isNoOngoingMsgs = numSentMsgs == 0;
    final SuperstepResultMsg resultMsg = SuperstepResultMsg.newBuilder()
        .setIsAllVerticesHalt(isAllVerticesHalt)
        .setIsNoOngoingMsgs(isNoOngoingMsgs)
        .build();

    final JobServerMsg jobServerMsg = JobServerMsg.newBuilder()
        .setJobId(jobId)
        .setSrcId(taskletId)
        .setJobMsg(ByteBuffer.wrap(AvroUtils.toBytes(resultMsg, SuperstepResultMsg.class)))
        .build();

    taskletCustomMsgSender.send(AvroUtils.toBytes(jobServerMsg, JobServerMsg.class));

    // 3. wait for a response
    try {
      syncLatch.await();
    } catch (final InterruptedException e) {
      throw new RuntimeException("Unexpected exception", e);
    }

    return goNextSuperstep;
  }
}
