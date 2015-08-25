/*
 * Copyright (C) 2015 Seoul National University
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
package edu.snu.cay.services.shuffle.evaluator.operator.impl;

import edu.snu.cay.services.shuffle.driver.impl.PushShuffleCode;
import edu.snu.cay.services.shuffle.evaluator.ControlMessageSynchronizer;
import edu.snu.cay.services.shuffle.evaluator.DataSender;
import edu.snu.cay.services.shuffle.evaluator.ESControlMessageSender;
import edu.snu.cay.services.shuffle.evaluator.operator.PushShuffleSender;
import edu.snu.cay.services.shuffle.network.ShuffleControlMessage;
import edu.snu.cay.services.shuffle.network.ShuffleTupleMessage;
import edu.snu.cay.services.shuffle.utils.StateMachine;
import org.apache.reef.io.Tuple;
import org.apache.reef.io.network.Message;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.util.Optional;
import org.apache.reef.wake.remote.transport.LinkListener;

import javax.inject.Inject;
import java.net.SocketAddress;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Default implementation for push-based shuffle sender.
 */
public final class PushShuffleSenderImpl<K, V> implements PushShuffleSender<K, V> {

  private static final Logger LOG = Logger.getLogger(PushShuffleSenderImpl.class.getName());

  private final DataSender<K, V> dataSender;
  private final ESControlMessageSender controlMessageSender;
  private final ControlMessageSynchronizer synchronizer;
  private final long senderTimeout;
  private final AtomicBoolean initialized;

  private final StateMachine stateMachine;

  @Inject
  private PushShuffleSenderImpl(
      final DataSender<K, V> dataSender,
      final ESControlMessageSender controlMessageSender,
      final ControlMessageSynchronizer synchronizer,
      @Parameter(SenderTimeout.class) final long senderTimeout) {
    this.dataSender = dataSender;
    dataSender.registerTupleLinkListener(new TupleLinkListener());
    this.controlMessageSender = controlMessageSender;
    this.synchronizer = synchronizer;
    this.senderTimeout = senderTimeout;
    this.initialized = new AtomicBoolean();

    this.stateMachine = createStateMachine();
  }

  /**
   * @return a state machine for PushShuffleSenderImpl
   */
  public static StateMachine createStateMachine() {
    return StateMachine.newBuilder()
        .addState("CREATED", "Wait for initialized message from the manager")
        .addState("SENDING", "Send tuples to receivers")
        .addState("WAITING", "Wait for all receivers received tuples from senders")
        .setInitialState("CREATED")
        .addTransition("CREATED", "SENDING",
            "When a SHUFFLE_INITIALIZED message arrived from the manager")
        .addTransition("SENDING", "WAITING",
            "When a user calls complete() method. It sends a SENDER_COMPLETED message to the manager.")
        .addTransition("WAITING", "SENDING",
            "When a ALL_RECEIVERS_RECEIVED message arrived from the manager."
                + "It wakes up a caller who is blocking on waitForReceiver() or completeAndWaitForReceiver().")
        .build();
  }

  @Override
  public void onControlMessage(final ShuffleControlMessage controlMessage) {

  }

  private final class TupleLinkListener implements LinkListener<Message<ShuffleTupleMessage<K, V>>> {

    @Override
    public void onSuccess(final Message<ShuffleTupleMessage<K, V>> message) {
      LOG.log(Level.FINE, "A ShuffleTupleMessage was successfully sent : {0}", message);
    }

    @Override
    public void onException(
        final Throwable cause,
        final SocketAddress socketAddress,
        final Message<ShuffleTupleMessage<K, V>> message) {
      LOG.log(Level.WARNING, "An exception occurred while sending a ShuffleTupleMessage. cause : {0}," +
          " socket address : {1}, message : {2}", new Object[]{cause, socketAddress, message});
      // TODO (#67) : failure handling.
      throw new RuntimeException("An exception occurred while sending a ShuffleTupleMessage", cause);
    }
  }

  @Override
  public List<String> sendTuple(final Tuple<K, V> tuple) {
    waitForInitializing();
    stateMachine.checkState("SENDING");
    return dataSender.sendTuple(tuple);
  }

  @Override
  public List<String> sendTuple(final List<Tuple<K, V>> tupleList) {
    waitForInitializing();
    stateMachine.checkState("SENDING");
    return dataSender.sendTuple(tupleList);
  }

  @Override
  public void sendTupleTo(final String receiverId, final Tuple<K, V> tuple) {
    waitForInitializing();
    stateMachine.checkState("SENDING");
    dataSender.sendTupleTo(receiverId, tuple);
  }

  @Override
  public void sendTupleTo(final String receiverId, final List<Tuple<K, V>> tupleList) {
    waitForInitializing();
    stateMachine.checkState("SENDING");
    dataSender.sendTupleTo(receiverId, tupleList);
  }

  private void waitForInitializing() {
    if (initialized.compareAndSet(false, true)) {
      controlMessageSender.sendToManager(PushShuffleCode.SENDER_INITIALIZED);
      final Optional<ShuffleControlMessage> shuffleInitializedMessage = synchronizer.waitOnLatch(
          PushShuffleCode.SHUFFLE_INITIALIZED, senderTimeout);

      if (!shuffleInitializedMessage.isPresent()) {
        // TODO (#33) : failure handling
        throw new RuntimeException("the specified time elapsed but the manager did not send an expected message.");
      }
      stateMachine.checkAndSetState("CREATED", "SENDING");
    }
  }

  @Override
  public void complete() {
    LOG.log(Level.INFO, "Complete to send tuples");
    stateMachine.checkAndSetState("SENDING", "WAITING");
    controlMessageSender.sendToManager(PushShuffleCode.SENDER_COMPLETED);
  }

  @Override
  public void waitForReceivers() {
    LOG.log(Level.INFO, "Wait for all receivers received");
    final Optional<ShuffleControlMessage> receiversReceivedMessage = synchronizer.waitOnLatch(
        PushShuffleCode.ALL_RECEIVERS_RECEIVED, senderTimeout);
    if (!receiversReceivedMessage.isPresent()) {
      // TODO (#67) : failure handling
      throw new RuntimeException("the specified time elapsed but the manager did not send an expected message.");
    }

    synchronizer.reopenLatch(PushShuffleCode.ALL_RECEIVERS_RECEIVED);
    stateMachine.checkAndSetState("WAITING", "SENDING");
  }

  @Override
  public void completeAndWaitForReceivers() {
    complete();
    waitForReceivers();
  }

  // default_value = 10 min
  @NamedParameter(doc = "the maximum time to wait message in milliseconds.", default_value = "600000")
  public static final class SenderTimeout implements Name<Long> {
  }
}
