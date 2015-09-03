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
package edu.snu.cay.services.shuffle.evaluator.impl;

import edu.snu.cay.services.shuffle.common.ShuffleDescription;
import edu.snu.cay.services.shuffle.driver.impl.PushShuffleCode;
import edu.snu.cay.services.shuffle.evaluator.ESNetworkSetup;
import edu.snu.cay.services.shuffle.evaluator.Shuffle;
import edu.snu.cay.services.shuffle.evaluator.operator.ShuffleOperatorProvider;
import edu.snu.cay.services.shuffle.evaluator.operator.ShuffleReceiver;
import edu.snu.cay.services.shuffle.evaluator.operator.ShuffleSender;
import edu.snu.cay.services.shuffle.network.ShuffleControlMessage;
import org.apache.reef.annotations.audience.EvaluatorSide;
import org.apache.reef.io.network.Message;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.remote.transport.LinkListener;

import javax.inject.Inject;
import java.net.SocketAddress;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Shuffle implementation for static push-based shuffling.
 *
 * The initial shuffle description can never be changed. Users cannot add or remove more tasks
 * to the shuffle and cannot change the key, value codecs and shuffling strategy after the Shuffle is created.
 */
@EvaluatorSide
public final class StaticPushShuffle<K, V> implements Shuffle<K, V> {

  private static final Logger LOG = Logger.getLogger(StaticPushShuffle.class.getName());

  private final ShuffleDescription shuffleDescription;

  private final ShuffleReceiver<K, V> shuffleReceiver;
  private final ShuffleSender<K, V> shuffleSender;

  private final ESNetworkSetup networkSetup;

  @Inject
  private StaticPushShuffle(
      final ShuffleDescription shuffleDescription,
      final ShuffleOperatorProvider<K, V> operatorProvider,
      final ESNetworkSetup networkSetup) {
    this.shuffleDescription = shuffleDescription;
    this.shuffleReceiver = operatorProvider.getReceiver();
    this.shuffleSender = operatorProvider.getSender();
    this.networkSetup = networkSetup;
    networkSetup.setControlMessageHandlerAndLinkListener(new ControlMessageHandler(), new ControlLinkListener());
  }

  /**
   * @return the ShuffleReceiver of the Shuffle
   */
  @Override
  public <T extends ShuffleReceiver<K, V>> T getReceiver() {
    return (T)shuffleReceiver;
  }

  /**
   * @return the ShuffleSender of the Shuffle
   */
  @Override
  public <T extends ShuffleSender<K, V>> T getSender() {
    return (T) shuffleSender;
  }

  /**
   * @return the initial shuffle description
   */
  @Override
  public ShuffleDescription getShuffleDescription() {
    return shuffleDescription;
  }

  /**
   * Close the network setup.
   */
  @Override
  public void close() {
    networkSetup.close();
  }

  private final class ControlMessageHandler implements EventHandler<Message<ShuffleControlMessage>> {

    @Override
    public void onNext(final Message<ShuffleControlMessage> message) {
      final ShuffleControlMessage controlMessage = message.getData().iterator().next();
      switch (controlMessage.getCode()) {

      // Control messages to senders.
      case PushShuffleCode.SENDER_CAN_SEND:
      case PushShuffleCode.SENDER_SHUTDOWN:
        shuffleSender.onControlMessage(message);
        break;

      // Control messages to receivers.
      case PushShuffleCode.SENDER_COMPLETED:
      case PushShuffleCode.RECEIVER_CAN_RECEIVE:
      case PushShuffleCode.RECEIVER_SHUTDOWN:
        shuffleReceiver.onControlMessage(message);
        break;

      default:
        throw new RuntimeException("Unknown code [ " + controlMessage.getCode() + " ] from " + message.getSrcId());
      }
    }
  }

  private final class ControlLinkListener implements LinkListener<Message<ShuffleControlMessage>> {

    @Override
    public void onSuccess(final Message<ShuffleControlMessage> networkControlMessage) {
      LOG.log(Level.FINE, "ShuffleControlMessage was successfully sent : {0}", networkControlMessage);
    }

    @Override
    public void onException(
        final Throwable throwable,
        final SocketAddress socketAddress,
        final Message<ShuffleControlMessage> networkControlMessage) {
      LOG.log(Level.WARNING, "An exception occurred while sending ShuffleControlMessage to driver. cause : {0}, " +
          "socket address : {1}, message : {2}", new Object[]{throwable, socketAddress, networkControlMessage});
      throw new RuntimeException("An exception occurred while sending ShuffleControlMessage to driver", throwable);
      // TODO #67: failure handling.
    }
  }
}
