/*
 * Copyright (C) 2016 Seoul National University
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
package edu.snu.cay.services.em.evaluator.impl;

import edu.snu.cay.services.em.avro.*;
import edu.snu.cay.services.em.common.parameters.KeyCodecName;
import edu.snu.cay.services.em.evaluator.api.BlockHandler;
import edu.snu.cay.services.em.evaluator.api.MigrationExecutor;
import edu.snu.cay.services.em.msg.api.EMMsgSender;
import edu.snu.cay.services.em.serialize.Serializer;
import edu.snu.cay.utils.trace.HTraceUtils;
import org.apache.reef.io.serialization.Codec;
import org.apache.reef.tang.InjectionFuture;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.util.Optional;
import org.htrace.Span;
import org.htrace.Trace;
import org.htrace.TraceInfo;
import org.htrace.TraceScope;

import javax.inject.Inject;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Data-first version of {@link MigrationExecutor}.
 * It focuses on fast migration and availability of data,
 * losing some updates on data values.
 *
 * Protocol:
 * 1) MoveInitMsg (Driver -> sender)
 * 2) DataMsg (sender -> receiver)
 * 3) OwnershipMsg (receiver -> Driver)
 * 4) OwnershipMsg (Driver -> sender)
 * 5) BlockMovedMsg (sender -> Driver)
 *
 * @param <K> Type of key in MemoryStore
 */
public final class DataFirstMigrationExecutor<K> implements MigrationExecutor {
  private static final int NUM_DATA_MSG_SENDER_THREADS = 2;
  private static final int NUM_DATA_MSG_RECEIVER_THREADS = 2;
  private static final int NUM_OWNERSHIP_MSG_RECEIVER_THREADS = 2;

  private final BlockHandler<K> blockHandler;
  private final OperationRouter router;
  private final InjectionFuture<EMMsgSender> sender;

  /**
   * Thread pools to handle the messages in separate threads to prevent NCS threads' overhead.
   */
  private final ExecutorService dataMsgSenderExecutor = Executors.newFixedThreadPool(NUM_DATA_MSG_SENDER_THREADS);
  private final ExecutorService dataMsgHandlerExecutor = Executors.newFixedThreadPool(NUM_DATA_MSG_RECEIVER_THREADS);
  private final ExecutorService ownershipMsgHandlerExecutor =
      Executors.newFixedThreadPool(NUM_OWNERSHIP_MSG_RECEIVER_THREADS);

  private final Codec<K> keyCodec;
  private final Serializer serializer;

  @Inject
  private DataFirstMigrationExecutor(final BlockHandler<K> blockHandler,
                                     final OperationRouter router,
                                     final InjectionFuture<EMMsgSender> sender,
                                     @Parameter(KeyCodecName.class)final Codec<K> keyCodec,
                                     final Serializer serializer) {
    this.blockHandler = blockHandler;
    this.router = router;
    this.sender = sender;
    this.keyCodec = keyCodec;
    this.serializer = serializer;
  }

  @Override
  public void onNext(final MigrationMsg msg) {
    switch (msg.getType()) {
    case MoveInitMsg:
      onMoveInitMsg(msg);
      break;

    case DataMsg:
      onDataMsg(msg);
      break;

    case OwnershipMsg:
      onOwnershipMsg(msg);
      break;

    default:
      throw new RuntimeException("Unexpected message: " + msg);
    }
  }

  /**
   * Initiates move by sending data messages to the destination evaluator.
   */
  private void onMoveInitMsg(final MigrationMsg msg) {
    final String operationId = msg.getOperationId().toString();
    final MoveInitMsg moveInitMsg = msg.getMoveInitMsg();
    final String receiverId = moveInitMsg.getReceiverId().toString();
    final List<Integer> blockIds = moveInitMsg.getBlockIds();

    // We should detach the span when we transit to another thread (local or remote),
    // and the detached span should call Trace.continueSpan(detached).close() explicitly
    // for stitching the spans from other threads as its children
    Span detached = null;

    Trace.setProcessId("src_eval");
    try (TraceScope onMoveInitMsgScope = Trace.startSpan("on_move_init_msg",
        HTraceUtils.fromAvro(msg.getTraceInfo()))) {

      detached = onMoveInitMsgScope.detach();
      final TraceInfo traceInfo = TraceInfo.fromSpan(detached);

      for (final int blockId : blockIds) {
        dataMsgSenderExecutor.submit(new Runnable() {
          @Override
          public void run() {
            final Map<K, Object> blockData = blockHandler.getBlock(blockId);

            final List<KeyValuePair> keyValuePairs;
            try (TraceScope encodeDataScope = Trace.startSpan("encode_data", traceInfo)) {
              keyValuePairs = toKeyValuePairs(blockData, serializer.getCodec());
            }

            sender.get().sendDataMsg(receiverId, keyValuePairs, blockId, operationId, traceInfo);
          }
        });
      }
    } finally {
      Trace.continueSpan(detached).close();
    }
  }

  /**
   * Puts the data message contents into own memory store.
   */
  private void onDataMsg(final MigrationMsg msg) {
    final DataMsg dataMsg = msg.getDataMsg();
    final String operationId = msg.getOperationId().toString();
    final int blockId = dataMsg.getBlockId();

    // We should detach the span when we transit to another thread (local or remote),
    // and the detached span should call Trace.continueSpan(detached).close() explicitly
    // for stitching the spans from other threads as its children
    Span detached = null;

    Trace.setProcessId("dst_eval");
    try (TraceScope onDataMsgScope = Trace.startSpan(String.format("on_data_msg. blockId: %d", blockId),
        HTraceUtils.fromAvro(msg.getTraceInfo()))) {

      detached = onDataMsgScope.detach();
      final TraceInfo traceInfo = TraceInfo.fromSpan(detached);

      dataMsgHandlerExecutor.submit(new Runnable() {
        @Override
        public void run() {
          final Map<K, Object> dataMap;
          try (TraceScope decodeDataScope = Trace.startSpan("decode_data", traceInfo)) {
            dataMap = toDataMap(dataMsg.getKeyValuePairs(), serializer.getCodec());
          }

          final String senderId = dataMsg.getSenderId().toString();

          final int newOwnerId = getStoreId(dataMsg.getReceiverId().toString());
          final int oldOwnerId = getStoreId(senderId);

          blockHandler.putBlock(blockId, dataMap);

          router.updateOwnership(blockId, oldOwnerId, newOwnerId);

          // Notify the driver that the ownership has been updated by setting empty destination id.
          sender.get().sendOwnershipMsg(Optional.empty(), senderId, operationId,
              blockId, oldOwnerId, newOwnerId, traceInfo);
        }
      });
    } finally {
      Trace.continueSpan(detached).close();
    }
  }

  private void onOwnershipMsg(final MigrationMsg msg) {
    final String operationId = msg.getOperationId().toString();
    final OwnershipMsg ownershipMsg = msg.getOwnershipMsg();
    final int blockId = ownershipMsg.getBlockId();
    final int oldOwnerId = ownershipMsg.getOldOwnerId();
    final int newOwnerId = ownershipMsg.getNewOwnerId();

    // We should detach the span when we transit to another thread (local or remote),
    // and the detached span should call Trace.continueSpan(detached).close() explicitly
    // for stitching the spans from other threads as its children
    Span detached = null;

    Trace.setProcessId("src_eval");
    try (TraceScope onOwnershipMsgScope = Trace.startSpan(String.format("on_ownership_msg. blockId: %d", blockId),
        HTraceUtils.fromAvro(msg.getTraceInfo()))) {

      detached = onOwnershipMsgScope.detach();
      final TraceInfo traceInfo = TraceInfo.fromSpan(detached);

      ownershipMsgHandlerExecutor.submit(new Runnable() {
        @Override
        public void run() {
          // Update the owner of the block to the new one.
          // Operations being executed keep a read lock on router while being executed.
          router.updateOwnership(blockId, oldOwnerId, newOwnerId);

          // After the ownership is updated, the data is never accessed locally,
          // so it is safe to remove the local data block.
          blockHandler.removeBlock(blockId);

          sender.get().sendBlockMovedMsg(operationId, blockId, traceInfo);
        }
      });
    } finally {
      Trace.continueSpan(detached).close();
    }
  }

  private <V> List<KeyValuePair> toKeyValuePairs(final Map<K, V> blockData,
                                                 final Codec<V> valueCodec) {
    final List<KeyValuePair> kvPairs = new ArrayList<>(blockData.size());
    for (final Map.Entry<K, V> entry : blockData.entrySet()) {
      kvPairs.add(KeyValuePair.newBuilder()
          .setKey(ByteBuffer.wrap(keyCodec.encode(entry.getKey())))
          .setValue(ByteBuffer.wrap(valueCodec.encode(entry.getValue())))
          .build());
    }
    return kvPairs;
  }

  private <V> Map<K, V> toDataMap(final List<KeyValuePair> keyValuePairs,
                                  final Codec<V> valueCodec) {
    final Map<K, V> dataMap = new HashMap<>(keyValuePairs.size());
    for (final KeyValuePair kvPair : keyValuePairs) {
      dataMap.put(
          keyCodec.decode(kvPair.getKey().array()),
          valueCodec.decode(kvPair.getValue().array()));
    }
    return dataMap;
  }

  /**
   * Converts evaluator id to store id.
   * TODO #509: remove assumption on the format of context Id
   */
  private int getStoreId(final String evalId) {
    // MemoryStoreId is the suffix of context id (Please refer to PartitionManager.registerEvaluator()
    // and EMConfiguration.getServiceConfigurationWithoutNameResolver()).
    return Integer.valueOf(evalId.split("-")[1]);
  }
}