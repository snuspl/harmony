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
package edu.snu.cay.services.et.evaluator.impl;

import edu.snu.cay.services.et.avro.*;
import edu.snu.cay.services.et.configuration.parameters.ExecutorIdentifier;
import edu.snu.cay.services.et.evaluator.api.Block;
import edu.snu.cay.services.et.evaluator.api.BlockPartitioner;
import edu.snu.cay.services.et.evaluator.api.MessageSender;
import edu.snu.cay.services.et.exceptions.BlockNotExistsException;
import edu.snu.cay.services.et.exceptions.TableNotExistException;
import edu.snu.cay.utils.StateMachine;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.reef.driver.parameters.DriverIdentifier;
import org.apache.reef.exception.evaluator.NetworkException;
import org.apache.reef.io.network.impl.StreamingCodec;
import org.apache.reef.io.serialization.Codec;
import org.apache.reef.tang.InjectionFuture;
import org.apache.reef.tang.annotations.Parameter;

import javax.annotation.Nullable;
import javax.inject.Inject;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A class that 1) handles remote access request msgs that other executors sent to this executor
 * and 2) sends the result of the operation to the origin.
 */
final class RemoteAccessOpHandler {
  private static final Logger LOG = Logger.getLogger(RemoteAccessOpHandler.class.getName());

  private final String driverId;
  private final String executorId;
  private final InjectionFuture<Tables> tablesFuture;
  private final InjectionFuture<MessageSender> msgSenderFuture;

  private final CommManager commManager;

  /**
   * Maintains an operation tracker for each table.
   */
  private final Map<String, TableOpTracker> tableOpTrackers = new ConcurrentHashMap<>();

  @Inject
  private RemoteAccessOpHandler(final InjectionFuture<Tables> tablesFuture,
                                final CommManager commManager,
                                @Parameter(DriverIdentifier.class) final String driverId,
                                @Parameter(ExecutorIdentifier.class) final String executorId,
                                final InjectionFuture<MessageSender> msgSenderFuture) {
    this.driverId = driverId;
    this.executorId = executorId;
    this.tablesFuture = tablesFuture;
    this.commManager = commManager;
    this.msgSenderFuture = msgSenderFuture;
  }

  @SuppressWarnings("unchecked")
  <K, V, U> void processOp(final DataOpMetadata opMetaData) {
    final String tableId = opMetaData.getTableId();
    final int blockId = opMetaData.getBlockId();

    LOG.log(Level.FINEST, "Process op: [OpId: {0}, origId: {1}, tableId: {2}, blockId: {3}]]",
        new Object[]{opMetaData.getOpId(), opMetaData.getOrigId(), tableId, blockId});

    final TableComponents<K, V, U> tableComponents = tableOpTrackers.get(tableId).tableComponents;
    final OwnershipCache ownershipCache = tableComponents.getOwnershipCache();

    final Pair<Optional<String>, Lock> remoteEvalIdWithLock = ownershipCache.resolveExecutorWithLock(blockId);
    try {
      final Optional<String> remoteEvalIdOptional = remoteEvalIdWithLock.getKey();
      final boolean isLocal = !remoteEvalIdOptional.isPresent();
      if (isLocal) {
        try {
          final BlockStore<K, V, U> blockStore = tableComponents.getBlockStore();
          final Block<K, V, U> block = blockStore.get(blockId);

          if (opMetaData.isSingleKey()) {
            final boolean[] isSuccess = {true};
            final V singleKeyOutput =
                (V) processSingleKeyOp(block, (SingleKeyDataOpMetadata) opMetaData, isSuccess);

            if (opMetaData.isReplyRequired()) {
              sendResultToOrigin(opMetaData, tableComponents.getSerializer(),
                  singleKeyOutput, Collections.emptyList(), isSuccess[0]);
            }
          } else {
            final boolean[] isSuccess = {true};
            final List<Pair<K, V>> multiKeyOutputs =
                (List<Pair<K, V>>) processMultiKeyOp(block, (MultiKeyDataOpMetadata) opMetaData, isSuccess);

            if (opMetaData.isReplyRequired()) {
              sendResultToOrigin(opMetaData, tableComponents.getSerializer(),
                  null, multiKeyOutputs, isSuccess[0]);
            }
          }
        } catch (final BlockNotExistsException e) {
          throw new RuntimeException(e);
        }

      } else {
        // a case that operation comes to this executor based on wrong or stale ownership info
        redirect(opMetaData, tableComponents, remoteEvalIdOptional.get());
      }
    } finally {
      final Lock ownershipLock = remoteEvalIdWithLock.getValue();
      ownershipLock.unlock();
    }

    deregisterOp(tableId);
  }

  private <K, V, U> V processSingleKeyOp(final Block<K, V, U> block,
                                         final SingleKeyDataOpMetadata<K, V, U> opMetadata,
                                         final boolean[] isSuccess) {
    final V output;
    final OpType opType = opMetadata.getOpType();
    switch (opType) {
    case PUT:
      output = block.put(opMetadata.getKey(), opMetadata.getValue().get());
      isSuccess[0] = true;
      break;
    case PUT_IF_ABSENT:
      output = block.putIfAbsent(opMetadata.getKey(), opMetadata.getValue().get());
      isSuccess[0] = true;
      break;
    case GET:
      output = block.get(opMetadata.getKey());
      isSuccess[0] = true;
      break;
    case GET_OR_INIT:
      output = block.getOrInit(opMetadata.getKey());
      isSuccess[0] = true;
      break;
    case REMOVE:
      output = block.remove(opMetadata.getKey());
      isSuccess[0] = true;
      break;
    case UPDATE:
      output = block.update(opMetadata.getKey(), opMetadata.getUpdateValue().get());
      isSuccess[0] = true;
      break;
    default:
      LOG.log(Level.WARNING, "Undefined type of opMetaData.");
      output = null;
      isSuccess[0] = false;
      break;
    }
    return output;
  }

  private <K, V, U> List<Pair<K, V>> processMultiKeyOp(final Block<K, V, U> block,
                                                       final MultiKeyDataOpMetadata<K, V, U> opMetadata,
                                                       final boolean[] isSuccess) {
    final List<Pair<K, V>> outputs = new ArrayList<>();
    final OpType opType = opMetadata.getOpType();
    switch (opType) {
    case PUT:
      for (int index = 0; index < opMetadata.getKeys().size(); index++) {
        final K key = opMetadata.getKeys().get(index);
        final V localOutput = block.put(key, opMetadata.getValues().get(index));
        if (localOutput != null) {
          outputs.add(Pair.of(key, localOutput));
        }
      }
      isSuccess[0] = true;
      break;
    case GET:
      opMetadata.getKeys().forEach(key -> {
        final V localOutput = block.get(key);
        if (localOutput != null) {
          outputs.add(Pair.of(key, localOutput));
        }
      });
      isSuccess[0] = true;
      break;
    case GET_OR_INIT:
      opMetadata.getKeys().forEach(key -> {
        final V localOutput = block.getOrInit(key);
        if (localOutput != null) {
          outputs.add(Pair.of(key, localOutput));
        }
      });
      isSuccess[0] = true;
      break;
    case UPDATE:
      for (int index = 0; index < opMetadata.getKeys().size(); index++) {
        final K key = opMetadata.getKeys().get(index);
        final V localOutput = block.update(key, opMetadata.getUpdateValues().get(index));
//        outputs.add(Pair.of(key, localOutput));
      }
      isSuccess[0] = true;
      break;
    //TODO #176: support multi-key versions of other op types (e.g. put_if_absent, remove)
    default:
      LOG.log(Level.WARNING, "Undefined type of opMetaData.");
      isSuccess[0] = false;
      break;
    }
    return outputs;
  }


  /**
   * Redirects an operation to the target executor.
   */
  private <K, V, U> void redirect(final DataOpMetadata opMetadata, final TableComponents<K, V, U> tableComponents,
                                  final String targetId) {
    LOG.log(Level.FINE, "Redirect Op for TableId: {0} to {2}",
        new Object[]{opMetadata.getTableId(), targetId});
    RemoteAccessOpSender.encodeAndSendRequestMsg(opMetadata, targetId, executorId,
        tableComponents, msgSenderFuture.get());
  }

  /**
   * Handles the data operation sent from the remote executor.
   */
  <K, V, U> void onTableAccessReqMsg(final long opId, final TableAccessReqMsg msg) {
    final String origEvalId = msg.getOrigId();
    final OpType opType = msg.getOpType();
    final boolean replyRequired = msg.getReplyRequired();
    final String tableId = msg.getTableId();

    final TableComponents<K, V, U> tableComponents;
    try {
      tableComponents = tablesFuture.get().getTableComponents(tableId);
    } catch (TableNotExistException e) {
      // If getTableComponents() fails, the operation is not registered and handled by the Driver with a fallback logic.
      redirectToMaster(opId, msg, origEvalId);
      return;
    }

    if (!registerOp(tableId, tableComponents)) {
      LOG.log(Level.INFO, "Operations for table {0} are being flushed. No more ops will be accepted.", tableId);
      redirectToMaster(opId, msg, origEvalId);
      return;
    }

    // after registration of operation, let's handle it in local
    final KVUSerializer<K, V, U> kvuSerializer = tableComponents.getSerializer();
    final StreamingCodec<K> keyCodec = kvuSerializer.getKeyCodec();
    final StreamingCodec<V> valueCodec = kvuSerializer.getValueCodec();
    final Codec<U> updateValueCodec = kvuSerializer.getUpdateValueCodec();
    final BlockPartitioner<K> blockPartitioner = tableComponents.getBlockPartitioner();

    if (msg.getIsSingleKey()) {
      final DataKey dataKey = msg.getDataKey();
      final DataValue dataValue = msg.getDataValue();
      final K decodedKey = keyCodec.decode(dataKey.getKey().array());

      // decode data values
      final V decodedValue = opType.equals(OpType.PUT) || opType.equals(OpType.PUT_IF_ABSENT) ?
          valueCodec.decode(dataValue.getValue().array()) : null;

      // decode update data value
      final U decodedUpdateValue = opType.equals(OpType.UPDATE) ?
          updateValueCodec.decode(dataValue.getValue().array()) : null;

      final int blockId = blockPartitioner.getBlockId(decodedKey);
      final SingleKeyDataOpMetadata<K, V, U> operation = new SingleKeyDataOpMetadata<>(origEvalId,
          opId, opType, replyRequired, tableId, blockId, decodedKey, decodedValue, decodedUpdateValue);

      commManager.enqueueHandlerOp(operation);

    } else {
      final DataKeys dataKeys = msg.getDataKeys();
      final DataValues dataValues = msg.getDataValues();

      final List<K> keyList = new ArrayList<>();
      dataKeys.getKeys().forEach(key -> keyList.add(keyCodec.decode(key.array())));

      final List<V> valueList;
      final List<U> updateValueList;

      switch (opType) {
      case PUT:
        valueList = new ArrayList<>();
        updateValueList = Collections.emptyList();
        dataValues.getValues().forEach(value -> valueList.add(valueCodec.decode(value.array())));
        break;
      case GET:
      case GET_OR_INIT:
        valueList = Collections.emptyList();
        updateValueList = Collections.emptyList();
        break;
      case UPDATE:
        valueList = Collections.emptyList();
        updateValueList = new ArrayList<>();
        dataValues.getValues().forEach(value -> updateValueList.add(updateValueCodec.decode(value.array())));
        break;
        //TODO #176: support multi-key versions of other op types (e.g. put_if_absent, remove)
      default:
        throw new RuntimeException("Undefined type of OpMetadata");
      }

      // All keys match to same block id
      final int blockId = blockPartitioner.getBlockId(keyList.get(0));
      final MultiKeyDataOpMetadata<K, V, ?> operation = new MultiKeyDataOpMetadata<>(origEvalId,
          opId, opType, replyRequired, tableId, blockId, keyList, valueList, updateValueList);

      commManager.enqueueHandlerOp(operation);
    }
  }

  /**
   * Redirects operation to the master, when it has no metadata of a requested table.
   */
  private void redirectToMaster(final long opId, final TableAccessReqMsg msg, final String origEvalId) {
    final String tableId = msg.getTableId();
    final OpType opType = msg.getOpType();
    final boolean replyRequired = msg.getReplyRequired();

    try {
      LOG.log(Level.WARNING, "The table access request (Table: {0}, opId: {1}) has failed." +
          " Will redirect the message to the Driver for fallback.", new Object[] {tableId, opId});
      if (msg.getIsSingleKey()) {
        msgSenderFuture.get().sendTableAccessReqMsg(origEvalId, driverId, opId, tableId, opType, replyRequired,
            msg.getDataKey(), msg.getDataValue());
      } else {
        msgSenderFuture.get().sendTableAccessReqMsg(origEvalId, driverId, opId, tableId, opType, replyRequired,
            msg.getDataKeys(), msg.getDataValues());
      }
    } catch (NetworkException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Sends the result to the original executor.
   */
  private <K, V> void sendResultToOrigin(final DataOpMetadata opMetadata,
                                         final KVUSerializer<K, V, ?> kvuSerializer,
                                         @Nullable final V localOutput,
                                         final List<Pair<K, V>> localOutputs,
                                         final boolean isSuccess) {
    LOG.log(Level.FINEST, "Send result to origin. OpId: {0}, OrigId: {1}",
        new Object[]{opMetadata.getOpId(), opMetadata.getOrigId()});

    final String tableId = opMetadata.getTableId();
    final String origEvalId = opMetadata.getOrigId();

    try {
      final StreamingCodec<K> keyCodec = kvuSerializer.getKeyCodec();
      final StreamingCodec<V> valueCodec = kvuSerializer.getValueCodec();
      if (opMetadata.isSingleKey()) {
        final DataValue dataValue;
        if (localOutput != null) {
          dataValue = new DataValue();
          dataValue.setValue(ByteBuffer.wrap(valueCodec.encode(localOutput)));
        } else {
          dataValue = null;
        }
        msgSenderFuture.get().sendTableAccessResMsg(origEvalId, opMetadata.getOpId(), tableId, dataValue, isSuccess);
      } else {
        final DataKeys dataKeys = new DataKeys();
        final DataValues dataValues = new DataValues();
        final List<ByteBuffer> encodedKeyList = new ArrayList<>(localOutputs.size());
        final List<ByteBuffer> encodedValueList = new ArrayList<>(localOutputs.size());

        localOutputs.forEach(pair -> {
          encodedKeyList.add(ByteBuffer.wrap(keyCodec.encode(pair.getKey())));
          encodedValueList.add(ByteBuffer.wrap(valueCodec.encode(pair.getValue())));
        });
        dataKeys.setKeys(encodedKeyList);
        dataValues.setValues(encodedValueList);
        msgSenderFuture.get().sendTableAccessResMsg(origEvalId, opMetadata.getOpId(),
            tableId, dataKeys, dataValues, isSuccess);
      }

    } catch (NetworkException e) {
      LOG.log(Level.INFO, "The origin {0} has been removed, so the message is just discarded", origEvalId);
    }
  }

  /**
   * Wait all ongoing operations for a given {@code tableId} to be finished.
   * @param tableId a table id
   */
  void waitOpsTobeFlushed(final String tableId) {
    final TableOpTracker opTracker = tableOpTrackers.get(tableId);
    if (opTracker == null) {
      return;
    }

    opTracker.flush();
    LOG.log(Level.INFO, "Start waiting {0} ops to be flushed", opTracker.getNumOps());
    while (!opTracker.isFlushed()) {
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        // ignore interrupt
      }
      LOG.log(Level.INFO, "The number of remaining ops: {0}", opTracker.getNumOps());
    }

    tableOpTrackers.remove(tableId);
    LOG.log(Level.INFO, "ops for {0} has been flushed out", tableId);
  }

  /**
   * Registers an operation before.
   */
  private boolean registerOp(final String tableId, final TableComponents tableComponents) {
    final TableOpTracker tableOpTracker =
        tableOpTrackers.computeIfAbsent(tableId, (tId) -> new TableOpTracker(tableComponents));

    return tableOpTracker.registerOp();
  }

  /**
   * Deregisters an operation after processing it.
   */
  private void deregisterOp(final String tableId) {
    final TableOpTracker tableOpTracker = tableOpTrackers.get(tableId);
    if (tableOpTracker == null) {
      throw new RuntimeException(String.format("No operations to degregister from table %s", tableId));
    }

    tableOpTracker.deregisterOp();
  }

  /**
   * A class that tracks the number of operations for a table.
   * Operations are accepted only during PROCESSING state.
   * This class maintains {@link TableComponents} to guarantee that ongoing operations can use it,
   * even if the table has been removed asynchronously.
   */
  private static class TableOpTracker {
    private final TableComponents tableComponents;
    private final AtomicInteger numOps;
    private final StateMachine stateMachine;

    private enum State {
      PROCESSING,
      FLUSHING,
      FLUSHED
    }

    TableOpTracker(final TableComponents tableComponents) {
      this.tableComponents = tableComponents;
      this.numOps = new AtomicInteger(0);
      this.stateMachine = initStateMachine();
    }

    private static StateMachine initStateMachine() {
      return StateMachine.newBuilder()
          .addState(State.PROCESSING, "An initial state that accepts incoming requests and process it.")
          .addState(State.FLUSHING, "A state to flush out all ongoing operations, accept no more operations.")
          .addState(State.FLUSHED, "A state that operation are completely flushed.")
          .addTransition(State.PROCESSING, State.FLUSHING, "Requested to flush operations.")
          .addTransition(State.FLUSHING, State.FLUSHED, "Operation flushing completes.")
          .setInitialState(State.PROCESSING)
          .build();
    }

    /**
     * Register a operation for a table.
     * @return true if operation has been accepted
     */
    synchronized boolean registerOp() {
      final boolean accept;
      switch ((State) stateMachine.getCurrentState()) {
      case PROCESSING:
        numOps.incrementAndGet();
        accept = true;
        break;
      case FLUSHING:
      case FLUSHED:
        accept = false;
        break;
      default:
        throw new RuntimeException("Unexpected state");
      }
      return accept;
    }

    /**
     * Deregister a operation for a table.
     */
    synchronized void deregisterOp() {
      switch ((State) stateMachine.getCurrentState()) {
      case PROCESSING:
        numOps.decrementAndGet();
        break;
      case FLUSHING:
        final int remainingOps = numOps.decrementAndGet();
        if (remainingOps == 0) {
          LOG.log(Level.INFO, "No ops. Transit to FLUSHED state");
          stateMachine.setState(State.FLUSHED);
        }
        break;
      case FLUSHED:
        throw new RuntimeException();
      default:
        throw new RuntimeException("Unexpected state");
      }
    }

    /**
     * Flushing the ongoing operations in the table.
     * It returns after all operations are flushed out.
     */
    synchronized void flush() {
      switch ((State) stateMachine.getCurrentState()) {
      case PROCESSING:
        // transit to FLUSHING state. do not accept more ops.
        stateMachine.setState(State.FLUSHING);
        if (numOps.get() == 0) {
          LOG.log(Level.INFO, "No ops. Transit to FLUSHED state");
          stateMachine.setState(State.FLUSHED);
        }
        break;
      case FLUSHING:
        LOG.log(Level.INFO, "Already in FLUSHING state");
        break;
      case FLUSHED:
        LOG.log(Level.INFO, "Already flushed.");
        break;
      default:
        throw new RuntimeException("Unexpected state");
      }
    }

    /**
     * @return the number of ongoing operations
     */
    int getNumOps() {
      return numOps.get();
    }

    /**
     * @return True if the operations are completely flushed
     */
    boolean isFlushed() {
      return this.stateMachine.getCurrentState().equals(State.FLUSHED);
    }
  }
}
