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
import edu.snu.cay.services.et.common.api.MessageHandler;
import edu.snu.cay.services.et.evaluator.api.BulkDataLoader;
import edu.snu.cay.services.et.evaluator.api.MessageSender;
import edu.snu.cay.services.et.exceptions.KeyGenerationException;
import edu.snu.cay.services.et.exceptions.TableNotExistException;
import edu.snu.cay.services.et.metric.MetricCollector;
import edu.snu.cay.services.et.metric.configuration.parameter.CustomMetricCodec;
import edu.snu.cay.services.et.metric.configuration.parameter.MetricFlushPeriodMs;
import edu.snu.cay.utils.AvroUtils;
import edu.snu.cay.utils.CatchableExecutors;
import edu.snu.cay.utils.SingleMessageExtractor;
import org.apache.reef.annotations.audience.EvaluatorSide;
import org.apache.reef.exception.evaluator.NetworkException;
import org.apache.reef.io.network.Message;
import org.apache.reef.io.serialization.Codec;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.InjectionFuture;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.tang.formats.ConfigurationSerializer;

import javax.inject.Inject;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A message handler implementation.
 */
@EvaluatorSide
public final class MessageHandlerImpl implements MessageHandler {
  private static final Logger LOG = Logger.getLogger(MessageHandlerImpl.class.getName());

  private static final int NUM_TBL_CTR_MSG_THREADS = 8;
  private static final int NUM_MIGRATION_MSG_THREADS = 8;
  private static final int NUM_METRIC_MSG_THREADS = 4;
  private static final int NUM_TBL_ACS_MSG_THREADS = 8;
  private static final int NUM_CHKP_THREADS = 8;
  private static final int NUM_TASKLET_MSG_THREADS = 4;
  private static final int NUM_DATA_LOAD_THREADS = 4;

  private final ExecutorService tableCtrMsgExecutor = CatchableExecutors.newFixedThreadPool(NUM_TBL_CTR_MSG_THREADS);
  private final ExecutorService migrationMsgExecutor = CatchableExecutors.newFixedThreadPool(NUM_MIGRATION_MSG_THREADS);
  private final ExecutorService metricMsgExecutor = CatchableExecutors.newFixedThreadPool(NUM_METRIC_MSG_THREADS);
  private final ExecutorService tableAccessMsgExecutor = CatchableExecutors.newFixedThreadPool(NUM_TBL_ACS_MSG_THREADS);
  private final ExecutorService chkpMsgExecutor = CatchableExecutors.newFixedThreadPool(NUM_CHKP_THREADS);
  private final ExecutorService taskletMsgExecutor = CatchableExecutors.newFixedThreadPool(NUM_TASKLET_MSG_THREADS);
  private final ExecutorService dataLoadExecutor = CatchableExecutors.newFixedThreadPool(NUM_DATA_LOAD_THREADS);

  private final InjectionFuture<Tables> tablesFuture;
  private final InjectionFuture<TaskletRuntime> taskletRuntimeFuture;
  private final InjectionFuture<LocalTaskUnitScheduler> taskUnitSchedulerFuture;

  private final ConfigurationSerializer confSerializer;
  private final InjectionFuture<MessageSender> msgSenderFuture;
  private final InjectionFuture<RemoteAccessOpHandler> remoteAccessHandlerFuture;
  private final InjectionFuture<RemoteAccessOpSender> remoteAccessSenderFuture;
  private final InjectionFuture<MigrationExecutor> migrationExecutorFuture;
  private final InjectionFuture<MetricCollector> metricCollectorFuture;
  private final InjectionFuture<ChkpManagerSlave> chkpManagerSlaveFuture;

  @Inject
  private MessageHandlerImpl(final InjectionFuture<Tables> tablesFuture,
                             final InjectionFuture<TaskletRuntime> taskletRuntimeFuture,
                             final InjectionFuture<LocalTaskUnitScheduler> taskUnitSchedulerFuture,
                             final ConfigurationSerializer confSerializer,
                             final InjectionFuture<MessageSender> msgSenderFuture,
                             final InjectionFuture<RemoteAccessOpHandler> remoteAccessHandlerFuture,
                             final InjectionFuture<RemoteAccessOpSender> remoteAccessSenderFuture,
                             final InjectionFuture<MigrationExecutor> migrationExecutorFuture,
                             final InjectionFuture<MetricCollector> metricCollectorFuture,
                             final InjectionFuture<ChkpManagerSlave> chkpManagerSlaveFuture) {
    this.tablesFuture = tablesFuture;
    this.taskletRuntimeFuture = taskletRuntimeFuture;
    this.taskUnitSchedulerFuture = taskUnitSchedulerFuture;
    this.confSerializer = confSerializer;
    this.msgSenderFuture = msgSenderFuture;
    this.remoteAccessHandlerFuture = remoteAccessHandlerFuture;
    this.remoteAccessSenderFuture = remoteAccessSenderFuture;
    this.migrationExecutorFuture = migrationExecutorFuture;
    this.metricCollectorFuture = metricCollectorFuture;
    this.chkpManagerSlaveFuture = chkpManagerSlaveFuture;
  }

  @Override
  public void onNext(final Message<ETMsg> msg) {

    final ETMsg etMsg = SingleMessageExtractor.extract(msg);
    switch (etMsg.getType()) {
    case TableAccessMsg:
      onTableAccessMsg(etMsg.getTableAccessMsg());
      break;

    case TableControlMsg:
      onTableControlMsg(AvroUtils.fromBytes(etMsg.getInnerMsg().array(), TableControlMsg.class));
      break;

    case TableChkpMsg:
      onTableChkpMsg(AvroUtils.fromBytes(etMsg.getInnerMsg().array(), TableChkpMsg.class));
      break;

    case MigrationMsg:
      onMigrationMsg(AvroUtils.fromBytes(etMsg.getInnerMsg().array(), MigrationMsg.class));
      break;

    case MetricMsg:
      onMetricMsg(AvroUtils.fromBytes(etMsg.getInnerMsg().array(), MetricMsg.class));
      break;

    case TaskletMsg:
      onTaskletMsg(AvroUtils.fromBytes(etMsg.getInnerMsg().array(), TaskletMsg.class));
      break;

    default:
      throw new RuntimeException("Unexpected message: " + msg);
    }
  }

  private void onMigrationMsg(final MigrationMsg msg) {
    migrationMsgExecutor.submit(() -> migrationExecutorFuture.get().onNext(msg));
  }

  private void onTableAccessMsg(final TableAccessMsg msg) {
    tableAccessMsgExecutor.submit(() -> {
      final Long opId = msg.getOperationId();
      switch (msg.getType()) {
      case TableAccessReqMsg:
        remoteAccessHandlerFuture.get().onTableAccessReqMsg(opId, msg.getTableAccessReqMsg());
        break;

      case TableAccessResMsg:
        remoteAccessSenderFuture.get().onTableAccessResMsg(opId, msg.getTableAccessResMsg());
        break;

      default:
        throw new RuntimeException("Unexpected message: " + msg);
      }
      return;
    });
  }

  private void onTableControlMsg(final TableControlMsg msg) {
    tableCtrMsgExecutor.submit(() -> {
      final Long opId = msg.getOperationId();
      switch (msg.getType()) {
      case TableInitMsg:
        onTableInitMsg(opId, msg.getTableInitMsg());
        break;

      case TableLoadMsg:
        onTableLoadMsg(opId, msg.getTableLoadMsg());
        break;

      case TableDropMsg:
        onTableDropMsg(opId, msg.getTableDropMsg());
        break;

      case OwnershipUpdateMsg:
        onOwnershipUpdateMsg(msg.getOwnershipUpdateMsg());
        break;

      case OwnershipSyncMsg:
        onOwnershipSyncMsg(opId, msg.getOwnershipSyncMsg());
        break;

      default:
        throw new RuntimeException("Unexpected message: " + msg);
      }
      return;
    });
  }

  private void onTableInitMsg(final long opId, final TableInitMsg msg) {
    try {
      final Configuration tableConf = confSerializer.fromString(msg.getTableConf());
      final List<String> blockOwners = msg.getBlockOwners();

      final String tableId = tablesFuture.get().initTable(tableConf, blockOwners);

      LOG.log(Level.INFO, "Table {0} has been initialized. opId: {1}", new Object[]{tableId, opId});

      msgSenderFuture.get().sendTableInitAckMsg(opId, tableId);

    } catch (final IOException e) {
      throw new RuntimeException("IOException while initializing a table", e);
    } catch (final InjectionException e) {
      throw new RuntimeException("Table configuration is incomplete to initialize a table", e);
    } catch (final NetworkException e) {
      throw new RuntimeException(e);
    }
  }

  private void onTableLoadMsg(final long opId, final TableLoadMsg msg) {
    try {
      final List<String> serializedHdfsSplitInfos = msg.getFileSplits();
      final String tableId = msg.getTableId();
      final BulkDataLoader bulkDataLoader = tablesFuture.get().getTableComponents(tableId).getBulkDataLoader();

      dataLoadExecutor.submit(() -> {
        try {
          bulkDataLoader.load(tableId, serializedHdfsSplitInfos);
        } catch (IOException | KeyGenerationException | TableNotExistException e) {
          throw new RuntimeException("Exception while loading data", e);
        }

        LOG.log(Level.INFO, "Bulk-loading for Table {0} has been done. opId: {1}", new Object[]{tableId, opId});
        try {
          msgSenderFuture.get().sendTableLoadAckMsg(opId, tableId);
        } catch (NetworkException e) {
          throw new RuntimeException(e);
        }
      });

    } catch (TableNotExistException e) {
      throw new RuntimeException(e);
    }
  }

  private void onTableDropMsg(final long opId, final TableDropMsg msg) {
    // remove a table after flushing out all operations for the table in sender and handler
    final String tableId = msg.getTableId();

    try {
      final TableComponents tableComponents = tablesFuture.get().getTableComponents(tableId);

      // op processing is impossible without table metadata and ownership cache
      remoteAccessSenderFuture.get().waitOpsTobeFlushed(tableId);
      remoteAccessHandlerFuture.get().waitOpsTobeFlushed(tableId);

      tableComponents.getOwnershipCache().completeAllOngoingSync();
      tablesFuture.get().remove(tableId);

    } catch (TableNotExistException e) {
      LOG.log(Level.WARNING, String.format("Table %s does not exist", tableId), e);
      // send a response message despite there's no table to drop
    }

    try {
      msgSenderFuture.get().sendTableDropAckMsg(opId, tableId);
    } catch (NetworkException e) {
      throw new RuntimeException(e);
    }
  }

  private void onOwnershipUpdateMsg(final OwnershipUpdateMsg msg) {
    try {
      final OwnershipCache ownershipCache = tablesFuture.get().getTableComponents(msg.getTableId())
          .getOwnershipCache();
      ownershipCache.update(msg.getBlockId(), msg.getOldOwnerId(), msg.getNewOwnerId());
    } catch (final TableNotExistException e) {
      // ignore. It may happen when dropping table, because unsubscription is done after drop is completed.
    }
  }

  private void onOwnershipSyncMsg(final long opId, final OwnershipSyncMsg msg) {
    try {
      final OwnershipCache ownershipCache = tablesFuture.get().getTableComponents(msg.getTableId()).getOwnershipCache();
      ownershipCache.syncUnassociation(opId, msg.getDeletedExecutorId());
    } catch (final TableNotExistException e) {
      LOG.log(Level.WARNING, String.format("Table %s does not exist", msg.getTableId()), e);
      // send a response message directly when there's no table to sync
      try {
        msgSenderFuture.get().sendOwnershipSyncAckMsg(opId, msg.getTableId(), msg.getDeletedExecutorId());
      } catch (NetworkException e1) {
        throw new RuntimeException(e1);
      }
    }
  }

  private void onTableChkpMsg(final TableChkpMsg msg) {
    chkpMsgExecutor.submit(() -> {
      switch (msg.getType()) {
      case ChkpStartMsg:
        try {
          final ChkpStartMsg chkpStartMsg = msg.getChkpStartMsg();
          chkpManagerSlaveFuture.get().checkpoint(msg.getChkpId(),
              chkpStartMsg.getTableId(), chkpStartMsg.getSamplingRatio());
        } catch (IOException | TableNotExistException e) {
          throw new RuntimeException(e);
        }
        break;
      case ChkpLoadMsg:
        final ChkpLoadMsg chkpLoadMsg = msg.getChkpLoadMsg();
        try {
          chkpManagerSlaveFuture.get().loadChkp(msg.getChkpId(), chkpLoadMsg.getTableId(),
              chkpLoadMsg.getBlockOwners(), chkpLoadMsg.getCommitted(), chkpLoadMsg.getBlockIds());
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
        break;
      default:
        throw new RuntimeException("Unexpected msg type");
      }
      return;
    });
  }

  private void onMetricMsg(final MetricMsg msg) {
    metricMsgExecutor.submit(() -> {
      if (msg.getType().equals(MetricMsgType.MetricControlMsg)) {
        final MetricControlMsg controlMsg = msg.getMetricControlMsg();

        if (controlMsg.getType().equals(MetricControlType.Start)) {
          final long metricSendingPeriodMs;
          final Codec metricCodec;

          try {
            final Configuration metricConf = confSerializer.fromString(controlMsg.getSerializedMetricConf());
            final Injector injector = Tang.Factory.getTang().newInjector(metricConf);

            metricSendingPeriodMs = injector.getNamedInstance(MetricFlushPeriodMs.class);
            metricCodec = injector.getNamedInstance(CustomMetricCodec.class);
          } catch (IOException | InjectionException e) {
            throw new RuntimeException("Exception while processing a given serialized metric conf", e);
          }

          metricCollectorFuture.get().start(metricSendingPeriodMs, metricCodec);

        } else { // MetricControlType.Stop
          metricCollectorFuture.get().stop();
        }
      } else {
        throw new RuntimeException("Unexpected msg type");
      }
    });
  }

  private void onTaskletMsg(final TaskletMsg msg) {
    taskletMsgExecutor.submit(() -> {
      switch (msg.getType()) {
      case TaskletCustomMsg:
        taskletRuntimeFuture.get().onTaskletMsg(msg.getTaskletId(), msg.getTaskletCustomMsg().array());
        break;

      case TaskletControlMsg:
        final TaskletControlMsg controlMsg = msg.getTaskletControlMsg();
        switch (controlMsg.getType()) {
        case Start:
          try {
            final Configuration taskletConf = confSerializer.fromString(controlMsg.getTaskConf());
            taskletRuntimeFuture.get().startTasklet(msg.getTaskletId(), taskletConf);
          } catch (InjectionException | IOException e) {
            throw new RuntimeException(e);
          }
          break;
        case Stop:
          taskletRuntimeFuture.get().stopTasklet(msg.getTaskletId());
          break;
        case Ready:
          taskUnitSchedulerFuture.get().onTaskUnitReady(msg.getTaskletId());
          break;
        default:
          throw new RuntimeException("Unexpected control msg type");
        }
        break;

      default:
        throw new RuntimeException("Unexpected msg type");
      }
      return;
    });
  }
}
