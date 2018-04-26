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
package edu.snu.cay.services.et.driver.impl;

import com.google.common.collect.Lists;
import edu.snu.cay.services.et.configuration.parameters.*;
import edu.snu.cay.services.et.driver.api.AllocatedTable;
import edu.snu.cay.services.et.evaluator.api.UpdateFunction;
import edu.snu.cay.services.et.evaluator.impl.ChkpManagerSlave;
import edu.snu.cay.services.et.common.util.concurrent.AggregateFuture;
import edu.snu.cay.services.et.common.util.concurrent.ListenableFuture;
import edu.snu.cay.services.et.common.util.concurrent.ResultFuture;
import edu.snu.cay.services.et.configuration.TableConfiguration;
import edu.snu.cay.services.et.driver.api.MessageSender;
import edu.snu.cay.services.et.exceptions.ChkpNotExistException;
import edu.snu.cay.services.et.exceptions.TableNotExistException;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.reef.io.network.impl.StreamingCodec;
import org.apache.reef.io.serialization.Codec;
import org.apache.reef.tang.InjectionFuture;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.tang.formats.ConfigurationSerializer;

import javax.inject.Inject;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A master-side checkpoint manager.
 * It performs table checkpoint, coordinating {@link ChkpManagerSlave}s in executors that have table blocks.
 * Checkpoint of a table is saved as files in local filesystem of those executors.
 * When the executors are being closed, they commit all locally saved checkpoints
 * to a place reachable by other executors (e.g., HDFS).
 * With a checkpoint, users can restore the table perfectly same with the original one.
 * Restoring a table from the checkpoint is done with two sets of executors,
 * depending on that blocks are committed or not.
 *  case 1. For blocks in a temporal stage, executors that have chkps in local filesystem do loading.
 *  case 2. For blocks in a commit stage, associators of the new table load their own blocks.
 * In both cases, when putting k-v pairs to the table, executors use table access mechanism.
 */
public final class ChkpManagerMaster {
  private static final Logger LOG = Logger.getLogger(ChkpManagerMaster.class.getName());

  private final InjectionFuture<MessageSender> msgSenderFuture;
  private final InjectionFuture<TableManager> tableManagerFuture;

  private final Map<String, AggregateFuture<Pair<String, List<Integer>>>> ongoingCheckpoint = new ConcurrentHashMap<>();
  private final Map<String, AggregateFuture<String>> ongoingLoad = new ConcurrentHashMap<>();

  /**
   * A map that maintains existing checkpoints.
   */
  private final Map<String, Checkpoint> checkpointMap = new ConcurrentHashMap<>();

  private final ConfigurationSerializer confSerializer;

  @Inject
  private ChkpManagerMaster(final InjectionFuture<MessageSender> msgSenderFuture,
                            final ConfigurationSerializer confSerializer,
                            final InjectionFuture<TableManager> tableManagerFuture) {
    this.msgSenderFuture = msgSenderFuture;
    this.tableManagerFuture = tableManagerFuture;
    this.confSerializer = confSerializer;
  }

  /**
   * A class that encapsulates the information of a checkpoint.
   * It knows the location of each block.
   */
  private final class Checkpoint {
    private final String checkpointId; // {tableId}-{timestamp}

    private final TableConfiguration tableConf; // need it when we restore a table from checkpoint

    // blocks saved in local FSes temporally
    private final Map<String, List<Integer>> executorToBlocksMap;

    // indicate whether blocks are moved to final location or not
    private final Set<Integer> blocksCommitted;

    Checkpoint(final String checkpointId,
               final TableConfiguration tableConf,
               final List<Pair<String, List<Integer>>> executorToBlocks) {
      this.checkpointId = checkpointId;
      this.tableConf = tableConf;

      this.executorToBlocksMap = new ConcurrentHashMap<>();
      executorToBlocks.forEach(pair -> executorToBlocksMap.put(pair.getLeft(), pair.getRight()));

      this.blocksCommitted = Collections.newSetFromMap(new ConcurrentHashMap<>());
    }

    String getCheckpointId() {
      return checkpointId;
    }

    TableConfiguration getTableConf() {
      return tableConf;
    }

    Map<String, List<Integer>> getTempBlocks() {
      return Collections.unmodifiableMap(executorToBlocksMap);
    }

    void committed(final String executorId) {
      final List<Integer> committedBlocks = executorToBlocksMap.remove(executorId);
      blocksCommitted.addAll(committedBlocks);
    }

    Set<Integer> getCommittedBlocks() {
      return Collections.unmodifiableSet(blocksCommitted);
    }
  }

  /**
   * Checkpoints a table in {@code executors}.
   * @param tableConf a table configuration
   * @param executors a set of executor ids
   * @return a checkpoint Id
   */
  ListenableFuture<String> checkpoint(final TableConfiguration tableConf, final Set<String> executors,
                                      final double samplingRatio) {
    final String checkpointId = tableConf.getId() + "-" + System.currentTimeMillis();
    LOG.log(Level.INFO, "Start checkpointing table {0} in executors: {1}. opId: {2}",
        new Object[]{tableConf.getId(), executors, checkpointId});

    final ResultFuture<String> resultFuture = new ResultFuture<>();

    final AggregateFuture<Pair<String, List<Integer>>> chkpFuture = new AggregateFuture<>(executors.size());
    ongoingCheckpoint.put(checkpointId, chkpFuture);

    // when checkpoint is done
    chkpFuture.addListener(blockSavedLocation -> {
      final Checkpoint checkpoint = new Checkpoint(checkpointId, tableConf, blockSavedLocation);
      checkpointMap.put(checkpoint.getCheckpointId(), checkpoint);
      ongoingCheckpoint.remove(checkpointId);
      resultFuture.onCompleted(checkpoint.getCheckpointId());

      LOG.log(Level.INFO, "Checkpoint done. tableId: {0}, chkpId: {1}",
          new Object[]{tableConf.getId(), checkpointId});
    });

    executors.forEach(executorId ->
        msgSenderFuture.get().sendChkpStartMsg(checkpointId, executorId, tableConf.getId(), samplingRatio));

    return resultFuture;
  }

  /**
   * Marks that a portion of table checkpoint started by {@link #checkpoint} has been done in an executor.
   * The checkpoint is completed when all executors respond.
   * @param checkpointId a checkpoint Id
   * @param executorId an executor Id
   * @param blocks blocks loaded by the executor
   */
  void chkpDone(final String checkpointId, final String executorId, final List<Integer> blocks) {
    LOG.log(Level.INFO, "chkpDone!");
    ongoingCheckpoint.get(checkpointId).onCompleted(Pair.of(executorId, blocks));
  }

  /**
   * Marks that an executor has committed its portion of a checkpoint.
   * It means that the location of this portion has changed.
   * @param checkpointId a checkpoint Id
   * @param executorId an executor Id
   */
  void chkpCommited(final String checkpointId, final String executorId) {
    final Checkpoint checkpoint = checkpointMap.get(checkpointId);
    checkpoint.committed(executorId);
  }

  /**
   * Gets a table configuration from the checkpoint.
   * @param checkpointId a checkpoint Id
   * @return a table configuration
   * @throws ChkpNotExistException when a checkpoint with {@code checkpointId} does not exist
   */
  TableConfiguration getTableConf(final String checkpointId) throws ChkpNotExistException {
    final Checkpoint chkp = checkpointMap.get(checkpointId);
    if (chkp == null) {
      throw new ChkpNotExistException(checkpointId);
    }

    return chkp.getTableConf();
  }

  private TableConfiguration getTableConfFromPath(final String checkpointPath) throws IOException {
    final org.apache.reef.tang.Configuration tableConf;
    try (FileSystem hdfs = FileSystem.get(new Configuration())) {
      tableConf = ChkpManagerSlave.readTableConf(confSerializer, hdfs, new Path(checkpointPath));
    }

    final Injector tableInjector = Tang.Factory.getTang().newInjector(tableConf);
    try {
      final String tableId = tableInjector.getNamedInstance(TableIdentifier.class);
      final StreamingCodec keyCodec = tableInjector.getNamedInstance(KeyCodec.class);
      final StreamingCodec valueCodec = tableInjector.getNamedInstance(ValueCodec.class);
      final Codec updateValueCodec = tableInjector.getNamedInstance(UpdateValueCodec.class);
      final UpdateFunction updateFunction = tableInjector.getInstance(UpdateFunction.class);
      final boolean isMutable = tableInjector.getNamedInstance(IsMutableTable.class);
      final boolean isOrdered = tableInjector.getNamedInstance(IsOrderedTable.class);
      final int chunkSize = tableInjector.getNamedInstance(ChunkSize.class);
      final int numTotalBlocks = tableInjector.getNamedInstance(NumTotalBlocks.class);

//      final Optional<String> inputPathOptional = tableInjector.getNamedInstance(Parameters.InputDir.class);
//      final DataParser dataParser = tableInjector.getInstance(DataParser.class);
//      final BulkDataLoader bulkDataLoader = tableInjector.getInstance(BulkDataLoader.class);

      return TableConfiguration.newBuilder()
          .setId(tableId)
          .setKeyCodecClass(keyCodec.getClass())
          .setValueCodecClass(valueCodec.getClass())
          .setUpdateValueCodecClass(updateValueCodec.getClass())
          .setUpdateFunctionClass(updateFunction.getClass())
          .setIsMutableTable(isMutable)
          .setIsOrderedTable(isOrdered)
          .setChunkSize(chunkSize)
          .setNumTotalBlocks(numTotalBlocks)
          .build();

    } catch (InjectionException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Gets an owner of committed blocks of a checkpoint.
   */
  private Map<String, Set<Integer>> getOwnerOfCommittedBlocks(final Set<Integer> blocksToLoad,
                                                              final TableConfiguration newTableConf) {

    final AllocatedTable table;
    try {
      table = tableManagerFuture.get().getAllocatedTable(newTableConf.getId());
    } catch (TableNotExistException e) {
      throw new RuntimeException(e);
    }

    final List<String> associators = new ArrayList<>(table.getAssociatedExecutorIds());

    final List<List<Integer>> partitionedBlocks = Lists.partition(new ArrayList<>(blocksToLoad), associators.size());

    final Map<String, Set<Integer>> partitionInfo = new HashMap<>();

    for (int i = 0; i < associators.size(); i++) {
      partitionInfo.put(associators.get(i), new HashSet<>(partitionedBlocks.get(i)));
    }

    return partitionInfo;
  }

  /**
   * Gets an owner of committed blocks of a checkpoint.
   */
  private Map<String, Set<Integer>> getOwnerOfCommittedBlocks(final Set<Integer> committedBlocks,
                                                              final String tableId) {
    final AllocatedTable table;
    try {
      table = tableManagerFuture.get().getAllocatedTable(tableId);
    } catch (TableNotExistException e) {
      throw new RuntimeException(e);
    }

    final List<String> emptyExecutors = new LinkedList<>();

    final Map<String, Set<Integer>> partitionInfo = table.getPartitionInfo();
    for (final Map.Entry<String, Set<Integer>> entry : partitionInfo.entrySet()) {
      final String executorId = entry.getKey();
      final Set<Integer> blocks = entry.getValue();

      blocks.retainAll(committedBlocks);
      if (blocks.isEmpty()) {
        emptyExecutors.add(executorId);
      }
    }

    partitionInfo.keySet().removeAll(emptyExecutors);

    return partitionInfo;
  }

  /**
   *
   * @param chkpPath
   * @return
   * @throws IOException
   */
  ListenableFuture<?> load(final TableConfiguration newTableConf, final String chkpPath) throws IOException {
    // read Checkpoint from path
    // assume the old and the new configurations are same except table Id and the number of blocks.
    final TableConfiguration oldTableConf = getTableConfFromPath(chkpPath);

    final ResultFuture<Void> resultFuture = new ResultFuture<>();

    final int oldNumTotalBlocks = oldTableConf.getNumTotalBlocks();
    final int newNumTotalBlocks = newTableConf.getNumTotalBlocks();

    final Set<Integer> blocksToLoad = new HashSet<>();
    for (int blockId = 0; blockId < oldNumTotalBlocks; blockId++) {
      blocksToLoad.add(blockId);
    }

    final Map<String, Set<Integer>> blocksInCommit;
    if (oldNumTotalBlocks == newNumTotalBlocks) {
      blocksInCommit = getOwnerOfCommittedBlocks(blocksToLoad, newTableConf.getId());
    } else {
      blocksInCommit = getOwnerOfCommittedBlocks(blocksToLoad, newTableConf);
    }

    LOG.log(Level.INFO, "ChkpPath: {0}, Executors to load committed block-checkpoints: {1}",
        new Object[]{chkpPath, blocksInCommit});

    final AggregateFuture<String> loadFuture =
        new AggregateFuture<>(blocksInCommit.size());
    ongoingLoad.put(chkpPath, loadFuture);

    loadFuture.addListener(o -> {
      ongoingLoad.remove(chkpPath);
      resultFuture.onCompleted(null);

      LOG.log(Level.INFO, "Load checkpoint done. ChkpPath: {0}", chkpPath);
    });

    // Let associators load their own blocks from a committed chkp.
    blocksInCommit.forEach((executorId, blockIds) -> {
      if (!blockIds.isEmpty()) {
        msgSenderFuture.get().sendChkpLoadMsg(chkpPath, executorId, newTableConf.getId(),
            new ArrayList<>(blockIds));
      }
    });

    return resultFuture;
  }

  /**
   * Loads a checkpoint into a new table.
   * Note that it does not create a table by itself.
   * @param checkpointId a checkpoint Id
   * @throws ChkpNotExistException when a checkpoint with {@code checkpointId} does not exist
   */
  ListenableFuture<?> load(final String checkpointId) throws ChkpNotExistException {
    LOG.log(Level.INFO, "Start loading checkpoint. chkpId: {0}", checkpointId);

    final Checkpoint chkp = checkpointMap.get(checkpointId);
    if (chkp == null) {
      throw new ChkpNotExistException(checkpointId);
    }

    final ResultFuture<Void> resultFuture = new ResultFuture<>();

    final Map<String, List<Integer>> blocksInTemp = chkp.getTempBlocks();
    final Map<String, Set<Integer>> blocksInCommit =
        getOwnerOfCommittedBlocks(chkp.getCommittedBlocks(), chkp.getTableConf().getId());

    LOG.log(Level.INFO, "ChkpId: {0}, Executors to load temporal block-checkpoints: {1}",
        new Object[]{checkpointId, blocksInTemp});
    LOG.log(Level.INFO, "ChkpId: {0}, Executors to load committed block-checkpoints: {1}",
        new Object[]{checkpointId, blocksInCommit});

    final AggregateFuture<String> loadFuture =
        new AggregateFuture<>(blocksInTemp.size() + blocksInCommit.size());
    ongoingLoad.put(checkpointId, loadFuture);

    loadFuture.addListener(o -> {
      ongoingLoad.remove(checkpointId);
      resultFuture.onCompleted(null);

      LOG.log(Level.INFO, "Load checkpoint done. chkpId: {0}", checkpointId);
    });

    // 1. Load chkps in temp stage
    // Executors that have a temporal chkp will put into a table.
    // Because they may not have this table,
    // we need to send some additional info (e.g., ownership info) to do remote table access.
    final List<String> ownershipStatus;
    try {
      ownershipStatus = tableManagerFuture.get().getAllocatedTable(chkp.getTableConf().getId()).getOwnershipStatus();

    } catch (TableNotExistException e) {
      throw new RuntimeException(e);
    }

    blocksInTemp.forEach((executorId, blockIds) ->
        msgSenderFuture.get().sendChkpLoadMsg(checkpointId, executorId, chkp.getTableConf().getId(),
            blockIds, false, ownershipStatus));

    // 2. Load chkps in commit stage.
    // Let associators load their own blocks from a committed chkp.
    blocksInCommit.forEach((executorId, blockIds) -> {
      if (!blockIds.isEmpty()) {
        msgSenderFuture.get().sendChkpLoadMsg(checkpointId, executorId, chkp.getTableConf().getId(),
            new ArrayList<>(blockIds), true, null);
      }
    });

    return resultFuture;
  }

  /**
   * Marks that loading a checkpoint started by {@link #load} has been done in an executor.
   * @param checkpointId a checkpoint Id
   * @param executorId an executor Id
   */
  void loadDone(final String checkpointId, final String executorId) {
    LOG.log(Level.INFO, "loadDone!");
    ongoingLoad.get(checkpointId).onCompleted(executorId);
  }
}
