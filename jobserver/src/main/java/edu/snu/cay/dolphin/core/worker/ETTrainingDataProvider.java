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

import com.google.common.collect.Iterators;
import edu.snu.cay.dolphin.DolphinParameters;
import edu.snu.cay.services.et.evaluator.api.Block;
import edu.snu.cay.services.et.evaluator.api.Table;
import edu.snu.cay.services.et.evaluator.api.TableAccessor;
import edu.snu.cay.services.et.evaluator.api.Tablet;
import edu.snu.cay.services.et.exceptions.TableNotExistException;
import org.apache.reef.annotations.audience.TaskSide;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Provides the training data to process in mini-batches, taking a block for each mini-batch.
 * @param <V> type of the training data
 */
@TaskSide
public final class ETTrainingDataProvider<K, V> implements TrainingDataProvider<K, V> {
  private static final Logger LOG = Logger.getLogger(ETTrainingDataProvider.class.getName());

  private volatile Iterator<Block<K, V, Object>> blockIterator = Iterators.emptyIterator();

  private final TableAccessor tableAccessor;
  private final String inputTableId;

  @Inject
  private ETTrainingDataProvider(@Parameter(DolphinParameters.InputTableId.class) final String inputTableId,
                                 final TableAccessor tableAccessor) throws TableNotExistException {
    this.tableAccessor = tableAccessor;
    this.inputTableId = inputTableId;
  }

  @Override
  public void prepareDataForEpoch() {
    final Table<K, V, Object> trainingDataTable;
    try {
      trainingDataTable = tableAccessor.getTable(inputTableId);
    } catch (TableNotExistException e) {
      throw new RuntimeException(e);
    }

    final Tablet<K, V, Object> tablet = trainingDataTable.getLocalTablet();

    LOG.log(Level.INFO, "Number of blocks: {0}, data items: {1}",
        new Object[]{tablet.getNumBlocks(), tablet.getNumDataItems()});

    blockIterator = tablet.getBlockIterator();
  }

  @Override
  public Collection<Map.Entry<K, V>> getNextBatchData() {
    if (blockIterator.hasNext()) {
      final Map<K, V> batchData = blockIterator.next().getAll();
      final List<Map.Entry<K, V>> entryList = new ArrayList<>(batchData.entrySet());

      Collections.shuffle(entryList); // shuffle to avoid bias

      LOG.log(Level.INFO, "Size of training data for next mini-batch: {0}", batchData.size());
      return entryList;
    }

    LOG.log(Level.INFO, "no more training data for current epoch");
    return Collections.emptyList();
  }

  @Override
  public Collection<Map.Entry<K, V>> getEpochData() {
    final Table<K, V, Object> trainingDataTable;
    try {
      trainingDataTable = tableAccessor.getTable(inputTableId);
    } catch (TableNotExistException e) {
      throw new RuntimeException(e);
    }

    return trainingDataTable.getLocalTablet().getDataMap().entrySet();
  }
  
  @Override
  public int getNumBatchesPerEpoch() {
    final Table<K, V, Object> trainingDataTable;
    try {
      trainingDataTable = tableAccessor.getTable(inputTableId);
    } catch (TableNotExistException e) {
      throw new RuntimeException(e);
    }

    return trainingDataTable.getLocalTablet().getNumBlocks();
  }
}
