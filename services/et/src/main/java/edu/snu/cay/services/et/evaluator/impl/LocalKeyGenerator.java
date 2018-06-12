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

import edu.snu.cay.services.et.exceptions.KeyGenerationException;
import org.apache.commons.lang3.tuple.Pair;

import javax.inject.Inject;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A class that generates keys for local blocks. It works only for ordered tables whose key type is {@link Long}.
 * Note that the result of {@link #getBlockToKeys(int)} can partially be wrong,
 * because blocks can move to other executors after return.
 */
final class LocalKeyGenerator {
  private final OrderingBasedBlockPartitioner orderingBasedBlockPartitioner;
  private final OwnershipCache ownershipCache;

  private int blockIdIndexer = 0;

  private final Map<Integer, AtomicLong> blockKeyCounters = new ConcurrentHashMap<>();

  @Inject
  private LocalKeyGenerator(final OwnershipCache ownershipCache,
                            final OrderingBasedBlockPartitioner orderingBasedBlockPartitioner) {
    this.ownershipCache = ownershipCache;
    this.orderingBasedBlockPartitioner = orderingBasedBlockPartitioner;
  }

  /**
   * Retrieves keys as many as {@code numKeys} that belong to local blocks.
   * It distributes keys to blocks as even as possible.
   * Note that this method is stateless, so calling multiple times gives the same result
   * if the entry of local blocks is not changed.
   * @param numKeys the number of keys
   * @return a map between block id and a list of keys that belong to this block
   * @throws KeyGenerationException when the number of keys in one block has exceeded its limit
   */
  synchronized Map<Integer, List<Long>> getBlockToKeys(final int numKeys) throws KeyGenerationException {
    // obtain blockToKeySpace info
    final Map<Integer, Pair<Long, Long>> blockToKeySpace = new HashMap<>();

    // assume that ownership status never change
    final List<Integer> localBlockIds = ownershipCache.getCurrentLocalBlockIds();
    localBlockIds.forEach(blockId ->
        blockToKeySpace.put(blockId, orderingBasedBlockPartitioner.getKeySpace(blockId)));

    final int numBlocks = blockToKeySpace.size();

    // actual number of keys per block is 'numKeysPerBlock' or 'numKeysPerBlock + 1'. (+1 is for handling remainders)
    final int numKeysPerBlock = numKeys / numBlocks;
    int numRemainingKeys = numKeys % numBlocks; // remainder after dividing equal amount of keys to all blocks

    final Map<Integer, List<Long>> blockIdToKeyListMap = new HashMap<>();

    for (int i = 0; i < localBlockIds.size(); i++, blockIdIndexer++) {
      final int blockIdx = blockIdIndexer % localBlockIds.size();
      final int blockId = localBlockIds.get(blockIdx);

      final List<Long> keys = new ArrayList<>(numKeysPerBlock);
      blockIdToKeyListMap.put(blockId, keys);

      final Pair<Long, Long> keySpace = blockToKeySpace.get(blockId);
      final long maxKey = keySpace.getRight();
      final long minKey = keySpace.getLeft();

      final int numKeysToAllocate;
      if (numRemainingKeys > 0) {
        numRemainingKeys--;
        numKeysToAllocate = numKeysPerBlock + 1;
      } else {
        numKeysToAllocate = numKeysPerBlock;
      }

      // next allocation will start from this block
      if (numKeysToAllocate == 0) {
        break;
      }

      blockKeyCounters.putIfAbsent(blockId, new AtomicLong());

      final long numUsedKeys = blockKeyCounters.get(blockId).getAndAdd(numKeysToAllocate);

      // though we may delegate an overflow to other blocks that have space, let's just throw exception
      if (maxKey - minKey + 1 - numUsedKeys < numKeysToAllocate) {
        throw new KeyGenerationException("The number of keys in one block has exceeded its limit.");
      }

      final long startKey = minKey + numUsedKeys;
      // simply start from minKey
      for (long key = startKey; key < startKey + numKeysToAllocate; key++) {
        keys.add(key);
      }
    }

    return blockIdToKeyListMap;
  }
}
