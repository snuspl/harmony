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
package edu.snu.cay.services.ps.driver.impl;

import org.apache.reef.annotations.audience.Private;

import java.util.List;
import java.util.Map;

/**
 * This class maintains the EM's routing table for Dynamic Partitioned Parameter Server.
 * Worker's push/pull requests are routed to the server that has the requested partition in its MemoryStore.
 */
@Private
public final class EMRoutingTable {
  private final Map<Integer, List<Integer>> storeIdToBlockIds;
  private final Map<Integer, String> storeIdToEndpointId;
  private final int numTotalBlocks;

  public EMRoutingTable(final Map<Integer, List<Integer>> storeIdToBlockIds,
                        final Map<Integer, String> storeIdToEndpointId,
                        final int numTotalBlocks) {
    this.storeIdToBlockIds = storeIdToBlockIds;
    this.storeIdToEndpointId = storeIdToEndpointId;
    this.numTotalBlocks = numTotalBlocks;
  }

  /**
   * @return The mapping between block ids and memory store ids.
   */
  public Map<Integer, List<Integer>> getStoreIdToBlockIds() {
    return storeIdToBlockIds;
  }

  public Map<Integer, String> getStoreIdToEndpointId() {
    return storeIdToEndpointId;
  }

  /**
   * @return The number of blocks across all MemoryStores.
   */
  public int getNumTotalBlocks() {
    return numTotalBlocks;
  }
}