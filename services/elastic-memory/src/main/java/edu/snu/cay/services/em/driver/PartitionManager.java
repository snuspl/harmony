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
package edu.snu.cay.services.em.driver;

import org.apache.commons.lang.math.LongRange;
import org.apache.reef.annotations.audience.DriverSide;

import javax.inject.Inject;
import java.util.*;

/**
 * Manager class for keeping track of partitions registered by evaluators.
 * TODO #110: Currently does not check whether ranges are disjoint or not.
 * TODO #111: Currently does not try to merge contiguous ranges.
 */
@DriverSide
public final class PartitionManager {

  private final Map<String, Map<String, TreeSet<LongRange>>> mapIdKeyRange;
  private final Map<String, TreeSet<LongRange>> globalKeyRanges;

  private Comparator<LongRange> longRangeComparator = new Comparator<LongRange>() {
    @Override
    public int compare(final LongRange o1, final LongRange o2) {
      return (int) (o1.getMinimumLong() - o2.getMinimumLong());
    }
  };

  @Inject
  private PartitionManager() {
    this.mapIdKeyRange = new HashMap<>();
    this.globalKeyRanges = new HashMap<>();
  }

  public void registerPartition(final String evalId,
                                final String key, final long unitStartId, final long unitEndId) {
    registerPartition(evalId, key, new LongRange(unitStartId, unitEndId));
  }

  public synchronized boolean registerPartition(final String evalId, final String key, final LongRange idRange) {
    // 1. Add a new partition into globalKeyRanges
    TreeSet<LongRange> rangeSet = globalKeyRanges.get(key);
    if (rangeSet != null) {
      if (rangeSet.contains(idRange)) {
        return false;
      }
    } else {
      rangeSet = new TreeSet<>(longRangeComparator);
      assert (globalKeyRanges.put(key, rangeSet) == null);
    }

    assert (rangeSet.add(idRange));

    if (!mapIdKeyRange.containsKey(evalId)) {
      mapIdKeyRange.put(evalId, new HashMap<String, TreeSet<LongRange>>());
    }

    final Map<String, TreeSet<LongRange>> mapKeyRange = mapIdKeyRange.get(evalId);
    if (!mapKeyRange.containsKey(key)) {
      mapKeyRange.put(key, new TreeSet<>(longRangeComparator));
    }

    return mapKeyRange.get(key).add(idRange);
  }

  public synchronized Set<LongRange> getRangeSet(final String evalId, final String key) {
    if (!mapIdKeyRange.containsKey(evalId)) {
      return new TreeSet<>();
    }

    final Map<String, TreeSet<LongRange>> mapKeyRange = mapIdKeyRange.get(evalId);
    if (!mapKeyRange.containsKey(key)) {
      return new TreeSet<>();
    }

    return new TreeSet<>(mapKeyRange.get(key));
  }

  public synchronized boolean remove(final String evalId, final String key, final LongRange longRange) {
    if (!mapIdKeyRange.containsKey(evalId)) {
      return false;
    }

    final Map<String, TreeSet<LongRange>> mapKeyRange = mapIdKeyRange.get(evalId);
    if (!mapKeyRange.containsKey(key)) {
      return false;
    }

    return mapKeyRange.get(key).remove(longRange);
  }
}
