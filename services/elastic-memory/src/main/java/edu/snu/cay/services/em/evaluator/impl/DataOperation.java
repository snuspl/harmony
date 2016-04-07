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

import edu.snu.cay.services.em.avro.AvroLongRange;
import edu.snu.cay.services.em.avro.DataOpType;
import org.apache.commons.lang.math.LongRange;
import org.apache.reef.annotations.audience.Private;
import org.apache.reef.util.Optional;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * A class that represents a single data operation.
 * It maintains metadata and states of the operation during execution.
 */
@Private
public final class DataOperation<T> {

  /**
   * Metadata of the operation.
   */
  private final Optional<String> origEvalId;
  private final String operationId;
  private final DataOpType operationType;
  private final String dataType;
  private final List<LongRange> dataKeyRanges;
  private final Optional<SortedMap<Long, T>> dataKeyValueMap;

  /**
   * States of the operation.
   */
  private CountDownLatch subOpCountDownLatch = new CountDownLatch(0);

  // ranges that remote sub operations failed to execute due to wrong routing
  // it happens only when ownership of data key are updated, unknown to the original store
  private final List<AvroLongRange> failedRanges = new LinkedList<>();
  private final ConcurrentMap<Long, T> outputData = new ConcurrentHashMap<>();

  private static <T> Optional<SortedMap<Long, T>> unmodifiableMap(final Optional<SortedMap<Long, T>> map) {
    if (map.isPresent()) {
      return Optional.of(Collections.unmodifiableSortedMap(map.get()));
    } else {
      return map;
    }
  }

  /**
   * A constructor for an operation composed of multiple data key ranges.
   * @param origEvalId an Optional with the id of the original evaluator where the operation is generated.
   *                   It is empty when the operation is requested from the local client.
   * @param operationId an id of operation
   * @param operationType a type of operation
   * @param dataType a type of data
   * @param dataKeyRanges a list of data key ranges
   * @param dataKeyValueMap an Optional with the map of the data keys and data values.
   *                        It is empty when the operation is one of GET or REMOVE.
   */
  DataOperation(final Optional<String> origEvalId, final String operationId, final DataOpType operationType,
                final String dataType, final List<LongRange> dataKeyRanges,
                final Optional<SortedMap<Long, T>> dataKeyValueMap) {
    this.origEvalId = origEvalId;
    this.operationId = operationId;
    this.operationType = operationType;
    this.dataType = dataType;
    this.dataKeyRanges = dataKeyRanges;
    this.dataKeyValueMap = dataKeyValueMap;
  }

  /**
   * A constructor for an operation composed of a single data key range.
   * @param origEvalId an Optional with the id of the original evaluator where the operation is generated.
   *                   It is empty when the operation is requested from the local client.
   * @param operationId an id of operation
   * @param operationType a type of operation
   * @param dataType a type of data
   * @param dataKeyRange a range of data keys
   * @param dataKeyValueMap an Optional with the map of the data keys and data values.
   *                        It is empty when the operation is one of GET or REMOVE.
   */
  DataOperation(final Optional<String> origEvalId, final String operationId, final DataOpType operationType,
                final String dataType, final LongRange dataKeyRange,
                final Optional<SortedMap<Long, T>> dataKeyValueMap) {
    this.origEvalId = origEvalId;
    this.operationId = operationId;
    this.operationType = operationType;
    this.dataType = dataType;

    final List<LongRange> keyRanges = new ArrayList<>(1);
    keyRanges.add(dataKeyRange);
    this.dataKeyRanges = keyRanges;

    this.dataKeyValueMap = dataKeyValueMap;
  }

  /**
   * A constructor for an operation composed of a single data key.
   * @param origEvalId an Optional with the id of the original evaluator where the operation is generated.
   *                   It is empty when the operation is requested from the local client.
   * @param operationId an id of operation
   * @param operationType a type of operation
   * @param dataType a type of data
   * @param dataKey a key of data
   * @param dataValue an Optional with the value of data.
   *                  It is empty when the operation is one of GET or REMOVE.
   */
  DataOperation(final Optional<String> origEvalId, final String operationId, final DataOpType operationType,
                final String dataType, final long dataKey, final Optional<T> dataValue) {
    this.origEvalId = origEvalId;
    this.operationId = operationId;
    this.operationType = operationType;
    this.dataType = dataType;

    final List<LongRange> keyRanges = new ArrayList<>(1);
    keyRanges.add(new LongRange(dataKey));
    this.dataKeyRanges = keyRanges;

    final SortedMap<Long, T> keyValueMap;
    if (dataValue.isPresent()) {
      keyValueMap = new TreeMap<>();
      keyValueMap.put(dataKey, dataValue.get());
      this.dataKeyValueMap = Optional.of(keyValueMap);
    } else {
      this.dataKeyValueMap = Optional.empty();
    }
  }

  /**
   * @return true if the operation is requested from the local client
   */
  boolean isFromLocalClient() {
    return !origEvalId.isPresent();
  }

  /**
   * @return an Optional with the id of evaluator that initially requested the operation
   */
  Optional<String> getOrigEvalId() {
    return origEvalId;
  }

  /**
   * @return an operation id issued by its origin memory store
   */
  String getOperationId() {
    return operationId;
  }

  /**
   * @returns a type of the operation
   */
  DataOpType getOperationType() {
    return operationType;
  }

  /**
   * @returns a type of data
   */
  String getDataType() {
    return dataType;
  }

  /**
   * Returns a range of data keys.
   * The method assumes that callers never try to modify the returned object.
   * @return a range of data keys
   */
  List<LongRange> getDataKeyRanges() {
    return dataKeyRanges;
  }

  /**
   * Returns an Optional with the map of input data keys and its values for PUT operation.
   * The method assumes that callers never try to modify the returned object.
   * It returns an empty Optional for GET and REMOVE operations.
   * @return an Optional with the map of input keys and its values
   */
  Optional<SortedMap<Long, T>> getDataKeyValueMap() {
    return dataKeyValueMap;
  }

  /**
   * Set a counter number that {@link #waitOperation(long)} will wait until the count becomes zero.
   * Only {@link #commitResult(Map, List<AvroLongRange>)} method counts down the latch.
   * @param numSubOps a number of sub operations
   */
  void setNumSubOps(final int numSubOps) {
    if (numSubOps == 0) {
      return;
    }
    subOpCountDownLatch = new CountDownLatch(numSubOps);
  }

  /**
   * Starts waiting for completion of the operation within a bounded time.
   * @param timeout a maximum waiting time in the milliseconds
   */
  boolean waitOperation(final long timeout) throws InterruptedException {
    return subOpCountDownLatch.await(timeout, TimeUnit.MILLISECONDS);
  }

  /**
   * Commits results of sub operations, which compose the operation.
   * It counts down the latch, so it might trigger a return of {@link #waitOperation(long)} method.
   * @param output an output data of the sub operation
   * @param failedRangeList a list of failed key ranges of the sub operation
   */
  void commitResult(final Map<Long, T> output, final List<AvroLongRange> failedRangeList) {
    this.outputData.putAll(output);
    synchronized (failedRanges) {
      this.failedRanges.addAll(failedRangeList);
    }
    subOpCountDownLatch.countDown();
  }

  /**
   * Returns a list of key ranges that the sub operations failed to locate.
   */
  List<AvroLongRange> getFailedRanges() {
    synchronized (failedRanges) {
      return Collections.unmodifiableList(failedRanges);
    }
  }

  /**
   * Returns an aggregated output data of the operation.
   * It returns an empty map for PUT operation.
   * @return an empty map with the output data
   */
  Map<Long, T> getOutputData() {
    return Collections.unmodifiableMap(outputData);
  }
}