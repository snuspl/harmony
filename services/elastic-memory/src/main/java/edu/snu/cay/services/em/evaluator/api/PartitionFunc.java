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
package edu.snu.cay.services.em.evaluator.api;

import edu.snu.cay.services.em.evaluator.impl.RangePartitionFunc;
import org.apache.reef.tang.annotations.DefaultImplementation;

/**
 * A partition function that partitions data ids into corresponding partitions.
 */
@DefaultImplementation(RangePartitionFunc.class)
public interface PartitionFunc {

  /**
   * Return a partition id of data with {@code dataId}.
   * @param dataId a id of data
   * @return an id of partition that the data
   */
  long partition(long dataId);
}