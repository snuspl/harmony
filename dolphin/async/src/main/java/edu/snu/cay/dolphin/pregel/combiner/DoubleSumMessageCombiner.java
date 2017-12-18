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
package edu.snu.cay.dolphin.pregel.combiner;

import javax.inject.Inject;

/**
 * A combiner that sums double-valued messages.
 */
public final class DoubleSumMessageCombiner implements MessageCombiner<Long, Double> {

  @Inject
  private DoubleSumMessageCombiner() {

  }

  @Override
  public Double combine(final Long vertexId,
                        final Double originalMessage, final Double messageToCombine) {
    return originalMessage + messageToCombine;
  }
}
