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
package edu.snu.cay.services.shuffle.strategy;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.List;

/**
 * Hash based key grouping shuffle strategy.
 */
public final class KeyShuffleStrategy<K> implements ShuffleStrategy<K> {

  @Inject
  private KeyShuffleStrategy() {
  }

  /**
   * Select one receiver based on hash function of the key.
   *
   * @param key key instance to select corresponding receivers
   * @param receiverIdList receiver id list of the shuffle
   * @return selected receiver id list
   */
  @Override
  public List<String> selectReceivers(final K key, final List<String> receiverIdList) {
    final int index = (key.hashCode() & Integer.MAX_VALUE) % receiverIdList.size();
    final List<String> list =  new ArrayList<>();
    list.add(receiverIdList.get(index));
    return list;
  }
}