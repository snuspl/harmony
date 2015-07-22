/**
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
package edu.snu.cay.services.shuffle.task;

import edu.snu.cay.services.shuffle.description.ShuffleGroupDescription;
import edu.snu.cay.services.shuffle.task.operator.ShuffleReceiver;
import edu.snu.cay.services.shuffle.task.operator.ShuffleSender;
import org.apache.reef.annotations.audience.TaskSide;

/**
 * Task side interface to communicate with corresponding ShuffleGroupManager in driver
 * to control the shuffle group. The users can obtain shuffle senders and receivers for
 * the specific shuffle.
 */
@TaskSide
public interface ShuffleGroup {

  /**
   * Return the ShuffleReceiver for the shuffle named shuffleName.
   *
   * It throws RuntimeException if the current task is not a receiver for the shuffle.
   *
   * @param shuffleName name of the shuffle
   * @param <K> key type
   * @param <V> value type
   * @return shuffle receiver
   */
  <K, V> ShuffleReceiver<K, V> getReceiver(String shuffleName);

  /**
   * Return the ShuffleSender for the shuffle named shuffleName.
   *
   * It throws RuntimeException if the current task is not a sender for the shuffle.
   *
   * @param shuffleName name of the shuffle
   * @param <K> key type
   * @param <V> value type
   * @return shuffle sender
   */
  <K, V> ShuffleSender<K, V> getSender(String shuffleName);

  /**
   * Return the shuffle group description with shuffle descriptions that
   * include the current task
   *
   * @return the shuffle group description
   */
  ShuffleGroupDescription getShuffleGroupDescription();
}
