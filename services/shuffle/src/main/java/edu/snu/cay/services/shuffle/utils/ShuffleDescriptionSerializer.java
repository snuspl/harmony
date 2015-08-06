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
package edu.snu.cay.services.shuffle.utils;

import edu.snu.cay.services.shuffle.description.ShuffleDescription;
import edu.snu.cay.services.shuffle.params.ShuffleParameters;
import edu.snu.cay.services.shuffle.evaluator.Shuffle;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.util.Optional;

import javax.inject.Inject;

/**
 * This serializes ShuffleDescriptions to Tang Configurations along with certain type of Shuffle.
 */
public final class ShuffleDescriptionSerializer {

  @Inject
  private ShuffleDescriptionSerializer() {
  }

  /**
   * Return serialized Configuration with certain Shuffle type.
   *
   * @param shuffleClass a type of Shuffle
   * @param shuffleDescription a shuffle description to serialize
   * @return serialized configuration
   */
  public Configuration serialize(
      final Class<? extends Shuffle> shuffleClass, final ShuffleDescription shuffleDescription) {
    final JavaConfigurationBuilder confBuilder = Tang.Factory.getTang().newConfigurationBuilder();
    confBuilder.bindImplementation(Shuffle.class, shuffleClass);
    confBuilder.bindNamedParameter(ShuffleParameters.ShuffleName.class, shuffleDescription.getShuffleName());
    confBuilder.bindNamedParameter(
        ShuffleParameters.ShuffleKeyCodecClassName.class, shuffleDescription.getKeyCodecClass().getName());
    confBuilder.bindNamedParameter(
        ShuffleParameters.ShuffleValueCodecClassName.class, shuffleDescription.getValueCodecClass().getName());
    confBuilder.bindNamedParameter(
        ShuffleParameters.ShuffleStrategyClassName.class, shuffleDescription.getShuffleStrategyClass().getName());

    for (final String senderId : shuffleDescription.getSenderIdList()) {
      confBuilder.bindSetEntry(ShuffleParameters.ShuffleSenderIdSet.class, senderId);
    }

    for (final String receiverId : shuffleDescription.getReceiverIdList()) {
      confBuilder.bindSetEntry(ShuffleParameters.ShuffleReceiverIdSet.class, receiverId);
    }
    return confBuilder.build();
  }

  /**
   * Return serialized Configuration with certain Shuffle type.
   * It returns Optional.empty if the shuffleDescription does not have
   * the endPointId as a sender or a receiver.
   *
   * @param shuffleClass a type of Shuffle
   * @param shuffleDescription a shuffle description to serialize
   * @param endPointId an end point identifier
   * @return serialized configuration
   */
  public Optional<Configuration> serialize(
      final Class<? extends Shuffle> shuffleClass,
      final ShuffleDescription shuffleDescription,
      final String endPointId) {
    if (!shuffleDescription.getSenderIdList().contains(endPointId) &&
        !shuffleDescription.getReceiverIdList().contains(endPointId)) {
      return Optional.empty();
    }

    return Optional.of(serialize(shuffleClass, shuffleDescription));
  }
}
