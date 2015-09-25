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

import java.util.Collection;

/**
 * Encapsulates the status of a migration.
 */
final class MigrationInfo {
  /**
   * Represents the status of the migration.
   */
  public enum State {
    SENDING_DATA, DATA_SENT, UPDATING_RECEIVER, PARTITION_UPDATED, UPDATING_SENDER, RECEIVER_UPDATED
  }

  private final String senderId;
  private final String receiverId;
  private final String dataType;
  private final Collection<LongRange> ranges; // TODO #95: Ranges may not be able to set at the first time
  private State state = State.SENDING_DATA;

  /**
   * Creates a new MigrationInfo when move() is requested.
   * @param senderId Identifier of the sender.
   * @param receiverId Identifier of the receiver.
   * @param dataType Type of data
   * @param ranges Set of ranges to move
   */
  public MigrationInfo(final String senderId,
                       final String receiverId,
                       final String dataType,
                       final Collection<LongRange> ranges) {
    this.senderId = senderId;
    this.receiverId = receiverId;
    this.dataType = dataType;
    this.ranges = ranges;
  }

  /**
   * @return Sender's identifier of the migration.
   */
  public String getSenderId() {
    return senderId;
  }

  /**
   * @return Receiver's identifier of the migration.
   */
  public String getReceiverId() {
    return receiverId;
  }

  /**
   * @return Data type of the migration.
   */
  public String getDataType() {
    return dataType;
  }

  /**
   * @return Ranges of the migration.
   * Note that this field can be initially null and updated afterward.
   */
  public Collection<LongRange> getRanges() {
    return ranges;
  }

  /**
   * @return The status of the migration.
   */
  public State getState() {
    return state;
  }

  /**
   * @param state Update the status of the migration.
   */
  public void setState(final State state) {
    this.state = state;
  }
}
