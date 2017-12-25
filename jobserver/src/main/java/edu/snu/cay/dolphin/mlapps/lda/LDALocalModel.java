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
package edu.snu.cay.dolphin.mlapps.lda;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/**
 * Representation of worker-side local model for a document.
 * This has topic assignments for words in the document
 * as well as a document-topic assignment table of the document maintained by servers.
 */
final class LDALocalModel {

  /**
   * The list index is the word id and the value is the topic id.
   */
  private final int[] wordTopicAssignments;

  /**
   * The number of words assigned to each topic.
   * The map's key is the topic id and the value is the count of corresponding words.
   */
  private final Map<Integer, Integer> topicCounts;

  /**
   * The number of total topics.
   */
  private final int numTopics;

  /**
   * Creates a local model with the given number of words and topics.
   * The initial topics for the words are assigned randomly.
   * @param numWords the number of words that the document contains
   * @param numTopics Number of topics determined by user parameter
   *                  ({@link LDAParameters.NumTopics})
   */
  LDALocalModel(final int numWords, final int numTopics) {
    this.wordTopicAssignments = new int[numWords];

    this.topicCounts = new HashMap<>(numWords); // the number of assigned topics is bound to the document's words
    this.numTopics = numTopics;

    initialize();
  }

  /**
   * Creates a local model with intermediate topic assignments that have been learned.
   * @param wordTopicAssignments Topic Index that a word is assigned to
   * @param topicCounts Number of words that are assigned to a topic
   * @param numTopics Number of topics determined by user parameter
   *                  ({@link LDAParameters.NumTopics})
   */
  LDALocalModel(final int[] wordTopicAssignments, final Map<Integer, Integer> topicCounts, final int numTopics) {
    this.wordTopicAssignments = wordTopicAssignments;
    this.topicCounts = topicCounts;
    this.numTopics = numTopics;
  }

  /**
   * Assigns a random topic to each word in the doc.
   */
  private void initialize() {
    final Random rand = new Random();
    for (int i = 0; i < wordTopicAssignments.length; i++) {
      final int topic = rand.nextInt(numTopics);
      wordTopicAssignments[i] = topic;
      topicCounts.compute(topic, (key, oldValue) -> {
        if (oldValue == null) {
          return 1;
        } else {
          return oldValue + 1;
        }
      });
    }
  }

  /**
   * @param index Index of the word
   * @return Topic Index that the word is assigned to
   */
  int getAssignment(final int index) {
    return wordTopicAssignments[index];
  }

  int[] getWordTopicAssignments() {
    return wordTopicAssignments;
  }

  void removeWordAtIndex(final int index) {
    final int oldTopic = wordTopicAssignments[index];
    topicCounts.compute(oldTopic, (key, oldValue) -> {
      if (oldValue == null || oldValue == 0) {
        return 0; // it happens due to inconsistency by migration of worker-side model
      } else {
        return oldValue - 1;
      }
    });
  }

  void addWordAtIndex(final int index, final int newTopic) {
    wordTopicAssignments[index] = newTopic;
    topicCounts.compute(newTopic, (key, oldValue) -> {
      if (oldValue == null) {
        return 1;
      } else {
        return oldValue + 1;
      }
    });
  }

  void setTopicCount(final int topicIdx, final int value) {
    topicCounts.put(topicIdx, value);
  }

  /**
   * @param topicIndex Index of a topic
   * @return Number of words that are assigned to the topic
   */
  int getTopicCount(final int topicIndex) {
    return topicCounts.getOrDefault(topicIndex, 0);
  }

  /**
   * @return the count of topics assigned to each word in this document.
   */
  Map<Integer, Integer> getTopicCounts() {
    return topicCounts;
  }
}
