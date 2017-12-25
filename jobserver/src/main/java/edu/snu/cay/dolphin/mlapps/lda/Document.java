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
package edu.snu.cay.dolphin.mlapps.lda;

import com.google.common.primitives.Ints;

import java.util.List;

/**
 * Representation of a document in a corpus.
 * This has words in the document.
 */
final class Document {
  private final List<Integer> words;

  /**
   * Creates a document with given words.
   * @param words Words that the document contains
   */
  Document(final int[] words) {
    this.words = Ints.asList(words);
  }

  int size() {
    return words.size();
  }

  int getWord(final int index) {
    return words.get(index);
  }

  List<Integer> getWords() {
    return words;
  }
}
