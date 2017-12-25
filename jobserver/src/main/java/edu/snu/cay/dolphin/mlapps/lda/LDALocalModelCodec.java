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

import org.apache.reef.io.network.impl.StreamingCodec;
import org.apache.reef.io.serialization.Codec;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.io.*;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A codec for (de-)serializing local model used in LDA application.
 */
final class LDALocalModelCodec implements Codec<LDALocalModel>, StreamingCodec<LDALocalModel> {
  private final int numTopics;

  @Inject
  private LDALocalModelCodec(@Parameter(LDAParameters.NumTopics.class) final int numTopics) {
    this.numTopics = numTopics;
  }

  @Override
  public byte[] encode(final LDALocalModel ldaLocalModel) {
    final int numBytes = (ldaLocalModel.getWordTopicAssignments().length + 1
        + ldaLocalModel.getTopicCounts().size() * 2 + 1) * Integer.BYTES;

    try (ByteArrayOutputStream baos = new ByteArrayOutputStream(numBytes);
         DataOutputStream daos = new DataOutputStream(baos)) {
      encodeToStream(ldaLocalModel, daos);
      return baos.toByteArray();
    } catch (final IOException e) {
      throw new RuntimeException(e.getCause());
    }
  }

  @Override
  public LDALocalModel decode(final byte[] bytes) {
    try (DataInputStream dis = new DataInputStream(new ByteArrayInputStream(bytes))) {
      return decodeFromStream(dis);
    } catch (final IOException e) {
      throw new RuntimeException(e.getCause());
    }
  }

  @Override
  public void encodeToStream(final LDALocalModel ldaLocalModel, final DataOutputStream daos) {
    try {
      final int[] wordTopicAssignments = ldaLocalModel.getWordTopicAssignments();
      daos.writeInt(wordTopicAssignments.length);
      for (final int assignment : wordTopicAssignments) {
        daos.writeInt(assignment);
      }

      final Map<Integer, Integer> topicCounts = ldaLocalModel.getTopicCounts();
      daos.writeInt(topicCounts.size());
      topicCounts.forEach((topicIdx, topicCount) -> {
        try {
          daos.writeInt(topicIdx);
          daos.writeInt(topicCount);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      });
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public LDALocalModel decodeFromStream(final DataInputStream dais) {
    try {
      final int numWords = dais.readInt();
      final int[] assignments = new int[numWords];

      for (int wordIdx = 0; wordIdx < numWords; wordIdx++) {
        assignments[wordIdx] = dais.readInt();
      }

      final Map<Integer, Integer> topicCounts = new ConcurrentHashMap<>(numWords);

      final int numEntries = dais.readInt();
      for (int i = 0; i < numEntries; i++) {
        final int topicIdx = dais.readInt();
        final int topicCount = dais.readInt();
        topicCounts.put(topicIdx, topicCount);
      }

      return new LDALocalModel(assignments, topicCounts, numTopics);
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
  }
}
