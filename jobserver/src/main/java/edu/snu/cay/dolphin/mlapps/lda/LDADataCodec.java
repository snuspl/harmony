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

import javax.inject.Inject;
import java.io.*;
import java.util.List;

/**
 * A codec for (de-)serializing data used in LDA application.
 */
final class LDADataCodec implements Codec<Document>, StreamingCodec<Document> {

  @Inject
  private LDADataCodec() {
  }

  @Override
  public byte[] encode(final Document document) {
    final int numBytes = (document.getWords().size() + 1) * Integer.BYTES;

    try (ByteArrayOutputStream baos = new ByteArrayOutputStream(numBytes);
         DataOutputStream daos = new DataOutputStream(baos)) {
      encodeToStream(document, daos);
      return baos.toByteArray();
    } catch (final IOException e) {
      throw new RuntimeException(e.getCause());
    }
  }

  @Override
  public Document decode(final byte[] bytes) {
    try (DataInputStream dis = new DataInputStream(new ByteArrayInputStream(bytes))) {
      return decodeFromStream(dis);
    } catch (final IOException e) {
      throw new RuntimeException(e.getCause());
    }
  }

  @Override
  public void encodeToStream(final Document document, final DataOutputStream daos) {
    try {
      final List<Integer> words = document.getWords();
      daos.writeInt(words.size());
      for (final int word : words) {
        daos.writeInt(word);
      }
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Document decodeFromStream(final DataInputStream dais) {
    try {
      final int numWords = dais.readInt();
      final int[] words = new int[numWords];

      for (int wordIdx = 0; wordIdx < numWords; wordIdx++) {
        words[wordIdx] = dais.readInt();
      }

      return new Document(words);
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
  }
}
