/*
 * Copyright (C) 2018 Seoul National University
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

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import javax.inject.Inject;

/**
 * Created by xyzi on 04/04/2018.
 */
public final class SparseArraySerializer extends Serializer<int[]> {

  @Inject
  private SparseArraySerializer() {

  }

  @Override
  public void write(final Kryo kryo, final Output output, final int[] array) {
    output.writeInt(array.length);
    output.writeInt(array[array.length - 1]);
    for (int i = 0; i < array.length - 1; ++i) {
      if (array[i] != 0) {
        output.writeInt(i);
        output.writeInt(array[i]);
      }
    }
  }

  @Override
  public int[] read(final Kryo kryo, final Input input, final Class<int[]> type) {
    final int arrayLength = input.readInt();
    final int numNonZeros = input.readInt();

    final int[] result = new int[arrayLength];
    for (int i = 0; i < numNonZeros; ++i) {
      result[input.readInt()] = input.readInt();
    }
    result[arrayLength - 1] = numNonZeros;
    return result;
  }
}
