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
package edu.snu.cay.services.et.common;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import edu.snu.cay.common.math.linalg.Vector;
import edu.snu.cay.common.math.linalg.VectorFactory;

import javax.inject.Inject;
import java.util.logging.Logger;

/**
 * Created by xyzi on 09/01/2018.
 */
public final class DenseVectorSerializer extends Serializer<Vector> {
  public static final Kryo KRYO = new Kryo();
  private static final Logger LOG = Logger.getLogger(DenseVectorSerializer.class.getName());

  private final VectorFactory vectorFactory;

  @Inject
  private DenseVectorSerializer(final VectorFactory vectorFactory) {
    this.vectorFactory = vectorFactory;
  }

  @Override
  public void write(final Kryo kryo, final Output output, final Vector vector) {
    if (!vector.isDense()) {
      LOG.warning("the given vector is not dense.");
    }

    output.writeInt(vector.length());
    for (int i = 0; i < vector.length(); i++) {
      output.writeFloat(vector.get(i));
    }
  }

  @Override
  public Vector read(final Kryo kryo, final Input input, final Class<Vector> type) {
    final int vecSize = input.readInt();
    final Vector vector = vectorFactory.createDenseZeros(vecSize);
    for (int i = 0; i < vecSize; i++) {
      vector.set(i, input.readFloat());
    }
    return vector;
  }
}
