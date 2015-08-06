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
package edu.snu.cay.services.em.serialize;

import org.apache.reef.io.serialization.Codec;
import org.apache.reef.io.serialization.SerializableCodec;

import javax.inject.Inject;

public final class JavaSerializer implements Serializer {

  private final Codec defaultCodec;

  @Inject
  private JavaSerializer() {
    defaultCodec = new SerializableCodec();
  }

  @Override
  public Codec getCodec(final String name) {
    return defaultCodec;
  }
}
