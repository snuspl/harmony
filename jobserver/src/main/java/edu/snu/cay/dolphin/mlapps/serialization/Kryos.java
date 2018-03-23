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
package edu.snu.cay.dolphin.mlapps.serialization;

import com.esotericsoftware.kryo.Kryo;

/**
 * A class for providing thread-local Kryo instances.
 * A Kryo instance is not thread-safe.
 */
public final class Kryos {
  // configure kryo instance, customize settings
  // Setup ThreadLocal of Kryo instances
  private static final ThreadLocal<Kryo> KRYOS = ThreadLocal.withInitial(Kryo::new);

  // Utility classes should not have a public or default constructor.
  private Kryos() {

  }

  /**
   * @return a thread-local Kryo instance
   */
  public static Kryo get() {
    return KRYOS.get();
  }
}
