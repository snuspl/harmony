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
package edu.snu.cay.utils;

/**
 * A class for providing thread-local Kryo instances.
 * A Kryo instance is not thread-safe.
 */
public final class Buffers {
  private static final int BUF_SIZE = 128 * 1024 * 1024;

  // configure kryo instance, customize settings
  // Setup ThreadLocal of Kryo instances
  private static final ThreadLocal<byte[]> BUFFERS = ThreadLocal.withInitial(() -> new byte[BUF_SIZE]);

  // Utility classes should not have a public or default constructor.
  private Buffers() {

  }

  /**
   * @return a thread-local buffer
   */
  public static byte[] get() {
    return BUFFERS.get();
  }
}
