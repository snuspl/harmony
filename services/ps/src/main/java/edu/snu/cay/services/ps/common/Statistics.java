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
package edu.snu.cay.services.ps.common;

/**
 * Class calculating statistics such as average and sum.
 */
public final class Statistics {
  private long sum = 0;
  private int count = 0;

  private Statistics() {
  }

  public void put(final long value) {
    sum += value;
    ++count;
  }

  public void reset() {
    sum = 0;
    count = 0;
  }

  public double avg() {
    return sum() / count;
  }

  public double sum() {
    return sum / 1e9D;
  }

  public double count() {
    return count;
  }

  public static Statistics newInstance() {
    return new Statistics();
  }

  public static Statistics[] newInstances(final int count) {
    final Statistics[] instances = new Statistics[count];
    for (int i = 0; i < count; ++i) {
      instances[i] = new Statistics();
    }
    return instances;
  }
}