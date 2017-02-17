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
package edu.snu.cay.dolphin.async.mlapps.lasso;

import edu.snu.cay.common.math.linalg.Vector;

/**
 * Abstraction for training data used in Lasso, consisting of feature vector and value (Double).
 */
final class LassoData {
  private final Vector feature;
  private final double value;

  LassoData(final Vector feature, final double value) {
    this.feature = feature;
    this.value = value;
  }

  Vector getFeature() {
    return feature;
  }

  double getValue() {
    return value;
  }
}