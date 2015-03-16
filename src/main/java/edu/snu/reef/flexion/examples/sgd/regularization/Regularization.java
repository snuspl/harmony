/**
 * Copyright (C) 2014 Seoul National University
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
package edu.snu.reef.flexion.examples.sgd.regularization;

import edu.snu.reef.flexion.examples.sgd.data.Model;
import org.apache.mahout.math.Vector;
import org.apache.reef.tang.annotations.DefaultImplementation;

/**
 * Regularization factor to be used with the loss function
 */
@DefaultImplementation(L2Regularization.class)
public interface Regularization {

  /**
   * @param model model to perform computation on
   * @return value of regularization factor for a certain model
   */
  public double regularize(final Model model);

  /**
   * calculate the derivative of the regularization factor for a certain model,
   * to use with gradient descent
   *
   * @param model model to perform computation on
   * @return value of the derivative of the regularization factor for a certain model
   */
  public Vector derivative(final Model model);
}
