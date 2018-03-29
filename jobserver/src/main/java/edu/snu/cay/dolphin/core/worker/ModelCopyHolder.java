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
package edu.snu.cay.dolphin.core.worker;

import edu.snu.cay.utils.Copyable;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;

/**
 * ModelHolder that provides model assigned to Trainer threads locally.
 */
@ThreadSafe
public final class ModelCopyHolder<M extends Copyable<M>> implements ModelHolder<M> {
  private M model;

  @Inject
  private ModelCopyHolder() {
  }

  @Override
  public void resetModel(final M newModel) {
    this.model = newModel;
  }

  @Override
  public M getModel() {
    return model.copyOf();
  }
}
