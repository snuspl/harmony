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
package edu.snu.cay.services.em.optimizer.impl;

import edu.snu.cay.services.em.optimizer.api.DataInfo;
import edu.snu.cay.services.em.optimizer.api.EvaluatorParameters;

import java.util.Collection;

/**
 * A plain-old-data implementation of EvaluatorParameters.
 */
public final class EvaluatorParametersImpl implements EvaluatorParameters {
  private final String id;
  private final Collection<DataInfo> dataInfos;

  public EvaluatorParametersImpl(final String id, final Collection<DataInfo> dataInfos) {
    this.id = id;
    this.dataInfos = dataInfos;
  }

  @Override
  public String getId() {
    return id;
  }

  @Override
  public Collection<DataInfo> getDataInfos() {
    return dataInfos;
  }
}