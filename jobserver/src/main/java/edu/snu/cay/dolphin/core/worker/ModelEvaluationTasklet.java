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

import edu.snu.cay.dolphin.DolphinParameters;
import edu.snu.cay.services.et.evaluator.api.Tasklet;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.logging.Logger;

/**
 * REEF Task for evaluating a model trained by training tasks.
 */
public final class ModelEvaluationTasklet implements Tasklet {
  private static final Logger LOG = Logger.getLogger(ModelEvaluationTasklet.class.getName());

  private final boolean modelEvaluation;

  private final ModelEvaluator modelEvaluator;

  @Inject
  private ModelEvaluationTasklet(@Parameter(DolphinParameters.ModelEvaluation.class) final boolean modelEvaluation,
                                 final ModelEvaluator modelEvaluator) {
    this.modelEvaluation = modelEvaluation;
    this.modelEvaluator = modelEvaluator;
  }

  @Override
  public void run() throws Exception {
    if (modelEvaluation) {
      modelEvaluator.evaluateCurrModel();
    } else {
      // evaluate all check-points of trained models
      modelEvaluator.evaluatePrevModels();
    }
  }

  @Override
  public void close() {

  }
}
