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
package edu.snu.cay.dolphin.examples.ml.algorithms.regression;

import edu.snu.cay.dolphin.core.ParseException;
import edu.snu.cay.dolphin.examples.ml.data.LinearModel;
import edu.snu.cay.dolphin.core.DataParser;
import edu.snu.cay.dolphin.core.UserComputeTask;
import edu.snu.cay.dolphin.examples.ml.data.Row;
import edu.snu.cay.dolphin.examples.ml.data.LinearRegSummary;
import edu.snu.cay.dolphin.examples.ml.loss.Loss;
import edu.snu.cay.dolphin.examples.ml.parameters.StepSize;
import edu.snu.cay.dolphin.examples.ml.regularization.Regularization;
import edu.snu.cay.dolphin.groupcomm.interfaces.DataBroadcastReceiver;
import edu.snu.cay.dolphin.groupcomm.interfaces.DataReduceSender;
import edu.snu.cay.services.em.evaluator.api.DataIdFactory;
import edu.snu.cay.services.em.evaluator.api.MemoryStore;
import edu.snu.cay.services.em.exceptions.IdGenerationException;
import org.apache.mahout.math.Vector;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.List;
import java.util.Map;

public class LinearRegCmpTask extends UserComputeTask
    implements DataReduceSender<LinearRegSummary>, DataBroadcastReceiver<LinearModel> {

  /**
   * Key used in Elastic Memory to put/get the data.
   */
  private static final String KEY_ROWS = "rows";

  private double stepSize;
  private final Loss loss;
  private final Regularization regularization;
  private DataParser<List<Row>> dataParser;
  private MemoryStore memoryStore;
  private final DataIdFactory<Long> dataIdFactory;
  private LinearModel model;
  private double lossSum = 0;

  @Inject
  public LinearRegCmpTask(@Parameter(StepSize.class) final double stepSize,
                          final Loss loss,
                          final Regularization regularization,
                          final DataParser<List<Row>> dataParser,
                          final MemoryStore memoryStore,
                          final DataIdFactory<Long> dataIdFactory) {
    this.stepSize = stepSize;
    this.loss = loss;
    this.regularization = regularization;
    this.dataParser = dataParser;
    this.memoryStore = memoryStore;
    this.dataIdFactory = dataIdFactory;
  }

  @Override
  public void initialize() throws ParseException, IdGenerationException {
    final List<Row> rows = dataParser.get();
    final List<Long> ids = dataIdFactory.getIds(rows.size());
    memoryStore.getElasticStore().putList(KEY_ROWS, ids, rows);
  }

  @Override
  public final void run(final int iteration) {

    // measure loss
    lossSum = 0;
    final Map<?, Row> rows = memoryStore.getElasticStore().getAll(KEY_ROWS);
    for (final Row row : rows.values()) {
      final double output = row.getOutput();
      final double predict = model.predict(row.getFeature());
      lossSum += loss.loss(predict, output);
    }

    // optimize
    for (final Row row : rows.values()) {
      final double output = row.getOutput();
      final Vector input = row.getFeature();
      final Vector gradient = loss.gradient(input, model.predict(input), output).plus(regularization.gradient(model));
      model.setParameters(model.getParameters().minus(gradient.times(stepSize)));
    }
  }

  @Override
  public final void receiveBroadcastData(final int iteration, final LinearModel modelData) {
    this.model = modelData;
  }

  @Override
  public LinearRegSummary sendReduceData(final int iteration) {
    return new LinearRegSummary(this.model, 1, this.lossSum);
  }
}
