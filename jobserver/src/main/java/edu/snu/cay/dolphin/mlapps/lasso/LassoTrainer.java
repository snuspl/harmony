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
package edu.snu.cay.dolphin.mlapps.lasso;

import com.google.common.util.concurrent.AtomicDouble;
import edu.snu.cay.common.math.linalg.Matrix;
import edu.snu.cay.common.math.linalg.MatrixFactory;
import edu.snu.cay.common.math.linalg.VectorFactory;
import edu.snu.cay.common.math.linalg.Vector;
import edu.snu.cay.common.param.Parameters;
import edu.snu.cay.dolphin.DolphinParameters;
import edu.snu.cay.dolphin.DolphinParameters.*;
import edu.snu.cay.dolphin.core.worker.ModelAccessor;
import edu.snu.cay.dolphin.core.worker.Trainer;
import edu.snu.cay.utils.CatchableExecutors;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * {@link Trainer} class for the LassoREEF application.
 * Based on lasso regression via stochastic coordinate descent, proposed in
 * S. Shalev-Shwartz and A. Tewari, Stochastic Methods for l1-regularized Loss Minimization, 2011.
 *
 * For each mini-batch, the trainer pulls the whole model from the server,
 * and then update all the model values.
 * The trainer computes and pushes the optimal model value for all the dimensions to
 * minimize the objective function - square loss with l1 regularization.
 */
final class LassoTrainer implements Trainer<Long, LassoData> {
  private static final Logger LOG = Logger.getLogger(LassoTrainer.class.getName());

  /**
   * Period(epochs) to print model parameters to check whether the training is working or not.
   */
  private static final int PRINT_MODEL_PERIOD = 50;

  /**
   * Threshold for a number to be regarded as zero.
   */
  private static final double ZERO_THRESHOLD = 1e-9;

  private final int numFeatures;
  private final float lambda;
  private float stepSize;
  private final double decayRate;
  private final int decayPeriod;

  private Vector oldModel;
  private Vector newModel;

  private final VectorFactory vectorFactory;
  private final MatrixFactory matrixFactory;

  private final ModelAccessor<Integer, Vector, Vector> modelAccessor;

  /**
   * A list from 0 to {@code numPartitions} that will be used during {@link #pullModels()} and {@link #pushGradients()}.
   */
  private List<Integer> modelPartitionIndices;

  /**
   * Number of model partitions.
   */
  private final int numPartitions;

  /**
   * Number of features of each model partition.
   */
  private final int numFeaturesPerPartition;

  /**
   * Executes the Trainer threads.
   */
  private final ExecutorService executor;

  /**
   * Number of Trainer threads that train concurrently.
   */
  private final int numTrainerThreads;

  @Inject
  private LassoTrainer(final ModelAccessor<Integer, Vector, Vector> modelAccessor,
                       @Parameter(Lambda.class) final float lambda,
                       @Parameter(NumFeatures.class) final int numFeatures,
                       @Parameter(StepSize.class) final float stepSize,
                       @Parameter(DecayRate.class) final double decayRate,
                       @Parameter(DecayPeriod.class) final int decayPeriod,
                       @Parameter(NumFeaturesPerPartition.class) final int numFeaturesPerPartition,
                       @Parameter(NumTrainerThreads.class) final int numTrainerThreads,
                       @Parameter(Parameters.HyperThreadEnabled.class) final boolean hyperThreadEnabled,
                       final VectorFactory vectorFactory,
                       final MatrixFactory matrixFactory) {
    this.modelAccessor = modelAccessor;
    this.numFeatures = numFeatures;
    this.lambda = lambda;
    this.stepSize = stepSize;
    this.decayRate = decayRate;
    if (decayRate <= 0.0 || decayRate > 1.0) {
      throw new IllegalArgumentException("decay_rate must be larger than 0 and less than or equal to 1");
    }
    this.decayPeriod = decayPeriod;
    if (decayPeriod <= 0) {
      throw new IllegalArgumentException("decay_period must be a positive value");
    }

    // Use the half of the processors if hyper-thread is on, since using virtual cores do not help for float-point ops.
    this.numTrainerThreads = numTrainerThreads == Integer.parseInt(DolphinParameters.NumTrainerThreads.UNSET_VALUE) ?
        Runtime.getRuntime().availableProcessors() / (hyperThreadEnabled ? 2 : 1) :
        numTrainerThreads;
    this.executor = CatchableExecutors.newFixedThreadPool(this.numTrainerThreads);

    this.vectorFactory = vectorFactory;
    this.matrixFactory = matrixFactory;
    this.oldModel = vectorFactory.createDenseZeros(numFeatures);
    if (numFeatures % numFeaturesPerPartition != 0) {
      throw new IllegalArgumentException("Uneven model partitions");
    }
    this.numFeaturesPerPartition = numFeaturesPerPartition;
    this.numPartitions = numFeatures / numFeaturesPerPartition;
    this.modelPartitionIndices = new ArrayList<>(numPartitions);
    for (int partitionIdx = 0; partitionIdx < numPartitions; partitionIdx++) {
      modelPartitionIndices.add(partitionIdx);
    }
  }

  @Override
  public void initGlobalSettings() {
  }

  private volatile Collection<Map.Entry<Long, LassoData>> miniBatchTrainingData;

  @Override
  public void setMiniBatchData(final Collection<Map.Entry<Long, LassoData>> newMiniBatchTrainingData) {
    this.miniBatchTrainingData = newMiniBatchTrainingData;
  }

  @Override
  public void pullModel() {
    pullModels();
  }

  /**
   * Compute the optimal value, dot(x_i, y - Sigma_{j != i} x_j * model(j)) / dot(x_i, x_i) for each dimension
   * (in cyclic).
   * When computing the optimal value, precalculate sigma_{all j} x_j * model(j) and calculate
   * Sigma_{j != i} x_j * model(j) fast by just subtracting x_i * model(i)
   */
  @Override
  public void localCompute() {
    final int numInstancesToProcess = miniBatchTrainingData.size();

    // After get feature vectors from each instances, make it concatenate them into matrix for the faster calculation.
    // Pre-calculate sigma_{all j} x_j * model(j) and assign the value into 'preCalculate' vector.
    final Pair<Matrix, Vector> featureMatrixAndValues = convertToFeaturesAndValues(miniBatchTrainingData);
    final Matrix featureMatrix = featureMatrixAndValues.getLeft();
    final Vector yValues = featureMatrixAndValues.getRight();

    final Vector preCalculate = featureMatrix.mmul(newModel);

    final int numFeaturesPerThread = numFeatures / numTrainerThreads;

    final CyclicBarrier barrier = new CyclicBarrier(numTrainerThreads);
    final CountDownLatch latch = new CountDownLatch(numTrainerThreads);

    for (int threadIdx = 0; threadIdx < numTrainerThreads; threadIdx++) {
      final int finalThreadIdx = threadIdx;

      executor.submit(() -> {
        final Vector columnVector = vectorFactory.createDenseZeros(numInstancesToProcess);
        final Vector preCalculateCopy = finalThreadIdx == 0 ? preCalculate : preCalculate.copy();
        barrier.await();

        final int startIdx = finalThreadIdx * numFeaturesPerThread;
        final int endIdx = finalThreadIdx == numTrainerThreads - 1 ? numFeatures : startIdx + numFeaturesPerThread;

        // For each dimension, compute the optimal value.
        for (int featureIdx = startIdx; featureIdx < endIdx; featureIdx++) {
          if (closeToZero(newModel.get(featureIdx))) {
            continue;
          }
          for (int rowIdx = 0; rowIdx < numInstancesToProcess; rowIdx++) {
            columnVector.set(rowIdx, featureMatrix.get(rowIdx, featureIdx));
          }
          final double columnNorm = columnVector.dot(columnVector);
          if (closeToZero(columnNorm)) {
            continue;
          }
          preCalculateCopy.subi(columnVector.scale(newModel.get(featureIdx)));
          newModel.set(featureIdx,
              (float) sthresh((columnVector.dot(yValues.sub(preCalculateCopy))) / columnNorm, lambda, columnNorm));
          preCalculateCopy.addi(columnVector.scale(newModel.get(featureIdx)));
        }
        latch.countDown();
        return null;
      });
    }

    try {
      latch.await();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void pushUpdate() {
    pushGradients();
  }

  @Override
  public void onEpochFinished(final int epochIdx) {
    if ((epochIdx + 1) % PRINT_MODEL_PERIOD == 0) {
      for (int i = 0; i < numFeatures; i++) {
        LOG.log(Level.INFO, "model : {0}", newModel.get(i));
      }
    }

    if (decayRate != 1 && (epochIdx + 1) % decayPeriod == 0) {
      final double prevStepSize = stepSize;
      stepSize *= decayRate;
      LOG.log(Level.INFO, "{0} epochs have passed. Step size decays from {1} to {2}",
          new Object[]{decayPeriod, prevStepSize, stepSize});
    }
  }

  @Override
  public Map<CharSequence, Double> evaluateModel(final Collection<Map.Entry<Long, LassoData>> inputData,
                                                 final Collection<LassoData> testData) {
    // Calculate the loss value.
    pullModels();

    final List<LassoData> inputDataList = new ArrayList<>(inputData.size());
    inputData.forEach(entry -> inputDataList.add(entry.getValue()));

    final double trainingLoss = computeLoss(inputDataList);
    final double testLoss = computeLoss(new ArrayList<>(testData));

    LOG.log(Level.INFO, "Training Loss: {0}, Test Loss: {1}", new Object[] {trainingLoss, testLoss});

    final Map<CharSequence, Double> map = new HashMap<>();
    map.put("training_loss", trainingLoss);
    map.put("test_loss", testLoss);

    return map;
  }

  /**
   * Convert the training data examples into a form for more efficient computation.
   * @param instances training data examples
   * @return the pair of feature matrix and vector composed of y values.
   */
  private Pair<Matrix, Vector> convertToFeaturesAndValues(final Collection<Map.Entry<Long, LassoData>> instances) {
    final List<Vector> features = new LinkedList<>();
    final Vector values = vectorFactory.createDenseZeros(instances.size());
    int instanceIdx = 0;
    for (final Map.Entry<Long, LassoData> instance : instances) {
      final LassoData lassoData = instance.getValue();
      features.add(lassoData.getFeature());
      values.set(instanceIdx++, lassoData.getValue());
    }
    return Pair.of(matrixFactory.horzcatVecSparse(features).transpose(), values);
  }

  @Override
  public void cleanup() {
  }

  /**
   * Pull up-to-date model parameters from server.
   */
  private void pullModels() {
    final List<Vector> partialModels = modelAccessor.pull(modelPartitionIndices);
    oldModel = vectorFactory.concatDense(partialModels);
    newModel = oldModel.copy();
  }

  /**
   * Push the gradients to parameter server.
   */
  private void pushGradients() {
    final Vector gradient = newModel.sub(oldModel);
    for (int partitionIndex = 0; partitionIndex < numPartitions; ++partitionIndex) {
      final int partitionStart = partitionIndex * numFeaturesPerPartition;
      final int partitionEnd = (partitionIndex + 1) * numFeaturesPerPartition;
      modelAccessor.push(partitionIndex, vectorFactory.createDenseZeros(numFeaturesPerPartition)
          .axpy(stepSize, gradient.slice(partitionStart, partitionEnd)));
    }
  }

  /**
   * Soft thresholding function, widely used with l1 regularization.
   */
  private static double sthresh(final double x, final double lambda, final double columnNorm) {
    if (Math.abs(x) <= lambda / columnNorm) {
      return 0;
    } else if (x >= 0) {
      return x - lambda / columnNorm;
    } else {
      return x + lambda / columnNorm;
    }
  }

  /**
   * Predict the y value for the feature value.
   */
  private double predict(final Vector feature) {
    return newModel.dot(feature);
  }

  /**
   * Compute the loss value for the data.
   * Only one input parameter is not null.
   */
  private double computeLoss(final List<LassoData> data) {
    final AtomicDouble squaredErrorSum = new AtomicDouble(0);

    final int numItemsPerThread = data.size() / numTrainerThreads;
    final CountDownLatch latch = new CountDownLatch(numTrainerThreads);

    for (int threadIdx = 0; threadIdx < numTrainerThreads; threadIdx++) {
      final int finalThreadIdx = threadIdx;
      executor.submit(() -> {
        final int startIdx = numItemsPerThread * finalThreadIdx;
        final int endIdx = finalThreadIdx == numTrainerThreads - 1 ? data.size() : startIdx + numItemsPerThread;

        for (final LassoData lassoData : data.subList(startIdx, endIdx)) {
          final Vector feature = lassoData.getFeature();
          final double value = lassoData.getValue();
          final double prediction = predict(feature);
          squaredErrorSum.addAndGet(Math.sqrt(value - prediction));
        }
        latch.countDown();
      });
    }
    try {
      latch.await();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
    return squaredErrorSum.get();
  }

  /**
   * @return {@code true} if the value is close to 0.
   */
  private boolean closeToZero(final double value) {
    return Math.abs(value) < ZERO_THRESHOLD;
  }
}
