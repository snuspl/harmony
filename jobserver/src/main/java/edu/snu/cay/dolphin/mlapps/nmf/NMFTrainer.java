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
package edu.snu.cay.dolphin.mlapps.nmf;

import com.google.common.collect.Sets;
import com.google.common.util.concurrent.AtomicDouble;
import edu.snu.cay.common.math.linalg.Vector;
import edu.snu.cay.common.math.linalg.VectorEntry;
import edu.snu.cay.common.math.linalg.VectorFactory;
import edu.snu.cay.common.param.Parameters;
import edu.snu.cay.dolphin.core.worker.ModelAccessor;
import edu.snu.cay.dolphin.core.worker.Trainer;
import edu.snu.cay.dolphin.core.worker.TrainingDataProvider;
import edu.snu.cay.dolphin.DolphinParameters;
import edu.snu.cay.services.et.evaluator.api.Table;
import edu.snu.cay.services.et.evaluator.api.TableAccessor;
import edu.snu.cay.services.et.exceptions.TableNotExistException;
import edu.snu.cay.utils.CatchableExecutors;
import edu.snu.cay.utils.ThreadUtils;
import edu.snu.cay.utils.Tuple3;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.*;
import java.util.concurrent.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/**
 * Trainer for non-negative matrix factorization via SGD.
 *
 * Assumes that indices in {@link NMFData} are one-based.
 */
final class NMFTrainer implements Trainer<Long, NMFData> {

  private static final Logger LOG = Logger.getLogger(NMFTrainer.class.getName());

  private final ModelAccessor<Integer, Vector, Vector> modelAccessor;
  private final VectorFactory vectorFactory;
  private final int rank;
  private float stepSize;
  private final float lambda;

  private final boolean printMatrices;

  /**
   * The step size drops by this rate.
   */
  private final float decayRate;

  /**
   * The step size drops after every {@code decayPeriod} epochs pass.
   */
  private final int decayPeriod;

  /**
   * Executes the Trainer threads.
   */
  private final ExecutorService executor;

  /**
   * Number of Trainer threads that train concurrently.
   */
  private final int numTrainerThreads;

  private final TrainingDataProvider<Long, NMFData> trainingDataProvider;

  private final Table<Long, Vector, Vector> localModelTable;

  @Inject
  private NMFTrainer(final ModelAccessor<Integer, Vector, Vector> modelAccessor,
                     final VectorFactory vectorFactory,
                     @Parameter(NMFParameters.Rank.class) final int rank,
                     @Parameter(DolphinParameters.StepSize.class) final float stepSize,
                     @Parameter(DolphinParameters.Lambda.class) final float lambda,
                     @Parameter(DolphinParameters.DecayRate.class) final float decayRate,
                     @Parameter(DolphinParameters.DecayPeriod.class) final int decayPeriod,
                     @Parameter(DolphinParameters.NumTotalMiniBatches.class) final int numTotalMiniBatches,
                     @Parameter(NMFParameters.PrintMatrices.class) final boolean printMatrices,
                     @Parameter(DolphinParameters.NumTrainerThreads.class) final int numTrainerThreads,
                     @Parameter(Parameters.HyperThreadEnabled.class) final boolean hyperThreadEnabled,
                     @Parameter(DolphinParameters.LocalModelTableId.class) final String localModelTableId,
                     final TableAccessor tableAccessor,
                     final TrainingDataProvider<Long, NMFData> trainingDataProvider) throws TableNotExistException {
    this.modelAccessor = modelAccessor;
    this.vectorFactory = vectorFactory;
    this.rank = rank;
    this.stepSize = stepSize;
    this.lambda = lambda;
    this.decayRate = decayRate;
    if (decayRate <= 0.0 || decayRate > 1.0) {
      throw new IllegalArgumentException("decay_rate must be larger than 0 and less than or equal to 1");
    }
    this.decayPeriod = decayPeriod;
    if (decayPeriod <= 0) {
      throw new IllegalArgumentException("decay_period must be a positive value");
    }
    this.printMatrices = printMatrices;
    this.trainingDataProvider = trainingDataProvider;
    this.localModelTable = tableAccessor.getTable(localModelTableId);

    // Use the half of the processors if hyper-thread is on, since using virtual cores do not help for float-point ops.
    this.numTrainerThreads = numTrainerThreads == Integer.parseInt(DolphinParameters.NumTrainerThreads.UNSET_VALUE) ?
        Runtime.getRuntime().availableProcessors() / (hyperThreadEnabled ? 2 : 1) :
        numTrainerThreads;
    this.executor = CatchableExecutors.newFixedThreadPool(this.numTrainerThreads);

    LOG.log(Level.INFO, "Number of Trainer threads = {0}", numTrainerThreads);
    LOG.log(Level.INFO, "Step size = {0}", stepSize);
    LOG.log(Level.INFO, "Number of total mini-batches in an epoch = {0}", numTotalMiniBatches);
  }

  @Override
  public void initGlobalSettings() {
  }

  private volatile Collection<Map.Entry<Long, NMFData>> miniBatchTrainingData;
  private volatile Pair<List<Long>, List<Integer>> miniBatchInputRowColumnKeys;

  private volatile NMFModel modelForMiniBatch;
  private volatile NMFLocalModel localModelForMiniBatch;

  // gradient vectors indexed by column indices each of which is gradient of a column of R matrix.
  private volatile Map<Integer, Vector> aggregatedGradients;

  @Override
  public void setMiniBatchData(final Collection<Map.Entry<Long, NMFData>> newMiniBatchTrainingData) {
    this.miniBatchTrainingData = newMiniBatchTrainingData;
    miniBatchInputRowColumnKeys = getInputColumnKeys(miniBatchTrainingData);
  }

  @Override
  public void pullModel() {
    // pull data when mini-batch is started
    modelForMiniBatch = pullModels(miniBatchInputRowColumnKeys.getRight());
  }

  @Override
  public void localCompute() {
    // initialize local LMatrix
    localModelForMiniBatch = getLocalModel(miniBatchInputRowColumnKeys.getLeft());
    miniBatchInputRowColumnKeys = null;

    final CountDownLatch latch = new CountDownLatch(numTrainerThreads);

    final BlockingQueue<Map.Entry<Long, NMFData>> instances = new ArrayBlockingQueue<>(miniBatchTrainingData.size());
    instances.addAll(miniBatchTrainingData);
    miniBatchTrainingData = null;

    // collect gradients computed in each thread
    final List<Future<Map<Integer, Vector>>> futures = new ArrayList<>(numTrainerThreads);
    try {
      // Threads drain multiple instances from shared queue, as many as nInstances / nThreads.
      final int drainSize = instances.size() / numTrainerThreads;

      for (int threadIdx = 0; threadIdx < numTrainerThreads; threadIdx++) {
        final Future<Map<Integer, Vector>> future = executor.submit(() -> {
          final List<Map.Entry<Long, NMFData>> drainedInstances = new ArrayList<>(drainSize);
          final Map<Integer, Vector> threadRGradient = new HashMap<>();

          int count = 0;
          while (true) {
            final int numDrained = instances.drainTo(drainedInstances, drainSize);
            if (numDrained == 0) {
              break;
            }

            final Map<Long, Vector> lMatrix = localModelForMiniBatch.getLMatrix();

            drainedInstances.forEach(instance -> updateGradient(instance,
                lMatrix.get(instance.getKey()), modelForMiniBatch, threadRGradient));
            drainedInstances.clear();
            count += numDrained;
          }
          latch.countDown();
          LOG.log(Level.INFO, "{0} has computed {1} instances",
              new Object[] {Thread.currentThread().getName(), count});
          return threadRGradient;
        });
        futures.add(future);
      }
      latch.await();
    } catch (final InterruptedException e) {
      LOG.log(Level.SEVERE, "Exception occurred.", e);
      throw new RuntimeException(e);
    }

    modelForMiniBatch = null;
    localModelForMiniBatch = null;

    final List<Map<Integer, Vector>> totalRGradients = ThreadUtils.retrieveResults(futures);
    aggregatedGradients = aggregateGradient(totalRGradients);
  }

  @Override
  public void pushUpdate() {
    modelAccessor.push(aggregatedGradients);
    aggregatedGradients.clear();
    aggregatedGradients = null;
  }

  @Override
  public void onEpochFinished(final int epochIdx) {
    if (decayRate != 1 && (epochIdx + 1) % decayPeriod == 0) {
      final float prevStepSize = stepSize;
      stepSize *= decayRate;
      LOG.log(Level.INFO, "{0} epochs have passed. Step size decays from {1} to {2}",
          new Object[]{decayPeriod, prevStepSize, stepSize});
    }
  }

  @Override
  public Map<CharSequence, Double> evaluateModel(final Collection<Map.Entry<Long, NMFData>> inputData,
                                                 final Collection<NMFData> testData,
                                                 final Table modelTable) {
    LOG.log(Level.INFO, "Pull model to compute loss value");
    final Pair<List<Long>, List<Integer>> inputRowColumnKeys = getInputColumnKeys(inputData);
    final NMFModel model = pullModelToEvaluate(inputRowColumnKeys.getRight(), modelTable);
    final NMFLocalModel localModel = getLocalModel(inputRowColumnKeys.getLeft());

    LOG.log(Level.INFO, "Start computing loss value");
    final Map<CharSequence, Double> map = new HashMap<>();
    map.put("loss", computeLoss(inputData, localModel, model));

    return map;
  }

  private NMFModel pullModelToEvaluate(final List<Integer> keys, final Table<Integer, Vector, Vector> modelTable) {
    final Map<Integer, Vector> rMatrix = new HashMap<>(keys.size());
    final List<Vector> vectors = modelAccessor.pull(keys, modelTable);
    for (int i = 0; i < keys.size(); ++i) {
      rMatrix.put(keys.get(i), vectors.get(i));
    }
    return new NMFModel(rMatrix);
  }

  private NMFLocalModel getLocalModel(final List<Long> keys) {
    try {
      final Map<Long, Vector> localModelMatrix = localModelTable.multiGetOrInit(keys, false).get();
      if (localModelMatrix.size() != keys.size()) {
        throw new RuntimeException();
      }

      return new NMFLocalModel(localModelMatrix);
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void cleanup() {
    // print generated matrices
    if (!printMatrices) {
      return;
    }
    // print L matrix
    final Collection<Map.Entry<Long, NMFData>> dataPairs = trainingDataProvider.getEpochData();

    final StringBuilder lsb = new StringBuilder();

    final Tuple3<List<Long>, List<Integer>, List<Integer>> inputRowColumnKeys = getInputRowColumnKeys(dataPairs);
    final NMFLocalModel localModel = getLocalModel(inputRowColumnKeys.getFirst());
    final Iterator<Long> inputKeyIterator = inputRowColumnKeys.getFirst().iterator();
    final Iterator<Integer> rowKeyIterator = inputRowColumnKeys.getSecond().iterator();

    while (inputKeyIterator.hasNext()) {
      final long inputKey = inputKeyIterator.next();
      final int rowKey = rowKeyIterator.next();

      lsb.append(String.format("L(%d, *):", rowKey));
      for (final VectorEntry valueEntry : localModel.getLMatrix().get(inputKey)) {
        lsb.append(' ');
        lsb.append(valueEntry.value());
      }
      lsb.append('\n');
    }

    LOG.log(Level.INFO, lsb.toString());

    // print transposed R matrix
    final NMFModel model = pullModels(inputRowColumnKeys.getThird());
    final StringBuilder rsb = new StringBuilder();
    for (final Map.Entry<Integer, Vector> entry : model.getRMatrix().entrySet()) {
      rsb.append(String.format("R(*, %d):", entry.getKey()));
      for (final VectorEntry valueEntry : entry.getValue()) {
        rsb.append(' ');
        rsb.append(valueEntry.value());
      }
      rsb.append('\n');
    }
    LOG.log(Level.INFO, rsb.toString());
  }

  /**
   * Pull up-to-date model parameters from server.
   * @param keys Column indices with which server stores the model parameters.
   */
  private NMFModel pullModels(final List<Integer> keys) {
    final Map<Integer, Vector> rMatrix = new HashMap<>(keys.size());
    final List<Vector> vectors = modelAccessor.pull(keys);
    for (int i = 0; i < keys.size(); ++i) {
      rMatrix.put(keys.get(i), vectors.get(i));
    }
    return new NMFModel(rMatrix);
  }

  /**
   * Processes one training data instance and update the intermediate model.
   * @param datumPair a pair of training data instance and its key
   * @param lVec L_{i, *} : i-th row of L matrix
   * @param model up-to-date NMFModel which is pulled from server
   * @param threadRGradient gradient matrix to update {@param model}
   */
  private void updateGradient(final Map.Entry<Long, NMFData> datumPair, final Vector lVec, final NMFModel model,
                              final Map<Integer, Vector> threadRGradient) {
    final Vector lGradSum;
    if (lambda != 0.0f) {
      // l2 regularization term. 2 * lambda * L_{i, *}
      lGradSum = lVec.scale(2.0f * lambda);
    } else {
      lGradSum = vectorFactory.createDenseZeros(rank);
    }

    for (final Pair<Integer, Float> column : datumPair.getValue().getColumns()) { // a pair of column index and value
      final int colIdx = column.getLeft();
      final Vector rVec = model.getRMatrix().get(colIdx); // R_{*, j} : j-th column of R
      final float error = lVec.dot(rVec) - column.getRight(); // e = L_{i, *} * R_{*, j} - D_{i, j}

      // compute gradients
      // lGrad = 2 * e * R_{*, j}'
      // rGrad = 2 * e * L_{i, *}'
      final Vector lGrad;
      final Vector rGrad;

      lGrad = rVec.scale(2.0f * error);
      rGrad = lVec.scale(2.0f * error);

      // aggregate L matrix gradients
      lGradSum.addi(lGrad);

      // accumulate R matrix's gradient at threadRGradient
      accumulateRMatrixGradient(colIdx, rGrad, model, threadRGradient);
    }

    // update L matrix
    localModelTable.updateNoReply(datumPair.getKey(), lGradSum);
  }

  /**
   * Aggregate the model computed by multiple threads, to get the gradients to push.
   * gradient[j] = sum(gradient_t[j]) where j is the column index of the gradient matrix.
   * @param totalRGradients list of threadRGradients computed by trainer threads
   * @return aggregated gradient matrix
   */
  private Map<Integer, Vector> aggregateGradient(final List<Map<Integer, Vector>> totalRGradients) {
    final Map<Integer, Vector> aggregated = new ConcurrentHashMap<>();

    final CountDownLatch latch = new CountDownLatch(numTrainerThreads);
    final int numItemsPerThread = totalRGradients.size() / numTrainerThreads;

    for (int threadIdx = 0; threadIdx < numTrainerThreads; threadIdx++) {
      final int startIdx = numItemsPerThread * threadIdx;
      final int endIdx = threadIdx == numTrainerThreads - 1 ? totalRGradients.size() : startIdx + numItemsPerThread;

      executor.submit(() -> {
        totalRGradients.subList(startIdx, endIdx).forEach(threadRGradient -> threadRGradient.forEach((k, v) -> {
          aggregated.compute(k, (integer, vector) -> {
            if (vector == null) {
              return v;
            } else {
              return vector.addi(v);
            }
          });
        }));
        latch.countDown();
      });
    }

    try {
      latch.await();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }

    return aggregated;
  }

  /**
   * Compute the loss value using the current models and given data instances.
   * May take long, so do not call frequently.
   * @param dataPairs The training data instances to evaluate training loss.
   * @return the loss value, computed by the sum of the errors.
   */
  private double computeLoss(final Collection<Map.Entry<Long, NMFData>> dataPairs,
                            final NMFLocalModel nmfLocalModel,
                            final NMFModel model) {
    final Map<Integer, Vector> rMatrix = model.getRMatrix();
    final Map<Long, Vector> lMatrix = nmfLocalModel.getLMatrix();

    final List<Map.Entry<Long, NMFData>> dataPairList = new ArrayList<>(dataPairs);

    final int numItemsPerThread = dataPairList.size() / numTrainerThreads;
    final int numRemainders = dataPairList.size() % numTrainerThreads;

    final AtomicDouble loss = new AtomicDouble(0);

    final CountDownLatch latch = new CountDownLatch(numTrainerThreads);

    for (int threadIdx = 0; threadIdx < numTrainerThreads; threadIdx++) {
      final int finalThreadIdx = threadIdx;
      executor.submit(() -> {
        final int startIdx = numItemsPerThread * finalThreadIdx;
        final int endIdx = startIdx + numItemsPerThread + (finalThreadIdx < numRemainders ? 1 : 0);

        for (final Map.Entry<Long, NMFData> dataPair : dataPairList.subList(startIdx, endIdx)) {
          final Vector lVec = lMatrix.get(dataPair.getKey()); // L_{i, *} : i-th row of L
          for (final Pair<Integer, Float> column : dataPair.getValue().getColumns()) {
            // a pair of column index and value
            final int colIdx = column.getLeft();
            final Vector rVec = rMatrix.get(colIdx); // R_{*, j} : j-th column of R
            final float error = lVec.dot(rVec) - column.getRight(); // e = L_{i, *} * R_{*, j} - D_{i, j}
            loss.addAndGet(error * error);
          }
        }
        latch.countDown();
      });
    }

    try {
      latch.await();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }

    return loss.get();
  }

  /**
   * Gets keys from given input dataset.
   * It has two groups of keys: row keys and column keys.
   * Row keys are for input data and local models.
   * Column keys are for server-side global models.
   * @param dataValues Dataset assigned to this worker
   * @return a pair of row keys and column keys
   */
  private Pair<List<Long>, List<Integer>> getInputRowKeys(
      final Collection<Map.Entry<Long, NMFData>> dataValues) {
    final ArrayList<Long> inputKeys = new ArrayList<>(dataValues.size());
    final ArrayList<Integer> rowKeys = new ArrayList<>(dataValues.size());

    // aggregate column indices
    for (final Map.Entry<Long, NMFData> datum : dataValues) {
      inputKeys.add(datum.getKey());
      rowKeys.add(datum.getValue().getRowIdx());
    }

    return Pair.of(inputKeys, rowKeys);
  }

  /**
   * Gets keys from given input dataset.
   * It has two groups of keys: row keys and column keys.
   * Row keys are for input data and local models.
   * Column keys are for server-side global models.
   * @param dataValues Dataset assigned to this worker
   * @return a pair of row keys and column keys
   */
  private Tuple3<List<Long>, List<Integer>, List<Integer>> getInputRowColumnKeys(
      final Collection<Map.Entry<Long, NMFData>> dataValues) {
    final ArrayList<Long> inputKeys = new ArrayList<>(dataValues.size());
    final ArrayList<Integer> rowKeys = new ArrayList<>(dataValues.size());
    final ArrayList<Integer> columnKeys = new ArrayList<>();

    final Set<Integer> columnKeySet = Sets.newTreeSet();
    // aggregate column indices
    for (final Map.Entry<Long, NMFData> datum : dataValues) {
      inputKeys.add(datum.getKey());
      rowKeys.add(datum.getValue().getRowIdx());

      columnKeySet.addAll(
          datum.getValue().getColumns()
              .stream()
              .distinct()
              .map(Pair::getLeft)
              .collect(Collectors.toList()));
    }
    columnKeys.ensureCapacity(columnKeySet.size());
    columnKeys.addAll(columnKeySet);

    return new Tuple3<>(inputKeys, rowKeys, columnKeys);
  }

  /**
   * Gets keys from given input dataset.
   * It has two groups of keys: row keys and column keys.
   * Row keys are for input data and local models.
   * Column keys are for server-side global models.
   * @param dataValues Dataset assigned to this worker
   * @return a pair of row keys and column keys
   */
  private Pair<List<Long>, List<Integer>> getInputColumnKeys(
      final Collection<Map.Entry<Long, NMFData>> dataValues) {
    final ArrayList<Long> inputKeys = new ArrayList<>(dataValues.size());
    final ArrayList<Integer> columnKeys = new ArrayList<>();

    final Set<Integer> columnKeySet = Sets.newTreeSet();
    // aggregate column indices
    for (final Map.Entry<Long, NMFData> datum : dataValues) {
      inputKeys.add(datum.getKey());

      columnKeySet.addAll(
          datum.getValue().getColumns()
              .stream()
              .distinct()
              .map(Pair::getLeft)
              .collect(Collectors.toList()));
    }
    columnKeys.ensureCapacity(columnKeySet.size());
    columnKeys.addAll(columnKeySet);

    return Pair.of(inputKeys, columnKeys);
  }

  /**
   * Accumulates a new gradient into the R Matrix's gradient.
   * @param colIdx index of the column that the gradient is associated with
   * @param newGrad new gradient vector to accumulate
   * @param model up-to-date NMFModel which is pulled from server
   * @param threadRGradient gradient matrix to update {@param rMatrix}
   */
  private void accumulateRMatrixGradient(final int colIdx, final Vector newGrad, final NMFModel model,
                                         final Map<Integer, Vector> threadRGradient) {
    final Vector grad = threadRGradient.get(colIdx);
    if (grad == null) {
      // l2 regularization term. 2 * lambda * R_{*, j}
      if (lambda != 0.0D) {
        newGrad.axpy(2.0f * lambda, model.getRMatrix().get(colIdx));
      }
      threadRGradient.put(colIdx, newGrad);
    } else {
      grad.addi(newGrad);
    }
  }
}
