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
package edu.snu.cay.dolphin.mlapps.lda;

import edu.snu.cay.common.param.Parameters;
import edu.snu.cay.dolphin.DolphinParameters;
import edu.snu.cay.dolphin.core.worker.ModelHolder;
import edu.snu.cay.dolphin.mlapps.lda.LDAParameters.*;
import edu.snu.cay.services.et.evaluator.api.Table;
import edu.snu.cay.services.et.evaluator.api.TableAccessor;
import edu.snu.cay.services.et.exceptions.TableNotExistException;
import edu.snu.cay.utils.CatchableExecutors;
import edu.snu.cay.utils.ThreadUtils;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.*;
import java.util.concurrent.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Sample a batch of documents using the word-topic assignment count matrix from parameter server and a local
 * document-topic assignment count vector. It follows SparseLDA algorithm in L. Yao, D. Mimno, and A. McCallum.
 * Efficient methods for topic model inference on streaming document collections. In Proceedings of the
 * 15th ACM SIGKDD international conference on Knowledge discovery and data mining, pages 937–946. ACM, 2009.
 */
final class SparseLDASampler {
  private static final String MSG_GET_MODEL_FAILED = "Model is not set via ModelHolder.resetModel()";
  private static final Logger LOG = Logger.getLogger(SparseLDASampler.class.getName());

  private final double alpha;
  private final double beta;
  private final int numTopics;
  private final int numVocabs;

  /**
   * Executes the Trainer threads.
   */
  private final ExecutorService executor;

  /**
   * Number of Trainer threads that train concurrently.
   */
  private final int numTrainerThreads;

  private final TableAccessor tableAccessor;
  private final String localModelTableId;

  /**
   * Allows to access and update the latest model.
   */
  private final ModelHolder<LDAModel> modelHolder;

  @Inject
  private SparseLDASampler(@Parameter(Alpha.class) final double alpha,
                           @Parameter(Beta.class) final double beta,
                           @Parameter(NumTopics.class) final int numTopics,
                           @Parameter(NumVocabs.class) final int numVocabs,
                           @Parameter(DolphinParameters.NumTrainerThreads.class) final int numTrainerThreads,
                           @Parameter(Parameters.HyperThreadEnabled.class) final boolean hyperThreadEnabled,
                           @Parameter(DolphinParameters.LocalModelTableId.class) final String localModelTableId,
                           final TableAccessor tableAccessor,
                           final ModelHolder<LDAModel> modelHolder) throws TableNotExistException {
    this.alpha = alpha;
    this.beta = beta;
    this.numTopics = numTopics;
    this.numVocabs = numVocabs;
    this.tableAccessor = tableAccessor;
    this.localModelTableId = localModelTableId;
    this.modelHolder = modelHolder;

    // Use the half of the processors if hyper-thread is on, since using virtual cores do not help for float-point ops.
    this.numTrainerThreads = numTrainerThreads == Integer.parseInt(DolphinParameters.NumTrainerThreads.UNSET_VALUE) ?
        Runtime.getRuntime().availableProcessors() / (hyperThreadEnabled ? 2 : 1) :
        numTrainerThreads;
    this.executor = CatchableExecutors.newFixedThreadPool(this.numTrainerThreads);
    LOG.log(Level.INFO, "Number of Trainer threads = {0}", this.numTrainerThreads);
  }

  List<TopicChanges> sample(final Collection<Map.Entry<Long, Document>> documentPairs) {
    final CountDownLatch latch = new CountDownLatch(numTrainerThreads);
    final BlockingQueue<Map.Entry<Long, Document>> instances = new ArrayBlockingQueue<>(documentPairs.size());
    instances.addAll(documentPairs);

    final List<Future<TopicChanges>> futures = new ArrayList<>(numTrainerThreads);
    try {
      // Threads drain multiple instances from shared queue, as many as nInstances / nThreads.
      final int drainSize = documentPairs.size() / numTrainerThreads;

      for (int threadIdx = 0; threadIdx < numTrainerThreads; threadIdx++) {
        final Future<TopicChanges> future = executor.submit(() -> {
          final List<Map.Entry<Long, Document>> drainedInstances = new ArrayList<>(drainSize);
          final LDAModel model = modelHolder.getModel();

          int count = 0;
          while (true) {
            final int numDrained = instances.drainTo(drainedInstances, drainSize);
            if (numDrained == 0) {
              break;
            }

            drainedInstances.forEach(instance -> updateModel(instance, model));
            drainedInstances.clear();
            count += numDrained;
          }
          latch.countDown();
          LOG.log(Level.INFO, "{0} has computed {1} instances",
              new Object[]{Thread.currentThread().getName(), count});
          return model.getTopicChanges();
        });
        futures.add(future);
      }
      latch.await();
    } catch (final InterruptedException e) {
      LOG.log(Level.SEVERE, "Exception occurred.", e);
      throw new RuntimeException(e);
    }

    return ThreadUtils.retrieveResults(futures);
  }

  /**
   * Processes one training data instance and update the intermediate model.
   * @param documentPair training data instance
   * @param model the latest model
   */
  private void updateModel(final Map.Entry<Long, Document> documentPair, final LDAModel model) {
    final Document document = documentPair.getValue();

    final Table<Long, LDALocalModel, ?> localModelTable;
    try {
      localModelTable = tableAccessor.getTable(localModelTableId);
    } catch (TableNotExistException e) {
      throw new RuntimeException(e);
    }

    final LDALocalModel localModel;
    try {
      final long docId = documentPair.getKey();
      localModel = localModelTable.get(docId, false).get();
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException(e);
    }

    final int[] topicSummaryVector = model.getTopicSummaryVector();
    final Map<Integer, int[]> wordTopicVectors = model.getWordTopicVectors();

    double sumS = 0.0;
    double sumR = 0.0;

    final double[] sTerms = new double[numTopics];
    final double[] rTerms = new double[numTopics];
    final List<Integer> nonZeroRTermIndices = new ArrayList<>(numTopics);
    final double[] qTerms = new double[numTopics];
    final List<Integer> nonZeroQTermIndices = new ArrayList<>(numTopics);

    final double[] qCoefficients = new double[numTopics];

    // Initialize auxiliary variables
    // Recalculate for each document to adapt changes from other workers.
    for (int i = 0; i < numTopics; i++) {
      final int topicCount = localModel.getTopicCount(i);
      final double denom = topicSummaryVector[i] + beta * numVocabs;
      qCoefficients[i] = (alpha + topicCount) / denom;
      // All s terms are not zero
      sTerms[i] = alpha * beta / denom;
      sumS += sTerms[i];

      if (topicCount != 0) {
        nonZeroRTermIndices.add(i);
        rTerms[i] = (topicCount * beta) / denom;
        sumR += rTerms[i];
      }
    }

    for (int wordIndex = 0; wordIndex < document.size(); wordIndex++) {
      final int word = document.getWord(wordIndex);
      final int oldTopic = localModel.getAssignment(wordIndex);
      final int oldTopicCount = localModel.getTopicCount(oldTopic);

      // Remove the current word from the document and update terms.
      final double denom = (topicSummaryVector[oldTopic] - 1) + beta * numVocabs;
      sumS -= sTerms[oldTopic];
      sTerms[oldTopic] = (alpha * beta) / denom;
      sumS += sTerms[oldTopic];

      sumR -= rTerms[oldTopic];
      rTerms[oldTopic] = ((oldTopicCount - 1) * beta) / denom;
      sumR += rTerms[oldTopic];

      // Remove from nonzero r terms if it goes to 0
      if (oldTopicCount == 1) {
        // Explicitly convert to Integer type not to call remove(int position)
        nonZeroRTermIndices.remove((Integer) oldTopic);
      }

      qCoefficients[oldTopic] = (alpha + oldTopicCount - 1) / denom;

      localModel.removeWordAtIndex(wordIndex);

      final int[] wordTopicCount = wordTopicVectors.get(word);

      // Calculate q terms
      nonZeroQTermIndices.clear();
      double sumQ = 0.0;

      for (int i = 0; i < wordTopicCount.length; i++) {
        final int topic = wordTopicCount[i++];
        final int count = wordTopicCount[i];
        qTerms[topic] = qCoefficients[topic] * count;
        sumQ += qTerms[topic];
        nonZeroQTermIndices.add(topic);
      }

      // Sample a new topic based on the terms
      final double randomVar = Math.random() * (sumS + sumR + sumQ);
      final int newTopic;

      if (randomVar < sumS) {
        // Hit the "smoothing only" bucket.
        newTopic = sampleFromTerms(randomVar, sTerms);
      } else if (sumS <= randomVar && randomVar < sumS + sumR) {
        // Hit the "document topic" bucket.
        newTopic = sampleFromTerms(randomVar - sumS, rTerms, nonZeroRTermIndices);
      } else {
        // Hit the "topic word" bucket. More than 90% hit here.
        newTopic = sampleFromTerms(randomVar - (sumS + sumR), qTerms, nonZeroQTermIndices);
      }

      final int newTopicCount = localModel.getTopicCount(newTopic);

      // Update the terms and add the removed word with the new topic.
      final double newDenom = (topicSummaryVector[newTopic] + 1) + beta * numVocabs;
      sumS -= sTerms[newTopic];
      sTerms[newTopic] = (alpha * beta) / newDenom;
      sumS += sTerms[newTopic];

      sumR -= rTerms[newTopic];
      rTerms[newTopic] = ((newTopicCount + 1) * beta) / newDenom;
      sumR += rTerms[newTopic];

      // Add to nonzero r terms if it goes to 1
      if (newTopicCount == 0) {
        nonZeroRTermIndices.add(newTopic);
      }

      qCoefficients[newTopic] = (alpha + newTopicCount + 1) / newDenom;

      localModel.addWordAtIndex(wordIndex, newTopic);

      // Accumulate the changes to TopicChanges
      if (newTopic != oldTopic) {
        final TopicChanges topicChanges = model.getTopicChanges();
        topicChanges.replace(word, oldTopic, newTopic, 1);

        // numVocabs-th row represents the total word-topic assignment count vector
        topicChanges.replace(numVocabs, oldTopic, newTopic, 1);
      }
    }
  }

  private int sampleFromTerms(final double randomVar, final double[] terms) {
    double val = randomVar;
    for (int i = 0; i < terms.length; i++) {
      if (val < terms[i]) {
        return i;
      }

      val -= terms[i];
    }

    throw new RuntimeException("randomVar has to be smaller than summation of all terms");
  }

  private int sampleFromTerms(final double randomVar, final double[] terms, final List<Integer> nonzeroIndices) {
    double val = randomVar;
    for (final int nonzeroIndex : nonzeroIndices) {
      if (val < terms[nonzeroIndex]) {
        return nonzeroIndex;
      }

      val -= terms[nonzeroIndex];
    }

    throw new RuntimeException("randomVar has to be smaller than summation of all nonzero terms");
  }
}
