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
package edu.snu.cay.dolphin.mlapps.lda;

import com.google.common.util.concurrent.AtomicDouble;
import edu.snu.cay.common.param.Parameters;
import edu.snu.cay.dolphin.DolphinParameters;
import edu.snu.cay.services.et.evaluator.api.Table;
import edu.snu.cay.services.et.evaluator.api.TableAccessor;
import edu.snu.cay.services.et.exceptions.TableNotExistException;
import edu.snu.cay.utils.CatchableExecutors;
import org.apache.commons.math3.special.Gamma;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;

/**
 * Compute log likelihoods of the model.
 * This follows T. L. Griffiths and M. Steyvers. Finding scientific topics. Proceedings of the National
 * Academy of Sciences of the United States of America, Vol. 101, No. Suppl 1. (6 April 2004), pp. 5228-5235.
 */
final class LDAStatCalculator {

  private final double alpha;
  private final double beta;
  private final int numTopics;
  private final int numVocabs;

  /**
   * Pre-computed constant to save the cost for computing a frequent term, log(Gamma(alpha)).
   */
  private final double logGammaAlpha;

  /**
   * Pre-computed constant to save the cost for computing a frequent term, log(Gamma(beta)).
   */
  private final double logGammaBeta;

  private final Table<Long, LDALocalModel, ?> localModelTable;

  private final int numTrainerThreads;

  private final ExecutorService executor;

  @Inject
  private LDAStatCalculator(@Parameter(LDAParameters.Alpha.class) final double alpha,
                            @Parameter(LDAParameters.Beta.class) final double beta,
                            @Parameter(LDAParameters.NumTopics.class) final int numTopics,
                            @Parameter(LDAParameters.NumVocabs.class) final int numVocabs,
                            @Parameter(DolphinParameters.NumTrainerThreads.class) final int numTrainerThreads,
                            @Parameter(Parameters.HyperThreadEnabled.class) final boolean hyperThreadEnabled,
                            @Parameter(DolphinParameters.LocalModelTableId.class) final String localModelTableId,
                            final TableAccessor tableAccessor) throws TableNotExistException {
    this.alpha = alpha;
    this.beta = beta;
    this.numTopics = numTopics;
    this.numVocabs = numVocabs;

    this.logGammaAlpha = Gamma.logGamma(alpha);
    this.logGammaBeta = Gamma.logGamma(beta);

    // Use the half of the processors if hyper-thread is on, since using virtual cores do not help for float-point ops.
    this.numTrainerThreads = numTrainerThreads == Integer.parseInt(DolphinParameters.NumTrainerThreads.UNSET_VALUE) ?
        Runtime.getRuntime().availableProcessors() / (hyperThreadEnabled ? 2 : 1) :
        numTrainerThreads;
    this.executor = CatchableExecutors.newFixedThreadPool(this.numTrainerThreads);

    this.localModelTable = tableAccessor.getTable(localModelTableId);
  }

  /**
   * Computes log likelihood for documents (P(z)) according to Eq. [3] in the reference.
   * <ul>
   *   <li>T: {@code numTopics}</li>
   *   <li>D: Total number of documents</li>
   *   <li>n(j, d): <i>j</i>th topic's number of assignments to <i>d</i>th document</li>
   * </ul>
   * @param documentPairs a collection of documents assigned to this trainer
   * @return a portion of log likelihood computed from the given documentPairs
   */
  double computeDocLLH(final Collection<Map.Entry<Long, Document>> documentPairs) {
    final List<Map.Entry<Long, Document>> documentPairList = new ArrayList<>(documentPairs);

    final int numItemsPerThread = documentPairList.size() / numTrainerThreads;
    final int numRemainders = documentPairList.size() % numTrainerThreads;

    final AtomicDouble result = new AtomicDouble(documentPairs.size()
        * (Gamma.logGamma(numTopics * alpha) - numTopics * Gamma.logGamma(alpha)));

    final CountDownLatch latch = new CountDownLatch(numTrainerThreads);

    for (int threadIdx = 0; threadIdx < numTrainerThreads; threadIdx++) {
      final int finalThreadIdx = threadIdx;
      executor.submit(() -> {
        final int startIdx = numItemsPerThread * finalThreadIdx;
        final int endIdx = startIdx + numItemsPerThread + (finalThreadIdx == numTrainerThreads - 1 ? numRemainders : 0);

        for (final Map.Entry<Long, Document> documentPair : documentPairList.subList(startIdx, endIdx)) {
          final Document document = documentPair.getValue();
          final LDALocalModel localModel;
          try {
            localModel = localModelTable.get(documentPair.getKey(), false).get();
          } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
          }

          for (int j = 0; j < numTopics; j++) {
            final int topicCount = localModel.getTopicCount(j);
            if (topicCount < 0) {
              localModel.setTopicCount(j, 0);
            }
            result.addAndGet(topicCount <= 0 ? logGammaAlpha : Gamma.logGamma(topicCount + alpha));
          }
          result.addAndGet(-Gamma.logGamma(document.size() + numTopics * alpha));
        }
        latch.countDown();
      });
    }

    try {
      latch.await();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }

    return result.get();
  }

  /**
   * Computes log likelihood for word-topic vectors (P(w|z)) according to Eq. [2] in the reference.
   * <ul>
   *   <li>T: {@code numTopics}</li>
   *   <li>W: {@code numVocabs}</li>
   *   <li>n(j, w): <i>j</i>th topic's number of assignments to <i>w</i>th vocabulary</li>
   * </ul>

   * @return a portion of log likelihood computed from the given word-topic vectors
   */
  double computeWordLLH(final Collection<int[]> wordTopicCounts, final int[] wordTopicCountsSummary) {
    double result = numTopics * (Gamma.logGamma(numVocabs * beta) - numVocabs * Gamma.logGamma(beta));
    for (final int[] wordTopicCount : wordTopicCounts) {
      // For computing log-likelihood, we need only the values. Please refer to SparseArrayCodec.
      for (int j = 1; j < wordTopicCount.length; j += 2) {
        result += Gamma.logGamma(wordTopicCount[j] + beta);
      }
      // handle the case of zero values separately
      result += logGammaBeta * (numTopics - wordTopicCount.length / 2);
    }
    for (int j = 1; j < wordTopicCountsSummary.length; j += 2) {
      result -= Gamma.logGamma(wordTopicCountsSummary[j] + numVocabs * beta);
    }
    // handle the case of zero values separately
    result -= Gamma.logGamma(numVocabs * beta) * (numTopics - wordTopicCountsSummary.length / 2);
    return result;
  }
}
