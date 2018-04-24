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

import com.google.common.collect.Table;
import edu.snu.cay.dolphin.core.worker.*;
import edu.snu.cay.dolphin.mlapps.lda.LDAParameters.*;
import edu.snu.cay.dolphin.DolphinParameters;
import edu.snu.cay.services.et.configuration.parameters.TaskletIdentifier;
import edu.snu.cay.services.et.evaluator.api.TableAccessor;
import edu.snu.cay.services.et.evaluator.impl.LocalTaskUnitScheduler;
import edu.snu.cay.services.et.evaluator.impl.TaskUnitInfo;
import edu.snu.cay.services.et.exceptions.TableNotExistException;
import edu.snu.cay.utils.CatchableExecutors;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.*;
import java.util.concurrent.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Assign a random topic to each word in all documents and block on a global barrier to make sure
 * all workers update their initial topic assignments. For each mini-batch, sequentially sampling documents,
 * it immediately pushes the changed topic assignment whenever each word is sampled to a new topic.
 */
final class LDATrainer implements Trainer<Long, Document> {

  private static final Logger LOG = Logger.getLogger(LDATrainer.class.getName());

  private final SparseLDASampler sampler;
  private final LDAStatCalculator statCalculator;
  private final int numVocabs;
  private final int numTopics;

  private final TrainingDataProvider<Long, Document> trainingDataProvider;

  private final ModelAccessor<Integer, int[], int[]> modelAccessor;

  private final TableAccessor tableAccessor;
  private final String localModelTableId;

  /**
   * Allows to access and update the latest model.
   */
  private final ModelHolder<LDAModel> modelHolder;

  private final int numTrainerThreads;

  private final LocalTaskUnitScheduler localTaskUnitScheduler;

  private final String taskletId;

  private final ExecutorService executor;

  @Inject
  private LDATrainer(final SparseLDASampler sampler,
                     final LDAStatCalculator statCalculator,
                     final TableAccessor tableAccessor,
                     @Parameter(DolphinParameters.LocalModelTableId.class) final String localModelTableId,
                     final TrainingDataProvider<Long, Document> trainingDataProvider,
                     final ModelAccessor<Integer, int[], int[]> modelAccessor,
                     final ModelHolder<LDAModel> modelHolder,
                     @Parameter(DolphinParameters.NumTrainerThreads.class) final int numTrainerThreads,
                     final LocalTaskUnitScheduler localTaskUnitScheduler,
                     @Parameter(TaskletIdentifier.class) final String taskletId,
                     @Parameter(NumVocabs.class) final int numVocabs,
                     @Parameter(NumTopics.class) final int numTopics,
                     @Parameter(DolphinParameters.NumTotalMiniBatches.class) final int numTotalMiniBatches)
      throws TableNotExistException {
    this.numTrainerThreads = numTrainerThreads;
    this.sampler = sampler;
    this.statCalculator = statCalculator;

    this.tableAccessor = tableAccessor;
    this.localModelTableId = localModelTableId;

    this.trainingDataProvider = trainingDataProvider;
    this.modelAccessor = modelAccessor;
    this.numVocabs = numVocabs;

    this.taskletId = taskletId;
    this.localTaskUnitScheduler = localTaskUnitScheduler;

    this.numTopics = numTopics;

    this.modelHolder = modelHolder;

    this.executor = CatchableExecutors.newFixedThreadPool(numTrainerThreads);

    this.gradientsToPush = Collections.synchronizedList(new ArrayList<>(numTrainerThreads));

    LOG.log(Level.INFO, "Number of total mini-batches in an epoch = {0}", numTotalMiniBatches);
    LOG.log(Level.INFO, "All random topic assignments are updated");
  }

  @Override
  public void initGlobalSettings() {
    // In LDA, topic counts should be initialized by pushing values before running.
    final List<Map.Entry<Long, Document>> epochData = new ArrayList<>(trainingDataProvider.getEpochData());

    final CyclicBarrier barrier = new CyclicBarrier(numTrainerThreads);
    final CountDownLatch latch = new CountDownLatch(numTrainerThreads);

    final int numDataPerThread = epochData.size() / numTrainerThreads;
    final int numRemainders = epochData.size() % numTrainerThreads;

    final TaskUnitInfo initCompTask = new TaskUnitInfo(taskletId, "INIT-COMP", TaskUnitInfo.ResourceType.CPU);
    final TaskUnitInfo initCommTask = new TaskUnitInfo(taskletId, "INIT-COMM", TaskUnitInfo.ResourceType.NET);

    final edu.snu.cay.services.et.evaluator.api.Table<Long, LDALocalModel, ?> localModelTable;
    try {
      localModelTable = tableAccessor.getTable(localModelTableId);
    } catch (TableNotExistException e) {
      throw new RuntimeException(e);
    }

    localTaskUnitScheduler.waitSchedule(initCompTask);
    for (int threadIdx = 0; threadIdx < numTrainerThreads; threadIdx++) {
      final int finalThreadIdx = threadIdx;
      executor.submit(() -> {
        final TopicChanges topicChanges = new TopicChanges();

        final List<Pair<Long, LDALocalModel>> localModels = new ArrayList<>(epochData.size());

        final int startIdx = numDataPerThread * finalThreadIdx;
        final int endIdx = startIdx + numDataPerThread + (finalThreadIdx == numTrainerThreads - 1 ? numRemainders : 0);

        for (final Map.Entry<Long, Document> documentPair : epochData.subList(startIdx, endIdx)) {
          final long documentId = documentPair.getKey();
          final Document document = documentPair.getValue();

          final LDALocalModel localModel = new LDALocalModel(document.size(), numTopics);
          localModels.add(Pair.of(documentId, localModel));

          for (int i = 0; i < document.size(); i++) {
            final int word = document.getWord(i);
            topicChanges.increment(word, localModel.getAssignment(i), 1);
            // numVocabs-th row represents the total word-topic assignment count vector
            topicChanges.increment(numVocabs, localModel.getAssignment(i), 1);
          }
        }

        LOG.log(Level.INFO, "Model init done");

        try {
          localModelTable.multiPut(localModels).get();
        } catch (InterruptedException | ExecutionException e) {
          throw new RuntimeException(e);
        }

        LOG.log(Level.INFO, "Local model put done");

        try {
          barrier.await();
        } catch (InterruptedException | BrokenBarrierException e) {
          throw new RuntimeException(e);
        }
        if (finalThreadIdx == 0) {
          localTaskUnitScheduler.onTaskUnitFinished(initCompTask);
          localTaskUnitScheduler.waitSchedule(initCommTask);
        }

        final Map<Integer, int[]> gradients = getGradientsToPush(topicChanges);
        modelAccessor.push(gradients);

        LOG.log(Level.INFO, "Init Push done");
        latch.countDown();
      });
    }

    try {
      latch.await();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }

    localTaskUnitScheduler.onTaskUnitFinished(initCommTask);
  }

  private volatile Collection<Map.Entry<Long, Document>> miniBatchTrainingData;

  private final List<Map<Integer, int[]>> gradientsToPush;

  private List<Integer> words;

  private List<int[]> denseTopicVectors;

  @Override
  public void setMiniBatchData(final Collection<Map.Entry<Long, Document>> newMiniBatchTrainingData) {
    this.miniBatchTrainingData = newMiniBatchTrainingData;
  }

  @Override
  public void pullModel() {
    words = getKeys(miniBatchTrainingData);
    denseTopicVectors = modelAccessor.pull(words);
  }

  @Override
  public void localCompute() {
    // initialize LDAModel after translating the pulled model into sparse form
    final int[] sparseTopicSummaryVector = denseToSparse(denseTopicVectors.remove(words.size() - 1));
    // i-th element of topicSummaryVector represents total number of assignments of i-th topic
    final int[] topicSummaryVector = new int[numTopics];
    for (int i = 0; i < sparseTopicSummaryVector.length; i++) {
      final int topic = sparseTopicSummaryVector[i++];
      final int count = sparseTopicSummaryVector[i];
      topicSummaryVector[topic] = count;
    }

    final Map<Integer, int[]> wordTopicVectors = new HashMap<>(denseTopicVectors.size());
    final int numTopicVectors = denseTopicVectors.size();
    for (int i = 0; i < numTopicVectors; ++i) {
      final int[] sparseTopicVector = denseToSparse(denseTopicVectors.remove(0));
      wordTopicVectors.put(words.get(i), sparseTopicVector);
    }
    words.clear();
    words = null;
    modelHolder.resetModel(new LDAModel(topicSummaryVector, wordTopicVectors));

    denseTopicVectors.clear();
    denseTopicVectors = null;

    // perform sampling
    final List<TopicChanges> results = sampler.sample(miniBatchTrainingData);

    miniBatchTrainingData = null;
    modelHolder.resetModel(null);

    final CountDownLatch latch = new CountDownLatch(numTrainerThreads);

    for (int threadIdx = 0; threadIdx < numTrainerThreads; threadIdx++) {
      final int finalThreadIdx = threadIdx;
      executor.submit(() -> {
        final TopicChanges topicChanges = results.get(finalThreadIdx);
        gradientsToPush.add(getGradientsToPush(topicChanges));
        latch.countDown();
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
    final CountDownLatch latch = new CountDownLatch(numTrainerThreads);
    for (final Map<Integer, int[]> gradients : gradientsToPush) {
      executor.submit(() -> {
        modelAccessor.push(gradients);
        gradients.clear();
        latch.countDown();
      });
    }

    try {
      latch.await();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }

    gradientsToPush.clear();
  }

  @Override
  public void onEpochFinished(final int epochIdx) {

  }

  @Override
  public Map<CharSequence, Double> evaluateModel(final Collection<Map.Entry<Long, Document>> inputData,
                                                 final Collection<Document> testData) {
    // key numVocabs is a summary vector of word-topic distribution, in a form of numTopics-dimensional vector
    final List<Integer> vocabList = new ArrayList<>(numVocabs + 1);
    for (int i = 0; i < numVocabs + 1; i++) {
      vocabList.add(i);
    }

    LOG.log(Level.INFO, "Pull model to compute log likelihood");
    final List<int[]> wordTopicCounts = modelAccessor.pull(vocabList);
    final int[] wordTopicCountsSummary = wordTopicCounts.remove(numVocabs);

    LOG.log(Level.INFO, "Start computing log likelihood");
    final Map<CharSequence, Double> map = new HashMap<>();
    map.put("docLLH", statCalculator.computeDocLLH(inputData));
    map.put("wordLLH", statCalculator.computeWordLLH(wordTopicCounts, wordTopicCountsSummary));

    return map;
  }

  /**
   * Translate dense array to sparse array.
   * It's required since the trainer handles the model in the sparse form,
   * while servers give the model in the dense form.
   * @param denseArray an array in the form of dense array
   * @return an array in the form of sparse array
   */
  private int[] denseToSparse(final int[] denseArray) {
    final int numNonZeros = denseArray[denseArray.length - 1];

    final int[] sparseArray = new int[2 * numNonZeros];
    int nonZeroCount = 0;
    for (int i = 0; i < denseArray.length - 1; i++) {
      if (denseArray[i] != 0) {
        sparseArray[2 * nonZeroCount] = i;
        sparseArray[2 * nonZeroCount + 1] = denseArray[i];
        nonZeroCount++;
      }
    }

    return sparseArray;
  }

  private Map<Integer, int[]> getGradientsToPush(final TopicChanges topicChanges) {
    final Table<Integer, Integer, Integer> changedTopicCount = topicChanges.getTable();

    final Map<Integer, int[]> keyToChangesMap = new HashMap<>(changedTopicCount.rowKeySet().size());

    for (final int changedWord : changedTopicCount.rowKeySet()) {
      final Map<Integer, Integer> changedTopicCountsForWord = changedTopicCount.row(changedWord);
      final int numChangedTopics = changedTopicCountsForWord.size();

      // Given a word, an even index represents a changed topic index and a corresponding odd index represents
      // a changed value for the topic index.
      final int[] parameters = new int[2 * numChangedTopics];
      int i = 0;
      for (final Map.Entry<Integer, Integer> entry : changedTopicCountsForWord.entrySet()) {
        parameters[2 * i] = entry.getKey();
        parameters[2 * i + 1] = entry.getValue();
        i++;
      }

      keyToChangesMap.put(changedWord, parameters);
    }

    changedTopicCount.clear();
    return keyToChangesMap;
  }

  @Override
  public void cleanup() {
  }

  private List<Integer> getKeys(final Collection<Map.Entry<Long, Document>> documentPairs) {
    final Set<Integer> keys = new TreeSet<>();
    for (final Map.Entry<Long, Document> documentPair : documentPairs) {
      final Document document = documentPair.getValue();
      keys.addAll(document.getWords());
    }

    final List<Integer> result = new ArrayList<>(keys.size() + 1);
    result.addAll(keys);
    // numVocabs-th row represents the total word-topic assignment count vector
    result.add(numVocabs);

    return result;
  }
}
