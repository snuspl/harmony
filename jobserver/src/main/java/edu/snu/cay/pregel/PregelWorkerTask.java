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
package edu.snu.cay.pregel;

import com.google.common.collect.Lists;
import edu.snu.cay.common.param.Parameters;
import edu.snu.cay.pregel.graph.api.Computation;
import edu.snu.cay.pregel.graph.api.Vertex;
import edu.snu.cay.pregel.graph.impl.ComputationCallable;
import edu.snu.cay.pregel.graph.impl.MessageManager;
import edu.snu.cay.pregel.graph.impl.Partition;
import edu.snu.cay.services.et.configuration.parameters.TaskletIdentifier;
import edu.snu.cay.services.et.evaluator.api.Table;
import edu.snu.cay.services.et.evaluator.api.TableAccessor;
import edu.snu.cay.services.et.evaluator.api.Tasklet;
import edu.snu.cay.services.et.evaluator.impl.TaskUnitInfo;
import edu.snu.cay.services.et.evaluator.impl.LocalTaskUnitScheduler;
import edu.snu.cay.services.et.exceptions.TableNotExistException;
import edu.snu.cay.utils.CatchableExecutors;
import org.apache.reef.annotations.audience.EvaluatorSide;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * REEF Task class to run a Pregel app.
 * @param <V> a value type
 * @param <E> a edge type
 * @param <M> a message type
 */
@EvaluatorSide
public final class PregelWorkerTask<V, E, M> implements Tasklet {
  private static final Logger LOG = Logger.getLogger(PregelWorkerTask.class.getName());

  /**
   * Manage message stores in this works.
   */
  private final MessageManager<Long, M> messageManager;

  private final WorkerMsgManager workerMsgManager;

  private final Computation<V, E, M> computation;

  private final Table<Long, Vertex<V, E>, ?> vertexTable;

  private final int numWorkerThreads;

  private final LocalTaskUnitScheduler taskUnitScheduler;
  private final TaskUnitInfo compTaskUnitInfo;
  private final TaskUnitInfo sendTaskUnitInfo;
  private final TaskUnitInfo syncTaskUnitInfo;

  @Inject
  private PregelWorkerTask(final MessageManager<Long, M> messageManager,
                           final LocalTaskUnitScheduler taskUnitScheduler,
                           final WorkerMsgManager workerMsgManager,
                           final Computation<V, E, M> computation,
                           final TableAccessor tableAccessor,
                           @Parameter(TaskletIdentifier.class) final String taskletId,
                           @Parameter(Parameters.HyperThreadEnabled.class) final boolean hyperThreadEnabled,
                           @Parameter(PregelParameters.NumWorkerThreads.class) final int numWorkerThreads,
                           @Parameter(PregelParameters.VertexTableId.class) final String vertexTableId)
      throws TableNotExistException {
    this.messageManager = messageManager;
    this.workerMsgManager = workerMsgManager;
    this.computation = computation;
    this.vertexTable = tableAccessor.getTable(vertexTableId);
    this.numWorkerThreads = numWorkerThreads == Integer.parseInt(PregelParameters.NumWorkerThreads.UNSET_VALUE) ?
        Runtime.getRuntime().availableProcessors() / (hyperThreadEnabled ? 2 : 1) :
        numWorkerThreads;

    this.taskUnitScheduler = taskUnitScheduler;
    this.compTaskUnitInfo = new TaskUnitInfo(taskletId, "COMP", TaskUnitInfo.ResourceType.CPU);
    this.sendTaskUnitInfo = new TaskUnitInfo(taskletId, "SEND", TaskUnitInfo.ResourceType.NET);
    this.syncTaskUnitInfo = new TaskUnitInfo(taskletId, "SYNC", TaskUnitInfo.ResourceType.VOID);
  }

  @Override
  public void run() throws Exception {

    LOG.log(Level.INFO, "Pregel task starts.");
    final ExecutorService executorService = CatchableExecutors.newFixedThreadPool(numWorkerThreads);

    int superStepCount = 0;
    // run supersteps until all vertices halt
    // each loop is a superstep
    while (true) {
      computation.initialize(superStepCount);
      final List<Future<Integer>> futureList = new ArrayList<>(numWorkerThreads);

      // partition local graph-dataset as the number of threads
      final Map<Long, Vertex<V, E>> vertexMap = vertexTable.getLocalTablet().getDataMap();
      final List<Partition<V, E>> vertexPartitions = partitionVertices(vertexMap, numWorkerThreads);

      taskUnitScheduler.waitSchedule(compTaskUnitInfo);
      // compute each partition with a thread pool
      for (int threadIdx = 0; threadIdx < numWorkerThreads; threadIdx++) {
        final Partition<V, E> partition = vertexPartitions.get(threadIdx);
        final Callable<Integer> computationCallable =
            new ComputationCallable<>(computation, partition, messageManager.getCurrentMessageTable());
        futureList.add(executorService.submit(computationCallable));
      }

      // aggregate the number of active vertices from the processed partitions
      int numActiveVertices = 0;
      for (final Future<Integer> computeFuture : futureList) {
        numActiveVertices += computeFuture.get();
      }
      taskUnitScheduler.onTaskUnitFinished(compTaskUnitInfo);

      taskUnitScheduler.waitSchedule(sendTaskUnitInfo);
      // before finishing superstep, confirm that all outgoing messages are completely sent out
      final boolean messageExist = messageManager.flushAllMessages();
      taskUnitScheduler.onTaskUnitFinished(sendTaskUnitInfo);

      LOG.log(Level.INFO, "Superstep {0} is finished", superStepCount);

      taskUnitScheduler.waitSchedule(syncTaskUnitInfo);
      // master will decide whether to continue or not
      final boolean continueSuperstep = workerMsgManager.waitForTryNextSuperstepMsg(numActiveVertices, messageExist);
      taskUnitScheduler.onTaskUnitFinished(syncTaskUnitInfo);

      if (!continueSuperstep) {
        break;
      }

      // prepare next superstep
      messageManager.prepareForNextSuperstep();
      superStepCount++;
    }

    LOG.log(Level.INFO, "Pregel job has been finished after {0} supersteps.", superStepCount);
    vertexTable.getLocalTablet().getDataMap().values().forEach(vertex ->
        LOG.log(Level.INFO, "Vertex id : {0}, value : {1}", new Object[]{vertex.getId(), vertex.getValue()}));
  }

  /**
   * Partition local graph-dataset as the number of threads.
   * @param vertexMap the vertex map to partition
   * @param numPartitions the number of partitions
   * @return a list of partition
   */
  private List<Partition<V, E>> partitionVertices(final Map<Long, Vertex<V, E>> vertexMap, final int numPartitions) {
    final List<Vertex<V, E>> vertexList = Lists.newArrayList(vertexMap.values());
    final int numVertices = vertexList.size();
    final int sizeByPartition = numVertices / numPartitions;
    int remainder = numVertices % numPartitions;

    final List<Partition<V, E>> vertexPartitions = new ArrayList<>(numPartitions);

    int partitionStartIdx = 0;
    for (int threadIdx = 0; threadIdx < numPartitions; threadIdx++) {
      final int partitionSize = sizeByPartition + (remainder-- > 0 ? 1 : 0);

      final List<Vertex<V, E>> partitionVertices = vertexList.subList(partitionStartIdx,
          partitionStartIdx + partitionSize);
      partitionStartIdx += partitionSize; // for next partition

      vertexPartitions.add(new Partition<>(partitionVertices));
    }
    return vertexPartitions;
  }

  @Override
  public void close() {

  }
}
