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
package edu.snu.cay.dolphin.async.plan;

import edu.snu.cay.dolphin.async.optimizer.parameters.Constants;
import edu.snu.cay.services.em.plan.api.Plan;
import edu.snu.cay.services.em.plan.api.PlanOperation;
import edu.snu.cay.services.em.plan.api.TransferStep;
import edu.snu.cay.utils.DAG;
import edu.snu.cay.utils.DAGImpl;

import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

import static edu.snu.cay.services.em.plan.impl.EMPlanOperation.*;
import static edu.snu.cay.dolphin.async.plan.DolphinPlanOperation.*;

/**
 * A plan implementation that supports EM's default plan operations and Dolphin-specific plan operations.
 * The builder checks the feasibility of plan and dependencies between detailed steps.
 */
// TODO #725: clean up duplicate code with the plan implementation of EM package
public final class PlanImpl implements Plan {
  private static final Logger LOG = Logger.getLogger(PlanImpl.class.getName());

  private final Map<String, Set<String>> evaluatorsToAdd;
  private final Set<String> workersToStart;
  private final Map<String, Set<String>> evaluatorsToDelete;
  private final Set<String> workersToStop;
  private final Set<String> serversToSync;
  private final Map<String, List<TransferStep>> allTransferSteps;

  private final DAG<PlanOperation> dependencyGraph;
  private final Set<PlanOperation> initialOps;
  private final int numTotalOperations;

  private PlanImpl(final Map<String, Set<String>> evaluatorsToAdd,
                   final Map<String, Set<String>> evaluatorsToDelete,
                   final Map<String, List<TransferStep>> allTransferSteps,
                   final DAG<PlanOperation> dependencyGraph) {
    this.evaluatorsToAdd = evaluatorsToAdd;
    this.workersToStart = evaluatorsToAdd.containsKey(Constants.NAMESPACE_WORKER) ?
        evaluatorsToAdd.get(Constants.NAMESPACE_WORKER) : Collections.emptySet();
    this.evaluatorsToDelete = evaluatorsToDelete;
    this.workersToStop = evaluatorsToDelete.containsKey(Constants.NAMESPACE_WORKER) ?
        evaluatorsToDelete.get(Constants.NAMESPACE_WORKER) : Collections.emptySet();
    this.serversToSync = evaluatorsToDelete.containsKey(Constants.NAMESPACE_SERVER) ?
        evaluatorsToDelete.get(Constants.NAMESPACE_SERVER) : Collections.emptySet();

    this.allTransferSteps = allTransferSteps;
    this.dependencyGraph = dependencyGraph;
    this.initialOps = new HashSet<>(dependencyGraph.getRootVertices());

    // count the total number of operations
    int numTotalOps = 0;
    for (final Set<String> evalsToAdd : evaluatorsToAdd.values()) {
      numTotalOps += evalsToAdd.size();
    }
    for (final Set<String> evalsToDel : evaluatorsToDelete.values()) {
      numTotalOps += evalsToDel.size();
    }
    for (final List<TransferStep> transferSteps : allTransferSteps.values()) {
      numTotalOps += transferSteps.size();
    }
    numTotalOps += workersToStart.size() + workersToStop.size() + serversToSync.size();

    this.numTotalOperations = numTotalOps;
  }

  @Override
  public int getPlanSize() {
    return numTotalOperations;
  }

  @Override
  public synchronized Set<PlanOperation> getInitialOps() {
    return initialOps;
  }

  @Override
  public synchronized Set<PlanOperation> onComplete(final PlanOperation operation) {
    final Set<PlanOperation> candidateOperations = dependencyGraph.getNeighbors(operation);
    final Set<PlanOperation> nextOpsToExecute = new HashSet<>();
    dependencyGraph.removeVertex(operation);

    for (final PlanOperation candidate : candidateOperations) {
      if (dependencyGraph.getInDegree(candidate) == 0) {
        nextOpsToExecute.add(candidate);
      }
    }
    return nextOpsToExecute;
  }

  @Override
  public Collection<String> getEvaluatorsToAdd(final String namespace) {
    if (!evaluatorsToAdd.containsKey(namespace)) {
      return Collections.emptyList();
    }
    return evaluatorsToAdd.get(namespace);
  }

  @Override
  public Collection<String> getEvaluatorsToDelete(final String namespace) {
    if (!evaluatorsToDelete.containsKey(namespace)) {
      return Collections.emptyList();
    }
    return evaluatorsToDelete.get(namespace);
  }

  @Override
  public Collection<TransferStep> getTransferSteps(final String namespace) {
    if (!allTransferSteps.containsKey(namespace)) {
      return Collections.emptyList();
    }
    return allTransferSteps.get(namespace);
  }

  @Override
  public String toString() {
    return "PlanImpl{" +
        "evaluatorsToAdd=" + evaluatorsToAdd +
        ", workersToStart=" + workersToStart +
        ", evaluatorsToDelete=" + evaluatorsToDelete +
        ", workersToStop=" + workersToStop +
        ", serversToSync=" + serversToSync +
        ", allTransferSteps=" + allTransferSteps +
        ", numTotalOperations=" + numTotalOperations +
        '}';
  }

  public static PlanImpl.Builder newBuilder() {
    return new Builder();
  }

  /**
   * A builder of PlanImpl.
   * Before instantiating a PlanImpl object, it checks the feasibility of plan.
   * If the given plan operations are feasible it builds a Plan,
   * constructing a dependency graph between steps, based on the following rules:
   *   1. Evaluators must be added before they participate in transfers. (add -> move)
   *   2. Evaluators must finish all transfers which they are a part of before they are deleted. (move -> del)
   *   3. One evaluator must be deleted before adding a new evaluator,
   *     because we are using the whole available resources. (del -> add)
   *   4. Tasks in newly added evaluators should start after migrating initial data (move -> start)
   *   5. Tasks in evaluators to be deleted should stop before migrating remaining data (stop -> move)
   */
  public static final class Builder implements org.apache.reef.util.Builder<PlanImpl> {
    private final Map<String, Set<String>> evaluatorsToAdd = new HashMap<>();
    private final Map<String, Set<String>> evaluatorsToDelete = new HashMap<>();
    private final Map<String, List<TransferStep>> allTransferSteps = new HashMap<>();

    // Optional.empty means that there's no resource limit
    private Optional<Integer> numAvailableExtraEvaluators = Optional.empty();

    private Builder() {
    }

    /**
     * Sets the limitation on the number of extra evaluators to use in plan execution.
     * If not specified, it assumes that there's no resource limit.
     * @param numAvailableExtraEvaluators the number of extra evaluators
     * @return the builder
     */
    public Builder setNumAvailableExtraEvaluators(final int numAvailableExtraEvaluators) {
      this.numAvailableExtraEvaluators = Optional.of(numAvailableExtraEvaluators);
      return this;
    }

    public Builder addEvaluatorToAdd(final String namespace, final String evaluatorId) {
      if (!evaluatorsToAdd.containsKey(namespace)) {
        evaluatorsToAdd.put(namespace, new HashSet<>());
      }
      final Set<String> evaluatorIds = evaluatorsToAdd.get(namespace);
      evaluatorIds.add(evaluatorId);
      return this;
    }

    public Builder addEvaluatorsToAdd(final String namespace, final Collection<String> evaluatorIdsToAdd) {
      if (!evaluatorsToAdd.containsKey(namespace)) {
        evaluatorsToAdd.put(namespace, new HashSet<>());
      }
      final Set<String> evaluatorIds = evaluatorsToAdd.get(namespace);
      evaluatorIds.addAll(evaluatorIdsToAdd);
      return this;
    }

    public Builder addEvaluatorToDelete(final String namespace, final String evaluatorId) {
      if (!evaluatorsToDelete.containsKey(namespace)) {
        evaluatorsToDelete.put(namespace, new HashSet<>());
      }
      final Set<String> evaluatorIds = evaluatorsToDelete.get(namespace);
      evaluatorIds.add(evaluatorId);
      return this;
    }

    public Builder addEvaluatorsToDelete(final String namespace, final Collection<String> evaluatorIdsToDelete) {
      if (!evaluatorsToDelete.containsKey(namespace)) {
        evaluatorsToDelete.put(namespace, new HashSet<>());
      }
      final Set<String> evaluatorIds = evaluatorsToDelete.get(namespace);
      evaluatorIds.addAll(evaluatorIdsToDelete);
      return this;
    }

    public Builder addTransferStep(final String namespace, final TransferStep transferStep) {
      if (!allTransferSteps.containsKey(namespace)) {
        allTransferSteps.put(namespace, new ArrayList<>());
      }
      final List<TransferStep> transferStepList = allTransferSteps.get(namespace);
      transferStepList.add(transferStep);
      return this;
    }

    public Builder addTransferSteps(final String namespace, final Collection<TransferStep> transferSteps) {
      if (!allTransferSteps.containsKey(namespace)) {
        allTransferSteps.put(namespace, new ArrayList<>());
      }
      final List<TransferStep> transferStepList = allTransferSteps.get(namespace);
      transferStepList.addAll(transferSteps);
      return this;
    }

    /**
     * {@inheritDoc}
     *
     * Builds a Plan embedding dependencies between operations.
     * It throws RuntimeException if the plan violates the resource limit.
     *
     * @return {@inheritDoc}
     * @throws IllegalStateException if a dependency in the plan forms a cycle
     */
    @Override
    public PlanImpl build() {
      // check the observance of invariants in the plan (details are described below)

      // 1. do not allow the use of same evaluator id for both in Add and Delete in the same namespace
      // note that we do not care the uniqueness of evaluator id across namespaces
      for (final String namespace : evaluatorsToAdd.keySet()) {
        for (final String evaluator : evaluatorsToAdd.get(namespace)) {
          if (evaluatorsToDelete.containsKey(namespace) &&
              evaluatorsToDelete.get(namespace).contains(evaluator)) {
            throw new RuntimeException(evaluator + " is planned for both addition and deletion.");
          }
        }
      }

      // 2. do not allow moving out/in of data from/to evaluators that will be added/deleted in a single plan
      for (final String namespace : allTransferSteps.keySet()) {
        for (final TransferStep transferStep : allTransferSteps.get(namespace)) {
          if (evaluatorsToDelete.containsKey(namespace) &&
              evaluatorsToDelete.get(namespace).contains(transferStep.getDstId())) {
            throw new RuntimeException(transferStep.getDstId() + " is planned for deletion.");
          } else if (evaluatorsToAdd.containsKey(namespace) &&
              evaluatorsToAdd.get(namespace).contains(transferStep.getSrcId())) {
            throw new RuntimeException(transferStep.getSrcId() + " is planned for addition.");
          }
        }
      }

      // build an execution graph considering dependency between steps
      final DAG<PlanOperation> dependencyGraph =
          constructDAG(evaluatorsToAdd, evaluatorsToDelete, allTransferSteps, numAvailableExtraEvaluators);

      return new PlanImpl(evaluatorsToAdd, evaluatorsToDelete, allTransferSteps, dependencyGraph);
    }

    /**
     * Constructs a directed acyclic graph based on the rules described in the comment of builder.
     * @throws IllegalStateException if the graph contains a cycle
     */
    private DAG<PlanOperation> constructDAG(final Map<String, Set<String>> namespaceToEvalsToAdd,
                                          final Map<String, Set<String>> namespaceToEvalsToDel,
                                          final Map<String, List<TransferStep>> namespaceToTransferSteps,
                                          final Optional<Integer> numAvailableExtraEvals) {

      final DAG<PlanOperation> dag = new DAGImpl<>();

      // add vertices of Delete and Stop
      final Map<String, PlanOperation> delOperations = new HashMap<>();
      final Map<String, PlanOperation> stopOperations = new HashMap<>();
      final Map<String, PlanOperation> syncOperations = new HashMap<>();
      for (final Map.Entry<String, Set<String>> entry : namespaceToEvalsToDel.entrySet()) {
        final String namespace = entry.getKey();
        final Set<String> evalsToDel = entry.getValue();
        for (final String evalToDel : evalsToDel) {
          final PlanOperation delOperation = new DeletePlanOperation(namespace, evalToDel);
          delOperations.put(evalToDel, delOperation);
          dag.addVertex(delOperation);

          if (namespace.equals(Constants.NAMESPACE_WORKER)) {
            final PlanOperation stopOperation
                = new StopPlanOperation(evalToDel);
            stopOperations.put(evalToDel, stopOperation);
            dag.addVertex(stopOperation);

            // add 'Stop->Del' edge for the exceptional case that Del does not accompany any Moves
            dag.addEdge(stopOperation, delOperation);

          } else { // NAMESPACE_SERVER
            final PlanOperation syncOperation
                = new SyncPlanOperation(evalToDel);
            syncOperations.put(evalToDel, syncOperation);
            dag.addVertex(syncOperation);

            // add 'Sync -> Del' edge here.
            // It means that Sync always precedes Del, even if Sync does not accompany any Moves
            dag.addEdge(syncOperation, delOperation);
          }
        }
      }

      // add vertices of Add and Start
      final Map<String, PlanOperation> addOperations = new HashMap<>();
      final Map<String, PlanOperation> startOperations = new HashMap<>();
      for (final Map.Entry<String, Set<String>> entry : namespaceToEvalsToAdd.entrySet()) {
        final String namespace = entry.getKey();
        final Set<String> evalsToAdd = entry.getValue();
        for (final String evalToAdd : evalsToAdd) {
          final PlanOperation addOperation = new AddPlanOperation(namespace, evalToAdd);
          addOperations.put(evalToAdd, addOperation);
          dag.addVertex(addOperation);

          if (namespace.equals(Constants.NAMESPACE_WORKER)) {
            final PlanOperation startOperation
                = new StartPlanOperation(evalToAdd);
            startOperations.put(evalToAdd, startOperation);
            dag.addVertex(startOperation);

            // add 'Add->Start' edge for the exceptional case that Add does not accompany any Moves
            dag.addEdge(addOperation, startOperation);
          }
        }
      }

      // add vertices of Move
      final List<PlanOperation> moveOperations = new LinkedList<>();
      for (final Map.Entry<String, List<TransferStep>> entry : namespaceToTransferSteps.entrySet()) {
        final String namespace = entry.getKey();
        final List<TransferStep> transferSteps = entry.getValue();

        for (final TransferStep transferStep : transferSteps) {
          final PlanOperation moveOperation = new MovePlanOperation(namespace, transferStep);
          moveOperations.add(moveOperation);
          dag.addVertex(moveOperation);
        }
      }

      // add edges representing dependencies between vertices

      // 1. del -> add
      // We need one Delete for each Add as much as the number of extra evaluator slots
      // is smaller than the number of evaluators to Add.
      // The current strategy simply maps one Delete and one Add that is not necessarily relevant with.
      if (numAvailableExtraEvals.isPresent()) {
        final int numRequiredExtraEvals = addOperations.size() - delOperations.size();
        if (numRequiredExtraEvals > numAvailableExtraEvals.get()) {
          throw new RuntimeException("Plan is infeasible, because it violates the resource limit");
        }

        final int numAddsShouldFollowDel = addOperations.size() - numAvailableExtraEvals.get();
        if (numAddsShouldFollowDel > 0) {
          LOG.log(Level.INFO, "{0} Adds should follow each one Delete.", numAddsShouldFollowDel);

          final Iterator<PlanOperation> delOperationsItr = delOperations.values().iterator();
          final Iterator<PlanOperation> addOperationsItr = addOperations.values().iterator();

          // pick each add/del operations with no special ordering
          for (int i = 0; i < numAddsShouldFollowDel; i++) {
            final PlanOperation addOperation = addOperationsItr.next();
            final PlanOperation delOperation = delOperationsItr.next();
            dag.addEdge(delOperation, addOperation);
          }
        }
      }

      for (final PlanOperation moveOperation : moveOperations) {
        final TransferStep transferStep = moveOperation.getTransferStep().get();
        final String srcId = transferStep.getSrcId();
        final String dstId = transferStep.getDstId();

        // 2. add -> move -> start
        if (addOperations.containsKey(dstId)) {
          final PlanOperation addOperation = addOperations.get(dstId);
          dag.addEdge(addOperation, moveOperation);

          if (moveOperation.getNamespace().equals(Constants.NAMESPACE_WORKER)) {
            final PlanOperation startOperation = startOperations.get(dstId);
            dag.addEdge(moveOperation, startOperation);
          }
        }

        // 3. move -> del
        if (delOperations.containsKey(srcId)) {
          final PlanOperation delOperation = delOperations.get(srcId);

          // worker: stop -> move -> del
          if (moveOperation.getNamespace().equals(Constants.NAMESPACE_WORKER)) {
            final PlanOperation stopOperation = stopOperations.get(srcId);
            dag.addEdge(stopOperation, moveOperation);
            dag.addEdge(moveOperation, delOperation);

          // server: move -> sync -> del
          } else { // NAMESPACE_SERVER
            final PlanOperation syncOperation = syncOperations.get(srcId);
            dag.addEdge(moveOperation, syncOperation);
            // 'Sync -> Del' edge is already added when adding the sync vertex
          }
        }
      }

      return dag;
    }
  }
}