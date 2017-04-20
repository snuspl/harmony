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
package edu.snu.cay.dolphin.async.plan;

import edu.snu.cay.dolphin.async.ETDolphinDriver;
import edu.snu.cay.services.em.plan.api.Plan;
import edu.snu.cay.services.em.plan.api.TransferStep;
import edu.snu.cay.services.et.configuration.ExecutorConfiguration;
import edu.snu.cay.services.et.plan.api.Op;
import edu.snu.cay.services.et.plan.impl.ETPlan;
import edu.snu.cay.services.et.plan.impl.op.*;
import edu.snu.cay.utils.DAG;
import edu.snu.cay.utils.DAGImpl;
import org.apache.reef.tang.InjectionFuture;

import javax.inject.Inject;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

import static edu.snu.cay.dolphin.async.ETModelAccessor.MODEL_TABLE_ID;
import static edu.snu.cay.dolphin.async.ETTrainingDataProvider.TRAINING_DATA_TABLE_ID;
import static edu.snu.cay.dolphin.async.optimizer.parameters.Constants.NAMESPACE_WORKER;
import static edu.snu.cay.dolphin.async.optimizer.parameters.Constants.NAMESPACE_SERVER;

/**
 * A plan compiler that compiles down Dolphin's plan to {@link ETPlan}.
 */
public final class PlanCompiler {
  private static final Logger LOG = Logger.getLogger(PlanCompiler.class.getName());

  private final InjectionFuture<ETDolphinDriver> etDolphinDriverFuture;

  @Inject
  private PlanCompiler(final InjectionFuture<ETDolphinDriver> etDolphinDriverFuture) {
    this.etDolphinDriverFuture = etDolphinDriverFuture;
  }

  /**
   * Compiles a Dolphin's plan to {@link ETPlan}.
   * @param dolphinPlan a Dolphin's plan
   * @param numAvailableExtraEvals an
   * @return a {@link ETPlan}
   */
  public ETPlan compile(final Plan dolphinPlan, final int numAvailableExtraEvals) {
    final Map<String, Collection<String>> namespaceToEvalsToAdd = new HashMap<>();
    namespaceToEvalsToAdd.put(NAMESPACE_WORKER, dolphinPlan.getEvaluatorsToAdd(NAMESPACE_WORKER));
    namespaceToEvalsToAdd.put(NAMESPACE_SERVER, dolphinPlan.getEvaluatorsToAdd(NAMESPACE_SERVER));

    final Map<String, Collection<String>> namespaceToEvalsToDel = new HashMap<>();
    namespaceToEvalsToDel.put(NAMESPACE_WORKER, dolphinPlan.getEvaluatorsToDelete(NAMESPACE_WORKER));
    namespaceToEvalsToDel.put(NAMESPACE_SERVER, dolphinPlan.getEvaluatorsToDelete(NAMESPACE_SERVER));

    final Map<String, Collection<String>> namespaceToEvalsToSwitch = Collections.emptyMap();

    final Map<String, Collection<TransferStep>> namespaceToTransferSteps = new HashMap<>();
    namespaceToTransferSteps.put(NAMESPACE_WORKER, dolphinPlan.getTransferSteps(NAMESPACE_WORKER));
    namespaceToTransferSteps.put(NAMESPACE_SERVER, dolphinPlan.getTransferSteps(NAMESPACE_SERVER));

    return buildPlan(namespaceToEvalsToAdd, namespaceToEvalsToDel, namespaceToEvalsToSwitch,
        namespaceToTransferSteps, numAvailableExtraEvals);
  }

  private ETPlan buildPlan(final Map<String, Collection<String>> namespaceToEvalsToAdd,
                           final Map<String, Collection<String>> namespaceToEvalsToDel,
                           final Map<String, Collection<String>> dstNamespaceToEvalsToSwitch,
                           final Map<String, Collection<TransferStep>> namespaceToTransferSteps,
                           final int numAvailableExtraEvals) {
    final DAG<Op> dag = new DAGImpl<>();

    final Map<String, AllocateOp> allocateOps = new HashMap<>();
    final Map<String, DeallocateOp> deallocateOps = new HashMap<>();

    final Map<String, StartOp> startOps = new HashMap<>();
    final Map<String, StopOp> stopOps = new HashMap<>();

    final Map<String, AssociateOp> associateOps = new HashMap<>();
    final Map<String, UnassociateOp> unassociateOps = new HashMap<>();

    final Map<String, SubscribeOp> subscribeOps = new HashMap<>();
    final Map<String, UnsubscribeOp> unsubscribeOps = new HashMap<>();

    final List<MoveOp> moveOps = new LinkedList<>();

    handleDelete(namespaceToEvalsToDel, dag, deallocateOps, stopOps, unassociateOps, unsubscribeOps);
    
    handleAdd(namespaceToEvalsToAdd, dag, allocateOps, startOps, associateOps, subscribeOps);

    resolveAddDelDependency(numAvailableExtraEvals, dag, allocateOps, deallocateOps);

    handleSwitch(dstNamespaceToEvalsToSwitch, dag);

    handleMove(namespaceToTransferSteps, dag, associateOps, unassociateOps, moveOps, stopOps, startOps);

    final int numTotalOps = allocateOps.size() + deallocateOps.size()
        + moveOps.size()
        + startOps.size() + stopOps.size()
        + associateOps.size() + unassociateOps.size()
        + subscribeOps.size() + unsubscribeOps.size();

    return new ETPlan(dag, numTotalOps);
  }

  private void resolveAddDelDependency(final int numAvailableExtraEvals,
                                       final DAG<Op> dag,
                                       final Map<String, AllocateOp> allocateOps,
                                       final Map<String, DeallocateOp> deallocateOps) {
    // We need one Delete for each Add as much as the number of extra evaluators slots
    // is smaller than the number of evaluators to Add.
    // The current strategy simply maps one Delete and one Add that is not necessarily relevant with.
    final int numRequiredExtraEvals = allocateOps.size() - deallocateOps.size();
    if (numRequiredExtraEvals > numAvailableExtraEvals) {
      throw new RuntimeException("Infeasible plan; it tries to use more resources than allowed.");
    }

    final int numAddsShouldFollowDel = allocateOps.size() - numAvailableExtraEvals;
    if (numAddsShouldFollowDel > 0) {
      LOG.log(Level.FINE, "{0} Allocates should follow the same number of Deallocates.", numAddsShouldFollowDel);

      final Iterator<DeallocateOp> deallocateOpsItr = deallocateOps.values().iterator();
      final Iterator<AllocateOp> allocateOpsItr = allocateOps.values().iterator();

      // pick each add/del operations with no special ordering
      for (int i = 0; i < numAddsShouldFollowDel; i++) {
        final Op addOp = allocateOpsItr.next();
        final Op delOp = deallocateOpsItr.next();
        dag.addEdge(delOp, addOp);
      }
    }
  }

  private void handleSwitch(final Map<String, Collection<String>> dstNamespaceToEvalsToSwitch,
                            final DAG<Op> dag) {
    // only workers need Start and Stop ops
    for (final Map.Entry<String, Collection<String>> entry : dstNamespaceToEvalsToSwitch.entrySet()) {
      final String dstNamespace = entry.getKey();
      final Collection<String> executors = entry.getValue();

      // server -> worker
      if (dstNamespace.equals(NAMESPACE_WORKER)) {
        for (final String executor : executors) {
          dag.addVertex(new StartOp(executor, etDolphinDriverFuture.get().getWorkerTaskConf()));
        }

      // worker -> server
      } else {
        for (final String executor : executors) {
          dag.addVertex(new StopOp(executor));
        }
      }
    }
  }

  private void handleDelete(final Map<String, Collection<String>> namespaceToEvalsToDel,
                            final DAG<Op> dag,
                            final Map<String, DeallocateOp> deallocateOps,
                            final Map<String, StopOp> stopOps,
                            final Map<String, UnassociateOp> unassociateOps,
                            final Map<String, UnsubscribeOp> unsubscribeOps) {
    for (final Map.Entry<String, Collection<String>> entry : namespaceToEvalsToDel.entrySet()) {
      final String namespace = entry.getKey();

      final String tableIdToUnassociate = namespace.equals(NAMESPACE_WORKER) ? TRAINING_DATA_TABLE_ID : MODEL_TABLE_ID;

      final Collection<String> evalsToDel = entry.getValue();
      for (final String evalToDel : evalsToDel) {
        final DeallocateOp deallocateOp = new DeallocateOp(evalToDel);
        deallocateOps.put(evalToDel, deallocateOp);
        dag.addVertex(deallocateOp);

        final UnassociateOp unassociateOp = new UnassociateOp(evalToDel, tableIdToUnassociate);
        unassociateOps.put(evalToDel, unassociateOp);
        dag.addVertex(unassociateOp);
        dag.addEdge(unassociateOp, deallocateOp);

        if (namespace.equals(NAMESPACE_WORKER)) {
          final StopOp stopOp = new StopOp(evalToDel);
          stopOps.put(evalToDel, stopOp);
          final UnsubscribeOp unsubscribeOp = new UnsubscribeOp(evalToDel, MODEL_TABLE_ID);
          unsubscribeOps.put(evalToDel, unsubscribeOp);

          dag.addVertex(stopOp);
          dag.addVertex(unsubscribeOp);
          dag.addEdge(stopOp, unsubscribeOp);
          dag.addEdge(unsubscribeOp, deallocateOp);
        }
      }
    }
  }

  private void handleAdd(final Map<String, Collection<String>> namespaceToEvalsToAdd,
                         final DAG<Op> dag,
                         final Map<String, AllocateOp> allocateOps,
                         final Map<String, StartOp> startOps,
                         final Map<String, AssociateOp> associateOps,
                         final Map<String, SubscribeOp> subscribeOps)  {
    for (final Map.Entry<String, Collection<String>> entry : namespaceToEvalsToAdd.entrySet()) {
      final String namespace = entry.getKey();
      final Collection<String> evalsToAdd = entry.getValue();

      final ExecutorConfiguration executorConf = namespace.equals(NAMESPACE_WORKER) ?
          etDolphinDriverFuture.get().getWorkerExecutorConf() : etDolphinDriverFuture.get().getServerExecutorConf();
      final String tableIdToAssociate = namespace.equals(NAMESPACE_WORKER) ? TRAINING_DATA_TABLE_ID : MODEL_TABLE_ID;

      for (final String evalToAdd : evalsToAdd) {
        final AllocateOp allocateOp = new AllocateOp(evalToAdd, executorConf);
        allocateOps.put(evalToAdd, allocateOp);
        dag.addVertex(allocateOp);

        final AssociateOp associateOp = new AssociateOp(evalToAdd, tableIdToAssociate);
        associateOps.put(evalToAdd, associateOp);
        dag.addVertex(associateOp);
        dag.addEdge(allocateOp, associateOp);

        if (namespace.equals(NAMESPACE_WORKER)) {
          final StartOp startOp = new StartOp(evalToAdd, etDolphinDriverFuture.get().getWorkerTaskConf());
          startOps.put(evalToAdd, startOp);
          final SubscribeOp subscribeOp = new SubscribeOp(evalToAdd, MODEL_TABLE_ID);
          subscribeOps.put(evalToAdd, subscribeOp);

          dag.addVertex(startOp);
          dag.addVertex(subscribeOp);
          dag.addEdge(allocateOp, subscribeOp);
          dag.addEdge(subscribeOp, startOp);
        }
      }
    }
  }

  private void handleMove(final Map<String, Collection<TransferStep>> namespaceToTransferSteps,
                          final DAG<Op> dag,
                          final Map<String, AssociateOp> associateOps, final Map<String, UnassociateOp> unassociateOps,
                          final List<MoveOp> moveOps,
                          final Map<String, StopOp> stopOps, final Map<String, StartOp> startOps) {
    // add vertices of Move
    for (final Map.Entry<String, Collection<TransferStep>> entry : namespaceToTransferSteps.entrySet()) {
      final String namespace = entry.getKey();
      final Collection<TransferStep> transferSteps = entry.getValue();

      final String tableId = namespace.equals(NAMESPACE_WORKER) ? TRAINING_DATA_TABLE_ID : MODEL_TABLE_ID;

      for (final TransferStep transferStep : transferSteps) {
        final MoveOp moveOp = new MoveOp(transferStep.getSrcId(), transferStep.getDstId(),
            tableId, transferStep.getDataInfo().getNumBlocks());
        moveOps.add(moveOp);
        dag.addVertex(moveOp);
      }
    }

    // resolve dependencies of Move
    for (final MoveOp moveOp : moveOps) {
      final String srcId = moveOp.getSrcExecutorId();
      final String dstId = moveOp.getDstExecutorId();

      // associate -> move dependency
      if (associateOps.containsKey(dstId)) {
        final AssociateOp associateOp = associateOps.get(dstId);
        dag.addEdge(associateOp, moveOp);
      }

      // move -> start dependency
      if (startOps.containsKey(dstId)) {
        final StartOp startOp = startOps.get(dstId);
        dag.addEdge(moveOp, startOp);
      }

      // move -> unassociate dependency
      if (unassociateOps.containsKey(srcId)) {
        final UnassociateOp unassociateOp = unassociateOps.get(srcId);
        dag.addEdge(moveOp, unassociateOp);
      }

      // stop -> move dependency
      if (stopOps.containsKey(srcId)) {
        final StopOp stopOp = stopOps.get(srcId);
        dag.addEdge(stopOp, moveOp);
      }
    }
  }
}