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
package edu.snu.cay.dolphin.optimizer.impl.hetero;

import edu.snu.cay.dolphin.optimizer.impl.DataInfoImpl;
import edu.snu.cay.dolphin.optimizer.parameters.Constants;
import edu.snu.cay.dolphin.plan.impl.ILPPlanDescriptor;
import edu.snu.cay.dolphin.plan.impl.TransferStepImpl;

import java.util.Comparator;
import java.util.PriorityQueue;

/**
 * Generate ILP plan with the result solved by ILP solver. Generated ILP plan is saved in {@link ILPPlanDescriptor}.
 */
public final class ILPPlanGenerator {
  private ILPPlanGenerator() {
  }

  /**
   * Generate {@link ILPPlanDescriptor} with solution solved by ILP solver.
   * 1. Check whether each evaluator's role is changed. Register the evaluator to the {@link ILPPlanDescriptor} if its
   *    role is changed.
   * 2. Register transferring plans to {@link ILPPlanDescriptor} for both workers and servers.
   *
   * @param oldRole role of each evaluator before optimization is applied.
   * @param oldDataBlockNum number of blocks of data in each evaluator before optimization is applied (for workers).
   * @param oldModelBlockNum number of blocks of models in each evaluator before optimization is applied (for servers).
   * @param newRole role of each evaluator after optimization is applied.
   * @param newDataBlockNum number of blocks of data in each evaluator after optimization is applied (for workers).
   * @param newModelBlockNum number of blocks of models in each evaluator after optimization is applied (for servers).
   * @return block transferring plan for optimization.
   */
  public static ILPPlanDescriptor generatePlanDescriptor(final String[] evalIds,
                                                         final int[] oldRole,
                                                         final int[] oldDataBlockNum,
                                                         final int[] oldModelBlockNum,
                                                         final int[] newRole,
                                                         final int[] newDataBlockNum,
                                                         final int[] newModelBlockNum) {
    final int numTotalEval = evalIds.length;
    final ILPPlanDescriptor.Builder planBuilder = ILPPlanDescriptor.newBuilder();
    
    // check whether each evaluator's role is changed or not
    for (int i = 0; i < numTotalEval; i++) {
      if (oldRole[i] != newRole[i]) {
        if (oldRole[i] == EvaluatorRole.WORKER.getValue()) {
          // this evaluator is changed from worker to server
          planBuilder.addEvaluatorToAdd(Constants.NAMESPACE_SERVER, evalIds[i]);
          planBuilder.addEvaluatorToDelete(Constants.NAMESPACE_WORKER, evalIds[i]);
        } else if (oldRole[i] == EvaluatorRole.SERVER.getValue()) {
          // this evaluator is changed from server to worker
          planBuilder.addEvaluatorToAdd(Constants.NAMESPACE_WORKER, evalIds[i]);
          planBuilder.addEvaluatorToDelete(Constants.NAMESPACE_SERVER, evalIds[i]);
        } else {
          throw new RuntimeException("Evaluator's role is wrongly assigned.");
        }
      }
    }
    
    // generate transfer steps for worker
    generateTransferSteps(Constants.NAMESPACE_WORKER, oldDataBlockNum, newDataBlockNum, planBuilder, evalIds);
    // generate transfer steps for server
    generateTransferSteps(Constants.NAMESPACE_SERVER, oldModelBlockNum, newModelBlockNum, planBuilder, evalIds);
    
    return planBuilder.build();
  }

  /**
   * Generate block transfer steps to apply optimization.
   * 1. Distinguish sender and receiver by comparing {@param oldBlockNum} and {@param newBlockNum}.
   * 2. Sort {@code senderPriorityQueue} and {@code receiverPriorityQueue} in descending order with the number of
   *    blocks to transfer.
   * 3. Add transferring plan while both queue is not empty.
   *
   * @param namespace indicates whether this plan is for server or worker.
   * @param oldBlockNum number of blocks in each evaluator before optimization is applied.
   * @param newBlockNum number of blocks in each evaluator after optimization is applied.
   */
  private static void generateTransferSteps(final String namespace, final int[] oldBlockNum, final int[] newBlockNum,
                                            final ILPPlanDescriptor.Builder planBuilder, final String[] evalIds) {
    final int numTotalEval = oldBlockNum.length;
    final PriorityQueue<BlockDelta> senderPriorityQueue =
        new PriorityQueue<>(numTotalEval, NUM_BLOCKS_TO_MOVE_COMPARATOR);
    final PriorityQueue<BlockDelta> receiverPriorityQueue =
        new PriorityQueue<>(numTotalEval, NUM_BLOCKS_TO_MOVE_COMPARATOR);
    for (int i = 0; i < numTotalEval; i++) {
      final int numBlocksToMove = oldBlockNum[i] - newBlockNum[i];
      if (numBlocksToMove > 0) {
        senderPriorityQueue.add(new BlockDelta(numBlocksToMove, i));
      } else if (numBlocksToMove < 0) {
        receiverPriorityQueue.add(new BlockDelta(-numBlocksToMove, i));
      }
    }
    
    while (!senderPriorityQueue.isEmpty() && !receiverPriorityQueue.isEmpty()) {
      final BlockDelta sender = senderPriorityQueue.poll();
      final BlockDelta receiver = receiverPriorityQueue.poll();
      
      final int numToSend = sender.getNumBlocksToMove();
      final int numToReceive = receiver.getNumBlocksToMove();
      final int numToMove = Math.min(sender.getNumBlocksToMove(), receiver.getNumBlocksToMove());
      
      planBuilder.addTransferStep(namespace,
          new TransferStepImpl(evalIds[sender.getEvalIdx()], evalIds[receiver.getEvalIdx()],
              new DataInfoImpl(numToMove)));
      
      if (numToSend == numToReceive) {
        continue;
      } else if (numToMove == numToSend) {
        receiver.setNumBlocksToMove(numToReceive - numToMove);
        receiverPriorityQueue.add(receiver);
      } else {
        sender.setNumBlocksToMove(numToSend - numToMove);
        senderPriorityQueue.add(sender);
      }
    }

    // Since total number of blocks in system should be constant, total number of receiving blocks and sending blocks
    // should be the same.
    if (!senderPriorityQueue.isEmpty() || !receiverPriorityQueue.isEmpty()) {
      throw new RuntimeException("Sender queue or receiver queue is not matched");
    }
  }
  
  private static final Comparator<BlockDelta> NUM_BLOCKS_TO_MOVE_COMPARATOR =
      (o1, o2) -> o2.getNumBlocksToMove() - o1.getNumBlocksToMove();
}
