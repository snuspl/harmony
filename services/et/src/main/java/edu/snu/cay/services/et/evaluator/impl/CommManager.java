/*
 * Copyright (C) 2018 Seoul National University
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
package edu.snu.cay.services.et.evaluator.impl;

import edu.snu.cay.services.et.configuration.parameters.remoteaccess.NumCommThreads;
import edu.snu.cay.services.et.configuration.parameters.remoteaccess.CommQueueSize;
import edu.snu.cay.utils.CatchableExecutors;
import org.apache.reef.tang.InjectionFuture;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by xyzi on 18/03/2018.
 */
public final class CommManager {
  private static final Logger LOG = Logger.getLogger(CommManager.class.getName());

  private final int numCommThreads;

  private final List<CommThread> commThreads;

  /**
   * A boolean flag that becomes true when {@link #close()} is called,
   * which consequently terminates all sender threads.
   */
  private volatile boolean closeFlag = false;

  private final InjectionFuture<RemoteAccessOpHandler> opHandlerFuture;
  private final InjectionFuture<RemoteAccessOpSender> opSenderFuture;

  @Inject
  private CommManager(final InjectionFuture<RemoteAccessOpHandler> opHandlerFuture,
                      final InjectionFuture<RemoteAccessOpSender> opSenderFuture,
                      @Parameter(CommQueueSize.class) final int queueSize,
                      @Parameter(NumCommThreads.class) final int numSenderThreads) {
    this.opHandlerFuture = opHandlerFuture;
    this.opSenderFuture = opSenderFuture;
    this.commThreads = initExecutor(numSenderThreads, queueSize);
    this.numCommThreads = numSenderThreads;
  }

  /**
   * Initialize threads that dequeue and execute operation from the {@code opQueue}.
   * That is, these threads serve operations requested from remote clients.
   */
  private List<CommThread> initExecutor(final int numThreads, final int queueSize) {
    LOG.log(Level.INFO, "Initializing {0} Handler threads with queue size: {1}",
        new Object[]{numThreads, queueSize});
    final ExecutorService executor = CatchableExecutors.newFixedThreadPool(numThreads);
    final List<CommThread> threads = new ArrayList<>(numThreads);
    for (int i = 0; i < numThreads; i++) {
      final CommThread commThread = new CommThread(queueSize);
      threads.add(commThread);
      executor.submit(commThread);
    }
    return threads;
  }

  /**
   * Close this {@link RemoteAccessOpHandler} by terminating all handler threads.
   */
  void close() {
    closeFlag = true;
  }

  private int getThreadIdx(final int blockId) {
    return blockId % numCommThreads;
  }

  void enqueueSenderOp(final RemoteDataOp senderOp) {
    final int threadIdx = getThreadIdx(senderOp.getMetadata().getBlockId());
    commThreads.get(threadIdx).enqueue(senderOp);
  }

  void enqueueHandlerOp(final DataOpMetadata handlerOpMetaData) {
    final RemoteDataOp handlerOp = new RemoteDataOp(handlerOpMetaData);
    final int threadIdx = getThreadIdx(handlerOpMetaData.getBlockId());
    commThreads.get(threadIdx).enqueue(handlerOp);
  }

  private final class CommThread implements Runnable {
    private static final int QUEUE_TIMEOUT_MS = 3000;
    private static final int DRAIN_PORTION = 16;
    private final PriorityBlockingQueue<RemoteDataOp> opQueue;

    private final ArrayList<RemoteDataOp> localOps;

    private final int drainSize;

    CommThread(final int queueSize) {
      this.opQueue = new PriorityBlockingQueue<>(queueSize);
      this.drainSize = queueSize >= DRAIN_PORTION ? queueSize / DRAIN_PORTION : 1;
      this.localOps = new ArrayList<>(drainSize);
    }

    @Override
    public void run() {
      while (!closeFlag) {
        try {
          final RemoteDataOp op = opQueue.poll(QUEUE_TIMEOUT_MS, TimeUnit.MILLISECONDS);
          if (op == null) {
            continue;
          }

          processOp(op);
        } catch (InterruptedException e) {
          continue;
        }

        opQueue.drainTo(localOps, drainSize);
        localOps.forEach(this::processOp);
        localOps.clear();
      }
    }

    /**
     * Enqueue operation into a thread's queue.
     * The operations will be processed sequentially by this thread.
     */
    void enqueue(final RemoteDataOp op) {
      LOG.log(Level.FINEST, "Enqueue Op. OpId: {0}, origId: {1}",
          new Object[]{op.getMetadata().getOpId(), op.getMetadata().getOrigId()});
      opQueue.put(op);
    }

    private <K, V, U> void processOp(final RemoteDataOp<K, V, U> op) {
      if (op.isHandlerOp()) {
        opHandlerFuture.get().processOp(op.getMetadata());
      } else {
        opSenderFuture.get().processOp(op);
      }
    }
  }
}
