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
package edu.snu.cay.dolphin.core.worker;

import org.apache.reef.exception.evaluator.NetworkException;
import org.apache.reef.io.network.group.impl.utils.ResettingCountDownLatch;

import javax.inject.Inject;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A worker-side barrier at every mini-batch to synchronize with other workers.
 */
public final class MiniBatchBarrier {
  private static final Logger LOG = Logger.getLogger(MiniBatchBarrier.class.getName());

  private final ResettingCountDownLatch countDownLatch = new ResettingCountDownLatch(1);

  private final WorkerSideMsgSender workerSideMsgSender;

  private final AtomicBoolean stopFlag = new AtomicBoolean(false);

  @Inject
  private MiniBatchBarrier(final WorkerSideMsgSender workerSideMsgSender) {
    this.workerSideMsgSender = workerSideMsgSender;
  }

  /**
   * Wait until all workers have met at the barrier.
   * @return True if workers have run all mini-batches
   */
  public boolean await() throws NetworkException {
    LOG.log(Level.INFO, "Sending a mini-batch sync message");
    workerSideMsgSender.sendMiniBatchSyncMsg();

    countDownLatch.awaitAndReset(1);
    LOG.log(Level.INFO, "Release from barrier");

    return stopFlag.get();
  }

  /**
   * Handles release msgs from driver.
   */
  synchronized void onRelease(final boolean stop) {
    LOG.log(Level.FINE, "Received a release message");
    stopFlag.compareAndSet(false, stop);
    countDownLatch.countDown();
  }
}
