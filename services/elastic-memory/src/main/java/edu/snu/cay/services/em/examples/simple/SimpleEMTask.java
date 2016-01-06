/*
 * Copyright (C) 2015 Seoul National University
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
package edu.snu.cay.services.em.examples.simple;

import edu.snu.cay.services.em.evaluator.api.DataIdFactory;
import edu.snu.cay.services.em.evaluator.api.MemoryStore;
import edu.snu.cay.services.em.evaluator.api.PartitionTracker;
import edu.snu.cay.services.em.exceptions.IdGenerationException;
import edu.snu.cay.services.em.examples.simple.parameters.Iterations;
import edu.snu.cay.services.em.examples.simple.parameters.PeriodMillis;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.task.HeartBeatTriggerManager;
import org.apache.reef.task.Task;

import javax.inject.Inject;
import java.util.List;
import java.util.logging.Logger;

final class SimpleEMTask implements Task {
  private static final Logger LOG = Logger.getLogger(SimpleEMTask.class.getName());
  public static final String DATATYPE = "INTEGER";
  private static final int NUM_DATA = 10;

  private final MemoryStore memoryStore;
  private final SimpleEMTaskReady simpleEMTaskReady;
  private final HeartBeatTriggerManager heartBeatTriggerManager;
  private final int iterations;
  private final long periodMillis;

  @Inject
  private SimpleEMTask(
      final MemoryStore memoryStore,
      final SimpleEMTaskReady simpleEMTaskReady,
      final HeartBeatTriggerManager heartBeatTriggerManager,
      final PartitionTracker partitionTracker,
      final DataIdFactory<Long> dataIdFactory,
      @Parameter(Iterations.class) final int iterations,
      @Parameter(PeriodMillis.class) final long periodMillis) throws IdGenerationException {
    this.memoryStore = memoryStore;
    this.simpleEMTaskReady = simpleEMTaskReady;
    this.heartBeatTriggerManager = heartBeatTriggerManager;
    this.iterations = iterations;
    this.periodMillis = periodMillis;

    final List<Long> ids = dataIdFactory.getIds(NUM_DATA);
    // Just use ids as data (data do not matter)
    this.memoryStore.putList(DATATYPE, ids, ids);

    partitionTracker.registerPartition(DATATYPE, ids.get(0), ids.get(NUM_DATA - 1));
  }

  public byte[] call(final byte[] memento) throws InterruptedException {
    LOG.info("SimpleEMTask commencing...");

    LOG.info("Before sleep, memory store contains: ");
    LOG.info(memoryStore.getAll(DATATYPE).toString());
    // Should be either
    // [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
    // or
    // [2^32, 2^32+1, 2^32+2, 2^32+3, 2^32+4, 2^32+5, 2^32+6, 2^32+7, 2^32+8, 2^32+9]

    simpleEMTaskReady.setReady(true);
    heartBeatTriggerManager.triggerHeartBeat();

    // Sleep one more period than the Driver, to give the Driver time to finish
    final long sleepMillis = (iterations + 1) * periodMillis;
    LOG.info("Sleep for: " + sleepMillis);
    Thread.sleep(sleepMillis);

    LOG.info("After sleep, memory store contains: ");
    LOG.info(memoryStore.getAll(DATATYPE).toString());
    // Evaluator that receives on the last iteration should have more than before
    // Evaluator that sends on the last iterations should have less than before

    return null;
  }
}
