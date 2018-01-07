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
package edu.snu.cay.services.et.evaluator;

import edu.snu.cay.services.et.evaluator.impl.TaskUnitInfo;
import edu.snu.cay.utils.CatchableExecutors;
import org.apache.commons.lang3.tuple.Pair;

import javax.inject.Inject;
import java.util.concurrent.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A class that schedules TaskUnits for CPU and Network resources.
 * Only one TaskUnit can run with each type of resources.
 */
public final class TaskUnitScheduler {
  private static final Logger LOG = Logger.getLogger(TaskUnitScheduler.class.getName());

  private final Semaphore cpuSemaphore = new Semaphore(1);
  private final Semaphore netSemaphore = new Semaphore(1);

  private final BlockingQueue<Pair<TaskUnitInfo, CountDownLatch>> cpuReadyQueue = new LinkedBlockingQueue<>();
  private final BlockingQueue<Pair<TaskUnitInfo, CountDownLatch>> netReadyQueue = new LinkedBlockingQueue<>();

  @Inject
  private TaskUnitScheduler() {
    CatchableExecutors.newSingleThreadExecutor().submit(() -> {
      while (true) {
        try {
          cpuSemaphore.acquire();
          final Pair<TaskUnitInfo, CountDownLatch> taskUnitPair = cpuReadyQueue.take();
          LOG.log(Level.INFO, "Schedule TaskUnit. TaskUnitInfo: {0}", taskUnitPair.getKey());
          taskUnitPair.getValue().countDown();
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      }
    });

    CatchableExecutors.newSingleThreadExecutor().submit(() -> {
      while (true) {
        try {
          netSemaphore.acquire();
          final Pair<TaskUnitInfo, CountDownLatch> taskUnitPair = netReadyQueue.take();
          LOG.log(Level.INFO, "Schedule TaskUnit. TaskUnitInfo: {0}", taskUnitPair.getKey());
          taskUnitPair.getValue().countDown();
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      }
    });
  }

  /**
   * Wait for TaskUnit to be scheduled.
   */
  public void waitSchedule(final TaskUnitInfo taskUnitInfo) {
    LOG.log(Level.INFO, "Wait for schedule. TaskUnitInfo: {0}", taskUnitInfo);
    try {
      final Pair<TaskUnitInfo, CountDownLatch> taskUnitPair = Pair.of(taskUnitInfo, new CountDownLatch(1));
      switch (taskUnitInfo.getResourceType()) {
      case CPU:
        cpuReadyQueue.put(taskUnitPair);
        break;
      case NET:
        netReadyQueue.put(taskUnitPair);
        break;
      default:
        throw new RuntimeException();
      }
      taskUnitPair.getValue().await();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Should be called when a TaskUnit has been finished.
   */
  public void onTaskUnitFinished(final TaskUnitInfo taskUnitInfo) {
    LOG.log(Level.INFO, "TaskUnit finished. TaskUnitInfo: {0}", taskUnitInfo);
    switch (taskUnitInfo.getResourceType()) {
    case CPU:
      cpuSemaphore.release();
      break;
    case NET:
      netSemaphore.release();
      break;
    default:
      throw new RuntimeException();
    }
  }
}
