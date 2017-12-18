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

import edu.snu.cay.jobserver.Parameters;
import edu.snu.cay.services.et.common.util.TaskletUtils;
import edu.snu.cay.services.et.configuration.TaskletConfiguration;
import edu.snu.cay.services.et.driver.api.AllocatedExecutor;
import edu.snu.cay.services.et.driver.api.AllocatedTable;
import edu.snu.cay.services.et.driver.impl.RunningTasklet;
import edu.snu.cay.utils.AvroUtils;
import edu.snu.cay.utils.ConfigurationUtils;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.exception.evaluator.NetworkException;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Configurations;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A Pregel master that communicates with workers using CentComm services.
 * It synchronizes all workers in a single superstep by checking messages that all workers have sent.
 */
@DriverSide
public final class PregelMaster {
  private static final Logger LOG = Logger.getLogger(PregelMaster.class.getName());
  private static final String WORKER_PREFIX = "Worker-";

  private final Set<String> workerIds;

  /**
   * These two values are updated by the results of every worker at the end of a single superstep.
   * Master checks whether 1) all vertices in workers are halt or not, 2) and any ongoing messages are exist or not.
   * When both values are true, {@link PregelMaster} stops workers to finish the job.
   */
  private volatile boolean isAllVerticesHalt;
  private volatile boolean isNoOngoingMsgs;

  private volatile CountDownLatch msgCountDownLatch;

  private final int numWorkers;
  private final AtomicInteger workerCounter = new AtomicInteger(0);

  private final Configuration taskConf;
  private final String vertexTableId;
  private final String msgTableId;

  private final String jobId;

  private final Map<String, RunningTasklet> runningTaskletMap = new ConcurrentHashMap<>();

  @Inject
  private PregelMaster(@Parameter(PregelParameters.SerializedTaskletConf.class) final String serializedTaskConf,
                       @Parameter(PregelParameters.VertexTableId.class) final String vertexTableId,
                       @Parameter(PregelParameters.MessageTableId.class) final String msgTableId,
                       @Parameter(Parameters.JobId.class) final String jobId,
                       @Parameter(PregelParameters.NumExecutors.class) final int numWorkers) throws IOException {
    this.msgCountDownLatch = new CountDownLatch(numWorkers);
    this.workerIds = Collections.synchronizedSet(new HashSet<String>(numWorkers));
    this.isAllVerticesHalt = true;
    this.isNoOngoingMsgs = true;
    this.numWorkers = numWorkers;
    this.taskConf = ConfigurationUtils.fromString(serializedTaskConf);
    this.vertexTableId = vertexTableId;
    this.msgTableId = msgTableId;
    this.jobId = jobId;
  }

  public void start(final List<AllocatedExecutor> executors,
                    final AllocatedTable vertexTable,
                    final AllocatedTable msgTable1,
                    final AllocatedTable msgTable2) {
    initControlThread();

    final List<Future<RunningTasklet>> taskletFutureList = new ArrayList<>();
    executors.forEach(executor -> taskletFutureList.add(executor.submitTasklet(buildTaskletConf())));

    taskletFutureList.forEach(taskletFuture -> {
      try {
        final RunningTasklet tasklet = taskletFuture.get();
        runningTaskletMap.put(tasklet.getId(), tasklet);
      } catch (InterruptedException | ExecutionException e) {
        throw new RuntimeException(e);
      }
    });

    TaskletUtils.waitAndCheckTaskletResult(taskletFutureList, true);
  }

  private TaskletConfiguration buildTaskletConf() {
    return TaskletConfiguration.newBuilder()
        .setId(WORKER_PREFIX + workerCounter.getAndIncrement())
        .setTaskletClass(PregelWorkerTask.class)
        .setTaskletMsgHandlerClass(WorkerMsgManager.class)
        .setUserParamConf(Configurations.merge(
            Tang.Factory.getTang().newConfigurationBuilder()
                .bindNamedParameter(Parameters.JobId.class, jobId)
                .bindNamedParameter(PregelParameters.VertexTableId.class, vertexTableId)
                .bindNamedParameter(PregelParameters.MessageTableId.class, msgTableId)
                .build(),
            taskConf)).build();
  }

  private void initControlThread() {

    LOG.log(Level.INFO, "Start a thread that controls workers...");
    // submit a runnable that controls workers' supersteps.
    new Thread(() -> {
      while (true) {
        try {
          msgCountDownLatch.await();
        } catch (final InterruptedException e) {
          throw new RuntimeException("Unexpected exception", e);
        }

        final ControlMsgType controlMsgType = isAllVerticesHalt && isNoOngoingMsgs
            ? ControlMsgType.Stop : ControlMsgType.Start;
        final SuperstepControlMsg controlMsg = SuperstepControlMsg.newBuilder()
            .setType(controlMsgType)
            .build();

        workerIds.forEach(workerId -> {
          try {
            runningTaskletMap.get(workerId).send(AvroUtils.toBytes(controlMsg, SuperstepControlMsg.class));
          } catch (NetworkException e) {
            throw new RuntimeException(e);
          }
        });

        if (controlMsgType.equals(ControlMsgType.Stop)) {
          break;
        }

        // reset for next superstep
        isAllVerticesHalt = true;
        isNoOngoingMsgs = true;
        msgCountDownLatch = new CountDownLatch(numWorkers);
      }
    }).start();
  }

  /**
   * Handles {@link SuperstepResultMsg} from workers.
   */
  public void onWorkerMsg(final String workerId, final SuperstepResultMsg resultMsg) {
    if (!workerIds.contains(workerId)) {
      workerIds.add(workerId);
    }

    LOG.log(Level.INFO, "isAllVerticesHalt : {0}, isNoOngoingMsgs : {1}",
        new Object[]{resultMsg.getIsAllVerticesHalt(), resultMsg.getIsNoOngoingMsgs()});
    isAllVerticesHalt &= resultMsg.getIsAllVerticesHalt();
    isNoOngoingMsgs &= resultMsg.getIsNoOngoingMsgs();

    msgCountDownLatch.countDown();
  }
}
