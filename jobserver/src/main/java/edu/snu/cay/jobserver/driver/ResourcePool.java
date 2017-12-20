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
package edu.snu.cay.jobserver.driver;

import edu.snu.cay.jobserver.Parameters;
import edu.snu.cay.services.et.configuration.ExecutorConfiguration;
import edu.snu.cay.services.et.configuration.RemoteAccessConfiguration;
import edu.snu.cay.services.et.configuration.ResourceConfiguration;
import edu.snu.cay.services.et.driver.api.AllocatedExecutor;
import edu.snu.cay.services.et.driver.api.ETMaster;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * Created by xyzi on 30/11/2017.
 */
final class ResourcePool {

  private final ETMaster etMaster;

  private final int numExecutors;
  private final ExecutorConfiguration executorConfiguration;
  private final Map<String, AllocatedExecutor> executorMap = new ConcurrentHashMap<>();

  @Inject
  private ResourcePool(final ETMaster etMaster,
                       @Parameter(Parameters.NumExecutors.class) final int numExecutors,
                       @Parameter(Parameters.ExecutorMemSize.class) final int executorMemSize,
                       @Parameter(Parameters.ExecutorNumCores.class) final int executorNumCores,
                       @Parameter(Parameters.ExecutorNumTasklets.class) final int executorNumTasklets,
                       @Parameter(Parameters.HandlerQueueSize.class) final int handlerQueueSize,
                       @Parameter(Parameters.SenderQueueSize.class) final int senderQueueSize,
                       @Parameter(Parameters.HandlerNumThreads.class) final int handlerNumThreads,
                       @Parameter(Parameters.SenderNumThreads.class) final int senderNumThreads) {
    this.etMaster = etMaster;
    this.numExecutors = numExecutors;
    this.executorConfiguration = ExecutorConfiguration.newBuilder()
        .setNumTasklets(executorNumTasklets)
        .setResourceConf(ResourceConfiguration.newBuilder()
            .setNumCores(executorNumCores)
            .setMemSizeInMB(executorMemSize)
            .build())
        .setRemoteAccessConf(RemoteAccessConfiguration.newBuilder()
            .setHandlerQueueSize(handlerQueueSize)
            .setSenderQueueSize(senderQueueSize)
            .setNumHandlerThreads(handlerNumThreads)
            .setNumSenderThreads(senderNumThreads)
            .build())
        .build();
  }

  void init() {
    try {
      final List<AllocatedExecutor> executors = etMaster.addExecutors(numExecutors, executorConfiguration).get();
      executors.forEach(executor -> executorMap.put(executor.getId(), executor));

    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException(e);
    }
  }

  void close() {
    final List<Future> executorCloseFutures = new ArrayList<>(executorMap.size());

    executorMap.values().forEach(executor -> executorCloseFutures.add(executor.close()));

    executorCloseFutures.forEach(future -> {
      try {
        future.get();
      } catch (InterruptedException | ExecutionException e) {
        throw new RuntimeException(e);
      }
    });
  }


  Map<String, AllocatedExecutor> getExecutors() {
    return Collections.unmodifiableMap(executorMap);
  }
}
