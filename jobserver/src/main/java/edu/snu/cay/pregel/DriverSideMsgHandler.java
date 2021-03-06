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

import edu.snu.cay.jobserver.TaskletJobMsg;
import edu.snu.cay.services.et.evaluator.api.TaskletCustomMsgHandler;
import edu.snu.cay.utils.AvroUtils;
import org.apache.reef.tang.InjectionFuture;

import javax.inject.Inject;

/**
 * A driver-side message handler that routes messages to {@link PregelMaster}.
 */
public final class DriverSideMsgHandler implements TaskletCustomMsgHandler {

  private final InjectionFuture<PregelMaster> pregelMasterFuture;

  @Inject
  private DriverSideMsgHandler(final InjectionFuture<PregelMaster> pregelMasterFuture) {
    this.pregelMasterFuture = pregelMasterFuture;
  }

  @Override
  public void onNext(final byte[] bytes) {
    final TaskletJobMsg taskletJobMsg = AvroUtils.fromBytes(bytes, TaskletJobMsg.class);
    final byte[] jobMsg = taskletJobMsg.getJobMsg().array();

    final SuperstepResultMsg resultMsg = AvroUtils.fromBytes(jobMsg, SuperstepResultMsg.class);

    pregelMasterFuture.get().onWorkerMsg(resultMsg);
  }
}
