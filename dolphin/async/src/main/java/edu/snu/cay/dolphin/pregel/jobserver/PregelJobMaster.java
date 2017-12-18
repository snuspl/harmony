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
package edu.snu.cay.dolphin.pregel.jobserver;

import edu.snu.cay.dolphin.jobserver.driver.JobMaster;
import edu.snu.cay.dolphin.pregel.PregelMaster;
import edu.snu.cay.dolphin.pregel.SuperstepResultMsg;
import edu.snu.cay.services.et.driver.api.AllocatedExecutor;
import edu.snu.cay.services.et.driver.api.AllocatedTable;
import edu.snu.cay.utils.AvroUtils;

import javax.inject.Inject;
import java.util.List;

/**
 * JobMaster implmentation for Pregel.
 */
public final class PregelJobMaster implements JobMaster {

  private final PregelMaster pregelMaster;

  @Inject
  private PregelJobMaster(final PregelMaster pregelMaster) {
    this.pregelMaster = pregelMaster;
  }

  @Override
  public void onMsg(final String srcId, final byte[] bytes) {
    final SuperstepResultMsg resultMsg = AvroUtils.fromBytes(bytes, SuperstepResultMsg.class);
    pregelMaster.onWorkerMsg(srcId, resultMsg);
  }

  @Override
  public void start(final List<List<AllocatedExecutor>> executorGroups, final List<AllocatedTable> tables) {
    final List<AllocatedExecutor> workers = executorGroups.get(0);

    final AllocatedTable vertexTable = tables.get(0);
    final AllocatedTable msgTable1 = tables.get(1);
    final AllocatedTable msgTable2 = tables.get(2);
    pregelMaster.start(workers, vertexTable, msgTable1, msgTable2);
  }
}
