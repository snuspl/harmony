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
package edu.snu.cay.dolphin.jobserver.driver;

import edu.snu.cay.services.et.driver.api.AllocatedExecutor;
import edu.snu.cay.services.et.driver.api.AllocatedTable;

import java.util.List;

/**
 * A Dolphin master, which runs a dolphin job with given executors and tables.
 */
public interface JobMaster {

  /**
   * Returns a msg handler, which handles a byte msg.
   * It should be called when driver-side msg handler has been called.
   */
  void onMsg(byte[] bytes);

  /**
   * Start running a job with given executors and tables.
   * It returns after checking the result of tasks.
   */
  void start(List<List<AllocatedExecutor>> executorGroups,
             List<AllocatedTable> tables);
}