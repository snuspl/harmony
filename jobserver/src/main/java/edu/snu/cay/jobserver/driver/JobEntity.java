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

import edu.snu.cay.services.et.driver.api.AllocatedExecutor;
import edu.snu.cay.services.et.driver.api.AllocatedTable;

import java.util.List;

/**
 * A class that encapsulates all information required to execute a job.
 */
public interface JobEntity {

  String getJobId();

  int getNumExecutors();

  JobMaster getJobMaster();

  /**
   * Setup executors.
   * @return executor groups
   */
  List<List<AllocatedExecutor>> setupExecutors();

  /**
   * Setup tables with the given executors.
   * @param executorGroups executor groups
   * @return tables
   */
  List<AllocatedTable> setupTables(List<List<AllocatedExecutor>> executorGroups);
}
