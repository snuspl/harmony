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
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.exceptions.InjectionException;

import java.io.IOException;
import java.util.List;

/**
 * A class that encapsulates all information required to execute a job.
 */
public interface JobEntity {

  /**
   * Gets a {@link JobEntity} from an injector forked from {@code jobBaseInjector} with {@code jobConf}.
   * It internally uses {@link JobEntityBuilder}.
   * @param jobBaseInjector a job base injector
   * @param jobConf a job configuration
   * @return a {@link JobEntity}
   * @throws InjectionException
   * @throws IOException
   */
  static JobEntity getJobEntity(final Injector jobBaseInjector, final Configuration jobConf)
      throws InjectionException, IOException {
    final Injector jobInjector = jobBaseInjector.forkInjector(jobConf);
    return jobInjector.getInstance(JobEntityBuilder.class).build();
  }

  /**
   * @return the job Id
   */
  String getJobId();

  /**
   * @return the number of executors for this job
   */
  int getNumExecutors();

  /**
   * @return a {@link JobMaster}
   */
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
