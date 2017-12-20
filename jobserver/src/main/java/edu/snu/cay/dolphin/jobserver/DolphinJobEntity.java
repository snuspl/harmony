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
package edu.snu.cay.dolphin.jobserver;

import edu.snu.cay.dolphin.DolphinParameters;
import edu.snu.cay.jobserver.driver.JobEntity;
import edu.snu.cay.jobserver.driver.JobMaster;
import edu.snu.cay.services.et.configuration.TableConfiguration;
import edu.snu.cay.services.et.driver.api.AllocatedExecutor;
import edu.snu.cay.services.et.driver.api.AllocatedTable;
import edu.snu.cay.services.et.driver.api.ETMaster;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.exceptions.InjectionException;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * Dolphin's {@link JobEntity} implementation.
 */
public final class DolphinJobEntity implements JobEntity {
  private final Injector jobInjector;
  private final String jobId;

  private final TableConfiguration serverTableConf;
  private final TableConfiguration workerTableConf;
  private final String inputPath;

  private DolphinJobEntity(final Injector jobInjector,
                           final String jobId,
                           final TableConfiguration serverTableConf,
                           final TableConfiguration workerTableConf,
                           final String inputPath) {
    this.jobInjector = jobInjector;
    this.jobId = jobId;
    this.serverTableConf = serverTableConf;
    this.workerTableConf = workerTableConf;
    this.inputPath = inputPath;
  }

  @Override
  public JobMaster getJobMaster() {
    try {
      return jobInjector.getInstance(JobMaster.class);
    } catch (InjectionException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public String getJobId() {
    return jobId;
  }

  public List<AllocatedTable> setupTables(final ETMaster etMaster, final List<AllocatedExecutor> executors) {
    jobInjector.bindVolatileParameter(DolphinParameters.NumWorkers.class, executors.size());

    final Future<AllocatedTable> modelTableFuture = etMaster.createTable(serverTableConf, executors);
    final Future<AllocatedTable> inputTableFuture = etMaster.createTable(workerTableConf, executors);

    final AllocatedTable modelTable;
    final AllocatedTable inputTable;
    try {
      modelTable = modelTableFuture.get();
      inputTable = inputTableFuture.get();

      inputTable.load(executors, inputPath).get();

    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException(e);
    }

    return Arrays.asList(inputTable, modelTable);
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public static final class Builder implements org.apache.reef.util.Builder<DolphinJobEntity> {

    private Injector jobInjector;
    private String jobId;

    private TableConfiguration serverTableConf;
    private TableConfiguration workerTableConf;
    private String inputPath;

    private Builder() {
    }

    public Builder setJobInjector(final Injector jobInjector) {
      this.jobInjector = jobInjector;
      return this;
    }

    public Builder setJobId(final String jobId) {
      this.jobId = jobId;
      return this;
    }

    public Builder setServerTableConf(final TableConfiguration serverTableConf) {
      this.serverTableConf = serverTableConf;
      return this;
    }

    public Builder setWorkerTableConf(final TableConfiguration workerTableConf) {
      this.workerTableConf = workerTableConf;
      return this;
    }

    public Builder setInputPath(final String inputPath) {
      this.inputPath = inputPath;
      return this;
    }

    @Override
    public DolphinJobEntity build() {
      return new DolphinJobEntity(jobInjector, jobId, serverTableConf, workerTableConf, inputPath);
    }
  }
}
