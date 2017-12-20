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
package edu.snu.cay.pregel.jobserver;

import edu.snu.cay.jobserver.driver.JobEntity;
import edu.snu.cay.jobserver.driver.JobMaster;
import edu.snu.cay.pregel.PregelParameters;
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
 * Pregel's {@link JobEntity} implementation.
 */
public final class PregelJobEntity implements JobEntity {
  private final Injector jobInjector;
  private final String jobId;

  private final TableConfiguration vertexTableConf;
  private final TableConfiguration msgTable1Conf;
  private final TableConfiguration msgTable2Conf;
  private final String inputPath;

  private PregelJobEntity(final Injector jobInjector,
                          final String jobId,
                          final TableConfiguration vertexTableConf,
                          final TableConfiguration msgTable1Conf,
                          final TableConfiguration msgTable2Conf,
                          final String inputPath) {
    this.jobInjector = jobInjector;
    this.jobId = jobId;
    this.vertexTableConf = vertexTableConf;
    this.msgTable1Conf = msgTable1Conf;
    this.msgTable2Conf = msgTable2Conf;
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
  public List<AllocatedTable> setupTables(final ETMaster etMaster, final List<AllocatedExecutor> executors) {
    jobInjector.bindVolatileParameter(PregelParameters.NumWorkers.class, executors.size());

    final Future<AllocatedTable> vertexTableFuture = etMaster.createTable(vertexTableConf, executors);
    final Future<AllocatedTable> msgTable1Future = etMaster.createTable(msgTable1Conf, executors);
    final Future<AllocatedTable> msgTable2Future = etMaster.createTable(msgTable2Conf, executors);

    try {
      final AllocatedTable vertexTable = vertexTableFuture.get();
      final AllocatedTable msgTable1 = msgTable1Future.get();
      final AllocatedTable msgTable2 = msgTable2Future.get();

      vertexTable.load(executors, inputPath).get();

      return Arrays.asList(vertexTable, msgTable1, msgTable2);
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public String getJobId() {
    return jobId;
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public static final class Builder implements org.apache.reef.util.Builder<PregelJobEntity> {

    private Injector jobInjector;
    private String jobId;

    private TableConfiguration vetexTableConf;
    private TableConfiguration msgTable1Conf;
    private TableConfiguration msgTable2Conf;
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

    public Builder setVertexTableConf(final TableConfiguration vertexTableConf) {
      this.vetexTableConf = vertexTableConf;
      return this;
    }

    public Builder setMsgTable1Conf(final TableConfiguration msgTable1Conf) {
      this.msgTable1Conf = msgTable1Conf;
      return this;
    }

    public Builder setMsgTable2Conf(final TableConfiguration msgTable2Conf) {
      this.msgTable2Conf = msgTable2Conf;
      return this;
    }

    public Builder setInputPath(final String inputPath) {
      this.inputPath = inputPath;
      return this;
    }

    @Override
    public PregelJobEntity build() {
      return new PregelJobEntity(jobInjector, jobId, vetexTableConf, msgTable1Conf, msgTable2Conf, inputPath);
    }
  }
}
