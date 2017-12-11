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

import edu.snu.cay.dolphin.jobserver.driver.JobDispatcher;
import edu.snu.cay.dolphin.jobserver.driver.JobEntity;
import edu.snu.cay.services.et.configuration.ExecutorConfiguration;
import edu.snu.cay.services.et.configuration.TableConfiguration;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.exceptions.InjectionException;

/**
 * A class for encapsulating a job waiting to be scheduled.
 */
public final class PregelJobEntity implements JobEntity {
  private final Injector jobInjector;
  private final String jobId;

  private final int numWorkers;
  private final ExecutorConfiguration workerExecutorConf;
  private final TableConfiguration vertexTableConf;
  private final TableConfiguration msgTable1Conf;
  private final TableConfiguration msgTable2Conf;
  private final String inputPath;

  private PregelJobEntity(final Injector jobInjector,
                          final String jobId,
                          final int numWorkers,
                          final ExecutorConfiguration workerExecutorConf,
                          final TableConfiguration vertexTableConf,
                          final TableConfiguration msgTable1Conf,
                          final TableConfiguration msgTable2Conf,
                          final String inputPath) {
    this.jobInjector = jobInjector;
    this.jobId = jobId;
    this.numWorkers = numWorkers;
    this.workerExecutorConf = workerExecutorConf;
    this.vertexTableConf = vertexTableConf;
    this.msgTable1Conf = msgTable1Conf;
    this.msgTable2Conf = msgTable2Conf;
    this.inputPath = inputPath;
  }

  @Override
  public Injector getJobInjector() {
    return jobInjector;
  }

  @Override
  public String getJobId() {
    return jobId;
  }

  @Override
  public int getNumRequiredExecutors() {
    return numWorkers;
  }

  public int getNumWorkers() {
    return numWorkers;
  }

  public ExecutorConfiguration getWorkerExecutorConf() {
    return workerExecutorConf;
  }


  public TableConfiguration getVertexTableConf() {
    return vertexTableConf;
  }

  public TableConfiguration getMsgTable1Conf() {
    return msgTable1Conf;
  }

  public TableConfiguration getMsgTable2Conf() {
    return msgTable2Conf;
  }

  @Override
  public String getInputPath() {
    return inputPath;
  }

  @Override
  public void executeJob() {
    try {
      jobInjector.getInstance(JobDispatcher.class).executeJob(this);
    } catch (InjectionException e) {
      throw new RuntimeException(e);
    }
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public static final class Builder implements org.apache.reef.util.Builder<PregelJobEntity> {

    private Injector jobInjector;
    private String jobId;

    private int numWorkers;
    private ExecutorConfiguration workerExecutorConf;
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

    public Builder setNumWorkers(final int numWorkers) {
      this.numWorkers = numWorkers;
      return this;
    }

    public Builder setWorkerExecutorConf(final ExecutorConfiguration workerExecutorConf) {
      this.workerExecutorConf = workerExecutorConf;
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
      return new PregelJobEntity(jobInjector, jobId, numWorkers, workerExecutorConf,
          vetexTableConf, msgTable1Conf, msgTable2Conf, inputPath);
    }
  }
}
