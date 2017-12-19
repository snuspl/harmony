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

import edu.snu.cay.jobserver.driver.JobDispatcher;
import edu.snu.cay.jobserver.driver.JobEntity;
import edu.snu.cay.jobserver.driver.JobMaster;
import edu.snu.cay.services.et.configuration.ExecutorConfiguration;
import edu.snu.cay.services.et.configuration.TableConfiguration;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.exceptions.InjectionException;

/**
 * Dolphin's {@link JobEntity} implementation.
 */
public final class DolphinJobEntity implements JobEntity {
  private final Injector jobInjector;
  private final String jobId;

  private final int numServers;
  private final ExecutorConfiguration serverExecutorConf;
  private final TableConfiguration serverTableConf;

  private final int numWorkers;
  private final ExecutorConfiguration workerExecutorConf;
  private final TableConfiguration workerTableConf;
  private final String inputPath;

  private DolphinJobEntity(final Injector jobInjector,
                           final String jobId,
                           final int numServers,
                           final ExecutorConfiguration serverExecutorConf,
                           final TableConfiguration serverTableConf,
                           final int numWorkers,
                           final ExecutorConfiguration workerExecutorConf,
                           final TableConfiguration workerTableConf,
                           final String inputPath) {
    this.jobInjector = jobInjector;
    this.jobId = jobId;
    this.numServers = numServers;
    this.serverExecutorConf = serverExecutorConf;
    this.serverTableConf = serverTableConf;
    this.numWorkers = numWorkers;
    this.workerExecutorConf = workerExecutorConf;
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

  @Override
  public int getNumExecutors() {
    return numServers + numWorkers;
  }

  public int getNumServers() {
    return numServers;
  }

  public ExecutorConfiguration getServerExecutorConf() {
    return serverExecutorConf;
  }

  public TableConfiguration getServerTableConf() {
    return serverTableConf;
  }

  public int getNumWorkers() {
    return numWorkers;
  }

  public ExecutorConfiguration getWorkerExecutorConf() {
    return workerExecutorConf;
  }

  public TableConfiguration getWorkerTableConf() {
    return workerTableConf;
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

  public static final class Builder implements org.apache.reef.util.Builder<DolphinJobEntity> {

    private Injector jobInjector;
    private String jobId;

    private int numServers;
    private ExecutorConfiguration serverExecutorConf;
    private TableConfiguration serverTableConf;

    private int numWorkers;
    private ExecutorConfiguration workerExecutorConf;
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

    public Builder setNumServers(final int numServers) {
      this.numServers = numServers;
      return this;
    }

    public Builder setServerExecutorConf(final ExecutorConfiguration serverExecutorConf) {
      this.serverExecutorConf = serverExecutorConf;
      return this;
    }

    public Builder setServerTableConf(final TableConfiguration serverTableConf) {
      this.serverTableConf = serverTableConf;
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
      return new DolphinJobEntity(jobInjector, jobId, numServers, serverExecutorConf, serverTableConf,
          numWorkers, workerExecutorConf, workerTableConf, inputPath);
    }
  }
}