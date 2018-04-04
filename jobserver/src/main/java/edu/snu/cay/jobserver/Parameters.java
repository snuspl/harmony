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
package edu.snu.cay.jobserver;

import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;

/**
 * Parameters used in JobServer.
 */
public final class Parameters {
  public static final String SUBMIT_COMMAND = "SUBMIT";
  public static final String SHUTDOWN_COMMAND = "SHUTDOWN";
  public static final String COMMAND_DELIMITER = " ";

  public static final int PORT_NUMBER = 7008;

  private Parameters() {

  }

  @NamedParameter(doc = "The number of workers", short_name = "num_executors")
  public final class NumExecutors implements Name<Integer> {

  }

  @NamedParameter(doc = "Desired memory size for each executor (MBs)", short_name = "executor_mem_size")
  public final class ExecutorMemSize implements Name<Integer> {

  }

  @NamedParameter(doc = "The number of CPU cores for each executor", short_name = "executor_num_cores")
  public final class ExecutorNumCores implements Name<Integer> {

  }

  @NamedParameter(doc = "The maximum number of tasklets in an executor", short_name = "executor_num_tasklets")
  public final class ExecutorNumTasklets implements Name<Integer> {

  }

  @NamedParameter(doc = "Max number of ops that can be queued in op handler", short_name = "handler_queue_size")
  public final class HandlerQueueSize implements Name<Integer> {

  }

  @NamedParameter(doc = "Max number of ops that can be queued for each sender thread", short_name = "sender_queue_size")
  public final class SenderQueueSize implements Name<Integer> {

  }

  @NamedParameter(doc = "The number of threads to handle remote access messages", short_name = "handler_num_threads")
  public final class HandlerNumThreads implements Name<Integer> {

  }

  @NamedParameter(doc = "The number of threads to send remote access messages", short_name = "sender_num_threads")
  public final class SenderNumThreads implements Name<Integer> {

  }

  @NamedParameter(doc = "Whether to kill running jobs and halt job server right now or wait jobs to finish",
      short_name = "kill")
  public final class KillRunningJobs implements Name<Boolean> {

  }

  @NamedParameter(doc = "An identifier of App.")
  public final class AppIdentifier implements Name<String> {

  }

  @NamedParameter(doc = "Job identifier", default_value = "job")
  public final class JobId implements Name<String> {
  }

  @NamedParameter(doc = "The number of workers for a job", short_name = "num_job_executors")
  public final class NumJobExecutors implements Name<Integer> {

  }

  @NamedParameter(doc = "A class of the scheduler",
      short_name = "scheduler",
      default_value = "edu.snu.cay.jobserver.driver.SchedulerImpl")
  public final class SchedulerClass implements Name<String> {
  }
}
