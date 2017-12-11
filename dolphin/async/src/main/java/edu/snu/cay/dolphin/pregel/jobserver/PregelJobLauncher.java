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

import edu.snu.cay.common.param.Parameters.InputDir;
import edu.snu.cay.dolphin.jobserver.Parameters.AppIdentifier;
import edu.snu.cay.dolphin.jobserver.client.CommandListener;
import edu.snu.cay.dolphin.jobserver.client.CommandSender;
import edu.snu.cay.dolphin.jobserver.client.JobServerClient;
import edu.snu.cay.dolphin.jobserver.driver.JobDispatcher;
import edu.snu.cay.dolphin.jobserver.driver.JobEntity;
import edu.snu.cay.dolphin.jobserver.driver.JobEntityBuilder;
import edu.snu.cay.dolphin.jobserver.driver.JobMaster;
import edu.snu.cay.dolphin.pregel.PregelConfiguration;
import edu.snu.cay.dolphin.pregel.PregelParameters.*;
import edu.snu.cay.dolphin.pregel.graph.api.Computation;
import edu.snu.cay.services.et.evaluator.api.DataParser;
import edu.snu.cay.utils.ConfigurationUtils;
import org.apache.reef.annotations.audience.ClientSide;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Configurations;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.tang.formats.CommandLine;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A class that submits a specific ML job dynamically to job server via {@link JobServerClient}.
 * It communicates with {@link JobServerClient}
 * through the connection between {@link CommandSender} and {@link CommandListener}.
 *
 * Users can run different apps with different parameters by changing
 * args and dolphin configuration for {@link #submitJob(String, String[], PregelConfiguration)}.
 */
@ClientSide
public final class PregelJobLauncher {

  private static final Logger LOG = Logger.getLogger(PregelJobLauncher.class.getName());

  // utility class should not be instantiated
  private PregelJobLauncher() {

  }

  /**
   * Submits a job to JobServer.
   * @param appId an app id
   * @param args arguments for app
   * @param pregelConf pregel configuration
   */
  public static void submitJob(final String appId,
                               final String[] args,
                               final PregelConfiguration pregelConf) {
    try {

      final List<Configuration> configurations = parseCommandLine(args, pregelConf.getUserParamList());
      final Configuration masterParamConf = configurations.get(0);
      final Configuration userParamConf = configurations.get(1);

      final Configuration taskletConf = Configurations.merge(userParamConf, getTaskletConf(pregelConf));

      final Configuration masterConf = Configurations.merge(masterParamConf, getMasterConf(pregelConf));

      // job configuration. driver will use this configuration to spawn a job
      final Configuration jobConf = getJobConfiguration(appId, taskletConf, masterConf, userParamConf);

      final CommandSender commandSender =
          Tang.Factory.getTang().newInjector().getInstance(CommandSender.class);

      LOG.log(Level.INFO, "Submit {0}", appId);
      commandSender.sendJobSubmitCommand(Configurations.toString(jobConf));

    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static List<Configuration> parseCommandLine(final String[] args,
                                                      final List<Class<? extends Name<?>>> userParamList)
      throws IOException {

    final List<Class<? extends Name<?>>> masterParamList = Arrays.asList(
        InputDir.class, NumExecutors.class, ExecutorMemSize.class, ExecutorNumCores.class);

    final CommandLine cl = new CommandLine();
    masterParamList.forEach(cl::registerShortNameOfClass);
    userParamList.forEach(cl::registerShortNameOfClass);

    final Configuration commandLineConf = cl.processCommandLine(args).getBuilder().build();
    final Configuration masterConf = ConfigurationUtils.extractParameterConf(masterParamList, commandLineConf);
    final Configuration userConf = ConfigurationUtils.extractParameterConf(userParamList, commandLineConf);

    return Arrays.asList(masterConf, userConf);
  }

  private static Configuration getMasterConf(final PregelConfiguration pregelConf) {
    return Tang.Factory.getTang().newConfigurationBuilder()
        .bindImplementation(DataParser.class, pregelConf.getDataParserClass())
        .bindNamedParameter(VertexValueCodec.class, pregelConf.getVertexValueCodecClass())
        .bindNamedParameter(EdgeCodec.class, pregelConf.getEdgeCodecClass())
        .bindNamedParameter(MessageValueCodec.class, pregelConf.getMessageValueCodecClass())
        .build();
  }

  private static Configuration getTaskletConf(final PregelConfiguration pregelConf) throws InjectionException {
    return Tang.Factory.getTang().newConfigurationBuilder()
        .bindImplementation(Computation.class, pregelConf.getComputationClass())
        .build();
  }

  /**
   * @return a configuration for spawning a {@link edu.snu.cay.dolphin.pregel.PregelMaster}.
   */
  private static Configuration getJobConfiguration(final String appId,
                                                   final Configuration taskletConf,
                                                   final Configuration masterConf,
                                                   final Configuration userParamConf) {
    return Configurations.merge(masterConf, userParamConf, Tang.Factory.getTang().newConfigurationBuilder()
        .bindNamedParameter(AppIdentifier.class, appId)
        .bindImplementation(JobMaster.class, PregelJobMaster.class)
        .bindImplementation(JobEntity.class, PregelJobEntity.class)
        .bindImplementation(JobEntityBuilder.class, PregelJobEntityBuilder.class)
        .bindImplementation(JobDispatcher.class, PregelJobDispatcher.class)
        .bindNamedParameter(SerializedTaskletConf.class, Configurations.toString(taskletConf))
        .build());
  }
}
