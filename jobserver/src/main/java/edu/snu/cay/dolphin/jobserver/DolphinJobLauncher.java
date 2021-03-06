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

import edu.snu.cay.common.param.Parameters.*;
import edu.snu.cay.dolphin.core.client.ETDolphinConfiguration;
import edu.snu.cay.dolphin.core.client.ETDolphinLauncher;
import edu.snu.cay.dolphin.core.master.DolphinMaster;
import edu.snu.cay.dolphin.core.worker.*;
import edu.snu.cay.jobserver.Parameters.*;
import edu.snu.cay.dolphin.metric.parameters.ServerMetricFlushPeriodMs;
import edu.snu.cay.jobserver.client.CommandListener;
import edu.snu.cay.jobserver.client.CommandSender;
import edu.snu.cay.jobserver.client.JobServerClient;
import edu.snu.cay.jobserver.driver.JobEntity;
import edu.snu.cay.jobserver.driver.JobEntityBuilder;
import edu.snu.cay.jobserver.driver.JobMaster;
import edu.snu.cay.dolphin.DolphinParameters.*;
import edu.snu.cay.services.et.configuration.parameters.KeyCodec;
import edu.snu.cay.services.et.configuration.parameters.UpdateValueCodec;
import edu.snu.cay.services.et.configuration.parameters.ValueCodec;
import edu.snu.cay.services.et.evaluator.api.DataParser;
import edu.snu.cay.services.et.evaluator.api.UpdateFunction;
import edu.snu.cay.utils.ConfigurationUtils;
import org.apache.reef.annotations.audience.ClientSide;
import org.apache.reef.tang.*;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.tang.formats.CommandLine;

import java.io.*;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

import static edu.snu.cay.utils.ConfigurationUtils.extractParameterConf;

/**
 * A class that submits a specific Dolphin job dynamically to job server via {@link JobServerClient}.
 * It communicates with {@link JobServerClient}
 * through the connection between {@link CommandSender} and {@link CommandListener}.
 *
 * Users can run different apps with different parameters by changing
 * args and dolphin configuration for {@link #submitJob(String, String[], ETDolphinConfiguration)}.
 */
@ClientSide
public final class DolphinJobLauncher {

  private static final Logger LOG = Logger.getLogger(DolphinJobLauncher.class.getName());

  // utility class should not be instantiated
  private DolphinJobLauncher() {

  }

  /**
   * Submits a job to JobServer.
   * @param appId an app id
   * @param args arguments for app
   * @param dolphinConf dolphin configuration
   */
  public static void submitJob(final String appId,
                               final String[] args,
                               final ETDolphinConfiguration dolphinConf) {
    try {

      final List<Configuration> configurations = parseCommandLine(args, dolphinConf.getParameterClassList());
      final Configuration masterParamConf = configurations.get(0);
      final Configuration serverParamConf = configurations.get(1);
      final Configuration workerParamConf = configurations.get(2);
      final Configuration userParamConf = configurations.get(3);

      final Configuration masterConf = Configurations.merge(
          masterParamConf,
          Tang.Factory.getTang().newConfigurationBuilder()
              .bindNamedParameter(HasLocalModelTable.class, Boolean.toString(dolphinConf.hasLocalModelTable()))
              .build());

      // server conf. servers will be spawned with this configuration
      final Configuration serverConf = Configurations.merge(
          serverParamConf, userParamConf,
          Tang.Factory.getTang().newConfigurationBuilder()
              .bindImplementation(UpdateFunction.class, dolphinConf.getModelUpdateFunctionClass())
              .bindNamedParameter(KeyCodec.class, dolphinConf.getModelKeyCodecClass())
              .bindNamedParameter(ValueCodec.class, dolphinConf.getModelValueCodecClass())
              .bindNamedParameter(UpdateValueCodec.class, dolphinConf.getModelUpdateValueCodecClass())
              .build());

      // worker conf. workers will be spawned with this configuration
      Configuration workerConf = Configurations.merge(
          workerParamConf, userParamConf,
          Tang.Factory.getTang().newConfigurationBuilder()
              .bindImplementation(Trainer.class, dolphinConf.getTrainerClass())
              .bindImplementation(DataParser.class, dolphinConf.getInputParserClass())
              .bindImplementation(TrainingDataProvider.class, ETTrainingDataProvider.class)
              .bindImplementation(ModelAccessor.class, ETModelAccessor.class)
              .bindNamedParameter(KeyCodec.class, dolphinConf.getInputKeyCodecClass())
              .bindNamedParameter(ValueCodec.class, dolphinConf.getInputValueCodecClass())
              .bindNamedParameter(HasLocalModelTable.class, Boolean.toString(dolphinConf.hasLocalModelTable()))
              .bindNamedParameter(HasInputDataKey.class, Boolean.toString(dolphinConf.hasInputDataKey()))
              .build());

      if (dolphinConf.hasLocalModelTable()) {
        final Configuration localModelTableConf = Tang.Factory.getTang().newConfigurationBuilder()
            .bindNamedParameter(ETDolphinLauncher.SerializedLocalModelTableConf.class,
                ConfigurationUtils.SERIALIZER.toString(
                    Configurations.merge(userParamConf,
                        Tang.Factory.getTang().newConfigurationBuilder()
                            .bindImplementation(UpdateFunction.class, dolphinConf.getLocalModelUpdateFunctionClass())
                            .bindNamedParameter(KeyCodec.class, dolphinConf.getLocalModelKeyCodecClass())
                            .bindNamedParameter(ValueCodec.class, dolphinConf.getLocalModelValueCodecClass())
                            .bindNamedParameter(UpdateValueCodec.class,
                                dolphinConf.getLocalModelUpdateValueCodecClass())
                            .build())
                )).build();

        workerConf = Configurations.merge(workerConf, localModelTableConf);
      }

      // job configuration. driver will use this configuration to spawn a job
      final Configuration jobConf = getJobConfiguration(appId, masterConf, serverConf, workerConf, userParamConf);

      final CommandSender commandSender =
          Tang.Factory.getTang().newInjector().getInstance(CommandSender.class);

      LOG.log(Level.INFO, "Submit {0}", appId);
      commandSender.sendJobSubmitCommand(Configurations.toString(jobConf));

    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static List<Configuration> parseCommandLine(
      final String[] args, final List<Class<? extends Name<?>>> customAppParamList)
      throws IOException, InjectionException, ClassNotFoundException {

    // parameters for master
    final List<Class<? extends Name<?>>> masterParamList = Arrays.asList(
        MaxNumEpochs.class, NumTotalMiniBatches.class, ClockSlack.class,
        ServerMetricFlushPeriodMs.class, OfflineModelEvaluation.class, ModelEvaluation.class
    );

    // commonly used parameters for ML apps
    final List<Class<? extends Name<?>>> commonAppParamList = Arrays.asList(
        NumFeatures.class, Lambda.class, DecayRate.class, DecayPeriod.class, StepSize.class,
        ModelGaussian.class, NumFeaturesPerPartition.class
    );

    // user param list is composed by common app parameters and custom app parameters
    final List<Class<? extends Name<?>>> userParamList = new ArrayList<>(commonAppParamList);
    userParamList.addAll(customAppParamList);

    // parameters for servers
    final List<Class<? extends Name<?>>> serverParamList = Arrays.asList(
        NumServerBlocks.class, ServerMetricFlushPeriodMs.class,
        LoadModel.class, ModelPath.class, LocalModelPath.class
    );

    // parameters for workers
    final List<Class<? extends Name<?>>> workerParamList = Arrays.asList(
        NumTrainerThreads.class,
        NumWorkerBlocks.class, HyperThreadEnabled.class, MaxNumEpochs.class,
        NumTotalMiniBatches.class, TestDataPath.class, InputDir.class, InputChkpPath.class
    );

    final CommandLine cl = new CommandLine();
    final Set<Class<? extends Name<?>>> paramSet = new HashSet<>();
    paramSet.addAll(masterParamList);
    paramSet.addAll(userParamList);
    paramSet.addAll(serverParamList);
    paramSet.addAll(workerParamList);

    paramSet.forEach(cl::registerShortNameOfClass);

    final Configuration commandLineConf = cl.processCommandLine(args).getBuilder().build();
    final Configuration masterConf = extractParameterConf(masterParamList, commandLineConf);
    final Configuration serverConf = extractParameterConf(serverParamList, commandLineConf);
    final Configuration workerConf = extractParameterConf(workerParamList, commandLineConf);
    final Configuration userConf = extractParameterConf(userParamList, commandLineConf);

    return Arrays.asList(masterConf, serverConf, workerConf, userConf);
  }

  /**
   * @return a configuration for spawning a {@link DolphinMaster}.
   */
  private static Configuration getJobConfiguration(final String appId,
                                                   final Configuration masterConf,
                                                   final Configuration serverConf,
                                                   final Configuration workerConf,
                                                   final Configuration userParamConf) {
    return Configurations.merge(masterConf, Tang.Factory.getTang().newConfigurationBuilder()
        .bindNamedParameter(AppIdentifier.class, appId)
        .bindImplementation(JobMaster.class, DolphinJobMaster.class)
        .bindImplementation(JobEntity.class, DolphinJobEntity.class)
        .bindImplementation(JobEntityBuilder.class, DolphinJobEntityBuilder.class)
        .bindNamedParameter(ETDolphinLauncher.SerializedServerConf.class, Configurations.toString(serverConf))
        .bindNamedParameter(ETDolphinLauncher.SerializedWorkerConf.class, Configurations.toString(workerConf))
        .bindNamedParameter(ETDolphinLauncher.SerializedParamConf.class, Configurations.toString(userParamConf))
        .build());
  }
}
