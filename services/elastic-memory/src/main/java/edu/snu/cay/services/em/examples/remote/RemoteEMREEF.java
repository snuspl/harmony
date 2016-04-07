/*
 * Copyright (C) 2016 Seoul National University
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

package edu.snu.cay.services.em.examples.remote;

import edu.snu.cay.common.aggregation.AggregationConfiguration;
import edu.snu.cay.services.em.driver.ElasticMemoryConfiguration;
import edu.snu.cay.utils.trace.HTraceParameters;
import org.apache.reef.client.DriverConfiguration;
import org.apache.reef.client.DriverLauncher;
import org.apache.reef.client.LauncherStatus;
import org.apache.reef.io.network.naming.LocalNameResolverConfiguration;
import org.apache.reef.io.network.naming.NameServerConfiguration;
import org.apache.reef.io.network.util.StringIdentifierFactory;
import org.apache.reef.runtime.local.client.LocalRuntimeConfiguration;
import org.apache.reef.runtime.yarn.client.YarnClientConfiguration;
import org.apache.reef.tang.*;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.tang.formats.CommandLine;
import org.apache.reef.util.EnvironmentUtils;
import org.apache.reef.wake.IdentifierFactory;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Client code for RemoteEM.
 */
public final class RemoteEMREEF {
  private static final Logger LOG = Logger.getLogger(RemoteEMREEF.class.getName());
  private static final int TIMEOUT = 100000;
  private static final Tang TANG = Tang.Factory.getTang();

  @NamedParameter(doc = "Whether or not to run on the local runtime", short_name = "onLocal", default_value = "true")
  public static final class OnLocal implements Name<Boolean> {
  }

  /**
   * Should not be instantiated.
   */
  private RemoteEMREEF() {
  }

  /**
   * Setup (register short names) and parse the command line, returning an Injector.
   */
  private static Injector parseCommandLine(final String[] args) throws InjectionException, IOException {
    final JavaConfigurationBuilder cb = TANG.newConfigurationBuilder();
    final CommandLine cl = new CommandLine(cb);

    cl.registerShortNameOfClass(OnLocal.class);
    HTraceParameters.registerShortNames(cl);

    cl.processCommandLine(args);
    return TANG.newInjector(cb.build());
  }

  /**
   * Get onLocal from the parsed command line Injector.
   */
  private static boolean getOnLocal(final Injector injector) throws InjectionException {
    return injector.getNamedInstance(OnLocal.class);
  }

  /**
   * Get HTraceParameters from the parsed command line Injector.
   */
  private static HTraceParameters getTraceParameters(final Injector injector) throws InjectionException {
    return injector.getInstance(HTraceParameters.class);
  }

  public static Configuration getDriverConfiguration() {
    final Configuration driverConfiguration = DriverConfiguration.CONF
        .set(DriverConfiguration.GLOBAL_LIBRARIES, EnvironmentUtils.getClassLocation(RemoteEMDriver.class))
        .set(DriverConfiguration.DRIVER_IDENTIFIER, "RemoteEMDriver")
        .set(DriverConfiguration.ON_EVALUATOR_ALLOCATED, RemoteEMDriver.EvaluatorAllocatedHandler.class)
        .set(DriverConfiguration.ON_DRIVER_STARTED, RemoteEMDriver.DriverStartHandler.class)
        .set(DriverConfiguration.ON_CONTEXT_ACTIVE, RemoteEMDriver.ActiveContextHandler.class)
        .build();

    final Configuration aggregationConf = AggregationConfiguration.newBuilder()
        .addAggregationClient(RemoteEMDriver.AGGREGATION_CLIENT_ID,
            DriverSideMsgHandler.class,
            EvalSideMsgHandler.class)
        .build()
        .getDriverConfiguration();

    // spawn the name server at the driver
    return Configurations.merge(driverConfiguration,
        ElasticMemoryConfiguration.getDriverConfiguration(),
        aggregationConf,
        NameServerConfiguration.CONF.build(),
        LocalNameResolverConfiguration.CONF.build(),
        Tang.Factory.getTang().newConfigurationBuilder()
            .bindImplementation(IdentifierFactory.class, StringIdentifierFactory.class)
            .build());
  }

  public static LauncherStatus runRemoteEM(final Configuration runtimeConf, final Configuration jobConf,
                                           final int timeout)
      throws InjectionException {
    final Configuration driverConf = getDriverConfiguration();

    return DriverLauncher.getLauncher(runtimeConf).run(Configurations.merge(driverConf, jobConf), timeout);
  }

  public static void main(final String[] args) throws InjectionException, IOException {
    final Injector injector = parseCommandLine(args);
    final boolean onLocal = getOnLocal(injector);
    final Configuration runtimeConf = onLocal ?
        LocalRuntimeConfiguration.CONF.build() :
        YarnClientConfiguration.CONF.build();

    final HTraceParameters traceParameters = getTraceParameters(injector);
    final Configuration traceConf = traceParameters.getConfiguration();

    final LauncherStatus status = runRemoteEM(runtimeConf, traceConf, TIMEOUT);
    LOG.log(Level.INFO, "REEF job completed: {0}", status);
  }
}