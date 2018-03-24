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
package edu.snu.cay.jobserver.client;

import edu.snu.cay.jobserver.Parameters;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.formats.CommandLine;

/**
 * Client for shutting down running job server. This is called by {#stop_jobserver.sh}
 */
public final class JobServerCloser {

  private JobServerCloser() {
  }

  public static void main(final String[] args) {
    try {
      final CommandLine cl = new CommandLine();
      cl.registerShortNameOfClass(Parameters.KillRunningJobs.class);
      final Configuration clConf = cl.processCommandLine(args).getBuilder().build();

      final Injector injector = Tang.Factory.getTang().newInjector(clConf);
      final CommandSender commandSender = injector.getInstance(CommandSender.class);
      commandSender.sendShutdownCommand(injector.getNamedInstance(Parameters.KillRunningJobs.class));

    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
