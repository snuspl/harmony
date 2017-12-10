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
package edu.snu.cay.dolphin.jobserver.driver;

import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;

import java.io.IOException;

/**
 * Created by xyzi on 08/12/2017.
 */
public interface JobEntityDecoder {

  static JobEntityDecoder get(final Configuration jobConf) throws InjectionException {
    return Tang.Factory.getTang().newInjector(jobConf).getInstance(JobEntityDecoder.class);
  }

  /**
   * Build a JobEntityImpl from a given job configuration.
   * @param jobConf a job configuration
   * @return a decoded {@link JobEntity}
   * @throws InjectionException
   */
  JobEntity from(Injector jobBaseInjector, Configuration jobConf) throws InjectionException, IOException;
}
