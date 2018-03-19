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
package edu.snu.cay.services.et.configuration;

import edu.snu.cay.services.et.configuration.parameters.remoteaccess.CommQueueSize;
import edu.snu.cay.services.et.configuration.parameters.remoteaccess.NumCommThreads;
import org.apache.reef.annotations.audience.Private;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;

/**
 * Configuration for remote access of executor.
 */
public final class RemoteAccessConfiguration {
  private final Configuration configuration;

  private RemoteAccessConfiguration(final Configuration configuration) {
    this.configuration = configuration;
  }

  /**
   * @return a builder
   */
  public static Builder newBuilder() {
    return new Builder();
  }

  /**
   * Get the configuration with the parameters bound.
   * Note that this method is for internal use only.
   */
  @Private
  public Configuration getConfiguration() {
    return configuration;
  }

  /**
   * A builder of ResourceConfiguration.
   */
  public static final class Builder implements org.apache.reef.util.Builder<RemoteAccessConfiguration> {

    private final JavaConfigurationBuilder innerBuilder = Tang.Factory.getTang().newConfigurationBuilder();

    private Builder() {
    }

    public Builder setCommQueueSize(final int senderQueueSize) {
      innerBuilder.bindNamedParameter(CommQueueSize.class, Integer.toString(senderQueueSize));
      return this;
    }

    public Builder setNumCommThreads(final int numSenderThreads) {
      innerBuilder.bindNamedParameter(NumCommThreads.class, Integer.toString(numSenderThreads));
      return this;
    }

    @Override
    public RemoteAccessConfiguration build() {
      return new RemoteAccessConfiguration(innerBuilder.build());
    }
  }
}
