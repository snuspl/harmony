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

import edu.snu.cay.dolphin.DolphinParameters.*;
import edu.snu.cay.dolphin.core.client.ETDolphinLauncher;
import edu.snu.cay.jobserver.Parameters;
import edu.snu.cay.jobserver.driver.JobEntity;
import edu.snu.cay.jobserver.driver.JobEntityBuilder;
import edu.snu.cay.services.et.configuration.TableConfiguration;
import edu.snu.cay.services.et.configuration.parameters.KeyCodec;
import edu.snu.cay.services.et.configuration.parameters.UpdateValueCodec;
import edu.snu.cay.services.et.configuration.parameters.ValueCodec;
import edu.snu.cay.services.et.evaluator.api.DataParser;
import edu.snu.cay.services.et.evaluator.api.UpdateFunction;
import edu.snu.cay.services.et.evaluator.impl.ExistKeyBulkDataLoader;
import edu.snu.cay.services.et.evaluator.impl.NoneKeyBulkDataLoader;
import edu.snu.cay.services.et.evaluator.impl.VoidUpdateFunction;
import edu.snu.cay.utils.ConfigurationUtils;
import org.apache.reef.io.network.impl.StreamingCodec;
import org.apache.reef.io.serialization.Codec;
import org.apache.reef.io.serialization.SerializableCodec;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;

import javax.inject.Inject;
import java.io.IOException;

/**
 * Dolphin's {@link JobEntityBuilder} implementation.
 */
public final class DolphinJobEntityBuilder implements JobEntityBuilder {
  private final Injector jobInjector;

  @Inject
  private DolphinJobEntityBuilder(final Injector jobInjector) {
    this.jobInjector = jobInjector;
  }

  @Override
  public JobEntity build() throws InjectionException, IOException {
    // generate different dolphin job id for each job
    final int jobCount = JOB_COUNTER.getAndIncrement();

    final String appId = jobInjector.getNamedInstance(Parameters.AppIdentifier.class);
    final String dolphinJobId = appId + "-" + jobCount;
    final String modelTableId = ModelTableId.DEFAULT_VALUE + jobCount;
    final String localModelTableId = LocalModelTableId.DEFAULT_VALUE + jobCount;

    jobInjector.bindVolatileParameter(Parameters.JobId.class, dolphinJobId);
    jobInjector.bindVolatileParameter(ModelTableId.class, modelTableId);
    jobInjector.bindVolatileParameter(LocalModelTableId.class, localModelTableId);

    final String serializedParamConf = jobInjector.getNamedInstance(ETDolphinLauncher.SerializedParamConf.class);
    final String serializedServerConf = jobInjector.getNamedInstance(ETDolphinLauncher.SerializedServerConf.class);
    final String serializedWorkerConf = jobInjector.getNamedInstance(ETDolphinLauncher.SerializedWorkerConf.class);

    // configuration commonly used in both workers and servers
    final Configuration userParamConf = ConfigurationUtils.fromString(serializedParamConf);

    // prepare server-side configurations
    final Configuration serverConf = ConfigurationUtils.fromString(serializedServerConf);
    final Injector serverInjector = Tang.Factory.getTang().newInjector(serverConf);
    final int numServerBlocks = serverInjector.getNamedInstance(NumServerBlocks.class);

    final TableConfiguration serverTableConf = buildServerTableConf(modelTableId,
        serverInjector, numServerBlocks, userParamConf);

    // prepare worker-side configurations
    final Configuration workerConf = ConfigurationUtils.fromString(serializedWorkerConf);
    final Injector workerInjector = Tang.Factory.getTang().newInjector(workerConf);
    final int numWorkerBlocks = workerInjector.getNamedInstance(NumWorkerBlocks.class);

    final String inputPath = workerInjector.getNamedInstance(edu.snu.cay.common.param.Parameters.InputDir.class);
    final String[] pathSplit = inputPath.split("/");
    final String inputTableId = pathSplit[pathSplit.length - 1]; // Use filename for table ID
    jobInjector.bindVolatileParameter(InputTableId.class, inputTableId);

    final TableConfiguration workerTableConf = buildWorkerTableConf(inputTableId, inputPath,
        workerInjector, numWorkerBlocks, userParamConf);

    final TableConfiguration localModelTableConf;
    final boolean hasLocalModelTable = workerInjector.getNamedInstance(HasLocalModelTable.class);
    if (hasLocalModelTable) {
      final Injector localModelTableInjector = Tang.Factory.getTang().newInjector(
          ConfigurationUtils.SERIALIZER.fromString(
              workerInjector.getNamedInstance(
                  ETDolphinLauncher.SerializedLocalModelTableConf.class)));
      localModelTableConf = buildLocalModelTableConf(localModelTableId,
          localModelTableInjector, numWorkerBlocks, userParamConf);
    } else {
      localModelTableConf = null;
    }

    return DolphinJobEntity.newBuilder()
        .setJobInjector(jobInjector)
        .setJobId(dolphinJobId)
        .setServerTableConf(serverTableConf)
        .setWorkerTableConf(workerTableConf)
        .setWorkerLocalModelTableConf(localModelTableConf)
        .build();
  }

  private static TableConfiguration buildWorkerTableConf(final String tableId,
                                                         final String inputPath,
                                                         final Injector workerInjector,
                                                         final int numTotalBlocks,
                                                         final Configuration userParamConf) throws InjectionException {
    final StreamingCodec keyCodec = workerInjector.getNamedInstance(KeyCodec.class);
    final StreamingCodec valueCodec = workerInjector.getNamedInstance(ValueCodec.class);
    final DataParser dataParser = workerInjector.getInstance(DataParser.class);
    final boolean hasInputDataKey = workerInjector.getNamedInstance(HasInputDataKey.class);

    return TableConfiguration.newBuilder()
        .setId(tableId)
        .setKeyCodecClass(keyCodec.getClass())
        .setValueCodecClass(valueCodec.getClass())
        .setUpdateValueCodecClass(SerializableCodec.class)
        .setUpdateFunctionClass(VoidUpdateFunction.class)
        .setNumTotalBlocks(numTotalBlocks)
        .setIsMutableTable(false)
        .setIsOrderedTable(false)
        .setInputPath(inputPath)
        .setDataParserClass(dataParser.getClass())
        .setBulkDataLoaderClass(hasInputDataKey ? ExistKeyBulkDataLoader.class : NoneKeyBulkDataLoader.class)
        .setUserParamConf(userParamConf)
        .build();
  }

  private static TableConfiguration buildLocalModelTableConf(final String tableId,
                                                             final Injector localModelTableInjector,
                                                             final int numTotalBlocks,
                                                             final Configuration userParamConf)
      throws InjectionException {
    final StreamingCodec keyCodec = localModelTableInjector.getNamedInstance(KeyCodec.class);
    final StreamingCodec valueCodec = localModelTableInjector.getNamedInstance(ValueCodec.class);
    final Codec updateValueCodec = localModelTableInjector.getNamedInstance(UpdateValueCodec.class);
    final UpdateFunction updateFunction = localModelTableInjector.getInstance(UpdateFunction.class);

    return TableConfiguration.newBuilder()
        .setId(tableId)
        .setKeyCodecClass(keyCodec.getClass())
        .setValueCodecClass(valueCodec.getClass())
        .setUpdateValueCodecClass(updateValueCodec.getClass())
        .setUpdateFunctionClass(updateFunction.getClass())
        .setNumTotalBlocks(numTotalBlocks)
        .setIsMutableTable(true)
        .setIsOrderedTable(false)
        .setUserParamConf(userParamConf)
        .build();
  }

  private static TableConfiguration buildServerTableConf(final String tableId,
                                                         final Injector serverInjector,
                                                         final int numTotalBlocks,
                                                         final Configuration userParamConf) throws InjectionException {
    final StreamingCodec keyCodec = serverInjector.getNamedInstance(KeyCodec.class);
    final StreamingCodec valueCodec = serverInjector.getNamedInstance(ValueCodec.class);
    final Codec updateValueCodec = serverInjector.getNamedInstance(UpdateValueCodec.class);
    final UpdateFunction updateFunction = serverInjector.getInstance(UpdateFunction.class);

    return TableConfiguration.newBuilder()
        .setId(tableId)
        .setKeyCodecClass(keyCodec.getClass())
        .setValueCodecClass(valueCodec.getClass())
        .setUpdateValueCodecClass(updateValueCodec.getClass())
        .setUpdateFunctionClass(updateFunction.getClass())
        .setNumTotalBlocks(numTotalBlocks)
        .setIsMutableTable(true)
        .setIsOrderedTable(false)
        .setUserParamConf(userParamConf)
        .build();
  }
}
