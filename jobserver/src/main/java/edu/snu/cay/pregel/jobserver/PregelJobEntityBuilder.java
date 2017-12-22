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
package edu.snu.cay.pregel.jobserver;

import edu.snu.cay.jobserver.Parameters;
import edu.snu.cay.jobserver.driver.JobEntity;
import edu.snu.cay.jobserver.driver.JobEntityBuilder;
import edu.snu.cay.pregel.PregelParameters;
import edu.snu.cay.pregel.common.DefaultVertexCodec;
import edu.snu.cay.pregel.common.MessageCodec;
import edu.snu.cay.pregel.common.MessageUpdateFunction;
import edu.snu.cay.services.et.configuration.TableConfiguration;
import edu.snu.cay.services.et.evaluator.api.DataParser;
import edu.snu.cay.services.et.evaluator.impl.ExistKeyBulkDataLoader;
import edu.snu.cay.services.et.evaluator.impl.VoidUpdateFunction;
import edu.snu.cay.utils.NullCodec;
import edu.snu.cay.utils.StreamingSerializableCodec;
import org.apache.reef.io.network.impl.StreamingCodec;
import org.apache.reef.io.serialization.Codec;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;

import javax.inject.Inject;
import java.io.IOException;

/**
 * Pregel's {@link JobEntityBuilder} implementation.
 */
public final class PregelJobEntityBuilder implements JobEntityBuilder {

  private final Injector jobInjector;

  @Inject
  private PregelJobEntityBuilder(final Injector jobInjector) {
    this.jobInjector = jobInjector;
  }

  @Override
  public JobEntity build() throws InjectionException, IOException {
    // generate different pregel job id for each job
    final int jobCount = JOB_COUNTER.getAndIncrement();

    final String appId = jobInjector.getNamedInstance(Parameters.AppIdentifier.class);
    final String pregelJobId = appId + "-" + jobCount;
    final String vertexTableId = PregelParameters.VertexTableId.DEFAULT_VALUE + jobCount;
    final String msgTableId = PregelParameters.MessageTableId.DEFAULT_VALUE + jobCount;

    final DataParser dataParser = jobInjector.getInstance(DataParser.class);
    final StreamingCodec vertexValueCodec = jobInjector.getNamedInstance(PregelParameters.VertexValueCodec.class);
    final StreamingCodec edgeCodec = jobInjector.getNamedInstance(PregelParameters.EdgeCodec.class);

    final TableConfiguration vertexTableConf = buildVertexTableConf(dataParser,
        vertexValueCodec, edgeCodec, vertexTableId);

    final StreamingCodec msgValueCodec = jobInjector.getNamedInstance(PregelParameters.MessageValueCodec.class);

    final TableConfiguration msgTable1Conf = buildMsgTableConf(msgValueCodec,
        msgTableId + PregelParameters.MSG_TABLE_1_ID_POSTFIX);
    final TableConfiguration msgTable2Conf = buildMsgTableConf(msgValueCodec,
        msgTableId + PregelParameters.MSG_TABLE_2_ID_POSTFIX);

    final String inputDir = jobInjector.getNamedInstance(edu.snu.cay.common.param.Parameters.InputDir.class);

    jobInjector.bindVolatileParameter(Parameters.JobId.class, pregelJobId);
    jobInjector.bindVolatileParameter(PregelParameters.VertexTableId.class, vertexTableId);
    jobInjector.bindVolatileParameter(PregelParameters.MessageTableId.class, msgTableId);

    return PregelJobEntity.newBuilder()
        .setJobInjector(jobInjector)
        .setJobId(pregelJobId)
        .setVertexTableConf(vertexTableConf)
        .setMsgTable1Conf(msgTable1Conf)
        .setMsgTable2Conf(msgTable2Conf)
        .setInputPath(inputDir)
        .build();
  }

  /**
   * Build a configuration of vertex table.
   * Need to provide codecs for vertex value and edge.
   *
   * @param tableId an identifier of {@link TableConfiguration}
   */
  private TableConfiguration buildVertexTableConf(final DataParser dataParser,
                                                  final StreamingCodec vertexValueCodec,
                                                  final StreamingCodec edgeCodec,
                                                  final String tableId) throws InjectionException {
    // configure vertex value codec, edge codec to vertex table
    final Configuration vertexComponentCodecConf = Tang.Factory.getTang().newConfigurationBuilder()
        .bindNamedParameter(PregelParameters.VertexValueCodec.class, vertexValueCodec.getClass())
        .bindNamedParameter(PregelParameters.EdgeCodec.class, edgeCodec.getClass())
        .build();

    return TableConfiguration.newBuilder()
        .setId(tableId)
        .setKeyCodecClass(StreamingSerializableCodec.class)
        .setValueCodecClass(DefaultVertexCodec.class) // TODO #1223: allow other types of vertex implementation
        .setUpdateValueCodecClass(NullCodec.class)
        .setUpdateFunctionClass(VoidUpdateFunction.class)
        .setIsMutableTable(true)
        .setIsOrderedTable(false)
        .setDataParserClass(dataParser.getClass())
        .setBulkDataLoaderClass(ExistKeyBulkDataLoader.class)
        .setUserParamConf(vertexComponentCodecConf)
        .build();
  }

  /**
   * Build a configuration of message table.
   * Type of value is {@link Iterable} so set {@link MessageCodec} to value codec class.
   *
   * @param tableId an identifier of {@link TableConfiguration}
   */
  private TableConfiguration buildMsgTableConf(final StreamingCodec messageValueCodec,
                                               final String tableId) throws InjectionException {
    // configure message value codec to message table
    final Configuration messageValueConf = Tang.Factory.getTang().newConfigurationBuilder()
        .bindNamedParameter(PregelParameters.MessageValueCodec.class, messageValueCodec.getClass())
        .build();

    return TableConfiguration.newBuilder()
        .setId(tableId)
        .setKeyCodecClass(StreamingSerializableCodec.class)
        .setValueCodecClass(MessageCodec.class)
        .setUpdateValueCodecClass(((Codec) messageValueCodec).getClass())
        .setUpdateFunctionClass(MessageUpdateFunction.class)
        .setIsMutableTable(true)
        .setIsOrderedTable(false)
        .setUserParamConf(messageValueConf)
        .build();
  }
}
