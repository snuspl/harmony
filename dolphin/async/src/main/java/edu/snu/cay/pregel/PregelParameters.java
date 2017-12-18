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
package edu.snu.cay.pregel;

import org.apache.reef.io.network.impl.StreamingCodec;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;

/**
 * Pregel specific parameters.
 */
public final class PregelParameters {
  public static final String MSG_TABLE_1_ID_POSTFIX = "-1";
  public static final String MSG_TABLE_2_ID_POSTFIX = "-2";

  private PregelParameters() {

  }

  @NamedParameter(doc = "The number of executors", short_name = "num_executors")
  public final class NumExecutors implements Name<Integer> {

  }

  @NamedParameter(doc = "Desired memory size for each executor (MBs)", short_name = "executor_mem_size")
  public final class ExecutorMemSize implements Name<Integer> {
  }

  @NamedParameter(doc = "The number of CPU cores for each executor", short_name = "executor_num_cores")
  public final class ExecutorNumCores implements Name<Integer> {

  }

  @NamedParameter(doc = "configuration for worker tasklet class, serialized as a string")
  public final class SerializedTaskletConf implements Name<String> {

  }

  @NamedParameter(doc = "The codec class for encoding and decoding message objects")
  public final class MessageValueCodec implements Name<StreamingCodec> {

  }

  @NamedParameter(doc = "The codec class for encoding and decoding vertex values")
  public final class VertexValueCodec implements Name<StreamingCodec> {

  }

  @NamedParameter(doc = "The codec class for encoding and decoding edge values")
  public final class EdgeCodec implements Name<StreamingCodec> {

  }

  @NamedParameter(doc = "Vertex table identifier", default_value = VertexTableId.DEFAULT_VALUE)
  public final class VertexTableId implements Name<String> {
    public static final String DEFAULT_VALUE = "vertex_table";

    private VertexTableId() {
    }
  }

  @NamedParameter(doc = "Message table identifier", default_value = MessageTableId.DEFAULT_VALUE)
  public final class MessageTableId implements Name<String> {
    public static final String DEFAULT_VALUE = "msg_table";

    private MessageTableId() {
    }
  }
}
