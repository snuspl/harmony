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
package edu.snu.cay.pregel.graphapps.pagerank;

import edu.snu.cay.pregel.PregelConfiguration;
import edu.snu.cay.pregel.PregelLauncher;
import edu.snu.cay.pregel.combiner.DoubleSumMessageCombiner;
import edu.snu.cay.pregel.common.NoneEdgeValueGraphParser;
import edu.snu.cay.pregel.common.NoneValueEdgeCodec;
import edu.snu.cay.utils.StreamingSerializableCodec;
import org.apache.reef.client.LauncherStatus;
import org.apache.reef.tang.exceptions.InjectionException;

import java.io.IOException;

/**
 * Application launching code for Pagerank.
 */
public final class PagerankET {

  /**
   * Should not be initialized this.
   */
  private PagerankET() {

  }

  /**
   * Runs app with given arguments.
   * @param args command line arguments for running app
   * @return a LauncherStatus
   */
  public static LauncherStatus runPagerank(final String[] args) throws IOException, InjectionException {
    return PregelLauncher.launch(PagerankET.class.getSimpleName(), args, PregelConfiguration.newBuilder()
        .setComputationClass(PagerankComputation.class)
        .setDataParserClass(NoneEdgeValueGraphParser.class)
        .setMessageCombinerClass(DoubleSumMessageCombiner.class)
        .setMessageValueCodecClass(StreamingSerializableCodec.class)
        .setVertexValueCodecClass(StreamingSerializableCodec.class)
        .setEdgeCodecClass(NoneValueEdgeCodec.class)
        .build());
  }

  public static void main(final String[] args) throws IOException, InjectionException {
    final LauncherStatus status = runPagerank(args);
    if (!status.equals(LauncherStatus.COMPLETED)) {
      throw new RuntimeException(String.format("Job failed. %s", status));
    }
  }
}
