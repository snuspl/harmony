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
package edu.snu.cay.dolphin.mlapps.gbt;


import edu.snu.cay.dolphin.core.client.ETDolphinConfiguration;
import edu.snu.cay.dolphin.jobserver.DolphinJobLauncher;
import edu.snu.cay.dolphin.mlapps.serialization.GBTreeCodec;
import edu.snu.cay.dolphin.mlapps.serialization.GBTreeListCodec;
import edu.snu.cay.utils.StreamingSerializableCodec;

/**
 * Application launching code for GBT with JobServer.
 */
public final class GBTJob {

  private GBTJob() {
  }

  public static void main(final String[] args) {
    DolphinJobLauncher.submitJob("GBT", args, ETDolphinConfiguration.newBuilder()
        .setTrainerClass(GBTTrainer.class)
        .setInputParserClass(GBTETDataParser.class)
        .setInputKeyCodecClass(StreamingSerializableCodec.class)
        .setInputValueCodecClass(GBTDataCodec.class)
        .setModelUpdateFunctionClass(GBTETModelUpdateFunction.class)
        .setModelKeyCodecClass(StreamingSerializableCodec.class)
        .setModelValueCodecClass(GBTreeListCodec.class)
        .setModelUpdateValueCodecClass(GBTreeCodec.class)
        .addParameterClass(GBTParameters.Gamma.class)
        .addParameterClass(GBTParameters.TreeMaxDepth.class)
        .addParameterClass(GBTParameters.LeafMinSize.class)
        .addParameterClass(GBTParameters.MetadataPath.class)
        .addParameterClass(GBTParameters.NumKeys.class)
        .build());
  }
}
