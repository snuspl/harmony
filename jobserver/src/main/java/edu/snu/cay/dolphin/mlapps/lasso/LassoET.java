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
package edu.snu.cay.dolphin.mlapps.lasso;

import edu.snu.cay.dolphin.core.client.ETDolphinConfiguration;
import edu.snu.cay.dolphin.core.client.ETDolphinLauncher;
import edu.snu.cay.dolphin.mlapps.serialization.DenseVectorCodec;
import edu.snu.cay.utils.StreamingSerializableCodec;

import javax.inject.Inject;

/**
 * Application launching code for LassoET.
 */
public final class LassoET {

  @Inject
  private LassoET() {
  }

  public static void main(final String[] args) {
    ETDolphinLauncher.launch("LassoET", args, ETDolphinConfiguration.newBuilder()
        .setTrainerClass(LassoTrainer.class)
        .setInputParserClass(LassoETParser.class)
        .setInputKeyCodecClass(StreamingSerializableCodec.class)
        .setInputValueCodecClass(LassoDataCodec.class)
        .setModelUpdateFunctionClass(LassoETModelUpdateFunction.class)
        .setModelKeyCodecClass(StreamingSerializableCodec.class)
        .setModelValueCodecClass(DenseVectorCodec.class)
        .setModelUpdateValueCodecClass(DenseVectorCodec.class)
        .build());
  }
}
