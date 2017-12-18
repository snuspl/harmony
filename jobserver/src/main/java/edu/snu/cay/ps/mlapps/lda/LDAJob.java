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
package edu.snu.cay.ps.mlapps.lda;

import edu.snu.cay.ps.core.client.ETDolphinConfiguration;
import edu.snu.cay.ps.jobserver.DolphinJobLauncher;
import edu.snu.cay.utils.StreamingSerializableCodec;
import org.apache.reef.io.serialization.SerializableCodec;

import javax.inject.Inject;

/**
 * Run Latent Dirichlet Allocation algorithm on dolphin JobServer.
 * Input dataset should be preprocessed to have continuous (no missing) vocabulary indices.
 */
public final class LDAJob {

  @Inject
  private LDAJob() {
  }

  public static void main(final String[] args) {
    DolphinJobLauncher.submitJob("LDA", args, ETDolphinConfiguration.newBuilder()
        .setTrainerClass(LDATrainer.class)
        .setInputParserClass(LDAETDataParser.class)
        .setInputKeyCodecClass(StreamingSerializableCodec.class)
        .setInputValueCodecClass(LDADataCodec.class)
        .setModelUpdateFunctionClass(LDAETModelUpdateFunction.class)
        .setModelKeyCodecClass(StreamingSerializableCodec.class)
        .setModelValueCodecClass(SparseArrayCodec.class)
        .setModelUpdateValueCodecClass(SerializableCodec.class)
        .addParameterClass(LDAParameters.Alpha.class)
        .addParameterClass(LDAParameters.Beta.class)
        .addParameterClass(LDAParameters.NumTopics.class)
        .addParameterClass(LDAParameters.NumVocabs.class)
        .build());
  }
}
