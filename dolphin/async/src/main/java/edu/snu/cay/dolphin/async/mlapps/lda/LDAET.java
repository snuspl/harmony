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
package edu.snu.cay.dolphin.async.mlapps.lda;

import edu.snu.cay.dolphin.async.core.client.ETDolphinConfiguration;
import edu.snu.cay.dolphin.async.core.client.ETDolphinLauncher;
import edu.snu.cay.dolphin.async.mlapps.lda.LDAParameters.Alpha;
import edu.snu.cay.dolphin.async.mlapps.lda.LDAParameters.Beta;
import edu.snu.cay.dolphin.async.mlapps.lda.LDAParameters.NumTopics;
import edu.snu.cay.dolphin.async.mlapps.lda.LDAParameters.NumVocabs;
import edu.snu.cay.utils.IntegerCodec;
import edu.snu.cay.utils.LongCodec;
import edu.snu.cay.utils.StreamingSerializableCodec;

import javax.inject.Inject;

/**
 * Run Latent Dirichlet Allocation algorithm on dolphin-async.
 * Input dataset should be preprocessed to have continuous (no missing) vocabulary indices.
 */
public final class LDAET {

  @Inject
  private LDAET() {
  }

  public static void main(final String[] args) {
    ETDolphinLauncher.launch("LDAET", args, ETDolphinConfiguration.newBuilder()
        .setTrainerClass(LDATrainer.class)
        .setInputParserClass(LDAETDataParser.class)
        .setInputKeyCodecClass(LongCodec.class)
        .setInputValueCodecClass(LDADataCodec.class)
        .setModelUpdateFunctionClass(LDAETModelUpdateFunction.class)
        .setModelKeyCodecClass(IntegerCodec.class)
        .setModelValueCodecClass(SparseArrayCodec.class)
        .setModelUpdateValueCodecClass(StreamingSerializableCodec.class)
        .addParameterClass(Alpha.class)
        .addParameterClass(Beta.class)
        .addParameterClass(NumTopics.class)
        .addParameterClass(NumVocabs.class)
        .build());
  }
}
