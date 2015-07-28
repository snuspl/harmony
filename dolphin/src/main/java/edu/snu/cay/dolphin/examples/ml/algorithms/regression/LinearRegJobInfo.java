/*
 * Copyright (C) 2015 Seoul National University
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
package edu.snu.cay.dolphin.examples.ml.algorithms.regression;

import edu.snu.cay.dolphin.core.StageInfo;
import edu.snu.cay.dolphin.core.metric.MetricTrackerTime;
import edu.snu.cay.dolphin.examples.ml.data.RegressionDataParser;
import edu.snu.cay.dolphin.examples.ml.parameters.CommunicationGroup;
import edu.snu.cay.dolphin.examples.ml.sub.LinearRegReduceFunction;
import edu.snu.cay.dolphin.examples.ml.sub.LinearRegSummaryCodec;
import edu.snu.cay.dolphin.core.DataParser;
import edu.snu.cay.dolphin.core.UserJobInfo;
import edu.snu.cay.dolphin.core.metric.MetricTrackerGC;
import edu.snu.cay.dolphin.examples.ml.sub.LinearModelCodec;

import javax.inject.Inject;
import java.util.LinkedList;
import java.util.List;

public final class LinearRegJobInfo implements UserJobInfo {

  @Inject
  public LinearRegJobInfo(){
  }

  @Override
  public List<StageInfo> getStageInfoList() {
    final List<StageInfo> stageInfoList = new LinkedList<>();

    stageInfoList.add(
        StageInfo.newBuilder(LinearRegCmpTask.class, LinearRegCtrlTask.class, CommunicationGroup.class)
            .setBroadcast(LinearModelCodec.class)
            .setReduce(LinearRegSummaryCodec.class, LinearRegReduceFunction.class)
            .addMetricTrackers(MetricTrackerTime.class, MetricTrackerGC.class)
            .build());

    return stageInfoList;
  }

  @Override
  public Class<? extends DataParser> getDataParser() {
    return RegressionDataParser.class;
  }
}
