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
package edu.snu.cay.dolphin.examples.ml.algorithms.graph;

import edu.snu.cay.dolphin.core.UserControllerTask;

import javax.inject.Inject;

public final class PageRankPreCtrlTask extends UserControllerTask {

  @Inject
  private PageRankPreCtrlTask() {

  }

  @Override
  public void run(final int iteration) {
    // do nothing
  }

  @Override
  public boolean isTerminated(final int iteration) {
    return iteration > 0;
  }
}