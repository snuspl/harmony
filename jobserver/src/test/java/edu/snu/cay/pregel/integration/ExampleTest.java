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
package edu.snu.cay.pregel.integration;

import edu.snu.cay.dolphin.examples.addvector.AddVectorET;
import edu.snu.cay.pregel.graphapps.pagerank.PagerankET;
import edu.snu.cay.pregel.graphapps.shortestpath.ShortestPathET;
import edu.snu.cay.utils.test.IntegrationTest;
import org.apache.reef.client.LauncherStatus;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

/**
 * Test class for running example pregel apps.
 */
@Category(IntegrationTest.class)
public final class ExampleTest {

  @Test
  public void testPagerank() throws IOException, InjectionException {
    final String[] args = getArguments();
    assertEquals("The job has been failed", LauncherStatus.COMPLETED, PagerankET.runPagerank(args));
  }

  @Test
  public void testShortestPath() throws IOException, InjectionException {
    final String[] args = getArguments();
    assertEquals("The job has been failed", LauncherStatus.COMPLETED, ShortestPathET.runShortestPath(args));
  }

  private String[] getArguments() {
    final int numWorkers = 3;

    return new String[] {
        "-max_num_eval_local", Integer.toString(numWorkers),
        "-num_pregel_workers", Integer.toString(numWorkers),
        "-input", ClassLoader.getSystemResource("data").getPath() + "/adj_list",
        "-pregel_worker_mem_size", Integer.toString(128),
        "-pregel_worker_num_cores", Integer.toString(1),
        "-driver_memory", Integer.toString(128),
        "-timeout", Integer.toString(300000)
    };
  }
}
