/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hive.test.capybara.infra;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hive.test.capybara.iface.ClusterManager;
import org.apache.hive.test.capybara.iface.ResultComparator;
import org.apache.hive.test.capybara.iface.TableComparator;

/**
 * Manage all aspects of the test.  This is the glue that holds all the pieces together.  The two
 * most important pieces here are the ClusterManager, which contains references to the cluster
 * being run on, and Benchmark, which references the generator of expected results for the tests.
 */
public class TestManager {

  private static TestManager self = null;

  private final TestConf testConf;
  private ClusterManager testCluster;
  private ClusterManager benchCluster;

  public static TestManager getTestManager() {
    if (self == null) {
      self = new TestManager();
    }
    return self;
  }

  private TestManager() {
    testConf = new TestConf();
  }

  /**
   * Get the cluster manager for this test.
   * @return cluster manager
   */
  public ClusterManager getTestClusterManager() {
    if (testCluster == null) {
      testCluster = testConf.getTestCluster();
    }
    return testCluster;
  }

  /**
   * Get the benchmark for this test.
   * @return benchmark
   */
  public ClusterManager getBenchmarkClusterManager() {
    if (benchCluster == null) {
      benchCluster = testConf.getBenchCluster();
    }
    return benchCluster;
  }

  /**
   * Get the ResultComparator to use with this Benchmark.
   * @param sort Whether the result sets should be sorted as part of doing the comparison.  If
   *             this is false it is assumed that the results are either already sorted or single
   *             valued.
   * @return ResultComparator
   */
  public ResultComparator getResultComparator(boolean sort) {
    if (sort) return new SortingComparator();
    else return new NonSortingComparator();
  }

  /**
   * Get a ResultComparator that will compare data already in a table.  This is intended for use
   * with insert statements.  Note that it is generally slower for small data sets than
   * {@link #getResultComparator(boolean)} but much faster for large ones since it can run in the
   * cluster.
   * @return TableComparator
   */
  public TableComparator getTableComparator() {
    return new TableComparator();
  }

  public TestConf getTestConf() {
    return testConf;
  }

  @VisibleForTesting
  void setClusterManager(ClusterManager clusterMgr) {
    testCluster = clusterMgr;
  }
}
