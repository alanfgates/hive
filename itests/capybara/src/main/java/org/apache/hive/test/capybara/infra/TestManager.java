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
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;

/**
 * Manage all aspects of the test.  This is the glue that holds all the pieces together.  The two
 * most important pieces here are the ClusterManager, which contains references to the cluster
 * being run on, and Benchmark, which references the generator of expected results for the tests.
 */
public class TestManager implements Configurable {

  private static TestManager self = null;

  private Configuration conf;
  private ClusterManager cluster;
  private Benchmark bench;
  private Benchmark oneTimeBench;

  public static TestManager getTestManager() {
    if (self == null) {
      self = new TestManager();
    }
    return self;
  }

  private TestManager() {

  }

  /**
   * Get the cluster manager for this test.
   * @return cluster manager
   */
  public ClusterManager getClusterManager() {
    if (cluster == null) {
      cluster = TestConf.onCluster() ? new ExternalClusterManager() : new MiniClusterManager();
      cluster.setConf(conf);
    }
    return cluster;
  }

  /**
   * Get the benchmark for this test.
   * @return benchmark
   */
  public Benchmark getBenchmark() {
    if (oneTimeBench != null) return oneTimeBench;
    if (bench == null) {
      bench = TestConf.onCluster() ? new PostgresBenchmark() : new DerbyBenchmark();
    }
    return bench;
  }

  /**
   * Set up a special Benchmark for this test.  This gives the user an opportunity to inject a
   * special Benchmark for a particular test, rather than using whatever is standard for the
   * current configuration.  This will be reset at the end of the test.
   * @param bench special Benchmark to use
   */
  public void setOneTimeBenchmark(Benchmark bench) {
    oneTimeBench = bench;
  }

  /**
   * Reset the Benchmark to the standard for the current configuration.  This will be called at
   * the end of each test by the system.
   */
  public void resetBenchmark() {
    oneTimeBench = null;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @VisibleForTesting
  void setClusterManager(ClusterManager clusterMgr) {
    cluster = clusterMgr;
  }
}
