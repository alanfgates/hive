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
package org.apache.hive.test.capybara.iface;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.test.capybara.infra.ClusterConf;

import java.io.IOException;
import java.util.Map;

/**
 * Manage a connection to a cluster.  This keeps track of all cluster oriented connections, such
 * as DFS, Hive, the URL for HS2, etc.
 */
public interface ClusterManager {

  /**
   * Prepare the cluster for testing.  This will be called once at the beginning of a set of
   * tests (in an @BeforeClass method).
   * @param clusterType Indicates whether this is a test or bench cluster.  Valid values are 
   * {@link org.apache.hive.test.capybara.infra.TestConf#TEST_CLUSTER} and 
   * {@link org.apache.hive.test.capybara.infra.TestConf#BENCH_CLUSTER}.
   * @throws IOException
   */
  void setup(String clusterType) throws IOException;

  /**
   * Tear down the cluster after testing.  This will be called once at the end of a set of tests
   * (in an @AfterClass method).
   * @throws IOException
   */
  void tearDown() throws IOException;

  /**
   * Called before each test is run.
   * @throws IOException
   */
  void beforeTest() throws IOException;

  /**
   * Called after each test is run.
   * @throws IOException
   */
  void afterTest() throws IOException;

  /**
   * Indicate whether this is a remote cluster.
   * @return true if it's remote, false if it's local using mini-clusters
   */
  boolean remote();

  /**
   * Get the file system associated with this cluster.
   * @return FileSystem handle
   * @throws IOException
   */
  FileSystem getFileSystem() throws IOException;

  /**
   * Get a DataStore that works for this cluster.
   * @return connection to DataStore
   */
  DataStore getStore() throws IOException;

  /**
   * Directory we can use for storing a temporary file.  This should be HDFS accessible if the 
   * implementing class is managing a real cluster.
   * @return Directory for dump files.
   */
  String getDirForDumpFile();

  /**
   * Get the configuration object for this cluster.
   * @return cluster conf
   */
  ClusterConf getClusterConf();

  /**
   * Get HiveConf object appropriate for this cluster.
   * @return hive conf
   */
  HiveConf getHiveConf();

  /**
   * Set a configuration value that will be passed to Hive.  How it is passed is up to the
   * cluster manager.
   * @param var variable to set
   * @param val value to set it to.
   */
  void setConfVar(String var, String val);

  /**
   * Get the configuration variables set in this test.
   * @return conf vars set.
   */
  Map<String, String> getConfVars();

  /**
   * Determine what type of cluster this is, test or benchmark.
   * @return valid values are {@link org.apache.hive.test.capybara.infra.TestConf#TEST_CLUSTER}
   * and {@link org.apache.hive.test.capybara.infra.TestConf#BENCH_CLUSTER}.
   */
  String getClusterType();
}
