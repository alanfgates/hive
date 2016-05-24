/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hive.test.capybara.infra;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.test.capybara.iface.ClusterManager;
import org.apache.hive.test.capybara.iface.DataStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * A base class which all ClusterManager's should extend.  Contains some basic methods everyone
 * will share.
 */
abstract class ClusterManagerBase implements ClusterManager {
  static final private Logger LOG = LoggerFactory.getLogger(ExternalClusterManager.class);

  protected Map<String, String> testSpecificConfVars;
  protected HiveConf conf;
  protected FileSystem fs;
  protected String clusterType;
  protected DataStore store;

  private ClusterConf clusterConf;

  @Override
  public void setup(String clusterType) throws IOException {
    this.clusterType = clusterType;
    getStore().setup(this);
  }

  @Override
  public void tearDown() throws IOException {
    getStore().tearDown();

  }

  @Override
  public void beforeTest() throws IOException {
    testSpecificConfVars = new HashMap<>();
    getStore().beforeTest();
  }

  @Override
  public void afterTest() throws IOException {
    getStore().afterTest();
  }

  @Override
  public final void setConfVar(String var, String val) {
    testSpecificConfVars.put(var, val);
  }

  @Override
  public final Map<String, String> getConfVars() {
    return testSpecificConfVars;
  }

  @Override
  public FileSystem getFileSystem() throws IOException {
    getHiveConf(); // Make sure we've set up the conf file
    if (fs == null) {
      fs = FileSystem.get(conf);
      LOG.debug("Returning file system, fs.defaultFS is " + conf.get("fs.defaultFS"));
    }
    return fs;
  }

  @Override
  public String getDirForDumpFile() {
    // Use '/' explicitly rather than file.separator property as HDFS always uses forward slash.
    String filename = new StringBuilder(getHiveConf().get("hadoop.tmp.dir"))
        .append('/')
        .append("capybara_")
        .append(UUID.randomUUID().toString())
        .toString();
    return filename;
  }

  @Override
  public final String getClusterType() {
    return clusterType;
  }

  @Override
  public final ClusterConf getClusterConf() {
    if (clusterConf == null) {
      if (clusterType.equals(TestConf.TEST_CLUSTER)) {
        clusterConf = TestManager.getTestManager().getTestConf().getTestClusterConf();
      } else if (clusterType.equals(TestConf.BENCH_CLUSTER)) {
        clusterConf = TestManager.getTestManager().getTestConf().getBenchClusterConf();
      } else {
        throw new RuntimeException("Unknown cluster type " + clusterType);
      }
    }
    return clusterConf;
  }
}
