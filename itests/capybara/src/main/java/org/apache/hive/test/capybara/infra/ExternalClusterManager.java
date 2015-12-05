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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Manage external clusters.
 */
public class ExternalClusterManager implements ClusterManager {
  static final private Logger LOG = LoggerFactory.getLogger(ExternalClusterManager.class);

  private Configuration conf;
  private FileSystem fs;
  private HiveStore hive;
  private Map<String, String> confVars;

  @Override
  public void setup() {
    // NOP
  }

  @Override
  public void tearDown() {
    // NOP
  }

  @Override
  public boolean remote() {
    return true;
  }

  @Override
  public FileSystem getFileSystem() throws IOException {
    assert conf != null;
    if (fs == null) {
      fs = FileSystem.get(conf);
      LOG.debug("Returning file system, fs.defaultFS is " + conf.get("fs.defaultFS"));
    }
    return fs;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public HiveStore getHive() {
    assert conf != null;
    if (hive == null) {
      hive = new ClusterCliHiveStore(this);
    }
    // Reset the conf file each time, because it may have changed
    hive.setConf(conf);
    return hive;
  }

  @Override
  public String getJdbcURL() {
    // TODO
    return null;
  }

  @Override
  public void setConfVar(String var, String val) {
    if (confVars == null) confVars = new HashMap<>();
    confVars.put(var, val);
  }

  @Override
  public Map<String, String> getConfVars() {
    return confVars;
  }

  @Override
  public void unsetHive() {
    // NOP, since we're not directly connected to Hive we don't need to build a new instance each
    // time to reset the config.

  }

  @Override
  public void registerTable(String dbName, String tableName) {
    // NOP, as we want tables to be long lived on external clusters.
  }
}
