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

import org.apache.hive.test.capybara.iface.ClusterManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Manage external clusters.
 */
public class ExternalClusterManager implements ClusterManager {
  static final private Logger LOG = LoggerFactory.getLogger(ExternalClusterManager.class);

  static final private String JDBC_URL = "HIVE_JDBC_URL";
  static final private String JDBC_USER = "HIVE_JDBC_USER";
  static final private String JDBC_PASSWD = "HIVE_JDBC_PASSWORD";

  private Configuration conf;
  private FileSystem fs;
  private HiveStore hive;
  private Map<String, String> confVars;
  private JdbcInfo jdbcInfo;

  @Override
  public void setup() {
    // NOP
  }

  @Override
  public void tearDown() {
    // NOP
  }

  @Override
  public void beforeTest() throws IOException {

  }

  @Override
  public void afterTest() throws IOException {

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
      if (TestConf.access().equals(TestConf.ACCESS_CLI)) hive = new ClusterCliHiveStore(this);
      else if (TestConf.access().equals(TestConf.ACCESS_JDBC)) hive = new ClusterJdbcHiveStore(this);
      else throw new RuntimeException("Unknown access method " + TestConf.access());
    }
    // Reset the conf file each time, because it may have changed
    hive.setConf(conf);
    return hive;
  }

  @Override
  public JdbcInfo getJdbcConnectionInfo() {
    if (jdbcInfo == null) {
      String jdbcUrl = System.getProperty(JDBC_URL);
      if (jdbcUrl == null) {
        throw new RuntimeException("You must set the property " + JDBC_URL +
            " to the URL for HiverServer2 to test against a cluster using JDBC");
      }
      String user = System.getProperty(JDBC_USER);
      if (user == null) {
        throw new RuntimeException("You must set the property " + JDBC_USER +
            " to the user to connect to HiverServer2 as");
      }
      String passwd = System.getProperty(JDBC_PASSWD);
      if (passwd == null) {
        throw new RuntimeException("You must set the property " + JDBC_PASSWD +
            " to the password to connect to HiverServer2 with");
      }
      Properties properties = new Properties();
      properties.put("user", user);
      properties.put("password", passwd);
      jdbcInfo = new JdbcInfo(jdbcUrl, properties);
    }
    return jdbcInfo;
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
  public void registerTable(String dbName, String tableName) {
    // NOP, as we want tables to be long lived on external clusters.
  }
}
