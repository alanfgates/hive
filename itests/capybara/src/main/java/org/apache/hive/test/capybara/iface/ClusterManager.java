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

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hive.test.capybara.infra.HiveStore;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;

/**
 * Manage a connection to a cluster.  This keeps track of all cluster oriented connections, such
 * as DFS, Hive, the URL for HS2, etc.
 */
public interface ClusterManager extends Configurable {

  public class JdbcInfo {
    public final String connectionString;
    public final Properties properties;

    public JdbcInfo(String connectionString, Properties properties) {
      this.connectionString = connectionString;
      this.properties = properties;
    }
  }

  /**
   * Prepare the cluster for testing.  This will be called once at the beginning of a set of
   * tests (in an @BeforeClass method).
   * @throws IOException
   */
  void setup() throws IOException;

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
   * Get a HiveStore that works for this cluster.
   * @return connection to Hive
   */
  HiveStore getHive();

  /**
   * Get information on how to talk to Hive via JDBC.  If the access method is not set to "jdbc"
   * the result of calling this method is undefined (i.e., it's likely to go up in flames on you).
   * @return URL and properties that can be used to connect to Hive.
   */
  JdbcInfo getJdbcConnectionInfo();

  /**
   * Register that a table was created in a cluster.  This is necessary because certain cluster
   * types tear down the FS but leave the metastore (eg mini-clusters).  To avoid issues we need
   * to explicitly drop tables when we tear down the cluster.
   * @param dbName name of the database the table is in, can be null if the table is in default
   * @param tableName name of the table
   */
  void registerTable(String dbName, String tableName);

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
}
