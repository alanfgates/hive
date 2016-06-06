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

import org.apache.hive.test.capybara.iface.DataStore;

public interface ClusterConf {
  /**
   * Get the appropriate way to access this cluster (CLI or JDBC).
   * @return {@link org.apache.hive.test.capybara.infra.TestConf#ACCESS_CLI} or
   * {@link org.apache.hive.test.capybara.infra.TestConf#ACCESS_JDBC}
   */
  public String getAccess();

  /**
   * Get an instance of the data store for the indicated cluster.
   * @return DataStore
   */
  public DataStore getDataStore();

  /**
   * Get the engine that should be used with this cluster.
   * @return engine type, possible values are
   * {@link org.apache.hive.test.capybara.infra.TestConf#ENGINE_TEZ} and
   * {@link org.apache.hive.test.capybara.infra.TestConf#ENGINE_MR}
   */
  public String getEngine();

  /**
   * Get directory where Hadoop configuration files are stored on the local machine.
   * @return directory
   */
  public String getHadoopConfDir();

  /**
   * Get directory where Hive is stored on the local machine.
   * @return directory
   */
  public String getHiveHome();

  /**
   * Get JDBC database.
   * @return db
   */
  public String getJdbcDatabase();

  /**
   * Get JDBC host.
   * @return hostname
   */
  public String getJdbcHost();

  /**
   * Get JDBC password.
   * @return password
   */
  public String getJdbcPasswd();

  /**
   * Get JDBC port.  Returned as a string (with ':' prepended) for easy manipulation in the URL.
   * @return :port
   */
  public String getJdbcPort();

  /**
   * Get JDBC user.
   * @return user
   */
  public String getJdbcUser();

  /**
   * Get the metastore that should be used with this cluster.
   * @return metastore type, possible values are
   * {@link org.apache.hive.test.capybara.infra.TestConf#METASTORE_RDBMS} and
   * {@link org.apache.hive.test.capybara.infra.TestConf#METASTORE_HBASE}
   */
  public String getMetastore();

  /**
   * Get the number of tez tasks that should be run in the mini-cluster.
   * @return number of tasks
   */
  public int getNumTezTasks();

  /**
   * Does this cluster have security turned on?
   * @return security
   */
  public boolean isSecure();
}
