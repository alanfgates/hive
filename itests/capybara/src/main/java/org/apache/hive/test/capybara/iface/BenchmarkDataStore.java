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
package org.apache.hive.test.capybara.iface;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * Methods for benchmark data stores.  These do not make sense for HiveStore to implement.  This
 * does not extend DataStore because all DataStores (including Hive) extend DataStoreBase.  This
 * interface is meant to be implemented by benchmark datastores in addition to extending
 * DataStoreBase.
 */
public interface BenchmarkDataStore extends DataStore {

  /**
   * Get the the class for the JDBC driver.  This will be used to grab the associated jar and
   * ship it to the cluster when needed.
   * @return Driver class.
   */
  Class getDriverClass();

  /**
   * Get a JDBC connection.  The intended use for this is to write records into the benchmark
   * that come from Hive streaming.
   * @param autoCommit whether autoCommit should be set for this connection
   * @return a JDBC connection.
   * @throws java.sql.SQLException
   */
  Connection getJdbcConnection(boolean autoCommit) throws SQLException;

  /**
   * Cleanup after a test has been run.  Benchmark implementation can use this to do things like
   * clean up any temporary state, etc.
   * @throws SQLException
   */
  void cleanupAfterTest() throws SQLException, IOException;
}
