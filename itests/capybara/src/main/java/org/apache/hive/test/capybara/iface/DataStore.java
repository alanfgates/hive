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

import org.apache.hadoop.hive.ql.QueryPlan;
import org.apache.hive.hcatalog.streaming.HiveEndPoint;
import org.apache.hive.test.capybara.data.DataSet;
import org.apache.hive.test.capybara.data.FetchResult;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

/**
 * Manage data stores for a test.  This is used to manage Hive and benchmark stores.  Classes that
 * implement this interface are expected to be intelligent about whether a requested table already
 * exists, and if so not re-create it.  They are also expected to be intelligent about caching
 * generated benchmarks and not re-creating them.  Methods for doing this are provided in
 * {@link org.apache.hive.test.capybara.infra.DataStoreBase}.  Classes that implement
 * DataStore will want to extend that rather than implement this interface directly.
 */
public interface DataStore {

  /**
   * Prepare the cluster for testing.  This will be called once at the beginning of a set of
   * tests (in an @BeforeClass method).
   * @param clusterMgr Cluster manager controlling this DataStore.
   * @throws IOException
   */
  void setup(ClusterManager clusterMgr) throws IOException;

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
   * Create a table.  This is intended for use by common tables that will be used in many tests.
   * If you are creating a small table for your single test or you want a table with special
   * setting (like you want to control parameters or something) you should use
   * {@link #executeSql} with a "create table" statement.
   * @param table definition of table to create
   * @return indicates whether the table was actually created, or if it already existed in the
   * desired format.  If true, the table was actually created.
   * @throws SQLException
   * @throws java.io.IOException
   */
  boolean createTable(TestTable table) throws SQLException, IOException;

  /**
   * Create a table, even if it already exists.  If the table already exists it will be dropped
   * and then re-created.  Use this carefully as it removes the system's ability to re-use
   * existing generated data.  It should only be used for tables that will be constructed as part
   * of the test rather than in prep for the test.
   * @param table table to create.
   * @throws SQLException
   * @throws IOException
   */
  void forceCreateTable(TestTable table) throws SQLException, IOException;

  /**
   * Execute SQL against a the DataStore.  This can be used for queries and for statements.
   * @param sql SQL to execute
   * @return a FetchResult with a ResultCode and, if this was a query and it succeeded, a DataSet
   * @throws SQLException anything thrown by the underlying implementaiton.
   * @throws java.io.IOException
   */
  FetchResult executeSql(String sql) throws SQLException, IOException;

  /**
   * Load data into a data store.  You should only call this if {@link #createTable} returned
   * true.  Otherwise you will double load data in the table.
   * @param table Table to load the data into.
   * @param rows data to be loaded into the table
   * @throws SQLException anything thrown by the underlying implementation.
   * @throws java.io.IOException
   */
  void loadData(TestTable table, DataSet rows) throws SQLException, IOException;

  /**
   * Dump a data set into a file in preparation for import to this DataStore.  This method may be
   * called more than once with the same DataSet.  In that case it should append the new rows
   * into the already existing file.
   * @param rows DataSet to dump to a file.
   * @throws IOException
   */
  void dumpToFileForImport(DataSet rows) throws IOException;

  /**
   * Get how this table is named in this store.  Stores may choose to mangle table names in order
   * to flatten out Hive's database structure.  This method needs to return the mangled name.
   * @return name of the table.
   */
  String getTableName(TestTable table);

  /**
   * Get information on how to talk to this DataStore via JDBC.  Non-JDBC compliant data stores
   * (such as Hive when being tested in CLI mode) will return a null.
   * @return URL and properties that can be used to connect to the DataStore, or null if JDBC is
   * not an option.
   */
  //JdbcInfo getJdbcConnectionInfo();

  /**
   * Get a JDBC connection to this DataStore.  Non-JDBC data stores will return a null.
   * @param autoCommit Whether to set autoCommit on this connection
   * @return
   * @throws java.sql.SQLException
   */
  Connection getJdbcConnection(boolean autoCommit) throws SQLException;

  /**
   * Get the class of the JDBC driver.  Non-JDBC data stores will return a null.
   * @return Class of the JDBC driver.
   */
  Class<? extends java.sql.Driver> getJdbcDriverClass();

  /**
   * Get an endpoint where streaming records can be sent.
   * @param testTable Table being tested
   * @param partVals Partition values in this stream, can be null if the table is not partitioned.
   * @return StreamingEndPoint
   */
  HiveEndPoint getStreamingEndPoint(TestTable testTable, List<String> partVals);

  /**
   * Explain a SQL query.  This should only be implemented by local implementations of Hive.
   * Others should return a null.
   * @param sql SQL to explain
   * @return plan for this SQL
   */
  QueryPlan explain(String sql);
}
