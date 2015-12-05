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

import java.io.IOException;
import java.sql.SQLException;

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
   * Create a table.  This is intended for use by common tables that will be used in many tests.
   * If you are creating a small table for your single test or you want a table with special
   * setting (like you want to control parameters or something) you should use
   * {@link #fetchData} with a "create table" statement.
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
   * Execute SQL against a the DataStore.  This can be used for queries (as the name suggests)
   * and for statements (which the name does not suggest).
   * @param sql SQL to execute
   * @return a FetchResult with a ResultCode and, if this was a query and it succeeded, a DataSet
   * @throws SQLException anything thrown by the underlying implementaiton.
   * @throws java.io.IOException
   */
  FetchResult fetchData(String sql) throws SQLException, IOException;

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
}
