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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.metastore.api.FieldSchema;

import java.io.IOException;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Properties;

/**
 * Base methods that most DataStores will want to use.
 */
abstract class DataStoreBase implements DataStore {
  static final private Logger LOG = LoggerFactory.getLogger(DataStoreBase.class.getName());

  protected final ClusterManager clusterManager;
  /**
   * SQL driver.  It is the responsibility of the sub-class to set this value.
   */
  protected Driver jdbcDriver;

  /**
   * Constructor for DataStores that will not be running on the cluster.  This should be all
   * databases, such as Derby and Postges.
   */
  protected DataStoreBase() {
    clusterManager = null;
  }

  /**
   * Constructor for DataStores that will be running on the cluster, including mini-clusters.  In
   * general this should just be Hive.
   * @param clusterManager ClusterManager for this test
   */
  protected DataStoreBase(ClusterManager clusterManager) {
    this.clusterManager = clusterManager;
  }

  /**
   * Check if a table for use in testing already exists and is in the correct state.  Being in
   * the correct state means that the scale is correct for this test, the storage format is
   * the correct on for this test (Hive tables only), and it is stored in the right type of
   * metastore (Hive tables only).
   * @param table table to check.
   * @return true if the table already exists, false if it does not or is in the wrong state.
   * @throws SQLException
   */
  protected boolean tableExistsInCorrectState(TestTable table) throws SQLException {
    return tableExistsInCorrectState(table, false);
  }

  private boolean tableExistsInCorrectState(TestTable table, boolean beenThereDoneThat)
      throws SQLException {
    StringBuilder sql = new StringBuilder();
    sql.append("select metastore, fileformat, scale from testtables where tablename = '")
        .append(getTableName(table))
        .append('\'');
    try {
      FetchResult fetch = fetchData(sql.toString());
      if (fetch.rc != FetchResult.ResultCode.SUCCESS) {
        if (beenThereDoneThat) {
          throw new RuntimeException("Unable to instantiate metadata table for testing.");
        } else {
          createTestTableTracker();
          return tableExistsInCorrectState(table, true);
        }
      } else {
        if (fetch.hasResults()) {
          if (fetch.data.getSchema() == null) {
            // If this is Hive we have to tell it the schema we expect in the query.
            fetch.data.setSchema(Arrays.asList(
                new FieldSchema("metastore", "varchar(5)", ""),
                new FieldSchema("fileformat", "varchar(10)", ""),
                new FieldSchema("scale", "int", "")
            ));
          }
          Iterator<DataSet.Row> iter = fetch.data.iterator();
          if (iter.hasNext()) {
            DataSet.Row row = iter.next();
            return TestConf.fileFormat().equalsIgnoreCase(row.get(1).asString()) &&
                TestConf.getScale() == row.get(2).asInt();
          } else {
            return false;
          }
        } else {
          return false;
        }
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private void createTestTableTracker() throws SQLException, IOException {
    FetchResult fetch = fetchData("create table testtables (tablename varchar(100) " +
        markColumnPrimaryKey() + ", metastore varchar(5), fileformat varchar(10), scale int)");
    if (fetch.rc != FetchResult.ResultCode.SUCCESS) {
      throw new RuntimeException("Unable to create test metadata table");
    }
    if (clusterManager != null) clusterManager.registerTable(null, "testtables");
  }

  /**
   * Record that a table was created for testing.  The table name, scale, file format, and which
   * type of metastore it is stored in will be recorded.
   * @param table table that was created.
   * @throws SQLException
   */
  protected void recordTableCreation(TestTable table) throws SQLException {
    StringBuilder sql = new StringBuilder();
    sql.append("insert into testtables (tablename, metastore, fileformat, scale) values ('")
        .append(getTableName(table))
        .append("', '")
        .append(TestConf.metastore())
        .append("', '")
        .append(TestConf.fileFormat())
        .append("', ")
        .append(TestConf.getScale())
        .append(')');
    try {
      FetchResult fetch = fetchData(sql.toString());
      if (fetch.rc != FetchResult.ResultCode.SUCCESS) {
        throw new RuntimeException("Unable to record data in test metadata table");
      }
      if (clusterManager != null) clusterManager.registerTable(table.getDbName(), table.getTableName());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Get the proper string for "if exists" for a database.  May be empty.  Should not be null.
   * @return if exists string
   */
  protected abstract String ifExists();

  /**
   * Get the proper string for marking a single column a primary key.  May be empty.  Should not
   * be null.
   * @return SQL string to mark a column as a primary key.
   */
  protected abstract String markColumnPrimaryKey();

  /**
   * Drop a table and remember that it's dropped.  This is for testing.
   * @param table table to drop
   * @throws SQLException
   * @throws IOException
   */
  @VisibleForTesting
  void dropTable(TestTable table) throws SQLException, IOException {
    StringBuilder sql = new StringBuilder();
    sql.append("delete from testtables where tablename = '")
        .append(getTableName(table))
        .append('\'');
    try {
      FetchResult dropFetch = fetchData("drop table " + ifExists() + " " + getTableName(table));
      if (dropFetch.rc != FetchResult.ResultCode.SUCCESS) {
        LOG.debug("Failed to drop table, most likely means it does not exist yet" +  dropFetch.rc);
      }
      FetchResult metaFetch = fetchData(sql.toString());
      if (metaFetch.rc != FetchResult.ResultCode.SUCCESS) {
        LOG.debug("Failed to drop table from metadata, most likely means we don't know about it " +
            "yet, code is " + metaFetch.rc);
      }
    } catch (SQLException | IOException e) {
      LOG.debug("Failed to drop table from metadata, most likely means we don't know about it " +
          "yet", e);
    }
  }

  /**
   * Get the URL to connect to the JDBC driver.  Subclasses that do not connect via JDBC can
   * return null for this.
   * @return URL
   */
  protected abstract String connectionURL();

  /**
   * Get any properties that need to be passed as part of connecting to JDBC.  Subclasses that do
   * not connect via JDBC can return null for this.
   * @return properties
   */
  protected abstract Properties connectionProperties();

  /**
   * Get a JDBC connection.
   * @param autoCommit whether or not the connection should be set to autoCommit
   * @return JDBC Connection object
   * @throws SQLException
   */
  protected Connection connect(boolean autoCommit) throws SQLException {
    Connection conn = jdbcDriver.connect(connectionURL(), connectionProperties());
    conn.setAutoCommit(autoCommit);
    return conn;
  }

  /**
   * Do a fetch via JDBC.  This assumes that {@link #connect} has already been called.
   * @param sql SQL string to be executed.  If it is a select then executeQuery() will be called,
   *            otherwise execute().
   * @param limit Limit the number of rows returned.  This is for implementations like Derby that
   *              do not support a LIMIT clause.
   * @param failureOk whether it's ok for this to fail.  If true, SUCCESS will be returned
   *                  regardless of the results of the operation.  This is useful to replicate
   *                  the functionality of "create table if not exists"
   * @return A FetchResult, which contains information on success/failure (assuming failureOk is
   * false) and the data (if there is a query)
   * @throws SQLException
   * @throws IOException
   */
  protected FetchResult jdbcFetch(String sql, long limit, boolean failureOk)
      throws SQLException, IOException {
    // If this is a DML or DDL statement we have to handle it separately because you can't use
    // stmt.executeQuery with those.
    if (!sql.toLowerCase().startsWith("select")) return executeStatement(sql, failureOk);
    Connection conn = connect(true);
    Statement stmt = null;
    try {

      stmt = conn.createStatement();
      ResultSet rs = stmt.executeQuery(sql);
      return new FetchResult(new ResultSetDataSet(rs, limit));
    } catch (SQLException e) {
      if (failureOk) {
        LOG.info("Failed to execute query <" + sql + "> but continuing anyway because we " +
            "think that's ok.");
        return new FetchResult(FetchResult.ResultCode.SUCCESS);
      }
      LOG.debug("Failed to run SQL query <" + sql + ">, got exception ", e);
      return new FetchResult(FetchResult.ResultCode.NON_RETRIABLE_FAILURE);
    } finally {
      if (stmt != null) stmt.close();
      conn.close();
    }
  }

  private FetchResult executeStatement(String sql, boolean failureOk) throws SQLException {
    Connection conn = connect(true);
    Statement stmt = null;
    try {
      stmt = conn.createStatement();
      stmt.execute(sql);
      return new FetchResult(FetchResult.ResultCode.SUCCESS);
    } catch (SQLException e) {
      if (failureOk) {
        LOG.info("Failed to execute statement <" + sql + "> but continuing anyway because we " +
            "think that's ok.");
        return new FetchResult(FetchResult.ResultCode.SUCCESS);
      }
      LOG.debug("Failed to run SQL query <" + sql + ">, got exception ", e);
      return new FetchResult(FetchResult.ResultCode.NON_RETRIABLE_FAILURE);
    } finally {
      if (stmt != null) stmt.close();
      conn.close();
    }
  }
}
