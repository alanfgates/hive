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

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.QueryPlan;
import org.apache.hive.hcatalog.streaming.HiveEndPoint;
import org.apache.hive.test.capybara.data.DataSet;
import org.apache.hive.test.capybara.data.FetchResult;
import org.apache.hive.test.capybara.data.ResultCode;
import org.apache.hive.test.capybara.iface.DataStore;
import org.apache.hive.test.capybara.iface.TestTable;

import java.io.IOException;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;

/**
 * For testing when you don't want to create a real test or benchmark cluster, but just something
 * that returns empty results everywhere.
 */
class NullCluster extends ClusterManagerBase {
  @Override
  public boolean remote() {
    return false;
  }

  @Override
  public DataStore getStore() {
    return new NullStore();
  }

  @Override
  public HiveConf getHiveConf() {
    return new HiveConf();
  }

  private class NullStore extends DataStoreBase {
    @Override
    protected String ifExists() {
      return "";
    }

    @Override
    protected String markColumnPrimaryKey() {
      return "";
    }

    @Override
    protected String connectionURL() {
      return "";
    }

    @Override
    protected Properties connectionProperties() {
      return new Properties();
    }

    @Override
    public boolean createTable(TestTable table) throws SQLException, IOException {
      return true;
    }

    @Override
    public void forceCreateTable(TestTable table) throws SQLException, IOException {

    }

    @Override
    public FetchResult executeSql(String sql) throws SQLException, IOException {
      return new FetchResult(ResultCode.SUCCESS);
    }

    @Override
    public void loadData(TestTable table, DataSet rows) throws SQLException, IOException {

    }

    @Override
    public void dumpToFileForImport(DataSet rows) throws IOException {

    }

    @Override
    public String getTableName(TestTable table) {
      return table.getTableName();
    }

    @Override
    public Connection getJdbcConnection(boolean autoCommit) throws SQLException {
      return null;
    }

    @Override
    public Class<? extends Driver> getJdbcDriverClass() {
      return null;
    }

    @Override
    public HiveEndPoint getStreamingEndPoint(TestTable testTable, List<String> partVals) {
      return null;
    }

    @Override
    public QueryPlan explain(String sql) {
      return null;
    }
  }

}
