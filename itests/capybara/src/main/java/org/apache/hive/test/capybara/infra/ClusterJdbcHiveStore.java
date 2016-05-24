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

import org.apache.hadoop.hive.ql.QueryPlan;
import org.apache.hive.test.capybara.data.FetchResult;

import java.io.IOException;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class ClusterJdbcHiveStore extends HiveStore {

  @Override
  public QueryPlan explain(String sql) {
    return null;
  }

  @Override
  public String getMetastoreUri() {
    return null;
  }

  @Override
  protected String connectionURL() {
    ClusterConf cc = clusterManager.getClusterConf();
    StringBuilder url = new StringBuilder("jdbc:hive2://")
        .append(cc.getJdbcHost())
        .append(cc.getJdbcPort());
    return url.toString();
  }

  @Override
  protected Properties connectionProperties() {
    ClusterConf cc = clusterManager.getClusterConf();
    Properties properties = new Properties();
    properties.put("user", cc.getJdbcUser());
    properties.put("password", cc.getJdbcPasswd());
    return properties;
  }

  @Override
  public FetchResult executeSql(String sql) throws SQLException, IOException {
    instantiateJdbcDriver();
    return jdbcFetch(sql);
  }

  @Override
  public Connection getJdbcConnection(boolean autoCommit) throws SQLException {
    instantiateJdbcDriver();
    Connection conn = jdbcDriver.connect(connectionURL(), connectionProperties());
    conn.setAutoCommit(autoCommit);
    return conn;
  }

  @Override
  public Class<? extends Driver> getJdbcDriverClass() {
    try {
      instantiateJdbcDriver();
      return jdbcDriver.getClass();
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  private void instantiateJdbcDriver() throws SQLException {
    if (jdbcDriver == null) {
      jdbcDriver = DriverManager.getDriver(connectionURL());
      if (jdbcDriver == null) {
        throw new RuntimeException("Unable to locate JDBC driver for URL " + connectionURL());
      }
    }
  }
}
