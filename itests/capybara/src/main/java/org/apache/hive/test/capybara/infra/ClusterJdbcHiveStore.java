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
import org.apache.hive.test.capybara.iface.ClusterManager;

import java.io.IOException;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class ClusterJdbcHiveStore extends HiveStore {
  public ClusterJdbcHiveStore(ClusterManager clusterManager) {
    super(clusterManager);
  }

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
    return clusterManager.getJdbcConnectionInfo().connectionString;
  }

  @Override
  protected Properties connectionProperties() {
    return clusterManager.getJdbcConnectionInfo().properties;
  }

  @Override
  public FetchResult fetchData(String sql) throws SQLException, IOException {
    if (jdbcDriver == null) {
      jdbcDriver = DriverManager.getDriver(connectionURL());
      if (jdbcDriver == null) {
        throw new RuntimeException("Unable to locate JDBC driver for URL " + connectionURL());
      }
    }
    return jdbcFetch(sql);
  }
}
