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

import org.apache.hive.test.capybara.data.FetchResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.util.Properties;

/**
 * A HiveStore for JDBC connections (via MiniHS2) on the local machine.
 */
class MiniHS2HiveStore extends MiniHiveStoreBase {
  private static final Logger LOG = LoggerFactory.getLogger(AnsiSqlStore.class.getName());

  @Override
  public FetchResult executeSql(String sql) throws SQLException, IOException {
    LOG.debug("Going to run query <" + sql + "> against MiniHS2");
    instantiateJdbcDriver();
    return jdbcFetch(sql, Long.MAX_VALUE, false);
  }

  @Override
  public Connection getJdbcConnection(boolean autoCommit) throws SQLException {
    instantiateJdbcDriver();
    Connection conn = jdbcDriver.connect("", null);
    return conn;
  }

  @Override
  public Class<? extends Driver> getJdbcDriverClass() {
    instantiateJdbcDriver();
    return jdbcDriver.getClass();
  }

  @Override
  protected String connectionURL() {
    return "";
  }

  @Override
  protected Properties connectionProperties() {
    return null;
  }

  private void instantiateJdbcDriver() {
    if (jdbcDriver == null) {
      try {
        Class cls = Class.forName("org.apache.hive.jdbc.HiveDriver");
        jdbcDriver = (Driver) cls.newInstance();
      } catch (ClassNotFoundException | IllegalAccessException | InstantiationException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
