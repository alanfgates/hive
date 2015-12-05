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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.derby.jdbc.EmbeddedDriver;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Controller for Derby.
 */
public class DerbyStore extends AnsiSqlStore {
  private static final Logger LOG = LoggerFactory.getLogger(DerbyStore.class.getName());
  private static final String DELIMITER_STR = ",";
  private static final String QUOTES = "\"";
  private static final String NULL_STR = "";
  private static final String DERBY_URL;

  private long limit;

  static {
    DERBY_URL = new StringBuilder("jdbc:derby:")
        .append(System.getProperty("java.io.tmpdir"))
        .append(System.getProperty("file.separator"))
        .append("hiveitestdb;create=true")
        .toString();
  }

  DerbyStore() {
    jdbcDriver = new EmbeddedDriver();
  }

  @Override
  public void loadData(TestTable table, DataSet rows)
      throws SQLException, IOException {
    dumpToFileForImport(rows);
    File file = dataSetDumps.get(rows.uniqueId());
    StringBuilder sql = new StringBuilder("CALL SYSCS_UTIL.SYSCS_IMPORT_TABLE (");
    if (table.getDbName().equalsIgnoreCase("default")) sql.append("null, '");
    else sql.append('\'').append(table.getDbName().toUpperCase()).append("', '");
    // Table name must be upper cased.
    sql.append(table.getTableName().toUpperCase())
        .append("', '")
        .append(file.getAbsolutePath())
        .append("', '")
        .append(DELIMITER_STR)
        .append("', '")
        .append(QUOTES)
        .append("', null, 0)");

    if (LOG.isDebugEnabled()) {
      LOG.debug("Going to import into Derby with command " + sql.toString());
    }
    Connection conn = connect(false);
    Statement stmt = null;
    try {
      stmt = conn.createStatement();
      stmt.execute(sql.toString());
    } finally {
      if (stmt != null) stmt.close();
      conn.close();
    }
  }

  @Override
  public String getTableName(TestTable table) {
    return "default".equals(table.getDbName()) ? table.getTableName() : table.toString();
  }

  @Override
  protected String hiveSqlToAnsiSql(String hiveSql) throws IOException, SQLException {
    // TODO this will not return the right result for subqueries with limits
    Matcher matcher =
        Pattern.compile(".*\\slimit\\s+([0-9]+).*", Pattern.CASE_INSENSITIVE).matcher(hiveSql);
    if (matcher.matches()) {
      limit = Long.valueOf(matcher.group(1));
      // Now remove the limit.
      matcher = Pattern.compile("\\slimit\\s+[0-9]+", Pattern.CASE_INSENSITIVE).matcher(hiveSql);
      hiveSql = matcher.replaceAll(" ");
    } else {
      limit = Long.MAX_VALUE;
    }
    return super.hiveSqlToAnsiSql(hiveSql);
  }

  @Override
  protected String ifExists() {
    return "";
  }

  @Override
  protected String markColumnPrimaryKey() {
    return "not null primary key";
  }

  @Override
  protected String connectionURL() {
    return DERBY_URL;
  }

  @Override
  protected Properties connectionProperties() {
    return new Properties();
  }

  @Override
  protected String fileColumnDelimiter() {
    return DELIMITER_STR;
  }

  @Override
  protected String fileNull() {
    return NULL_STR;
  }

  @Override
  protected String fileStringQuotes() {
    return QUOTES;
  }

  @Override
  public Class getDriverClass() {
    return jdbcDriver.getClass();
  }

  @Override
  protected long getLimit() {
    return limit;
  }
}
