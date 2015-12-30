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

import org.apache.hive.test.capybara.data.DataSet;
import org.apache.hive.test.capybara.iface.TestTable;
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
  protected String getTempTableCreate() {
    return "declare global temporary table ";
  }

  @Override
  public Class getDriverClass() {
    return jdbcDriver.getClass();
  }

  @Override
  protected long getLimit() {
    return limit;
  }

  private SQLTranslator derbyTranslator = new SQLTranslator() {
    @Override
    protected String translateCreateDatabase(String hiveSql) throws
        TranslationException {
      String benchSql = super.translateCreateDatabase(hiveSql);
      // Remove 'if not exists'
      Matcher m = Pattern.compile("create schema if not exists").matcher(benchSql);
      if (m.find()) {
        failureOk = true;
        return m.replaceAll("create schema");
      }
      return benchSql;
    }

    @Override
    protected String translateDropDatabase(String hiveSql) throws TranslationException {
      String benchSql = super.translateDropDatabase(hiveSql);
      // Remove 'if exists'
      Matcher m = Pattern.compile("drop schema if exists").matcher(benchSql);
      if (m.find()) {
        failureOk = true;
        benchSql = m.replaceAll("drop schema");
      }

      // Derby requires restrict, so put it there if it's missing
      m = Pattern.compile("cascade$").matcher(benchSql);
      benchSql = m.replaceAll("restrict");
      if (!benchSql.contains("restrict")) {
        benchSql = benchSql + " restrict";
      }
      return benchSql;
    }

    @Override
    protected String translateCreateTableLike(Matcher matcher) {
      StringBuilder sql = new StringBuilder();
      if (matcher.group(1) != null && matcher.group(1).equals("temporary ")) {
        sql.append("declare global temporary table ");
      } else {
        sql.append("create table ");
      }
      if (matcher.group(2) != null) failureOk = true;
      sql.append(matcher.group(3))
          .append(" as select * from ")
          .append(matcher.group(4));
      return sql.toString();
    }

    @Override
    protected String translateCreateTableAs(Matcher matcher) throws TranslationException {
      StringBuilder sql = new StringBuilder();
      if (matcher.group(1) != null && matcher.group(1).equals("temporary ")) {
        sql.append("declare global temporary table ");
      } else {
        sql.append("create table ");
      }
      if (matcher.group(2) != null) failureOk = true;
      sql.append(matcher.group(3))
          .append(" as ")
          .append(translateSelect(matcher.group(4)));
      return sql.toString();
    }

    @Override
    protected String translateCreateTableWithColDefs(Matcher matcher) {
      StringBuilder sql = new StringBuilder();
      if (matcher.group(1) != null && matcher.group(1).equals("temporary ")) {
        sql.append("declare global temporary table ");
      } else {
        sql.append("create table ");
      }
      if (matcher.group(2) != null) failureOk = true;
      sql.append(matcher.group(3))
          .append(" (")
          .append(translateDataTypes(parseOutColDefs(matcher.group(4))));
      return sql.toString();
    }

    @Override
    protected String translateDataTypes(String hiveSql) {
      Matcher m = Pattern.compile(" string").matcher(hiveSql);
      hiveSql = m.replaceAll(" varchar(255)");
      m = Pattern.compile(" tinyint").matcher(hiveSql);
      hiveSql = m.replaceAll(" smallint");
      m = Pattern.compile(" binary").matcher(hiveSql);
      hiveSql = m.replaceAll(" blob");
      return hiveSql;
    }

    @Override
    protected String translateDropTable(String hiveSql) throws TranslationException {
      // Need to remove purge and 'if exists' if they are there
      Matcher m = Pattern.compile("drop table (if exists )?(" + TABLE_NAME_REGEX + ")").matcher(hiveSql);
      if (m.lookingAt()) {
        if (m.group(1) != null) failureOk = true;
        return "drop table " + m.group(2);
      } else {
        throw new TranslationException("drop table", hiveSql);
      }
    }

    @Override
    protected String translateAlterTableRename(String tableName, String remainder) throws
        TranslationException {
      LOG.error("Derby does not support alter table rename");
      throw new TranslationException("alter table rename", remainder);
    }

    @Override
    protected String translateConstants(String hiveSql) throws TranslationException {
      String benchSql = super.translateConstants(hiveSql);
      Matcher m = Pattern.compile("date " + QUOTE_START).matcher(benchSql);
      benchSql = m.replaceAll(QUOTE_START);
      m = Pattern.compile("timestamp " + QUOTE_START).matcher(benchSql);
      benchSql = m.replaceAll(QUOTE_START);
      return benchSql;
    }

    @Override
    protected String translateLimit(String hiveSql) throws TranslationException {
      Matcher m = Pattern.compile("([0-9]+)").matcher(hiveSql);
      if (m.find()) {
        limit = Integer.valueOf(m.group(1));
        return "";
      } else {
        throw new TranslationException("limit", hiveSql);
      }
    }

    @Override
    protected void fillOutUdfMapping() {
      super.fillOutUdfMapping();
      udfMapping.put("substring", "substr");
    }

    @Override
    protected char identifierQuote() {
      return '"';
    }
  };

  @Override
  protected SQLTranslator getTranslator() {
    return derbyTranslator;
  }
}
