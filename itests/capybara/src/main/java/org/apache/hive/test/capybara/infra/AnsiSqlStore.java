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
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.junit.Assert;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * An abstract class for all DataStores that are ANSI SQL compliant.  It contains convenience
 * methods for converting Hive SQL into ANSI SQL.  It also contains implementations of common
 * methods that don't change between JDBC drivers.
 */
abstract class AnsiSqlStore extends DataStoreBase implements BenchmarkDataStore {
  private static final Logger LOG = LoggerFactory.getLogger(AnsiSqlStore.class.getName());

  /**
   * A map from DataSets to places they've been dumped.  The key is obtained by calling
   * DataStore.uniqueId().  The value is a File where the data is stored.
   */
  protected Map<Integer, File> dataSetDumps;

  /**
   * Certain statements might fail for an ANSI SQL store where they would not for Hive, such as
   * 'create table if not exists'.  If we detect this condition note it so that the ANSI store
   * knows that failure is acceptable.
   */
  protected boolean failureOk;

  protected AnsiSqlStore() {
    dataSetDumps = new HashMap<>();
  }

  @Override
  public boolean createTable(TestTable table) throws SQLException {
    return createTableInternal(table, false);
  }

  @Override
  public void forceCreateTable(TestTable table) throws SQLException, IOException {
    createTableInternal(table, true);
  }

  @Override
  public FetchResult fetchData(String sql) throws SQLException, IOException {
    sql = hiveSqlToAnsiSql(sql);
    LOG.debug("Going to run query <" + sql + "> against ANSI SQL store");
    return jdbcFetch(sql, getLimit(), failureOk);
  }

  @Override
  public void dumpToFileForImport(DataSet rows) throws IOException {
    File dumpFile = dataSetDumps.get(rows.uniqueId());
    if (dumpFile == null) {
      dumpFile = File.createTempFile("capybara_", "_data_set_" + rows.uniqueId());
      dataSetDumps.put(rows.uniqueId(), dumpFile);
    }
    FileWriter writer = new FileWriter(dumpFile, true);
    Iterator<String> iter =
        rows.stringIterator(fileColumnDelimiter(), fileNull(), fileStringQuotes());
    String lineSep = System.getProperty("line.separator");
    while (iter.hasNext()) {
      writer.write(iter.next() + lineSep);
    }
    writer.close();
  }

  @Override
  public Connection getJdbcConnection(boolean autoCommit) throws SQLException {
    return connect(autoCommit);
  }

  /**
   * Get the string used to delimit columns in a file that will be loaded to the database.
   * @return delimiter
   */
  protected abstract String fileColumnDelimiter();

  /**
   * Get the string that represents null in a file that will be loaded into the database.
   * @return null string
   */
  protected abstract String fileNull();

  /**
   * Get the string used to quote values in a file that will be loaded into the database.
   * @return quote
   */
  protected abstract String fileStringQuotes();

  /**
   * Get the limit that should be applied to a query.  This is to deal with the fact that derby
   * doesn't support limit.  If a database supports limit it should not override this method.
   * @return limit value.
   */
  protected long getLimit() {
    return Long.MAX_VALUE;
  }

  /**
   * Convert Hive SQL to ANSI SQL.  This works via a fuzzy parser, so it's not fool proof.
   * @param hiveSql Hive SQL string to convert
   * @return ANSI SQL
   */
  protected String hiveSqlToAnsiSql(String hiveSql) throws IOException, SQLException {
    failureOk = false;

    // If they preprended the table name with 'default.' strip it so that it works
    Matcher matcher = Pattern.compile("default\\.", Pattern.CASE_INSENSITIVE).matcher(hiveSql);
    hiveSql = matcher.replaceAll("");

    // Get rid of if [not] exists
    matcher = Pattern.compile("if\\s+(not\\s+)?exists", Pattern.CASE_INSENSITIVE).matcher(hiveSql);
    String tmpSql = matcher.replaceAll("");
    if (!tmpSql.equals(hiveSql)) {
      // We changed something
      failureOk = true;
      hiveSql = tmpSql;
    }

    // Get rid of clustered by
    matcher = Pattern.compile("clustered\\s+by\\s+\\(.*?\\)\\s+into\\s+[0-9]+\\s+buckets",
        Pattern.CASE_INSENSITIVE).matcher(hiveSql);
    hiveSql = matcher.replaceAll("");

    // Get rid of stored as
    matcher = Pattern.compile("stored\\s+as\\s+[a-z]+", Pattern.CASE_INSENSITIVE).matcher(hiveSql);
    hiveSql = matcher.replaceAll("");

    // Get rid of table properties
    matcher = Pattern.compile("tblproperties\\s+\\(.*?\\)", Pattern.CASE_INSENSITIVE).matcher(hiveSql);
    hiveSql = matcher.replaceAll("");

    // Get rid of partitioned by
    matcher = Pattern.compile("partitioned\\s+by\\s+\\(.*?\\)", Pattern.CASE_INSENSITIVE).matcher(hiveSql);
    hiveSql = matcher.replaceAll("");

    // Get rid of partitions (in an insert statement)
    matcher = Pattern.compile("partition\\s+\\(.*?\\)", Pattern.CASE_INSENSITIVE).matcher(hiveSql);
    hiveSql = matcher.replaceAll("");

    // Get rid of Skewed by
    matcher = Pattern.compile("skewed\\s+by\\s+\\(.*?\\)\\s+ON\\s+\\(.*\\)",
        Pattern.CASE_INSENSITIVE).matcher(hiveSql);
    hiveSql = matcher.replaceAll("");

    // Get rid of table in 'insert into table'
    matcher = Pattern.compile("insert\\s+into\\s+table", Pattern.CASE_INSENSITIVE).matcher(hiveSql);
    hiveSql = matcher.replaceAll("insert into ");

    // Handle insert overwrite.  This takes a little more since we need to drop all the rows from
    // a table when this is issued.
    matcher = Pattern.compile("insert\\s+overwrite\\s+table\\s+([a-z_0-9]+)", Pattern.CASE_INSENSITIVE).matcher(
        hiveSql);
    if (matcher.find()) {
      FetchResult result = fetchData("delete from " + matcher.group(1));
      Assert.assertEquals(FetchResult.ResultCode.SUCCESS, result.rc);
      hiveSql = matcher.replaceAll("insert into " + matcher.group(1));
    }

    // Switch string columns to varchar columns
    while (true) {
      matcher = Pattern.compile("([ (])string([,)])", Pattern.CASE_INSENSITIVE).matcher(hiveSql);
      if (matcher.find()) {
        hiveSql = matcher.replaceFirst(matcher.group(1) + "varchar(1023)" + matcher.group(2));
      } else {
        break;
      }
    }

    // Remove any hints
    matcher = Pattern.compile("/\\*.*\\*/").matcher(hiveSql);
    hiveSql = matcher.replaceAll("");

    return hiveSql;
  }

  protected String convertType(String hiveType) {
    if (hiveType.equalsIgnoreCase("string"))  return "varchar(255)";
    else if (hiveType.equalsIgnoreCase("tinyint")) return "smallint";
    else if (hiveType.equalsIgnoreCase("binary")) return "blob";
    else return hiveType;
  }

  private boolean createTableInternal(TestTable table, boolean force) throws SQLException {
    if (!force && tableExistsInCorrectState(table)) return false;

    try {
      // if exists isn't proper ANSI SQL, but it signals the system to allow failure here if the
      // table already exists.
      fetchData("drop table if exists " + table.toString());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    List<FieldSchema> allCols = new ArrayList<>();
    allCols.addAll(table.getCols());
    // If there are any partition columns add those at the end
    if (table.getPartCols() != null) {
      allCols.addAll(table.getPartCols());
    }

    Connection conn = connect(true);
    Statement stmt = null;
    try {
      StringBuilder sql = new StringBuilder("create table ")
          .append(getTableName(table))
          .append(" (");
      for (FieldSchema col : allCols) {
        sql.append(col.getName())
            .append(" ")
            .append(convertType(col.getType()))
            .append(",");
      }
      sql.deleteCharAt(sql.length() - 1).append(") ");
      stmt = conn.createStatement();
      LOG.debug("Going to run <" + sql.toString() + "> on ANSI SQL store");
      stmt.execute(sql.toString());
      if (!force) recordTableCreation(table);
    } finally {
      if (stmt != null) stmt.close();
      conn.close();
    }
    return true;
  }
}
