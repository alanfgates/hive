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

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hive.test.capybara.data.DataSet;
import org.apache.hive.test.capybara.data.FetchResult;
import org.apache.hive.test.capybara.data.ResultCode;
import org.apache.hive.test.capybara.iface.BenchmarkDataStore;
import org.apache.hive.test.capybara.iface.TestTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
  public FetchResult fetchData(String hiveSql) throws SQLException, IOException {
    String sql = hiveSqlToAnsiSql(hiveSql);
    if (sql.equals("")) {
      // This means this is a NOP for the benchmark
      LOG.debug("Hive SQL <" + hiveSql + "> is NOP on benchmark");
      return new FetchResult(ResultCode.SUCCESS);
    }
    LOG.debug("Translated Hive SQL <" + hiveSql + "> to <" + sql +
        ">, going to run against benchmark");
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
   * Get the appropriate SQL translator for this database.
   * @return translator
   */
  protected abstract SQLTranslator getTranslator();

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

    try {
      SQLTranslator translator = getTranslator();
      String benchSql = translator.translate(hiveSql);
      failureOk = translator.isFailureOk();
      return benchSql;
    } catch (TranslationException e) {
      LOG.warn("Unable to translate Hive SQL <" + hiveSql +
          "> for your benchmark, risking it using original Hive SQL");
      return hiveSql;
    }
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

    Statement stmt = null;
    try (Connection conn = connect(true)) {
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

    }
    return true;
  }

}
