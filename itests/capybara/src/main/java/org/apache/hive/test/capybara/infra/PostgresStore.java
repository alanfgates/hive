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

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hive.test.capybara.data.DataSet;
import org.apache.hive.test.capybara.iface.TestTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * DataStore for Postgres.  The way data is loaded in this class makes the assumption that the
 * postgres server is local to the machine running the tests.  Not sure if this is reasonable or
 * not.
 */
class PostgresStore extends AnsiSqlStore {
  private static final Logger LOG = LoggerFactory.getLogger(PostgresStore.class.getName());

  private static final String DELIMITER_STR = "\t";
  private static final String NULL_STR = "\\N";
  private static final String QUOTES = "";

  private String connectionUrl;
  private Properties connectionProperties;

  PostgresStore() {
    try {
      Class cls = Class.forName("org.postgresql.Driver");
      jdbcDriver = (Driver)cls.newInstance();
    } catch (ClassNotFoundException|IllegalAccessException|InstantiationException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public String getTableName(TestTable table) {
    return "default".equals(table.getDbName()) ? table.getTableName() : table.toString();
  }

  @Override
  protected String ifExists() {
    return "";
  }

  @Override
  protected String markColumnPrimaryKey() {
    return "primary key";
  }

  @Override
  public void loadData(TestTable table, DataSet rows) throws SQLException, IOException {
    if (rows.getClusterLocation() != null) {
      // The data has already been dumped onto the cluster.  Fetch it back from there and load it.
      // This will be slow, but I suspect it's still faster than loading it via JDBC from the
      // cluster.
      loadFromCluster(table, rows);
    } else {
      dumpToFileForImport(rows);
      File file = dataSetDumps.get(rows.uniqueId());
      loadFromFile(table, file);
    }
  }

  @Override
  protected String connectionURL() {
    if (connectionUrl == null) {
      ClusterConf cc = clusterManager.getClusterConf();
      StringBuilder url = new StringBuilder("jdbc:postgresql:");
      String host = cc.getJdbcHost();
      if (host != null) url.append("//").append(host);
      String port = cc.getJdbcPort();
      if (port != null) url.append(port);
      String db = cc.getJdbcDatabase();
      if (db != null) {
        if (host != null) url.append("/");
        url.append(db);
      } else {
        url.append("/");
      }
      connectionUrl = url.toString();
    }
    return connectionUrl;
  }

  @Override
  protected Properties connectionProperties() {
    if (connectionProperties == null) {
      ClusterConf cc = clusterManager.getClusterConf();
      connectionProperties = new Properties();
      connectionProperties.setProperty("user", cc.getJdbcUser());
      connectionProperties.setProperty("password", cc.getJdbcPasswd());
    }
    return connectionProperties;
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
  protected String getTempTableCreate(String tableName) {
    return "create temporary table ";
  }

  private static SQLTranslator postgresTranslator = new SQLTranslator() {
    @Override
    protected String translateDataTypes(String hiveSql) {
      Matcher m = Pattern.compile(" string").matcher(hiveSql);
      hiveSql = m.replaceAll(" varchar(255)");
      m = Pattern.compile(" double").matcher(hiveSql);
      hiveSql = m.replaceAll(" double precision");
      m = Pattern.compile(" float").matcher(hiveSql);
      hiveSql = m.replaceAll(" real");
      m = Pattern.compile(" tinyint").matcher(hiveSql);
      hiveSql = m.replaceAll(" smallint");
      m = Pattern.compile(" binary").matcher(hiveSql);
      hiveSql = m.replaceAll(" blob");
      return hiveSql;
    }

    @Override
    protected String translateAlterTableRename(String tableName, String remainder)
        throws TranslationException {
      Matcher m =
          Pattern.compile("rename to (" + SQLTranslator.TABLE_NAME_REGEX + ")").matcher(remainder);
      if (m.lookingAt()) {
        return "alter table " + tableName + " rename to " + translateTableNames(m.group(1));
      } else {
        throw new TranslationException("alter table rename", remainder);
      }
    }

    @Override
    protected char identifierQuote() {
      return '"';
    }
  };

  @Override
  protected SQLTranslator getTranslator() {
    return postgresTranslator;
  }

  @Override
  protected String convertType(String hiveType) {
    if (hiveType.equalsIgnoreCase("double")) return "double precision";
    else if (hiveType.equalsIgnoreCase("float")) return "real";
    else if (hiveType.equalsIgnoreCase("string")) return "varchar(255)";
    else if (hiveType.toLowerCase().startsWith("varchar")) return hiveType + " collate \"POSIX\"";
    else if (hiveType.toLowerCase().startsWith("char")) return hiveType + " collate \"POSIX\"";
    else return super.convertType(hiveType);
  }

  private void loadFromFile(TestTable table, File file) throws SQLException {
    StringBuilder sql = new StringBuilder("COPY ")
        .append(getTableName(table))
        .append(" FROM '")
        .append(file.getAbsolutePath())
        .append("' WITH (DELIMITER '")
        .append(DELIMITER_STR)
        .append("', NULL '")
        .append(NULL_STR)
        .append("')");
    if (LOG.isDebugEnabled()) {
      LOG.debug("Going to import into Postgres with command " + sql.toString());
    }
    Connection conn = connect(true);
    Statement stmt = null;
    try {
      stmt = conn.createStatement();
      stmt.execute(sql.toString());
    } finally {
      if (stmt != null) stmt.close();
      conn.close();
    }
  }

  private void loadFromCluster(TestTable table, DataSet rows) throws IOException, SQLException {
    // Find each file in the directory, bring each back, and then load it.  Since we're making
    // the assumption that the postgres instance is local to the test machine, pulling it back
    // from HDFS via copyToLocal is no worse than loading it from the cluster and less likely to
    // overwhelm postgres.
    // The data is pulled back by one thread while another thread loads the data into postgres.
    ConcurrentLinkedDeque<File> localFiles = new ConcurrentLinkedDeque<>();
    HdfsFilePuller puller = new HdfsFilePuller(localFiles, rows);
    puller.start();
    while ((puller.isAlive() || localFiles.peek() != null) && puller.stashed == null) {
      File f = localFiles.poll();
      if (f != null) {
        loadFromFile(table, f);
        // Delete the local file to save space
        f.delete();
      } else {
        try {
          Thread.sleep(250);
        } catch (InterruptedException e) {
          // Not sure what to do about this, going to ignore it.
        }
      }
    }
    if (puller.stashed != null) {
      throw puller.stashed;
    }
  }

  private class HdfsFilePuller extends Thread {
    private final ConcurrentLinkedDeque<File> localFiles;
    private final DataSet rows;
    IOException stashed;

    HdfsFilePuller(ConcurrentLinkedDeque<File> localFiles, DataSet rows) {
      this.localFiles = localFiles;
      this.rows = rows;
    }

    @Override
    public void run() {
      try {
        Path dir = rows.getClusterLocation();
        FileSystem fs = clusterManager.getFileSystem();
        FileStatus[] stats = fs.listStatus(dir);
        for (FileStatus stat : stats) {
          Path clusterFile = stat.getPath();
          File f = File.createTempFile("capybara_", "_data_set_" + rows.uniqueId());
          Path localFile = new Path(f.getAbsolutePath());
          LOG.debug("Copying cluster file " + clusterFile.getName() + " to local file " +
              localFile.getName());
          fs.copyToLocalFile(clusterFile, localFile);
          localFiles.add(f);
        }
      } catch (IOException e) {
        LOG.debug("HdfsFilePuller thread got IOException, giving up", e);
        stashed = e;
      }
    }
  }

  @Override
  public Class getJdbcDriverClass() {
    return jdbcDriver.getClass();
  }
}
