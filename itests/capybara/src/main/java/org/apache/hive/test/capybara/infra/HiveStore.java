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
import org.apache.hive.test.capybara.data.FetchResult;
import org.apache.hive.test.capybara.data.ResultCode;
import org.apache.hive.test.capybara.iface.ClusterManager;
import org.apache.hive.test.capybara.iface.TestTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.ObjectPair;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.ql.QueryPlan;
import org.junit.Assert;

import java.io.IOException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;

/**
 * A class for a Hive store.  All implementations of this that will run on the cluster must have
 * a no-args constructor.
 */
public abstract class HiveStore extends DataStoreBase implements Configurable {
  public static final char DELIMITER = '\u0001';
  public static final String DELIMITER_STR = new String(new char[]{DELIMITER});
  public static final String NULL_STR = "\u0005";
  public static final String QUOTES = "";
  private static final Logger LOG = LoggerFactory.getLogger(HiveStore.class.getName());

  protected HiveConf conf;
  protected IMetaStoreClient msClient;
  /**
   * A map from DataSets to places they've been dumped.  The key is obtained by calling
   * DataStore.uniqueId().  The value is a pair, with the first element being the HDFS directory
   * the data is dumped in, and the second being the actual file.
   */
  private Map<Integer, ObjectPair<Path, Path>> dataSetDumps;

  /**
   * A constructor for use on the cluster when we need don't have a clusterManager.
   */
  HiveStore() {

  }

  protected HiveStore(ClusterManager clusterManager) {
    super(clusterManager);
    dataSetDumps = new HashMap<>();
  }

  public IMetaStoreClient getMetastoreConnection() throws MetaException {
    assert conf != null;
    if (msClient == null) {
      msClient = new HiveMetaStoreClient(conf);
    }
    return msClient;
  }

  @Override
  public boolean createTable(TestTable table) throws SQLException, IOException {
    return createTableInternal(table, false);
  }

  @Override
  public void forceCreateTable(TestTable table) throws SQLException, IOException {
    createTableInternal(table, true);
  }

  @Override
  public void loadData(TestTable table, DataSet rows)
      throws SQLException, IOException {
    // Dump the data into an HDFS file.  Then create a table with that as it's location.
    // Finally run a SQL statement to insert into the destination table from there.
    String tmpTableName = table.getDbName() + "_" + table.getTableName() + "_tmp_load";

    // Check whether this data has already been dumped.  If so, don't do it again.
    Path dir = rows.getClusterLocation();
    if (dir == null) {
      // It hasn't been dumped, so do it now.
      dumpToFileForImport(rows);
      dir = dataSetDumps.get(rows.uniqueId()).getFirst();
    }


    // Drop the temp table in case we've previously created it.  Ignore any errors.
    fetchData("drop table " + tmpTableName);

    // Create a temp table with the file in the right location.
    StringBuilder sql = new StringBuilder("create table ").append(tmpTableName).append(" (");
    boolean first = true;
    for (FieldSchema colSchema : table.getCols()) {
      if (first) first = false;
      else sql.append(", ");
      sql.append(colSchema.getName())
          .append(' ')
          .append(colSchema.getType());
    }
    if (table.getPartCols() != null) {
      for (FieldSchema partSchema : table.getPartCols()) {
        sql.append(", ")
          .append(partSchema.getName())
          .append(' ')
          .append(partSchema.getType());
      }
    }
    sql.append(") row format delimited fields terminated by '\\001' null defined as '\\005' ")
        .append("stored as textfile ")
        .append("location '").append(dir.toUri().toString()).append("'");

    FetchResult res = fetchData(sql.toString());
    Assert.assertEquals(ResultCode.SUCCESS, res.rc);

    // Now, insert from the temp table to the target table.
    sql = new StringBuilder("insert into ")
        .append(table.toString());
    if (table.getPartCols() != null) {
      sql.append(" partition (");
      first = true;
      for (FieldSchema partCol : table.getPartCols()) {
        if (first) first = false;
        else sql.append(", ");
        sql.append(partCol.getName());
      }
      sql.append(")");
    }
    sql.append(" select * from ").append(tmpTableName);

    LOG.debug("Going to send to Hive: " + sql.toString());
    res = fetchData(sql.toString());
    Assert.assertEquals(ResultCode.SUCCESS, res.rc);
  }

  @Override
  public void dumpToFileForImport(DataSet rows) throws IOException {
    ClusterManager clusterMgr = TestManager.getTestManager().getClusterManager();
    FileSystem fs = clusterMgr.getFileSystem();
    ObjectPair<Path, Path> pathPair = dataSetDumps.get(rows.uniqueId());
    if (pathPair == null) {
      Path dir = new Path(getDirForDumpFile());
      LOG.debug("Going to dump data for import to " + dir.toUri().toString());
      fs.mkdirs(dir);
      Path file = new Path(dir, "file");
      FSDataOutputStream output = fs.create(file);
      writeToFile(output, rows);
      dataSetDumps.put(rows.uniqueId(), new ObjectPair<>(dir, file));
    } else {
      Path file = pathPair.getSecond();
      FSDataOutputStream output = fs.append(file);
      writeToFile(output, rows);
    }
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = (HiveConf)conf;

  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  public HiveConf getHiveConf() {
    return conf;
  }

  @Override
  public String getTableName(TestTable table) {
    return table.toString();
  }

  /**
   * Explain a SQL query.  This should only be implemented by local implementations, not on the
   * cluster when the test harness may not share the configuration with the cluster.
   * @param sql SQL to explain
   * @return plan for this SQL
   */
  public abstract QueryPlan explain(String sql);

  /**
   * Get a URI to connect to the metastore.  In the local case this should be null.
   * @return uri
   */
  public abstract String getMetastoreUri();

  /**
   * Generate a random directory to dump generated data in.
   * @return directory name
   */
  public static String getDirForDumpFile() {
    // Use '/' explicitly rather than file.separator property as HDFS always uses forward slash.
    String filename = new StringBuilder(TestManager.getTestManager().getConf().get("hadoop.tmp.dir"))
        .append('/')
        .append("capybara_")
        .append(UUID.randomUUID().toString())
        .toString();
    return filename;
  }

  /**
   * Generate a random file name to dump generated data in.
   * @return filename
   */
  static String getFileForDump() {
    return UUID.randomUUID().toString();
  }

  /**
   * Write generated data to a file.
   * @param output output stream to use.
   * @param data DataSet to dump.
   * @throws IOException
   */
  static void writeToFile(FSDataOutputStream output, DataSet data) throws IOException {
    Iterator<String> iter = data.stringIterator(DELIMITER_STR, NULL_STR, QUOTES);
    String lineSep = System.getProperty("line.separator");
    while (iter.hasNext()) {
      output.writeBytes(iter.next() + lineSep);
    }
    output.close();
  }

  @Override
  protected String ifExists() {
    return " if exists ";
  }

  @Override
  protected String markColumnPrimaryKey() {
    return "";
  }

  private boolean createTableInternal(TestTable table, boolean force)
      throws SQLException, IOException {
    if (!force && tableExistsInCorrectState(table)) return false;
    FetchResult rc = fetchData("drop table if exists " + table.toString());
    Assert.assertEquals(ResultCode.SUCCESS, rc.rc);

    StringBuilder builder =  new StringBuilder();
    builder.append("create table ")
        .append(table.toString())
        .append(" (");
    boolean first = true;
    for (FieldSchema col : table.getCols()) {
      if (first) first = false;
      else builder.append(", ");
      builder.append(col.getName())
          .append(' ')
          .append(col.getType());
    }
    builder.append(')');
    if (table.getPartCols() != null) {
      builder.append(" partitioned by (");
      first = true;
      for (FieldSchema partCol : table.getPartCols()) {
        if (first) first = false;
        else builder.append(", ");
        builder.append(partCol.getName())
            .append(' ')
            .append(partCol.getType());
      }
      builder.append(')');
    }

    if (table.getNumBuckets() > 0) {
      builder.append(" clustered by (");
      first = true;
      for (String bucketCol : table.getBucketCols()) {
        if (first) first = false;
        else builder.append(", ");
        builder.append(bucketCol);
      }
      builder.append(") into ")
          .append(table.getNumBuckets())
          .append(" buckets");
    }

    if (table.isAcid()) {
      if (!TestConf.fileFormat().equals(TestConf.FILE_FORMAT_ORC)) {
        builder.append(" stored as orc ");
      }
      builder.append(" tblproperties ('transactional'='true')");
    }

    rc = fetchData(builder.toString());
    Assert.assertEquals(ResultCode.SUCCESS, rc.rc);
    if (!force) recordTableCreation(table);
    return true;
  }
}
