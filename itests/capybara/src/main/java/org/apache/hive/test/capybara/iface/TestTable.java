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
package org.apache.hive.test.capybara.iface;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hive.test.capybara.data.DataSet;
import org.apache.hive.test.capybara.data.FetchResult;
import org.apache.hive.test.capybara.data.ResultCode;
import org.apache.hive.test.capybara.data.Row;
import org.apache.hive.test.capybara.infra.HiveStore;
import org.apache.hive.test.capybara.infra.TestManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * A table to use in testing.  This table may or may not yet exist in Hive or the benchmark.
 */
public class TestTable implements Serializable {

  static final private Logger LOG = LoggerFactory.getLogger(TestTable.class.getName());

  private final TestManager testManager;
  private final DataStore testStore;
  private final DataStore benchStore;
  private final String dbName;
  private final String tableName;
  private final List<FieldSchema> cols;
  private final List<FieldSchema> partCols;
  private final PrimaryKey primaryKey;
  private final List<ForeignKey> foreignKeys;
  private final boolean isAcid;
  private final int numBuckets;
  private final String[] bucketCols;
  private final boolean isTemporary;
  private List<Row> partVals;
  private int numParts;
  private boolean hiveCreated, benchCreated;
  // If true, cache generated data.  This is used for generating data that will be referenced in
  // a foreign key.
  private boolean cacheData;
  private DataSet cachedData;

  /**
   * Build a TestTable based on an already existing Hive table.  This is useful if you want to
   * build the table yourself (for example if it needs DDL beyond the columns and partitions) and
   * then use it as a TestTable.  The way to use this is to first use
   * {@link org.apache.hive.test.capybara.IntegrationTest#runQuery} to drop the table, and then
   * create the table also using runQuery().  This causes the table to be dropped and created in
   * both Hive and the Benchmark.  Then when you call
   * {@link #populate(org.apache.hive.test.capybara.iface.DataGenerator)} below it will load data
   * into
   * both.
   * @param dbName database table is located in.
   * @param tableName name of the table
   * @return TestTable
   * @throws java.io.IOException
   */
  public static TestTable fromHiveMetastore(String dbName, String tableName) throws IOException {
    try {
      ClusterManager testCluster = TestManager.getTestManager().getTestClusterManager();
      DataStore testStore = testCluster.getStore();
      if (!(testStore instanceof HiveStore)) {
        throw new RuntimeException("fromMetadata not yet supported for non-Hive stores");
      }
      HiveStore hive = (HiveStore)testStore;
      IMetaStoreClient msClient = hive.getMetastoreConnection();
      Table msTable = msClient.getTable(dbName, tableName);
      // If we haven't started a session, start one, as we'll need it
      if (SessionState.get() == null) SessionState.start(testCluster.getHiveConf());
      // If we haven't set the transaction manager, we need to do it
      if (SessionState.get().getTxnMgr() == null) SessionState.get().initTxnMgr(testCluster.getHiveConf());
      TestTable testTable = new TestTable(dbName, tableName, msTable.getSd().getCols(),
          msTable.getPartitionKeysSize() > 0 ? msTable.getPartitionKeys() : null,
          null, null, null, 0,
          AcidUtils.isTablePropertyTransactional(msTable.getParameters()),
          msTable.getSd().getBucketCols().toArray(new String[msTable.getSd().getBucketColsSize()]),
          msTable.getSd().getNumBuckets(), msTable.isTemporary());
      testTable.benchCreated = testTable.hiveCreated = true;
      return testTable;
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  public static class Builder {
    private String dbName;
    private String tableName;
    private List<FieldSchema> cols;
    private List<FieldSchema> partCols;
    private DataGenerator partValsGenerator;
    private int numParts = 0;
    private PrimaryKey pk;
    private List<ForeignKey> fks;
    private boolean isAcid = false;
    private String[] bucketCols;
    private int numBuckets = 0;
    private boolean isTemporary = false;

    private Builder(String tableName) {
      this.tableName = tableName;
    }

    /**
     * Set the database name for this table.  If this is not set, "default" will be used.
     * @param dbName dbname
     * @return this
     */
    public Builder setDbName(String dbName) {
      this.dbName = dbName;
      return this;
    }

    /**
     * Set the columns for this table.  You must set the columns before calling {@link #build},
     * either with this or {@link #addCol}.
     * @param cols list of FieldSchemas
     * @return this
     */
    public Builder setCols(List<FieldSchema> cols) {
      this.cols = cols;
      return this;
    }

    /**
     * Add a column to this table.  You must set the columns before calling {@link #build},
     * either with call(s) to this or via {@link #setCols}.
     * @param name column name
     * @param type column data type
     * @return this
     */
    public Builder addCol(String name, String type) {
      if (cols == null) cols = new ArrayList<>();
      cols.add(new FieldSchema(name, type, ""));
      return this;
    }

    /**
     * Set partition columns for this table.  If you do not call this or {@link #addPartCol} then
     * the table will be created non-partitioned.
     * @param partCols list of FieldSchemas
     * @return this
     */
    public Builder setPartCols(List<FieldSchema> partCols) {
      this.partCols = partCols;
      return this;
    }

    /**
     * Add a partition columns to the table.  If you do not call this or {@link #setPartCols}
     * then the table will be created non-partitioned.
     * @param name column name
     * @param type column data type
     * @return
     */
    public Builder addPartCol(String name, String type) {
      if (partCols == null) partCols = new ArrayList<>();
      partCols.add(new FieldSchema(name, type, ""));
      return this;
    }

    /**
     * Provide a DataGenerator to give values for the partitions.  This is not required.  By
     * setting this you can control how many partitions are created and their values.  You cannot
     * call this and {@link #setNumParts}.
     * @param partValsGenerator DataGenerator that will provide partition values
     * @return this
     */
    public Builder setPartValsGenerator(DataGenerator partValsGenerator) {
      this.partValsGenerator = partValsGenerator;
      return this;
    }

    /**
     * Set the number of partitions this table should have without specifying the values.  You
     * cannot call this and {@link #setPartValsGenerator}.
     * @param numParts number of partitions the table should have
     * @return this
     */
    public Builder setNumParts(int numParts) {
      this.numParts = numParts;
      return this;
    }

    /**
     * Set a primary key on the table.  This does not affect how the table is created in Hive or
     * the benchmark, but it will be used in data generation.
     * @param pk primary key
     * @return this
     */
    public Builder setPrimaryKey(PrimaryKey pk) {
      this.pk = pk;
      return this;
    }

    /**
     * Add a new foreign key on this table.  This does not affect how the table is created in
     * Hive or the benchmark, but it will be used in data generation.
     * @param fk foreign key
     * @return this
     */
    public Builder addForeignKey(ForeignKey fk) {
      if (fks == null) fks = new ArrayList<>();
      fks.add(fk);
      return this;
    }

    /**
     * Set up this table to work with ACID.
     * @param isAcid true for acid, false for non-transactional
     * @return this
     */
    public Builder setAcid(boolean isAcid) {
      this.isAcid = isAcid;
      return this;
    }

    /**
     * Set the columns to be used in bucketing.
     * @param bucketCols column names
     * @return this
     */
    public Builder setBucketCols(String... bucketCols) {
      this.bucketCols = bucketCols;
      return this;
    }

    /**
     * Set number of buckets
     * @param numBuckets number of buckets
     * @return this
     */
    public Builder setNumBuckets(int numBuckets) {
      this.numBuckets = numBuckets;
      return this;
    }

    /**
     * Set whether this table is temporary.
     * @param isTemporary whether the table is temporary.
     * @return this
     */
    public Builder setTemporary(boolean isTemporary) {
      this.isTemporary = isTemporary;
      return this;
    }

    /**
     * Create the TestTable.  This only creates the Java object.  The table is not created in
     * Hive or the benchmark.
     * @return the table
     */
    public TestTable build() {
      if (dbName == null) dbName = "default";
      if (tableName == null || cols == null) {
        throw new RuntimeException("You must provide a tablename and columns before building");
      }
      return new TestTable(dbName, tableName, cols, partCols, pk, fks, partValsGenerator,
          numParts, isAcid, bucketCols, numBuckets, isTemporary);
    }
  }

  /**
   * Get a builder to construct the table.
   * @param tableName name of the table.
   * @return builder
   */
  public static Builder getBuilder(String tableName) {
    return new Builder(tableName);
  }

  private TestTable(String dbName, String tableName, List<FieldSchema> cols,
                    List<FieldSchema> partCols, PrimaryKey pk, List<ForeignKey> fk,
                    DataGenerator partValsGenerator, int numParts, boolean isAcid,
                    String[] bucketCols, int numBuckets, boolean isTemporary) {
    try {
      testManager = TestManager.getTestManager();
      testStore = testManager.getTestClusterManager().getStore();
      benchStore = testManager.getBenchmarkClusterManager().getStore();
      this.isTemporary = isTemporary;
      this.dbName = dbName == null ? "default" : dbName;
      this.tableName = tableName;
      this.cols = cols;
      this.partCols = partCols;
      primaryKey = pk;
      foreignKeys = fk;
      this.isAcid = isAcid;
      this.bucketCols = bucketCols;
      this.numBuckets = numBuckets;
      if (partValsGenerator != null) {
        assert numParts == 0;
        partVals = new ArrayList<>();
        // This is very meta, but we need a TestTable definition just to build these few rows.
        // That TestTable needs to match the schema of the partition columns, not the regular
        // columns, thus we can't use 'this' here.
        TestTable meta = new TestTable("fake", "fake", partCols, null, null, null, null, 0, false,
            null, 0, isTemporary);
        DataSet ds = partValsGenerator.generateData(meta);
        for (Row row : ds) partVals.add(row);
        this.numParts = partVals.size();
      } else {
        partVals = null;
        if (numParts != 0) {
          this.numParts = numParts;
        }
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Create the test table in Hive and the Benchmark.  This will only be done if the table hasn't
   * already been created with the proper characteristics (scale, storage format, etc.).
   * @throws SQLException
   * @throws IOException
   */
  public void create() throws SQLException, IOException {
    hiveCreated = testStore.createTable(this);
    benchCreated = benchStore.createTable(this);
  }

  /**
   * Populate a table.  You must call {@link #create} before calling this.  Data will only
   * written into the table if the table was created by create.  This will use default values for
   * scale and pctnull.
   * @param generator generator to use to generate the data.
   * @throws SQLException any exception thrown from the benchmark
   * @throws java.io.IOException
   */
   public void populate(DataGenerator generator) throws SQLException, IOException {
     populate(generator, -1, null);
  }

  /**
   * Populate a table.  You must call {@link #create} before calling this.  Data will only
   * written into the table if the table was created by create.
   * @param generator generator to use to generate the data.
   * @param scale scale to populate table at.  To get the scale from the configuration pass -1
   * @param pctNull array with one element for each column excluding partition columns which
   *                describes the percentage of entries that are null.  0.0 is no nulls, 1.0 is
   *                all nulls, 0.5 is 50% nulls.  If null is passed, then the default value of 1%
   *                for each column will be used.
   * @throws SQLException any exception thrown from the benchmark
   * @throws java.io.IOException
   */
  public void populate(DataGenerator generator, int scale, double[] pctNull)
      throws SQLException, IOException {
    scale = scale == -1 ? testManager.getTestConf().getScale() : scale;

    DataSet data = null;
    if (hiveCreated || benchCreated) {
      data = generator.generateData(this, scale, pctNull);
    }
    HiveLoader hiveLoader = null;
    if (hiveCreated) {
      hiveLoader = new HiveLoader(this, data, testStore);
      hiveLoader.start();
    }
    if (benchCreated) {
      benchStore.loadData(this, data);
    }
    if (hiveLoader != null) {
      try {
        hiveLoader.join();
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
      if (hiveLoader.stashedIoException != null) throw hiveLoader.stashedIoException;
      if (hiveLoader.stashedSqlException != null) throw hiveLoader.stashedSqlException;
      if (hiveLoader.stashedThrowable != null) throw new RuntimeException(hiveLoader.stashedThrowable);
    }
    /*
    */
    if (cacheData) cachedData = data;
  }

  /**
   * Create the table, even if it already exists.  If the table already exists it will be
   * dropped and re-created.  This will not populate the table.  This should
   * not be used for tables to read from (for that use {@link #create}).  This should
   * only be used for tables to be written to as part of the query.  If you are calling this and
   * then {@link #populate} you are most likely doing it wrong.
   * @throws SQLException
   * @throws java.io.IOException
   */
  public void createTargetTable() throws IOException, SQLException {
    testStore.forceCreateTable(this);
    benchStore.forceCreateTable(this);
  }

  private class HiveLoader extends Thread {
    final TestTable table;
    final DataSet data;
    final DataStore hive;
    SQLException stashedSqlException;
    IOException stashedIoException;
    Throwable stashedThrowable;

    public HiveLoader(TestTable table, DataSet data, DataStore hive) {
      this.table = table;
      this.data = data;
      this.hive = hive;
    }

    @Override
    public void run() {
      try {
        hive.loadData(table, data);
      } catch (SQLException e) {
        stashedSqlException = e;
      } catch (IOException e) {
        stashedIoException = e;
      } catch (Throwable t) {
        LOG.error("HiveLoader caught throwable: ", t);
        stashedThrowable = t;
      }
    }
  }

  public String getDbName() {
    return dbName;
  }

  /**
   * Get just the table name without the database.
   * @return table name
   */
  public String getTableName() {
    return tableName;
  }

  /**
   * Get fully qualified table name.
   * @return dbname.tablename
   */
  public String getFullName() {
    return new StringBuilder(dbName).append('.').append(tableName).toString();
  }

  public List<FieldSchema> getCols() {
    return cols;
  }

  /**
   * Get the partition columns
   * @return part cols
   */
  public List<FieldSchema> getPartCols() {
    return partCols;
  }

  public PrimaryKey getPrimaryKey() {
    return primaryKey;
  }

  public List<ForeignKey> getForeignKeys() {
    return foreignKeys;
  }

  /**
   * Get the partition values.  This may be null, which indicates that partitions values have not
   * been determined yet.
   * @return part vals
   */
  public List<Row> getPartVals() {
    return partVals;
  }

  /**
   * Get the number of partitions.  Even for a partitioned table this may be 0, which indicates
   * that the number of partitions will be determined by the test scale.
   * @return num parts
   */
  public int getNumParts() {
    return numParts;
  }

  public void setPartVals(List<Row> partVals) {
    this.partVals = partVals;
  }

  public boolean isAcid() {
    return isAcid;
  }

  public int getNumBuckets() {
    return numBuckets;
  }

  public String[] getBucketCols() {
    return bucketCols;
  }

  public boolean isTemporary() {
    return isTemporary;
  }

  /**
   * Get a PreparedStatement that can be used to load data into this table.  This statement can
   * be passed to {@link org.apache.hive.test.capybara.data.Column#load}.
   * @param conn JDBC connection
   * @param store DataStore this statement is being prepared for
   * @return a prepared statement
   * @throws SQLException
   */
  public PreparedStatement getLoadingStatement(Connection conn, DataStore store)
      throws SQLException {
    StringBuilder sql = new StringBuilder("insert into ")
        .append(store.getTableName(this))
        .append(" (");
    boolean first = true;
    for (FieldSchema col : cols) {
      if (first) first = false;
      else sql.append(", ");
      sql.append(col.getName());
    }
    sql.append(" ) values (");
    first = true;
    for (FieldSchema col : cols) {
      if (first) first = false;
      else sql.append(", ");
      sql.append("?");
    }
    sql.append(")");
    LOG.debug("Going to prepare statement <" + sql.toString() + ">");
    return conn.prepareStatement(sql.toString());
  }

  @Override
  public String toString() {
    return getFullName();
  }

  /**
   * Build a schema that combines the columns with the partition columns.  This is not guaranteed
   * to be a copy, so don't mess with it (I wish Java had const).
   * @return combined schema.
   */
  public List<FieldSchema> getCombinedSchema() {
    if (partCols == null) return cols;
    List<FieldSchema> combined = new ArrayList<>(cols);
    combined.addAll(partCols);
    return combined;
  }


  /**
   * A primary key.  For now it is limited to a single column.
   */
  public static class PrimaryKey implements Serializable {
    public final int colNum;

    public PrimaryKey(int colNum) {
      this.colNum = colNum;
    }

    public boolean isSequence() { return false; }
  }

  /**
   * A sequence, guaranteed to be monotonically increasing.
   */
  public static class Sequence extends PrimaryKey implements Serializable {
    public Sequence(int colNum) {
      super(colNum);
    }

    @Override
    public boolean isSequence() { return true; }
  }

  /**
   * A foreign key.  This takes a reference to a
   * {@link TestTable.PrimaryKey} in a target table.
   */
  public static class ForeignKey implements Serializable {
    public transient DataSet targetTable;
    public final int colNumInTarget;
    public final int colNumInSrc;
    public Path pkFile; // if non-null, points to the file that contains the primary key data.

    public ForeignKey(DataSet targetTable, int targetColNum, int srcColNum) {
      this.targetTable = targetTable;
      this.colNumInTarget = targetColNum;
      this.colNumInSrc = srcColNum;
    }

    public ForeignKey(String pkFile, int targetColNum, int srcColNum) {
      // TODO populate the dataset from the file.
      this.colNumInTarget = targetColNum;
      this.colNumInSrc = srcColNum;
    }
  }

  /**
   * Tell TestTable to keep a hold of the data after it is generated.  In general you don't want
   * to do this because it will eat memory.  But it's useful if you're planning on building a
   * foreign key in another table referencing this table.
   * @param cacheData whether to cache the data.
   */
  public void setCacheData(boolean cacheData) {
    this.cacheData = cacheData;
  }

  /**
   * Get a DataSet that represents the contents of this table.
   * @return data for this data
   */
  public DataSet getData() throws IOException, SQLException {
    if (cachedData == null) {
      // TODO - create a FetchingDataSet that only fetches the data when requested.  Because it's
      // quite likely we won't actually need to fetch the data.
      FetchResult fetch = benchStore.executeSql("select * from " + toString());
      if (fetch.rc != ResultCode.SUCCESS) {
        throw new RuntimeException("Unable to fetch results from already populated table " +
            toString());
      }
      cachedData = fetch.data;
    }
    return cachedData;
  }
}
