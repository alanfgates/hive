/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.metastore.sqlbin;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.RawStore;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.*;
import org.apache.hadoop.hive.metastore.hbase.HBaseStore;
import org.apache.orc.IntegerColumnStatistics;
import org.junit.*;

import java.sql.SQLException;
import java.util.*;

public class TestPostgresStore {

  // If this property is defined, we'll know that the user intends to connect to an external
  // postgres, all tests will be ignored.
  private static final String POSTGRES_JDBC = "hive.test.posgres.jdbc";
  private static final String POSTGRES_USER = "hive.test.posgres.user";
  private static final String POSTGRES_PASSWD = "hive.test.posgres.password";
  private static PostgresStore store;
  // If you create any tables with partitions, you must add the corresponding partition table into
  // this list so that it gets dropped at test start time.
  private static List<String> tablesToDrop = new ArrayList<>();

  @BeforeClass
  public static void connect() throws MetaException, SQLException {
    tablesToDrop.add(PostgresKeyValue.DB_TABLE.getName());
    tablesToDrop.add(PostgresKeyValue.TABLE_TABLE.getName());
    tablesToDrop.add(PostgresKeyValue.FUNC_TABLE.getName());
    String jdbc = System.getProperty(POSTGRES_JDBC);
    if (jdbc != null) {
      HiveConf conf = new HiveConf();
      conf.setVar(HiveConf.ConfVars.METASTORE_RAW_STORE_IMPL,
          PostgresStore.class.getCanonicalName());
      conf.setVar(HiveConf.ConfVars.METASTORECONNECTURLKEY, jdbc);
      String user = System.getProperty(POSTGRES_USER);
      if (user == null) user = "hive";
      conf.setVar(HiveConf.ConfVars.METASTORE_CONNECTION_USER_NAME, user);
      String passwd = System.getProperty(POSTGRES_PASSWD);
      if (passwd == null) passwd = "hive";
      conf.setVar(HiveConf.ConfVars.METASTOREPWD, passwd);
      conf.set(PostgresKeyValue.CACHE_OFF, "true");

      store = new PostgresStore();
      store.setConf(conf);
    }

  }

  @AfterClass
  public static void cleanup() throws SQLException {
    PostgresKeyValue psql = store.connectionForTest();
    try {
      psql.begin();
      for (String table : tablesToDrop) {
        try {
          psql.dropPostgresTable(table);
        } catch (SQLException e) {
          // Ignore it, as it likely just means we haven't created the tables previously
        }
      }
    } finally {
      psql.commit();
    }

  }

  @Before
  public void checkExternalPostgres() {
    Assume.assumeNotNull(System.getProperty(POSTGRES_JDBC));
  }

  @Test
  public void dbs() throws InvalidObjectException, MetaException, NoSuchObjectException {
    Database db = new Database("dbtest1", "description", "file:/somewhere",
        Collections.<String, String>emptyMap());
    store.createDatabase(db);

    db = store.getDatabase("dbtest1");

    Assert.assertNotNull(db);
    Assert.assertEquals("dbtest1", db.getName());
    Assert.assertEquals("description", db.getDescription());
    Assert.assertEquals("file:/somewhere", db.getLocationUri());
    Assert.assertEquals(0, db.getParametersSize());
  }

  @Test
  public void tables() throws InvalidObjectException, MetaException {
    String dbName = "default";
    String tableName = "tbl1";

    List<FieldSchema> cols = Collections.singletonList(
        new FieldSchema("a", "varchar(32)", "")
    );
    SerDeInfo serde = new SerDeInfo("serde", "serde", Collections.<String, String>emptyMap());
    StorageDescriptor sd = new StorageDescriptor(cols, "file:/tmp/tbl1", "inputformat",
        "outputformat", false, 0, serde, null, null, Collections.<String, String>emptyMap());
    Table table = new Table(tableName, dbName, "me", 1, 2, 3, sd, null,
        Collections.<String, String>emptyMap(), null, null, TableType.MANAGED_TABLE.name());
    store.createTable(table);

    table = store.getTable("default", "tbl1");

    // I assume thrift de/serialization works, so I'm not going to check every field.
    Assert.assertEquals(tableName, table.getTableName());
    Assert.assertEquals(dbName, table.getDbName());
    Assert.assertEquals("me", table.getOwner());
    cols = table.getSd().getCols();
    Assert.assertEquals(1, cols.size());
    Assert.assertEquals("a", cols.get(0).getName());

  }

  @Test
  public void functions() throws InvalidObjectException, MetaException {
    String dbName = "default";
    Function count = new Function("count", dbName, "o.a.h.count", "me", PrincipalType.ROLE, 1,
        FunctionType.JAVA, Collections.<ResourceUri>emptyList());
    Function sum = new Function("sum", dbName, "o.a.h.count", "me", PrincipalType.ROLE, 1,
        FunctionType.JAVA, Collections.<ResourceUri>emptyList());
    store.createFunction(count);
    store.createFunction(sum);

    List<Function> funcs = store.getAllFunctions();
    Assert.assertEquals(2, funcs.size());
    Assert.assertTrue(
        (funcs.get(0).getFunctionName().equals("count") &&
            funcs.get(1).getFunctionName().equals("sum")) ||
        (funcs.get(0).getFunctionName().equals("sum") &&
            funcs.get(1).getFunctionName().equals("count")));
  }

  @Test
  public void partitions() throws InvalidObjectException, MetaException, NoSuchObjectException {
    String dbName = "default";
    String tableName = "ptbl1";
    List<String> pVals = Collections.singletonList("a");

    List<FieldSchema> cols = Collections.singletonList(
        new FieldSchema("a", "varchar(32)", "")
    );
    SerDeInfo serde = new SerDeInfo("serde", "serde", Collections.<String, String>emptyMap());
    StorageDescriptor sd = new StorageDescriptor(cols, "file:/tmp/tbl1", "inputformat",
        "outputformat", false, 0, serde, null, null, Collections.<String, String>emptyMap());
    List<FieldSchema> pcols = Collections.singletonList(
        new FieldSchema("pcol", "string", "")
    );
    // We need to create a real table because all of the partition calls reference the table table
    Table table = new Table("ptbl1", "default", "me", 1, 2, 3, sd, pcols,
        Collections.<String, String>emptyMap(), null, null, TableType.MANAGED_TABLE.name());
    store.createTable(table);
    tablesToDrop.add(PostgresKeyValue.buildPartTableName(dbName, tableName));

    Partition part = new Partition(pVals, dbName, tableName, 1, 2, sd,
        Collections.<String, String>emptyMap());
    store.addPartition(part);

    part = store.getPartition(dbName, tableName, pVals);

    Assert.assertEquals(dbName, part.getDbName());
    Assert.assertEquals(tableName, part.getTableName());
    Assert.assertEquals(pVals.get(0), part.getValues().get(0));
  }

  @Test
  public void tableStats() throws InvalidObjectException, MetaException, NoSuchObjectException, InvalidInputException {
    String dbName = "default";
    String tableName = "stbl1";

    List<FieldSchema> cols = Arrays.asList(
        new FieldSchema("a", "varchar(32)", ""),
        new FieldSchema("b", "int", ""),
        new FieldSchema("c", "decimal(10,2)", "")
    );
    SerDeInfo serde = new SerDeInfo("serde", "serde", Collections.<String, String>emptyMap());
    StorageDescriptor sd = new StorageDescriptor(cols, "file:/tmp/tbl1", "inputformat",
        "outputformat", false, 0, serde, null, null, Collections.<String, String>emptyMap());
    Table table = new Table(tableName, dbName, "me", 1, 2, 3, sd, null,
        Collections.<String, String>emptyMap(), null, null, TableType.MANAGED_TABLE.name());
    store.createTable(table);

    ColumnStatisticsData csd_a = ColumnStatisticsData.stringStats(
        new StringColumnStatsData(317, 87.2, 57, 98732)
    );

    LongColumnStatsData ldata = new LongColumnStatsData(97, 102920);
    ldata.setLowValue(0);
    ldata.setHighValue(14123123);

    ColumnStatisticsData csd_b = ColumnStatisticsData.longStats(ldata);

    ColumnStatisticsDesc statsDec = new ColumnStatisticsDesc(true, dbName, tableName);
    List<ColumnStatisticsObj> statsObjs = Arrays.asList(
        new ColumnStatisticsObj("a", "varchar(32)", csd_a),
        new ColumnStatisticsObj("b", "int", csd_b)
    );
    ColumnStatistics cs = new ColumnStatistics(statsDec, statsObjs);
    store.updateTableColumnStatistics(cs);

    cs = store.getTableColumnStatistics(dbName, tableName, null);
    ColumnStatisticsObj cso1 = cs.getStatsObj().get(0);
    Assert.assertEquals("a", cso1.getColName());
    Assert.assertEquals(317, cso1.getStatsData().getStringStats().getMaxColLen());
    cso1 = cs.getStatsObj().get(1);
    Assert.assertEquals("b", cso1.getColName());
    Assert.assertEquals(97, cso1.getStatsData().getLongStats().getNumNulls());

    // Now add some new stats, make sure the merge happens properly
    ldata = new LongColumnStatsData(107, 202820);
    ldata.setLowValue(0);
    ldata.setHighValue(98797433);
    csd_b = ColumnStatisticsData.longStats(ldata);

    DecimalColumnStatsData ddata = new DecimalColumnStatsData(7, 1384729);
    ColumnStatisticsData csd_c = ColumnStatisticsData.decimalStats(ddata);
    statsObjs = Arrays.asList(
        new ColumnStatisticsObj("b", "int", csd_b),
        new ColumnStatisticsObj("c", "decimal(10, 2)", csd_c)
    );
    cs = new ColumnStatistics(statsDec, statsObjs);
    store.updateTableColumnStatistics(cs);

    cs = store.getTableColumnStatistics(dbName, tableName, null);
    boolean sawA = false, sawB = false, sawC = false;

    Assert.assertEquals(3, cs.getStatsObjSize());
    for (int i = 0; i < 3; i++) {
      cso1 = cs.getStatsObj().get(i);
      if ("a".equals(cso1.getColName())) {
        sawA = true;
        Assert.assertEquals(317, cso1.getStatsData().getStringStats().getMaxColLen());
      } else if ("b".equals(cso1.getColName())) {
        sawB = true;
        Assert.assertEquals(107, cso1.getStatsData().getLongStats().getNumNulls());
      } else if ("c".equals(cso1.getColName())) {
        sawC = true;
        Assert.assertEquals(7, cso1.getStatsData().getDecimalStats().getNumNulls());
      } else {
        Assert.fail("Unknown column " + cso1.getColName());
      }
    }
    Assert.assertTrue(sawA);
    Assert.assertTrue(sawB);
    Assert.assertTrue(sawC);
  }

  @Test
  public void partStats() throws InvalidObjectException, MetaException, NoSuchObjectException, InvalidInputException {
    String dbName = "default";
    String tableName = "sptbl1";
    List<String> pVals = Collections.singletonList("a");

    List<FieldSchema> cols = Arrays.asList(
        new FieldSchema("a", "varchar(32)", ""),
        new FieldSchema("b", "int", ""),
        new FieldSchema("c", "decimal(10,2)", "")
    );
    SerDeInfo serde = new SerDeInfo("serde", "serde", Collections.<String, String>emptyMap());
    StorageDescriptor sd = new StorageDescriptor(cols, "file:/tmp/tbl1", "inputformat",
        "outputformat", false, 0, serde, null, null, Collections.<String, String>emptyMap());
    List<FieldSchema> pcols = Collections.singletonList(
        new FieldSchema("pcol", "string", "")
    );
    Table table = new Table(tableName, dbName, "me", 1, 2, 3, sd, pcols,
        Collections.<String, String>emptyMap(), null, null, TableType.MANAGED_TABLE.name());
    store.createTable(table);
    tablesToDrop.add(PostgresKeyValue.buildPartTableName(dbName, tableName));

    Partition part = new Partition(pVals, dbName, tableName, 1, 2, sd,
        Collections.<String, String>emptyMap());
    store.addPartition(part);

    ColumnStatisticsData csd_a = ColumnStatisticsData.stringStats(
        new StringColumnStatsData(317, 87.2, 57, 98732)
    );

    LongColumnStatsData ldata = new LongColumnStatsData(97, 102920);
    ldata.setLowValue(0);
    ldata.setHighValue(14123123);

    ColumnStatisticsData csd_b = ColumnStatisticsData.longStats(ldata);

    ColumnStatisticsDesc statsDec = new ColumnStatisticsDesc(false, dbName, tableName);
    String partName = HBaseStore.buildExternalPartName(table, pVals);
    statsDec.setPartName(partName);
    List<ColumnStatisticsObj> statsObjs = Arrays.asList(
        new ColumnStatisticsObj("a", "varchar(32)", csd_a),
        new ColumnStatisticsObj("b", "int", csd_b)
    );
    ColumnStatistics cs = new ColumnStatistics(statsDec, statsObjs);
    store.updatePartitionColumnStatistics(cs, pVals);

    cs = null; // make sure we don't accidently test against this
    List<ColumnStatistics> statsList = store.getPartitionColumnStatistics(dbName, tableName,
        Collections.singletonList(partName), null);
    ColumnStatisticsObj cso1 = statsList.get(0).getStatsObj().get(0);
    Assert.assertEquals("a", cso1.getColName());
    Assert.assertEquals(317, cso1.getStatsData().getStringStats().getMaxColLen());
    cso1 = statsList.get(0).getStatsObj().get(1);
    Assert.assertEquals("b", cso1.getColName());
    Assert.assertEquals(97, cso1.getStatsData().getLongStats().getNumNulls());

    // Now add some new stats, make sure the merge happens properly
    ldata = new LongColumnStatsData(107, 202820);
    ldata.setLowValue(0);
    ldata.setHighValue(98797433);
    csd_b = ColumnStatisticsData.longStats(ldata);

    DecimalColumnStatsData ddata = new DecimalColumnStatsData(7, 1384729);
    ColumnStatisticsData csd_c = ColumnStatisticsData.decimalStats(ddata);
    statsObjs = Arrays.asList(
        new ColumnStatisticsObj("b", "int", csd_b),
        new ColumnStatisticsObj("c", "decimal(10, 2)", csd_c)
    );
    cs = new ColumnStatistics(statsDec, statsObjs);
    store.updatePartitionColumnStatistics(cs, pVals);

    cs = null;
    statsList = store.getPartitionColumnStatistics(dbName, tableName,
        Collections.singletonList(partName), null);
    boolean sawA = false, sawB = false, sawC = false;

    Assert.assertEquals(3, statsList.get(0).getStatsObjSize());
    for (int i = 0; i < 3; i++) {
      cso1 = statsList.get(0).getStatsObj().get(i);
      if ("a".equals(cso1.getColName())) {
        sawA = true;
        Assert.assertEquals(317, cso1.getStatsData().getStringStats().getMaxColLen());
      } else if ("b".equals(cso1.getColName())) {
        sawB = true;
        Assert.assertEquals(107, cso1.getStatsData().getLongStats().getNumNulls());
      } else if ("c".equals(cso1.getColName())) {
        sawC = true;
        Assert.assertEquals(7, cso1.getStatsData().getDecimalStats().getNumNulls());
      } else {
        Assert.fail("Unknown column " + cso1.getColName());
      }
    }
    Assert.assertTrue(sawA);
    Assert.assertTrue(sawB);
    Assert.assertTrue(sawC);
  }
}
