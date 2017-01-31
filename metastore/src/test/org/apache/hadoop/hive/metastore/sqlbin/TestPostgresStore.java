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
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.AggrStats;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsDesc;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.DecimalColumnStatsData;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Function;
import org.apache.hadoop.hive.metastore.api.FunctionType;
import org.apache.hadoop.hive.metastore.api.InvalidInputException;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.LongColumnStatsData;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.ResourceUri;
import org.apache.hadoop.hive.metastore.api.Role;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.StringColumnStatsData;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.hbase.HBaseStore;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class TestPostgresStore {

  // If this property is defined, we'll know that the user intends to connect to an external
  // postgres, all tests will be ignored.
  private static PostgresStore store;
  // If you create any tables with partitions, you must add the corresponding partition table into
  // this list so that it gets dropped at test start time.
  private static List<String> tablesToDrop = new ArrayList<>();

  @BeforeClass
  public static void connect() throws MetaException, SQLException {
    store = PostgresKeyValue.connectForTest(new HiveConf(), tablesToDrop);
  }

  @AfterClass
  public static void cleanup() throws SQLException {
    PostgresKeyValue.cleanupAfterTest(store, tablesToDrop);
  }

  @Before
  public void checkExternalPostgres() {
    Assume.assumeNotNull(System.getProperty(PostgresKeyValue.TEST_POSTGRES_JDBC));
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
  public void tables() throws InvalidObjectException, MetaException, NoSuchObjectException, InvalidInputException {
    String dbName = "tbltest_db";
    String tableName = "tbl1";
    String tableName2 = "xxx_tbl2";

    Database db = new Database(dbName, "description", "file:/somewhere",
        Collections.<String, String>emptyMap());
    store.createDatabase(db);

    List<FieldSchema> cols = Collections.singletonList(
        new FieldSchema("a", "varchar(32)", "")
    );
    SerDeInfo serde = new SerDeInfo("serde", "serde", Collections.<String, String>emptyMap());
    StorageDescriptor sd = new StorageDescriptor(cols, "file:/tmp/tbl1", "inputformat",
        "outputformat", false, 0, serde, null, null, Collections.<String, String>emptyMap());
    Table table = new Table(tableName, dbName, "me", 1, 2, 3, sd, null,
        Collections.<String, String>emptyMap(), null, null, TableType.MANAGED_TABLE.name());
    store.createTable(table);

    table = store.getTable(dbName, "tbl1");

    // I assume thrift de/serialization works, so I'm not going to check every field.
    Assert.assertEquals(tableName, table.getTableName());
    Assert.assertEquals(dbName, table.getDbName());
    Assert.assertEquals("me", table.getOwner());
    cols = table.getSd().getCols();
    Assert.assertEquals(1, cols.size());
    Assert.assertEquals("a", cols.get(0).getName());

    table = new Table(tableName2, dbName, "me", 1, 2, 3, sd, null,
        Collections.<String, String>emptyMap(), null, null, TableType.MANAGED_TABLE.name());
    store.createTable(table);
    List<String> names = store.getTables(dbName, null);
    Assert.assertEquals(2, names.size());
    String[] namesArray = names.toArray(new String[names.size()]);
    Arrays.sort(namesArray);
    Assert.assertEquals(tableName, namesArray[0]);
    Assert.assertEquals(tableName2, namesArray[1]);

    names = store.getTables(dbName, "xxx.*");
    Assert.assertEquals(1, names.size());
    Assert.assertEquals(tableName2, names.get(0));

    store.dropTable(dbName, tableName2);
    names = store.getTables(dbName, null);
    Assert.assertEquals(1, names.size());
    Assert.assertEquals(tableName, names.get(0));
  }

  @Test(expected = NoSuchObjectException.class)
  public void nonExistentTable() throws MetaException, InvalidInputException, NoSuchObjectException, InvalidObjectException {
    Table table = store.getTable("default", "nosuchtable");
    Assert.assertNull(table);

    store.dropTable("default", "nosuchtable");
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
  public void partitions() throws InvalidObjectException, MetaException, NoSuchObjectException, InvalidInputException {
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
    Table table = new Table(tableName, dbName, "me", 1, 2, 3, sd, pcols,
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

    List<Partition> parts = store.getPartitions(dbName, tableName, -1);
    Assert.assertEquals(1, parts.size());

    // Add some more partitions so that we can drop them
    part = new Partition(Collections.singletonList("b"), dbName, tableName, 1, 2, sd,
        Collections.<String, String>emptyMap());
    store.addPartition(part);
    part = new Partition(Collections.singletonList("c"), dbName, tableName, 1, 2, sd,
        Collections.<String, String>emptyMap());
    store.addPartition(part);

    parts = store.getPartitions(dbName, tableName, -1);
    Assert.assertEquals(3, parts.size());

    store.dropPartition(dbName, tableName, pVals);
    parts = store.getPartitions(dbName, tableName, -1);
    Assert.assertEquals(2, parts.size());
    Assert.assertNotEquals("a", parts.get(0).getValues().get(0));
    Assert.assertNotEquals("a", parts.get(1).getValues().get(0));

    store.dropPartitions(dbName, tableName, Arrays.asList("pcol=b", "pcol=c"));
    parts = store.getPartitions(dbName, tableName, -1);
    Assert.assertEquals(0, parts.size());
  }

  @Test(expected = NoSuchObjectException.class)
  public void nonExistentPartition() throws InvalidObjectException, MetaException, NoSuchObjectException, InvalidInputException {
    String dbName = "default";
    String tableName = "pdtbl1";

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
    Table table = new Table(tableName, dbName, "me", 1, 2, 3, sd, pcols,
        Collections.<String, String>emptyMap(), null, null, TableType.MANAGED_TABLE.name());
    store.createTable(table);
    tablesToDrop.add(PostgresKeyValue.buildPartTableName(dbName, tableName));

    store.dropPartition(dbName, tableName, Collections.singletonList("nosuchpart"));
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

    cs = store.getTableColumnStatistics(dbName, tableName, Collections.singletonList("a"));
    Assert.assertEquals(1, cs.getStatsObjSize());
    Assert.assertEquals("a", cs.getStatsObj().get(0).getColName());
  }

  @Test
  public void partStats() throws InvalidObjectException, MetaException, NoSuchObjectException,
                                 InvalidInputException {
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

    statsList = store.getPartitionColumnStatistics(dbName, tableName,
        Collections.singletonList(partName), Collections.singletonList("a"));
    Assert.assertEquals(1, statsList.get(0).getStatsObjSize());
    Assert.assertEquals("a", statsList.get(0).getStatsObj().get(0).getColName());
  }

  @Test
  public void aggrStats() throws InvalidObjectException, MetaException, NoSuchObjectException,
      InvalidInputException {
    String dbName = "default";
    String tableName = "astbl";
    List<List<String>> pValLists = Arrays.asList(
        Collections.singletonList("a"),
        Collections.singletonList("b"),
        Collections.singletonList("c")
        );
    List<String> partNames = new ArrayList<>();


    List<FieldSchema> cols = Collections.singletonList(new FieldSchema("a", "varchar(32)", ""));
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

    long maxColLen = 100;
    double avgColLen = 10;
    long numNulls = 10;
    long numDVs = 100;
    for (List<String> pVals : pValLists) {
      Partition part = new Partition(pVals, dbName, tableName, 1, 2, sd,
          Collections.<String, String>emptyMap());
      store.addPartition(part);
      partNames.add(HBaseStore.buildExternalPartName(table, pVals));

      ColumnStatisticsData csd_a = ColumnStatisticsData.stringStats(
          new StringColumnStatsData(maxColLen, avgColLen, numNulls, numDVs)
      );
      maxColLen *= 2;
      avgColLen *= 2;
      numNulls *= 2;
      numDVs *= 2;
      ColumnStatisticsDesc statsDec = new ColumnStatisticsDesc(false, dbName, tableName);
      statsDec.setPartName(HBaseStore.buildExternalPartName(table, pVals));
      List<ColumnStatisticsObj> statsObjs =
          Collections.singletonList(new ColumnStatisticsObj("a", "varchar(32)", csd_a));
      ColumnStatistics cs = new ColumnStatistics(statsDec, statsObjs);
      store.updatePartitionColumnStatistics(cs, pVals);
    }

    AggrStats aggrStats = store.get_aggr_stats_for(dbName, tableName, partNames,
        Collections.singletonList("a"));

    Assert.assertNotNull(aggrStats);
    Assert.assertEquals(3, aggrStats.getPartsFound());
    Assert.assertEquals(1, aggrStats.getColStatsSize());
    Assert.assertEquals("a", aggrStats.getColStats().get(0).getColName());
    StringColumnStatsData scsd = aggrStats.getColStats().get(0).getStatsData().getStringStats();
    Assert.assertEquals(400, scsd.getMaxColLen());
    // Can't calculate average col length because we don't know the number of distinct values
    Assert.assertEquals(70, scsd.getNumNulls());
    // Don't know what to do with numDVs, but if the others are working I assume it is too
  }

  @Test
  public void roles() throws NoSuchObjectException, InvalidObjectException, MetaException {
    String roleName = "thisisarole";
    String roleOwner = "me";
    store.addRole(roleName, roleOwner);

    Role role = store.getRole(roleName);

    Assert.assertNotNull(role);
    Assert.assertEquals(roleName, role.getRoleName());
    Assert.assertEquals(roleOwner, role.getOwnerName());
  }

}
