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

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hive.test.capybara.data.DataSet;
import org.apache.hive.test.capybara.data.FetchResult;
import org.apache.hive.test.capybara.data.ResultCode;
import org.apache.hive.test.capybara.data.Row;
import org.apache.hive.test.capybara.iface.ClusterManager;
import org.apache.hive.test.capybara.iface.DataStore;
import org.apache.hive.test.capybara.iface.TestTable;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class TestHiveStore {

  static final private Logger LOG = LoggerFactory.getLogger(TestHiveStore.class.getName());

  //private static HiveConf conf;
  private static ClusterManager mgr;
  private static DataStore testStore;

  @BeforeClass
  public static void setup() throws IOException {

    /*
    conf.setVar(HiveConf.ConfVars.DYNAMICPARTITIONINGMODE, "nonstrict");
    conf.setVar(HiveConf.ConfVars.HIVEMAPREDMODE, "nonstrict");
    TestConf.setEngine(TestConf.ENGINE_UNSPECIFIED);
    mgr = new MiniClusterManager();
    mgr.setConf(conf);
    mgr.setup(TestConf.TEST_CLUSTER);
    testStore = mgr.getStore();
    TestManager.getTestManager().setClusterManager(mgr);
    TestManager.getTestManager().setConf(conf);
    */
    TestManager testMgr = TestManager.getTestManager();
    testMgr.getTestConf().getProperties().setProperty(TestConf.BENCH_CLUSTER +
        TestConf.CLUSTER_CLUSTER_MANAGER, NullCluster.class.getName());
    testMgr.getBenchmarkClusterManager().setup(TestConf.BENCH_CLUSTER);
    mgr = testMgr.getTestClusterManager();
    mgr.setup(TestConf.TEST_CLUSTER);
    testStore = mgr.getStore();

  }

  @AfterClass
  public static void tearDown() throws IOException {
    mgr.tearDown();
  }

  @Test
  public void hive() throws Exception {
    // Load some data, then read it back.
    FetchResult fetch = testStore.executeSql("create table foo (c1 int, c2 varchar(32))");
    Assert.assertEquals(ResultCode.SUCCESS, fetch.rc);

    TestTable table = TestTable.fromHiveMetastore("default", "foo");

    List<String> rows = Arrays.asList("1,fred", "2,bob");
    StaticDataGenerator gen = new StaticDataGenerator(rows, ",");
    DataSet data = gen.generateData(table);

    testStore.loadData(table, data);

    fetch = testStore.executeSql("select c1 from foo");
    Assert.assertEquals(ResultCode.SUCCESS, fetch.rc);

    fetch.data.setSchema(Arrays.asList(new FieldSchema("c1", "int", "")));
    Iterator<Row> iter = fetch.data.iterator();
    Assert.assertTrue(iter.hasNext());
    Row row = iter.next();
    Assert.assertEquals(1, row.get(0).asInt());
    Assert.assertTrue(iter.hasNext());

    row = iter.next();
    Assert.assertEquals(2, row.get(0).asInt());
    Assert.assertFalse(iter.hasNext());
  }

  @Test
  public void sameNameDifferentDbs() throws Exception {
    boolean createdSchema = false;
    try {
      List<FieldSchema> cols = Arrays.asList(
          new FieldSchema("c1", "int", ""),
          new FieldSchema("c2", "varchar(25)", "")
      );
      TestTable defaultTable = TestTable.getBuilder("tind").setCols(cols).build();
      testStore.executeSql("drop table " + defaultTable.getFullName());
      Assert.assertTrue(testStore.createTable(defaultTable));

      List<String> rows = Arrays.asList("1,fred", "2,bob");
      StaticDataGenerator gen = new StaticDataGenerator(rows, ",");
      DataSet data = gen.generateData(defaultTable);

      testStore.loadData(defaultTable, data);

      testStore.executeSql("create database testschema");
      createdSchema = true;
      TestTable otherTable = TestTable.getBuilder("tind").setDbName("testschema").setCols(cols).build();
      rows = Arrays.asList("3,mary", "4,elizabeth");
      gen = new StaticDataGenerator(rows, ",");
      data = gen.generateData(otherTable);
      testStore.executeSql("drop table " + otherTable.getFullName());
      Assert.assertTrue(testStore.createTable(otherTable));
      testStore.loadData(otherTable, data);

      FetchResult fetch = testStore.executeSql("select c1 from tind");
      Assert.assertEquals(ResultCode.SUCCESS, fetch.rc);

      fetch.data.setSchema(Arrays.asList(new FieldSchema("c1", "int", "")));
      Iterator<Row> iter = fetch.data.iterator();
      Assert.assertTrue(iter.hasNext());
      Row row = iter.next();
      Assert.assertEquals(1, row.get(0).asInt());
      Assert.assertTrue(iter.hasNext());

      row = iter.next();
      Assert.assertEquals(2, row.get(0).asInt());
      Assert.assertFalse(iter.hasNext());

      fetch = testStore.executeSql("select c1 from testschema.tind");
      Assert.assertEquals(ResultCode.SUCCESS, fetch.rc);


      fetch.data.setSchema(Arrays.asList(new FieldSchema("c1", "int", "")));
      iter = fetch.data.iterator();
      Assert.assertTrue(iter.hasNext());
      row = iter.next();
      Assert.assertEquals(3, row.get(0).asInt());
      Assert.assertTrue(iter.hasNext());

      row = iter.next();
      Assert.assertEquals(4, row.get(0).asInt());
      Assert.assertFalse(iter.hasNext());
    } finally {
      if (createdSchema) testStore.executeSql("drop database testschema cascade");
    }
  }

  @Test
  public void hiveWithCreateTable() throws Exception {
    // Load some data, then read it back.

    TestTable table = TestTable.getBuilder("foozle")
        .addCol("c1", "int")
        .addCol("c2", "varchar(25)")
        .build();
    testStore.executeSql("drop table " + table.getFullName());
    testStore.createTable(table);

    // Re-create and if it returns true re-load the table to make sure that piece works correctly.
    if (testStore.createTable(table)) {
      Assert.fail();
    }

    List<String> rows = Arrays.asList("1,fred", "2,bob");
    StaticDataGenerator gen = new StaticDataGenerator(rows, ",");
    DataSet data = gen.generateData(table);

    testStore.loadData(table, data);

    FetchResult fetch = testStore.executeSql("select c1 from foozle");
    Assert.assertEquals(ResultCode.SUCCESS, fetch.rc);

    fetch.data.setSchema(Arrays.asList(new FieldSchema("c1", "int", "")));
    Iterator<Row> iter = fetch.data.iterator();
    Assert.assertTrue(iter.hasNext());

    Row row = iter.next();
    Assert.assertEquals(1, row.get(0).asInt());
    Assert.assertTrue(iter.hasNext());
    row = iter.next();
    Assert.assertEquals(2, row.get(0).asInt());
    Assert.assertFalse(iter.hasNext());
  }

  @Test
  public void createTable() throws Exception {
    //testStore.setConf(conf);
     List<FieldSchema> cols = Arrays.asList(
        new FieldSchema("c1", "int", ""),
        new FieldSchema("c2", "varchar(25)", "")
    );
    testStore.createTable(TestTable.getBuilder("foodle").setCols(cols).build());

    // Make sure we drop and re-create the table as necessary
    testStore.createTable(TestTable.getBuilder("foodle").setCols(cols).build());
  }

  @Test
  public void createPartitionedTable() throws Exception {
    //testStore.setConf(conf);
    TestTable table = TestTable.getBuilder("foo_part")
        .addCol("c1", "int")
        .addCol("c2", "varchar(25)")
        .addPartCol("pcol", "string")
        .build();
    testStore.executeSql("drop table " + table.getFullName());
    testStore.createTable(table);

    List<String> rows = Arrays.asList("1,fred,3", "2,bob,3");
    StaticDataGenerator gen = new StaticDataGenerator(rows, ",");
    DataSet data = gen.generateData(table);

    testStore.loadData(table, data);

    FetchResult fetch = testStore.executeSql("select count(*) from foo_part");
    Assert.assertEquals(ResultCode.SUCCESS, fetch.rc);
    fetch.data.setSchema(Arrays.asList(new FieldSchema("c0", "bigint", "")));
    Iterator<String> output = fetch.data.stringIterator(",", "", "\"");
    LOG.debug("Query result: " + StringUtils.join(output, "\n"));
  }
}
