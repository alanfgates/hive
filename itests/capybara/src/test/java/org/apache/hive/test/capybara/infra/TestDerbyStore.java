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
import org.apache.hive.test.capybara.data.Row;
import org.apache.hive.test.capybara.iface.ClusterManager;
import org.apache.hive.test.capybara.iface.DataGenerator;
import org.apache.hive.test.capybara.iface.DataStore;
import org.apache.hive.test.capybara.iface.TestTable;
import org.hamcrest.core.IsInstanceOf;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class TestDerbyStore {
  static final private Logger LOG = LoggerFactory.getLogger(TestDerbyStore.class.getName());
  static private DerbyStore derby;

  @BeforeClass
  public static void setup() throws IOException {
    TestManager testMgr = TestManager.getTestManager();
    // Make it so we get the null cluster manager
    testMgr.getTestConf().getProperties().setProperty(TestConf.TEST_CLUSTER +
        TestConf.CLUSTER_CLUSTER_MANAGER, NullCluster.class.getName());
    testMgr.getTestClusterManager().setup(TestConf.TEST_CLUSTER);
    ClusterManager clusterManager = testMgr.getBenchmarkClusterManager();
    clusterManager.setup(TestConf.BENCH_CLUSTER);
    DataStore tmpStore = clusterManager.getStore();
    Assert.assertThat(tmpStore, new IsInstanceOf(DerbyStore.class));
    derby = (DerbyStore)tmpStore;
  }

  @Test
  public void allTypes() throws Exception {
    TestTable table = TestTable.getBuilder("derbyAllTypes")
        .addCol("c1", "bigint")
        .addCol("c2", "int")
        .addCol("c3", "smallint")
        .addCol("c4", "tinyint")
        .addCol("c5", "float")
        .addCol("c6", "double")
        .addCol("c7", "decimal(19,2)")
        .addCol("c8", "date")
        .addCol("c9", "timestamp")
        .addCol("c10", "varchar(32)")
        .addCol("c11", "char(32)")
        .addCol("c12", "string")
        .addCol("c13", "boolean")
        // Binary doesn't work on derby at the moment
        .build();
    derby.dropTable(table);
    derby.createTable(table);

    DataGenerator gen = new RandomDataGenerator(1);
    derby.loadData(table, gen.generateData(table));
  }

  @Test
  public void derby() throws Exception {
    TestTable table = TestTable.getBuilder("foo")
        .addCol("c1", "int")
        .addCol("c2", "varchar(25)")
        .build();

    derby.dropTable(table);
    derby.createTable(table);

    List<String> rows = Arrays.asList("1,fred", "2,bob");
    StaticDataGenerator gen = new StaticDataGenerator(rows, ",");
    DataSet data = gen.generateData(table);

    derby.loadData(table, data);

    // Re-create and if it returns true re-load the table to make sure that piece works correctly.
    if (derby.createTable(table)) {
      Assert.fail();
    }

    FetchResult fetch = derby.executeSql("select c1 from foo");
    Assert.assertEquals(ResultCode.SUCCESS, fetch.rc);

    Iterator<Row> iter = fetch.data.iterator();
    Assert.assertTrue(iter.hasNext());
    Assert.assertEquals(1, iter.next().get(0).asInt());
    Assert.assertTrue(iter.hasNext());
    Assert.assertEquals(2, iter.next().get(0).asInt());
    Assert.assertFalse(iter.hasNext());
  }

  @Test
  public void sameNameDifferentDbs() throws Exception {
    TestTable otherTable = null;
    boolean createdSchema = false;
    try {
      List<FieldSchema> cols = Arrays.asList(
          new FieldSchema("c1", "int", ""),
          new FieldSchema("c2", "varchar(25)", "")
      );
      TestTable defaultTable = TestTable.getBuilder("tind").setCols(cols).build();
      derby.dropTable(defaultTable);
      derby.createTable(defaultTable);

      List<String> rows = Arrays.asList("1,fred", "2,bob");
      StaticDataGenerator gen = new StaticDataGenerator(rows, ",");
      DataSet data = gen.generateData(defaultTable);

      derby.loadData(defaultTable, data);

      derby.executeSql("create schema testschema");
      createdSchema = true;
      otherTable = TestTable.getBuilder("tind").setDbName("testschema").setCols(cols).build();
      rows = Arrays.asList("3,mary", "4,elizabeth");
      gen = new StaticDataGenerator(rows, ",");
      data = gen.generateData(otherTable);
      derby.dropTable(otherTable);
      derby.createTable(otherTable);
      derby.loadData(otherTable, data);

      FetchResult fetch = derby.executeSql("select c1 from tind");
      Assert.assertEquals(ResultCode.SUCCESS, fetch.rc);

      Iterator<Row> iter = fetch.data.iterator();
      Assert.assertTrue(iter.hasNext());
      Assert.assertEquals(1, iter.next().get(0).asInt());
      Assert.assertTrue(iter.hasNext());
      Assert.assertEquals(2, iter.next().get(0).asInt());
      Assert.assertFalse(iter.hasNext());

      fetch = derby.executeSql("select c1 from testschema.tind");
      Assert.assertEquals(ResultCode.SUCCESS, fetch.rc);

      iter = fetch.data.iterator();
      Assert.assertTrue(iter.hasNext());
      Assert.assertEquals(3, iter.next().get(0).asInt());
      Assert.assertTrue(iter.hasNext());
      Assert.assertEquals(4, iter.next().get(0).asInt());
      Assert.assertFalse(iter.hasNext());
    } finally {
      if (otherTable != null) derby.dropTable(otherTable);
      if (createdSchema) derby.executeSql("drop schema testschema restrict");
    }
  }

  @Test
  public void createTable() throws Exception {
    List<FieldSchema> cols = Arrays.asList(
        new FieldSchema("c1", "int", ""),
        new FieldSchema("c2", "varchar(25)", "")
    );
    derby.createTable(TestTable.getBuilder("foodle").setCols(cols).build());

    // Do it twice so we can see that we handle it properly if it already exists.
    derby.createTable(TestTable.getBuilder("foodle").setCols(cols).build());

  }

  @Test
  public void createPartitionedTable() throws Exception {
    TestTable table = TestTable.getBuilder("part_voo")
        .addCol("c1", "int")
        .addCol("c2", "varchar(25)")
        .addPartCol("p", "int")
        .build();
    derby.createTable(table);

    List<String> rows = Arrays.asList("1,fred,3", "2,bob,3");
    StaticDataGenerator gen = new StaticDataGenerator(rows, ",");
    DataSet data = gen.generateData(table);

    derby.loadData(table, data);
  }

  @Test
  public void failureOk() throws Exception {
    FetchResult fetch = derby.executeSql("drop table if exists fred");
    Assert.assertEquals(ResultCode.SUCCESS, fetch.rc);

    fetch = derby.executeSql("create table fred (a int)");
    Assert.assertEquals(ResultCode.SUCCESS, fetch.rc);

    fetch = derby.executeSql("create table if not exists fred (a int)");
    Assert.assertEquals(ResultCode.SUCCESS, fetch.rc);
  }
}
