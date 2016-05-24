/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hive.test.capybara.infra;

import org.apache.hadoop.hive.conf.HiveConf;
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

public class TestMiniHS2HiveStore {
  static final private Logger LOG = LoggerFactory.getLogger(TestHiveStore.class.getName());

  private static HiveConf conf;
  private static ClusterManager mgr;
  private static DataStore testStore;

  @BeforeClass
  public static void setup() throws IOException {
    /*conf = new HiveConf();
    conf.setVar(HiveConf.ConfVars.DYNAMICPARTITIONINGMODE, "nonstrict");
    //conf.setBoolVar(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY, false);

    TestManager.getTestManager().setConf(conf);
    TestConf.setEngine(TestConf.ENGINE_UNSPECIFIED);
    */
    TestManager.getTestManager().getTestConf().getProperties().setProperty(
        TestConf.TEST_CLUSTER + TestConf.CLUSTER_ACCESS, TestConf.ACCESS_JDBC);
    mgr = TestManager.getTestManager().getTestClusterManager();
    mgr.setup(TestConf.TEST_CLUSTER);
    mgr.setConfVar(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY.varname, "false");
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
}
