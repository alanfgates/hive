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

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hive.test.capybara.data.DataSet;
import org.apache.hive.test.capybara.data.Row;
import org.apache.hive.test.capybara.iface.DataGenerator;
import org.apache.hive.test.capybara.iface.TestTable;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class TestStaticDataGenerator {

  @Before
  public void setup() throws IOException {
    TestManager testMgr = TestManager.getTestManager();
    testMgr.getTestConf().getProperties().setProperty(TestConf.TEST_CLUSTER +
        TestConf.CLUSTER_CLUSTER_MANAGER, NullCluster.class.getName());
    testMgr.getTestConf().getProperties().setProperty(TestConf.BENCH_CLUSTER +
        TestConf.CLUSTER_CLUSTER_MANAGER, NullCluster.class.getName());
    testMgr.getTestClusterManager().setup(TestConf.TEST_CLUSTER);
    testMgr.getBenchmarkClusterManager().setup(TestConf.BENCH_CLUSTER);
  }

  @Test
  public void allTypes() throws SQLException, IOException {

    final String tableName = "alltypes";


    List<FieldSchema> cols = Arrays.asList(
        new FieldSchema("col_bi", "bigint", ""),
        new FieldSchema("col_i", "int", ""),
        new FieldSchema("col_si", "smallint", ""),
        new FieldSchema("col_ti", "tinyint", ""),
        new FieldSchema("col_bin", "binary", ""),
        new FieldSchema("col_bool", "boolean", ""),
        new FieldSchema("col_ch", "char(8)", ""),
        new FieldSchema("col_vc", "varchar(89)", ""),
        new FieldSchema("col_str", "string", ""),
        new FieldSchema("col_date", "date", ""),
        new FieldSchema("col_dec", "decimal(10,2)", ""),
        new FieldSchema("col_fl", "float", ""),
        new FieldSchema("col_dbl", "double", ""),
        new FieldSchema("col_tm", "timestamp", "")
    );

    TestTable table = TestTable.getBuilder(tableName).setCols(cols).build();

    List<String> rows = new ArrayList<>();
    rows.add("6022141300000000000,299792458,1432,7,abc,true,bob,mary had a little lamb,her" +
        " fleece was white as snow,2015-08-04,371.89,1.234,6.0221413e+23,2015-08-04 17:16:32");
    rows.add("NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL");
    DataGenerator gen = new StaticDataGenerator(rows, ",");
    DataSet ds = gen.generateData(table);

    Iterator<Row> iter = ds.iterator();
    Assert.assertTrue(iter.hasNext());
    Row row = iter.next();
    Assert.assertEquals(6022141300000000000L, row.get(0).asLong());
    Assert.assertEquals(299792458, row.get(1).asInt());
    Assert.assertEquals((short)1432, row.get(2).asShort());
    Assert.assertEquals((byte) 7, row.get(3).asByte());
    // Have to wash this back and forth through the encode/decoder to get the same result.
    Assert.assertEquals("abc", Base64.encodeBase64URLSafeString(row.get(4).asBytes()));
    Assert.assertEquals(true, row.get(5).asBoolean());
    Assert.assertEquals("bob", row.get(6).asString());
    Assert.assertEquals("mary had a little lamb", row.get(7).asString());
    Assert.assertEquals("her fleece was white as snow", row.get(8).asString());
    Assert.assertEquals(Date.valueOf("2015-08-04"), row.get(9).asDate());
    Assert.assertEquals(new BigDecimal("371.89"), row.get(10).asBigDecimal());
    Assert.assertEquals(1.234f, row.get(11).asFloat(), 0.01);
    Assert.assertEquals(6.0221413e+23, row.get(12).asDouble(), 1e+16);
    Assert.assertEquals(Timestamp.valueOf("2015-08-04 17:16:32"), row.get(13).asTimestamp());

    Assert.assertTrue(iter.hasNext());
    row = iter.next();
    for (int i = 0; i < cols.size(); i++) {
      Assert.assertTrue(row.get(i).isNull());
    }
    Assert.assertFalse(iter.hasNext());
  }

  @Test
  public void partitioned() throws SQLException, IOException {

    final String tableName = "alltypes";


    TestTable table = TestTable.getBuilder(tableName)
        .addCol("col_bi", "bigint")
        .addPartCol("partcol", "string")
        .build();

    List<String> rows = new ArrayList<>();
    rows.add("6,na");
    rows.add("7,eu");
    DataGenerator gen = new StaticDataGenerator(rows, ",");
    DataSet ds = gen.generateData(table);

    Iterator<Row> iter = ds.iterator();
    Assert.assertTrue(iter.hasNext());
    Row row = iter.next();
    Assert.assertEquals(6L, row.get(0).asLong());
    Assert.assertEquals("na", row.get(1).asString());

    Assert.assertTrue(iter.hasNext());
    row = iter.next();
    Assert.assertEquals(7L, row.get(0).asLong());
    Assert.assertEquals("eu", row.get(1).asString());
    Assert.assertFalse(iter.hasNext());
  }
}
