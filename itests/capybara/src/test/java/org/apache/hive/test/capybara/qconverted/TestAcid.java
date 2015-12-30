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
package org.apache.hive.test.capybara.qconverted;

import org.apache.hive.test.capybara.IntegrationTest;
import org.apache.hive.test.capybara.annotations.AcidOn;
import org.apache.hive.test.capybara.annotations.NoCli;
import org.apache.hive.test.capybara.annotations.NoParquet;
import org.apache.hive.test.capybara.annotations.NoRcFile;
import org.apache.hive.test.capybara.annotations.NoTextFile;
import org.apache.hive.test.capybara.iface.DataGenerator;
import org.apache.hive.test.capybara.iface.TestTable;
import org.apache.hive.test.capybara.infra.RandomDataGenerator;
import org.apache.hive.test.capybara.infra.StaticDataGenerator;
import org.junit.Test;

import java.util.Arrays;

@AcidOn @NoParquet @NoTextFile @NoRcFile
public class TestAcid extends IntegrationTest {

  @Test
  public void insertValuesDynamicPartitioned() throws Exception {
    TestTable ivdp = TestTable.getBuilder("ivdp")
        .addCol("i", "int")
        .addCol("de", "decimal(5,2)")
        .addCol("vc", "varchar(128)")
        .addPartCol("ds", "string")
        .setBucketCols("i")
        .setNumBuckets(2)
        .setAcid(true)
        .build();
    ivdp.create();

    runQuery("insert into table ivdp partition(ds)values " +
              "(1, 109.23, 'and everywhere that mary went', 'today'), " +
              "(6553, 923.19, 'the lamb was sure to go', 'tomorrow')");

    runQuery("select * from ivdp");
    sortAndCompare();
  }

  @Test
  public void insertValuesPartitioned() throws Exception {
    TestTable acid_ivp = TestTable.getBuilder("acid_ivp")
        .addCol("ti", "tinyint")
        .addCol("si", "smallint")
        .addCol("i", "int")
        .addCol("bi", "bigint")
        .addCol("f", "float")
        .addCol("d", "double")
        .addCol("de", "decimal(5,2)")
        .addCol("t", "timestamp")
        .addCol("dt", "date")
        .addCol("s", "string")
        .addCol("vc", "varchar(128)")
        .addCol("ch", "char(12)")
        .addPartCol("ds", "string")
        .setBucketCols("i")
        .setNumBuckets(2)
        .setAcid(true)
        .build();
    acid_ivp.create();

    runQuery("insert into table acid_ivp partition (ds='today') values " +
        "(1, 257, 65537, 4294967297, 3.14, 3.141592654, 109.23, '2014-08-25 17:21:30.0', '2014-08-25', 'mary had a little lamb', 'ring around the rosie', 'red'), " +
        "(3, 25, 6553, 429496729, 0.14, 1923.141592654, 1.2301, '2014-08-24 17:21:30.0', '2014-08-26', 'its fleece was white as sno" +"w', 'a pocket full of posies', 'blue')");

    runQuery("select * from acid_ivp");
    sortAndCompare();
  }

  @Test
  public void insertValuesNonPartitioned() throws Exception {
    TestTable acid_ivnp = TestTable.getBuilder("acid_ivnp")
        .addCol("ti", "tinyint")
        .addCol("si", "smallint")
        .addCol("i", "int")
        .addCol("bi", "bigint")
        .addCol("f", "float")
        .addCol("d", "double")
        .addCol("de", "decimal(5,2)")
        .addCol("t", "timestamp")
        .addCol("dt", "date")
        .addCol("b", "boolean")
        .addCol("s", "string")
        .addCol("vc", "varchar(128)")
        .addCol("ch", "char(12)")
        .setBucketCols("i")
        .setNumBuckets(2)
        .setAcid(true)
        .build();
    acid_ivnp.create();

    runQuery("insert into table acid_ivnp values " +
        "(1, 257, 65537, 4294967297, 3.14, 3.141592654, 109.23, '2014-08-25 17:21:30.0', '2014-08-25', true, 'mary had a little lamb', 'ring around the rosie', 'red'), " +
        "(null, null, null, null, null, null, null, null, null, null, null, null, null), " +
    "(3, 25, 6553, null, 0.14, 1923.141592654, 1.2301, '2014-08-24 17:21:30.0', '2014-08-26', false, 'its fleece was white as snow', 'a pocket full of posies', 'blue' )");

    runQuery("select * from acid_ivnp");
    sortAndCompare();
  }

  // since Hive statements aren't guaranteed to run in a single session this doesn't
  // work for CLI based testing.
  @Test @NoCli
  public void insertValuesTmpTable() throws Exception {
    TestTable acid_ivtt = TestTable.getBuilder("acid_ivtt")
        .addCol("i", "int")
        .addCol("de", "decimal(5,2)")
        .addCol("vc", "varchar(128)")
        .addPartCol("ds", "string")
        .setBucketCols("vc")
        .setNumBuckets(2)
        .setAcid(true)
        .setTemporary(true)
        .build();
    acid_ivtt.create();

    runQuery("insert into table acid_ivtt values (1, 109.23, 'mary had a little lamb'), (429496729, 0.14, 'its fleece was white as snow'), (-29496729, -0.14, 'negative values test')");

    runQuery("select * from acid_ivtt");
    sortAndCompare();
  }

  @Test
  public void updateAllNonPartitioned() throws Exception {
    TestTable acid_uanp = TestTable.getBuilder("acid_uanp")
        .addCol("a", "int")
        .addCol("b", "varchar(128)")
        .setBucketCols("a")
        .setNumBuckets(2)
        .setAcid(true)
        .build();
    acid_uanp.create();
    DataGenerator generator = new RandomDataGenerator(873);
    acid_uanp.populate(generator);

    runQuery("update acid_uanp set b = 'fred'");

    runQuery("select * from acid_uanp");
    sortAndCompare();
  }

  @Test
  public void updateAllPartitioned() throws Exception {
    TestTable acid_uap = TestTable.getBuilder("acid_uap")
        .addCol("a", "int")
        .addCol("b", "varchar(128)")
        .addPartCol("ds", "string")
        .setNumParts(2)
        .setBucketCols("a")
        .setNumBuckets(2)
        .setAcid(true)
        .build();
    acid_uap.create();
    DataGenerator generator = new RandomDataGenerator(874);
    acid_uap.populate(generator);

    runQuery("update acid_uap set b = 'fred'");

    runQuery("select * from acid_uap");
    sortAndCompare();
  }

  @Test
  public void updateWhereNonPartitioned() throws Exception {
    TestTable acid_uwnp = TestTable.getBuilder("acid_uwnp")
        .addCol("a", "int")
        .addCol("b", "varchar(128)")
        .setBucketCols("a")
        .setNumBuckets(2)
        .setAcid(true)
        .build();
    acid_uwnp.create();
    DataGenerator generator = new StaticDataGenerator(Arrays.asList(
        "1,abc", "2,def", "3,ghi", "4,jkl", "5,mno", "6,pqr", "7,stu", "8,vwx", "9,yz"), ",");
    acid_uwnp.populate(generator);

    runQuery("update acid_uwnp set b = 'fred' where a = 1");

    runQuery("select * from acid_uwnp");
    sortAndCompare();
  }

  @Test
  public void updateWherePartitioned() throws Exception {
    TestTable acid_uwp = TestTable.getBuilder("acid_uwp")
        .addCol("a", "int")
        .addCol("b", "varchar(128)")
        .setBucketCols("a")
        .setNumBuckets(2)
        .addPartCol("ds", "string")
        .setAcid(true)
        .build();
    acid_uwp.create();
    DataGenerator generator = new StaticDataGenerator(Arrays.asList(
        "1,abc,today", "2,def,today", "3,ghi,today", "4,jkl,today",
        "5,mno,tomorrow", "6,pqr,tomorrow", "7,stu,tomorrow", "8,vwx,tomorrow", "9,yz,tomorrow"), ",");
    acid_uwp.populate(generator);

    runQuery("update acid_uwp set b = 'fred' where a = 1");

    runQuery("select * from acid_uwp");
    sortAndCompare();
  }

  @Test
  public void updateWhereNoMatch() throws Exception {
    TestTable acid_uwnm = TestTable.getBuilder("acid_uwnm")
        .addCol("a", "int")
        .addCol("b", "varchar(128)")
        .setBucketCols("a")
        .setNumBuckets(2)
        .setAcid(true)
        .build();
    acid_uwnm.create();
    DataGenerator generator = new StaticDataGenerator(Arrays.asList(
        "1,abc", "2,def", "3,ghi", "4,jkl", "5,mno", "6,pqr", "7,stu", "8,vwx", "9,yz"), ",");
    acid_uwnm.populate(generator);

    runQuery("update acid_uwnm set b = 'fred' where b = 'nosuchvalue'");

    runQuery("select * from acid_uwnm");
    sortAndCompare();
  }

  @Test
  public void updateAllTypes() throws Exception {
    TestTable acid_uat = TestTable.getBuilder("acid_uat")
        .addCol("ti", "tinyint")
        .addCol("si", "smallint")
        .addCol("i", "int") // Can't change this one because I need a bucket key
        .addCol("j", "int")
        .addCol("bi", "bigint")
        .addCol("f", "float")
        .addCol("d", "double")
        .addCol("de", "decimal(5,2)")
        .addCol("t", "timestamp")
        .addCol("dt", "date")
        .addCol("s", "string")
        .addCol("vc", "varchar(128)")
        .addCol("c", "char(28)")
        .addCol("b", "boolean")
        .setBucketCols("i")
        .setNumBuckets(2)
        .setAcid(true)
        .build();
    acid_uat.create();
    DataGenerator generator = new RandomDataGenerator(875);
    acid_uat.populate(generator);

    runQuery("update acid_uat set " +
        "ti = 1, " +
        "si = 2, " +
        "j = 3, " +
        "bi = 4, " +
        "f = 3.14, " +
        "d = 6.28, " +
        "de = 5.99, " +
        "t = '2014-09-01 09:44:23.23', " +
        "dt = '2014-09-01', " +
        "s = 'its a beautiful day in the neighbhorhood', " +
        "vc = 'a beautiful day for a neighbor', " +
        "c = 'wont you be mine', " +
        "b = true");

    runQuery("select * from acid_uat");
    sortAndCompare();

    runQuery("update acid_uat set " +
        "ti = ti * 2, " +
        "si = cast(f as int), " +
        "d = floor(de)");


    runQuery("select * from acid_uat");
    sortAndCompare();
  }

  // TODO - can't replicate update_orig_table and delete_orig_table

  @Test
  public void insertUpdateDelete() throws Exception {
    TestTable acid_iud = TestTable.getBuilder("acid_iud")
        .addCol("i", "int")
        .addCol("de", "decimal(5,2)")
        .addCol("vc", "varchar(128)")
        .setBucketCols("i")
        .setNumBuckets(2)
        .setAcid(true)
        .build();
    acid_iud.create();

    runQuery("insert into table acid_iud values " +
        "(1, 109.23, 'mary had a little lamb'), " +
        "(6553, 923.19, 'its fleece was white as snow')");

    runQuery("insert into table acid_iud values " +
        "(10, 119.23, 'and everywhere that mary went'), " +
        "(65530, 823.19, 'the lamb was sure to go')");

    runQuery("select * from acid_iud");
    sortAndCompare();

    runQuery("update acid_iud set de = 3.14 where de = 109.23 or de = 119.23");

    runQuery("select * from acid_iud");
    sortAndCompare();

    runQuery("delete from acid_iud where vc = 'mary had a little lamb'");

    runQuery("select * from acid_iud");
    sortAndCompare();
  }

  @Test
  public void updateMultipleCols() throws Exception {
    TestTable acid_umc = TestTable.getBuilder("acid_umc")
        .addCol("a", "int")
        .addCol("b", "varchar(128)")
        .addCol("c", "float")
        .setBucketCols("a")
        .setNumBuckets(2)
        .setAcid(true)
        .build();
    acid_umc.create();
    DataGenerator generator = new RandomDataGenerator(876);
    acid_umc.populate(generator);

    runQuery("update acid_umc set b = 'fred',c = 3.14");

    runQuery("select * from acid_umc");
    sortAndCompare();
  }

  // since Hive statements aren't guaranteed to run in a single session this doesn't
  // work for CLI based testing.
  @Test @NoCli
  public void updateTmpTable() throws Exception {
    TestTable acid_utt = TestTable.getBuilder("acid_utt")
        .addCol("i", "int")
        .addCol("de", "decimal(5,2)")
        .addCol("vc", "varchar(128)")
        .addPartCol("ds", "string")
        .setBucketCols("vc")
        .setNumBuckets(2)
        .setAcid(true)
        .setTemporary(true)
        .build();
    acid_utt.create();
    DataGenerator generator = new RandomDataGenerator(877);
    acid_utt.populate(generator);

    runQuery("update acid_utt set i = 5");

    runQuery("select * from acid_utt");
    sortAndCompare();
  }

  @Test
  public void deleteAllNonPartitioned() throws Exception {
    TestTable acid_danp = TestTable.getBuilder("acid_danp")
        .addCol("a", "int")
        .addCol("b", "varchar(128)")
        .setBucketCols("a")
        .setNumBuckets(2)
        .setAcid(true)
        .build();
    acid_danp.create();
    DataGenerator generator = new RandomDataGenerator(879);
    acid_danp.populate(generator);

    runQuery("delete from acid_danp");

    runQuery("select * from acid_danp");
    assertEmpty();
  }

  @Test
  public void deleteAllPartitioned() throws Exception {
    TestTable acid_dap = TestTable.getBuilder("acid_dap")
        .addCol("a", "int")
        .addCol("b", "varchar(128)")
        .addPartCol("ds", "string")
        .setNumParts(2)
        .setBucketCols("a")
        .setNumBuckets(2)
        .setAcid(true)
        .build();
    acid_dap.create();
    DataGenerator generator = new RandomDataGenerator(878);
    acid_dap.populate(generator);

    runQuery("delete from acid_dap");

    runQuery("select * from acid_dap");
    assertEmpty();
  }

  @Test
  public void deleteWhereNonPartitioned() throws Exception {
    TestTable acid_dwnp = TestTable.getBuilder("acid_dwnp")
        .addCol("a", "int")
        .addCol("b", "varchar(128)")
        .setBucketCols("a")
        .setNumBuckets(2)
        .setAcid(true)
        .build();
    acid_dwnp.create();
    DataGenerator generator = new StaticDataGenerator(Arrays.asList(
        "1,abc", "2,def", "3,ghi", "4,jkl", "5,mno", "6,pqr", "7,stu", "8,vwx", "9,yz"), ",");
    acid_dwnp.populate(generator);

    runQuery("delete from acid_dwnp where a = 1");

    runQuery("select * from acid_dwnp");
    sortAndCompare();
  }

  @Test
  public void deleteWherePartitioned() throws Exception {
    TestTable acid_dwp = TestTable.getBuilder("acid_dwp")
        .addCol("a", "int")
        .addCol("b", "varchar(128)")
        .setBucketCols("a")
        .setNumBuckets(2)
        .addPartCol("ds", "string")
        .setAcid(true)
        .build();
    acid_dwp.create();
    DataGenerator generator = new StaticDataGenerator(Arrays.asList(
        "1,abc,today", "2,def,today", "3,ghi,today", "4,jkl,today",
        "5,mno,tomorrow", "6,pqr,tomorrow", "7,stu,tomorrow", "8,vwx,tomorrow", "9,yz,tomorrow"), ",");
    acid_dwp.populate(generator);

    runQuery("delete from acid_dwp where a = 1");

    runQuery("select * from acid_dwp");
    sortAndCompare();
  }

  @Test
  public void deleteWhereNoMatch() throws Exception {
    TestTable acid_dwnm = TestTable.getBuilder("acid_dwnm")
        .addCol("a", "int")
        .addCol("b", "varchar(128)")
        .setBucketCols("a")
        .setNumBuckets(2)
        .setAcid(true)
        .build();
    acid_dwnm.create();
    DataGenerator generator = new StaticDataGenerator(Arrays.asList(
        "1,abc", "2,def", "3,ghi", "4,jkl", "5,mno", "6,pqr", "7,stu", "8,vwx", "9,yz"), ",");
    acid_dwnm.populate(generator);

    runQuery("delete from acid_dwnm where b = 'nosuchvalue'");

    runQuery("select * from acid_dwnm");
    sortAndCompare();
  }

  @Test
  public void deleteWholePartition() throws Exception {
    TestTable acid_dp = TestTable.getBuilder("acid_dp")
        .addCol("a", "int")
        .addCol("b", "varchar(128)")
        .setBucketCols("a")
        .setNumBuckets(2)
        .addPartCol("ds", "string")
        .setAcid(true)
        .build();
    acid_dp.create();
    DataGenerator generator = new StaticDataGenerator(Arrays.asList(
        "1,abc,today", "2,def,today", "3,ghi,today", "4,jkl,today",
        "5,mno,tomorrow", "6,pqr,tomorrow", "7,stu,tomorrow", "8,vwx,tomorrow", "9,yz,tomorrow"), ",");
    acid_dp.populate(generator);

    runQuery("delete from acid_dp where ds = 'today'");

    runQuery("select * from acid_dp");
    sortAndCompare();
  }

  // since Hive statements aren't guaranteed to run in a single session this doesn't
  // work for CLI based testing.
  @Test @NoCli
  public void deleteTmpTable() throws Exception {
    TestTable acid_dtt = TestTable.getBuilder("acid_dtt")
        .addCol("i", "int")
        .addCol("de", "decimal(5,2)")
        .addCol("vc", "varchar(128)")
        .addPartCol("ds", "string")
        .setBucketCols("vc")
        .setNumBuckets(2)
        .setAcid(true)
        .setTemporary(true)
        .build();
    acid_dtt.create();
    DataGenerator generator = new RandomDataGenerator(880);
    acid_dtt.populate(generator);

    runQuery("delete from acid_dtt");

    runQuery("select * from acid_dtt");
    assertEmpty();
  }
}
