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
import org.apache.hive.test.capybara.infra.RandomDataGenerator;
import org.apache.hive.test.capybara.infra.TestTable;
import org.junit.Test;

public class TestSkewJoin extends IntegrationTest {

  @Test
  public void skewjoin_union_remove_1() throws Exception {
    set("hive.optimize.skewjoin.compiletime", true);
    set("hive.mapred.supports.subdirectories", true);

    set("hive.stats.autogather", false);
    set("hive.optimize.union.remove", true);

    set("hive.merge.mapfiles", false);
    set("hive.merge.mapredfiles", false);
    set("hive.merge.sparkfiles", false);
    set("mapred.input.dir.recursive", true);

    // This is to test the union->selectstar->filesink and skewjoin optimization
    // Union of 2 map-reduce subqueries is performed for the skew join
    // There is no need to write the temporary results of the sub-queries, and then read them
    // again to process the union. The union can be removed completely.
    // INCLUDE_HADOOP_MAJOR_VERSIONS(0.23)
    // Since this test creates sub-directories for the output, it might be easier to run the test
    // only on hadoop 23
    RandomDataGenerator generator = new RandomDataGenerator(37);

    runQuery("drop table if exists T1");
    runQuery("CREATE TABLE T1(k STRING, val STRING) SKEWED BY (k) ON ((2))");

    TestTable t1 = TestTable.fromHiveMetastore("default", "T1");
    t1.populate(generator);

    runQuery("drop table if exists T2");
    runQuery("CREATE TABLE T2(k STRING, val STRING) SKEWED BY (k) ON ((3))");

    TestTable t2 = TestTable.fromHiveMetastore("default", "T2");
    t2.populate(generator);

    // a simple join query with skew on both the tables on the join key

    explain("SELECT * FROM T1 a JOIN T2 b ON a.k = b.k");
    // TODO - should look for something here.

    set("hive.input.format", "org.apache.hadoop.hive.ql.io.HiveInputFormat");

    runQuery("SELECT * FROM T1 a JOIN T2 b ON a.k = b.k ORDER BY a.k, b.k, a.val, b.val");

    // test outer joins also

    explain("SELECT a.*, b.* FROM T1 a RIGHT OUTER JOIN T2 b ON a.k = b.k");
    // TODO - should look for something here.

    runQuery(
        "SELECT a.*, b.* FROM T1 a RIGHT OUTER JOIN T2 b ON a.k = b.k ORDER BY a.k, b.k, a.val, b.val");

    runQuery("drop table if exists DEST1");
    runQuery("create table DEST1(k1 STRING, val1 STRING, k2 STRING, val2 STRING)");

    explain("INSERT INTO TABLE DEST1 SELECT * FROM T1 a JOIN T2 b ON a.k = b.k");
    // TODO - should look for something here.

    runQuery("INSERT INTO TABLE DEST1 SELECT * FROM T1 a JOIN T2 b ON a.k = b.k");

    runQuery("SELECT * FROM DEST1 ORDER BY k1, k2, val1, val2");

    explain("INSERT OVERWRITE TABLE DEST1 SELECT * FROM T1 a RIGHT OUTER JOIN T2 b ON a.k = b.k");
    // TODO - should look for something here.

    runQuery("INSERT OVERWRITE TABLE DEST1 SELECT * FROM T1 a RIGHT OUTER JOIN T2 b ON a.k = b.k");

    runQuery("SELECT * FROM DEST1 ORDER BY k1, k2, val1, val2");
  }

  @Test
  public void skewjoin_union_remove_2() throws Exception {
    set("hive.optimize.skewjoin.compiletime", true);
    set("hive.mapred.supports.subdirectories", true);

    set("hive.stats.autogather", false);
    set("hive.optimize.union.remove", true);

    set("hive.merge.mapfiles", false);
    set("hive.merge.mapredfiles", false);
    set("hive.merge.sparkfiles", false);
    set("mapred.input.dir.recursive", true);
    RandomDataGenerator generator = new RandomDataGenerator(37);

    runQuery("drop table if exists T1");
    runQuery("CREATE TABLE T1(k STRING, val STRING) SKEWED BY (k) ON ((2), (8))");
    TestTable t1 = TestTable.fromHiveMetastore("default", "T1");
    t1.populate(generator);

    runQuery("drop table if exists T2");
    runQuery("CREATE TABLE T2(k STRING, val STRING) SKEWED BY (k) ON ((3), (8))");
    TestTable t2 = TestTable.fromHiveMetastore("default", "T2");
    t2.populate(generator);

    runQuery("drop table if exists T3");
    runQuery("CREATE TABLE T3(k STRING, val STRING)");
    TestTable t3 = TestTable.fromHiveMetastore("default", "T3");
    t3.populate(generator);

    // This is to test the union->selectstar->filesink and skewjoin optimization
   // Union of 3 map-reduce subqueries is performed for the skew join
   // There is no need to write the temporary results of the sub-queries, and then read them
   // again to process the union. The union can be removed completely.
    // INCLUDE_HADOOP_MAJOR_VERSIONS(0.23)
   // Since this test creates sub-directories for the output table, it might be easier
   // to run the test only on hadoop 23

    explain("SELECT a.*, b.*, c.* FROM T1 a JOIN T2 b ON a.k = b.k JOIN T3 c on a.k = c.k");
    // TODO - should look at something here

    set("hive.input.format", "org.apache.hadoop.hive.ql.io.HiveInputFormat");

    runQuery("SELECT a.*, b.*, c.* FROM T1 a JOIN T2 b ON a.k = b.k JOIN T3 c on a.k = c.k " +
        "ORDER BY a.k, b.k, c.k, a.val, b.val, c.val");
  }
}
