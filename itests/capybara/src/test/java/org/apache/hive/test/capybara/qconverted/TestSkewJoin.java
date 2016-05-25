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
import org.apache.hive.test.capybara.TableTool;
import org.apache.hive.test.capybara.infra.RandomDataGenerator;
import org.apache.hive.test.capybara.iface.TestTable;
import org.junit.Ignore;
import org.junit.Test;

public class TestSkewJoin extends IntegrationTest {

  @Ignore // This needs some work
  public void skewjoin() throws Exception {
    setConfVarForOneTest("hive.explain.user", false);
    setConfVarForOneTest("hive.optimize.skewjoin", true);
    setConfVarForOneTest("hive.skewjoin.key", 2);

    // SORT_QUERY_RESULTS
    TestTable t1 = TestTable.getBuilder("SJ1")
        .addCol("k", "string")
        .addCol("val", "string")
        .build();
    t1.create();

    TestTable t2 = TestTable.getBuilder("SJ2")
        .addCol("k", "string")
        .addCol("val", "string")
        .build();
    t2.create();

    TestTable t3 = TestTable.getBuilder("SJ3")
        .addCol("k", "string")
        .addCol("val", "string")
        .build();
    t3.create();

    TestTable t4 = TestTable.getBuilder("SJ4")
        .addCol("k", "string")
        .addCol("val", "string")
        .build();
    t4.create();

    TestTable dest_j1 = TestTable.getBuilder("dest_j1")
        .addCol("k", "string")
        .addCol("value", "string")
        .build();
    dest_j1.create();

    RandomDataGenerator generator = new RandomDataGenerator(39);
    t1.populate(generator);
    t2.populate(generator);
    t3.populate(generator);
    t4.populate(generator);
    TableTool.createCapySrc();

    runQuery("INSERT OVERWRITE TABLE dest_j1 SELECT src1.k, src2.value FROM capysrc src1 JOIN " +
        "capysrc src2 ON (src1.k = src2.k)");
    runQuery("SELECT k, value FROM dest_j1");
    sortAndCompare();

    runQuery("SELECT /*+ STREAMTABLE(a) */ * FROM SJ1 a JOIN SJ2 b ON a.k = b.k JOIN SJ3 c ON b" +
        ".k = c.k JOIN SJ4 d ON c.k = d.k");
    assertEmpty();

    runQuery("SELECT /*+ STREAMTABLE(a,c) */ * FROM SJ1 a JOIN SJ2 b ON a.k = b.k JOIN SJ3 c ON " +
        "b.k = c.k JOIN SJ4 d ON c.k = d.k");
    sortAndCompare();

    runQuery("SELECT Y.k, Y.value FROM (SELECT * FROM capysrc) x JOIN (SELECT * FROM capysrc) Y" +
        " ON (x.k = Y.k)");
    sortAndCompare();

    runQuery("SELECT Y.k, Y.value FROM (SELECT * FROM capysrc) x JOIN (SELECT * FROM capysrc) Y" +
        " ON (x.k = Y.k and substring(x.value, 5)=substring(y.value, 5))");
    sortAndCompare();

    runQuery("SELECT src1.c1, src2.c4 FROM (SELECT k as c1, value as c2 from capysrc) src1 " +
        "JOIN (SELECT k as c3, value as c4 from capysrc) src2 ON src1.c1 = src2.c3 AND src1" +
        ".c1 < 100 JOIN (SELECT k as c5, value as c6 from capysrc) src3 ON src1.c1 = src3" +
        ".c5 AND src3.c5 < 80");
    sortAndCompare();

    runQuery("SELECT /*+ mapjoin(v)*/ k.k, v.val FROM SJ1 k LEFT OUTER JOIN SJ1 v ON k.k=v.k;");
    sortAndCompare();

    runQuery("select /*+ mapjoin(k)*/ k.k, v.val from SJ1 k join SJ1 v on k.k=v.val");
    sortAndCompare();

    runQuery("select /*+ mapjoin(k)*/ k.k, v.val from SJ1 k join SJ1 v on k.k=v.k");
    sortAndCompare();

    runQuery("select k.k, v.val from SJ1 k join SJ1 v on k.k=v.k");
    sortAndCompare();

    runQuery("select count(1) from  SJ1 a join SJ1 b on a.k = b.k");
    sortAndCompare();

    runQuery("SELECT a.k, a.val, c.k FROM SJ1 a LEFT OUTER JOIN SJ2 c ON c.k=a.k ");
    sortAndCompare();

    runQuery("SELECT /*+ STREAMTABLE(a) */ a.k, a.val, c.k FROM SJ1 a RIGHT OUTER JOIN SJ2 c ON" +
        " c.k=a.k ");
    sortAndCompare();

    runQuery("SELECT /*+ STREAMTABLE(a) */ a.k, a.val, c.k FROM SJ1 a FULL OUTER JOIN SJ2 c ON " +
        "c.k=a.k ");
    sortAndCompare();

    runQuery("SELECT src1.k, src1.val, src2.k FROM SJ1 src1 LEFT OUTER JOIN SJ2 src2 ON src1" +
        ".k = src2.k RIGHT OUTER JOIN SJ2 src3 ON src2.k = src3.k");
    sortAndCompare();

    runQuery("SELECT src1.k, src1.val, src2.k FROM SJ1 src1 JOIN SJ2 src2 ON src1.k+1 = src2" +
        ".k JOIN SJ2 src3 ON src2.k = src3.k");
    sortAndCompare();

    runQuery("select /*+ mapjoin(v)*/ k.k, v.val from SJ1 k left outer join SJ1 v on k.k=v.k");
    sortAndCompare();
  }

  @Test
  public void skewjoin_union_remove_1() throws Exception {
    setConfVarForOneTest("hive.optimize.skewjoin.compiletime", true);
    setConfVarForOneTest("hive.mapred.supports.subdirectories", true);

    setConfVarForOneTest("hive.stats.autogather", false);
    setConfVarForOneTest("hive.optimize.union.remove", true);

    setConfVarForOneTest("hive.merge.mapfiles", false);
    setConfVarForOneTest("hive.merge.mapredfiles", false);
    setConfVarForOneTest("hive.merge.sparkfiles", false);
    setConfVarForOneTest("mapred.input.dir.recursive", true);

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

    setConfVarForOneTest("hive.input.format", "org.apache.hadoop.hive.ql.io.HiveInputFormat");

    runQuery("SELECT * FROM T1 a JOIN T2 b ON a.k = b.k ORDER BY a.k, b.k, a.val, b.val");

    // test outer joins also

    runQuery(
        "SELECT a.*, b.* FROM T1 a RIGHT OUTER JOIN T2 b ON a.k = b.k ORDER BY a.k, b.k, a.val, b.val");

    runQuery("drop table if exists DEST1");
    runQuery("create table DEST1(k1 STRING, val1 STRING, k2 STRING, val2 STRING)");

    runQuery("INSERT INTO TABLE DEST1 SELECT * FROM T1 a JOIN T2 b ON a.k = b.k");

    runQuery("SELECT * FROM DEST1 ORDER BY k1, k2, val1, val2");

    runQuery("INSERT OVERWRITE TABLE DEST1 SELECT * FROM T1 a RIGHT OUTER JOIN T2 b ON a.k = b.k");

    runQuery("SELECT * FROM DEST1 ORDER BY k1, k2, val1, val2");
  }

  @Test
  public void skewjoin_union_remove_2() throws Exception {
    setConfVarForOneTest("hive.optimize.skewjoin.compiletime", true);
    setConfVarForOneTest("hive.mapred.supports.subdirectories", true);

    setConfVarForOneTest("hive.stats.autogather", false);
    setConfVarForOneTest("hive.optimize.union.remove", true);

    setConfVarForOneTest("hive.merge.mapfiles", false);
    setConfVarForOneTest("hive.merge.mapredfiles", false);
    setConfVarForOneTest("hive.merge.sparkfiles", false);
    setConfVarForOneTest("mapred.input.dir.recursive", true);
    RandomDataGenerator generator = new RandomDataGenerator(38);

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

    setConfVarForOneTest("hive.input.format", "org.apache.hadoop.hive.ql.io.HiveInputFormat");

    runQuery("SELECT a.*, b.*, c.* FROM T1 a JOIN T2 b ON a.k = b.k JOIN T3 c on a.k = c.k " +
        "ORDER BY a.k, b.k, c.k, a.val, b.val, c.val");
  }

  /*
   TODO:
ql/src/test/queries/clientpositive//skewjoin.q
ql/src/test/queries/clientpositive//skewjoin_mapjoin1.q
ql/src/test/queries/clientpositive//skewjoin_mapjoin10.q
ql/src/test/queries/clientpositive//skewjoin_mapjoin11.q
ql/src/test/queries/clientpositive//skewjoin_mapjoin2.q
ql/src/test/queries/clientpositive//skewjoin_mapjoin3.q
ql/src/test/queries/clientpositive//skewjoin_mapjoin4.q
ql/src/test/queries/clientpositive//skewjoin_mapjoin5.q
ql/src/test/queries/clientpositive//skewjoin_mapjoin6.q
ql/src/test/queries/clientpositive//skewjoin_mapjoin7.q
ql/src/test/queries/clientpositive//skewjoin_mapjoin8.q
ql/src/test/queries/clientpositive//skewjoin_mapjoin9.q
ql/src/test/queries/clientpositive//skewjoin_noskew.q
ql/src/test/queries/clientpositive//skewjoin_onesideskew.q
ql/src/test/queries/clientpositive//skewjoinopt1.q
ql/src/test/queries/clientpositive//skewjoinopt10.q
ql/src/test/queries/clientpositive//skewjoinopt11.q
ql/src/test/queries/clientpositive//skewjoinopt12.q
ql/src/test/queries/clientpositive//skewjoinopt13.q
ql/src/test/queries/clientpositive//skewjoinopt14.q
ql/src/test/queries/clientpositive//skewjoinopt15.q
ql/src/test/queries/clientpositive//skewjoinopt16.q
ql/src/test/queries/clientpositive//skewjoinopt17.q
ql/src/test/queries/clientpositive//skewjoinopt18.q
ql/src/test/queries/clientpositive//skewjoinopt19.q
ql/src/test/queries/clientpositive//skewjoinopt2.q
ql/src/test/queries/clientpositive//skewjoinopt20.q
ql/src/test/queries/clientpositive//skewjoinopt21.q
ql/src/test/queries/clientpositive//skewjoinopt3.q
ql/src/test/queries/clientpositive//skewjoinopt4.q
ql/src/test/queries/clientpositive//skewjoinopt5.q
ql/src/test/queries/clientpositive//skewjoinopt6.q
ql/src/test/queries/clientpositive//skewjoinopt7.q
ql/src/test/queries/clientpositive//skewjoinopt8.q
ql/src/test/queries/clientpositive//skewjoinopt9.q
   */
}
