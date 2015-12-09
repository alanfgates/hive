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

import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class TestPostgresTranslator {
  private SQLTranslator translator;

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Before
  public void getTranslator() {
    translator = new PostgresStore().getTranslator();
  }

  @Test
  public void unrecognized() throws Exception {
    thrown.expect(TranslationException.class);
    translator.translate("bla bla bla");
  }

  @Test
  public void createDatabase() throws Exception {
    Assert.assertEquals("create schema add_part_test_db",
        translator.translate("CREATE DATABASE add_part_test_db"));
    Assert.assertFalse(translator.isFailureOk());
    Assert.assertEquals("create schema newdb",
        translator.translate("create database newDB location \"/tmp/\""));
    Assert.assertEquals("create schema if not exists dummydb",
        translator.translate("create database if not exists dummydb"));
    Assert.assertEquals("create schema if not exists test_db",
        translator.translate("CREATE DATABASE IF NOT EXISTS test_db COMMENT 'Hive test database'"));
    Assert.assertFalse(translator.isFailureOk());
    Assert.assertEquals("create schema test_db",
        translator.translate("CREATE DATABASE test_db COMMENT 'Hive test database'"));
    Assert.assertEquals("create schema db2",
        translator.translate("create database db2 with dbproperties (\n" +
          "'mapred.jobtracker.url'='http://my.jobtracker.com:53000',\n" +
          "'hive.warehouse.dir' = '/user/hive/warehouse',\n" +
          "'mapred.scratch.dir' = 'hdfs://tmp.dfs.com:50029/tmp')"));
    Assert.assertEquals("create schema test_db",
        translator.translate(
            "create database test_db with dbproperties ('key1' = 'value1', 'key2' = 'value2')"));
    Assert.assertEquals("create schema if not exists jsondb1",
        translator.translate("CREATE DATABASE IF NOT EXISTS jsondb1 COMMENT 'Test database' " +
            "LOCATION '${hiveconf:hive.metastore.warehouse.dir}/jsondb1' WITH DBPROPERTIES ('id' = 'jsondb1')"));
    Assert.assertEquals("create schema some_database",
        translator.translate("CREATE DATABASE some_database comment 'for show create db test' WITH DBPROPERTIES ('somekey'='somevalue')"));
    Assert.assertEquals("create schema \"name with a space\"",
        translator.translate("CREATE DATABASE `name with a space` location somewhere"));

    Assert.assertEquals("create schema add_part_test_db",
        translator.translate("CREATE SCHEMA add_part_test_db"));
    Assert.assertEquals("create schema newdb",
        translator.translate("create schema newDB location \"/tmp/\""));
    Assert.assertEquals("create schema if not exists dummydb",
        translator.translate("create schema if not exists dummydb"));
    Assert.assertEquals("create schema if not exists test_db",
        translator.translate("CREATE SCHEMA IF NOT EXISTS test_db COMMENT 'Hive test database'"));
    Assert.assertEquals("create schema test_db",
        translator.translate("CREATE SCHEMA test_db COMMENT 'Hive test database'"));
    Assert.assertEquals("create schema db2",
        translator.translate("create schema db2 with dbproperties (\n" +
            "'mapred.jobtracker.url'='http://my.jobtracker.com:53000'\n" +
            "'hive.warehouse.dir' = '/user/hive/warehouse'\n" +
            "'mapred.scratch.dir' = 'hdfs://tmp.dfs.com:50029/tmp')"));
    Assert.assertEquals("create schema test_db",
        translator.translate("create schema test_db with dbproperties ('key1' = 'value1', 'key2' " +
            "= 'value2')"));
    Assert.assertEquals("create schema if not exists jsondb1",
        translator.translate("CREATE SCHEMA IF NOT EXISTS jsondb1 COMMENT 'Test database' " +
            "LOCATION '${hiveconf:hive.metastore.warehouse.dir}/jsondb1' WITH DBPROPERTIES ('id' = 'jsondb1')"));
    Assert.assertEquals("create schema some_database",
        translator.translate("CREATE SCHEMA some_database comment 'for show create db test' WITH " +
            "DBPROPERTIES ('somekey'='somevalue')"));
    Assert.assertEquals("create schema \"name with a space\"",
        translator.translate("CREATE SCHEMA `name with a space`"));
  }

  @Test
  public void dropDatabase() throws Exception {
    Assert.assertEquals("drop schema add_part_test_db",
        translator.translate("DROP DATABASE add_part_test_db"));
    Assert.assertEquals("drop schema if exists statsdb1",
        translator.translate("drop database if exists statsdb1"));
    Assert.assertEquals("drop schema to_drop_db1 cascade",
        translator.translate("DROP DATABASE to_drop_db1 CASCADE"));
    Assert.assertEquals("drop schema if exists non_exists_db3 restrict",
        translator.translate("DROP DATABASE IF EXISTS non_exists_db3 RESTRICT"));
    Assert.assertEquals("drop schema to_drop_db4 restrict",
        translator.translate("DROP DATABASE to_drop_db4 RESTRICT"));

    Assert.assertEquals("drop schema add_part_test_db",
        translator.translate("DROP SCHEMA add_part_test_db"));
    Assert.assertEquals("drop schema if exists statsdb1",
        translator.translate("drop schema if exists statsdb1"));
    Assert.assertEquals("drop schema to_drop_db1 cascade",
        translator.translate("DROP SCHEMA to_drop_db1 CASCADE"));
    Assert.assertEquals("drop schema if exists non_exists_db3 restrict",
        translator.translate("DROP SCHEMA IF EXISTS non_exists_db3 RESTRICT"));
    Assert.assertEquals("drop schema to_drop_db4 restrict",
        translator.translate("DROP SCHEMA to_drop_db4 RESTRICT"));
  }

  @Test
  public void createTableLike() throws Exception {
    Assert.assertEquals("create table alter3_like like alter3",
        translator.translate("create table alter3_like like alter3"));
    Assert.assertEquals("create table if not exists emp_orc like emp_staging",
        translator.translate("create table if not exists emp_orc like emp_staging"));
    Assert.assertEquals("create table source.srcpart like default.srcpart",
        translator.translate("create table source.srcpart like default.srcpart;"));
    Assert.assertFalse(translator.isFailureOk());
  }

  @Test
  public void createTableAs() throws Exception {
    Assert.assertEquals("create table src_stat as select * from src1",
        translator.translate("create table src_stat as select * from src1"));
    Assert.assertEquals("create table dest_grouped_old1 as select 1+1, " +
            "2+2 as zz, src.key, src.value, count(src.value), count(src.value)" +
            ", count(src.value), sum(value) from src group by src.key",
        translator.translate("create table dest_grouped_old1 as select 1+1, " +
            "2+2 as zz, src.key, src.value, count(src.value), count(src.value)" +
            ", count(src.value), SUM(value) from src group by src.key"));
  }

  @Test
  public void createTableWithCols() throws Exception {
    Assert.assertEquals("create table acidjoin1 (name varchar(50), age int)",
        translator.translate("create table acidjoin1(name varchar(50), age int) clustered by " +
            "(age) into 2 buckets stored as orc TBLPROPERTIES (\"transactional\"=\"true\")"));
    Assert.assertEquals("create table alter1 (a int, b int)",
        translator.translate("create table alter1(a int, b int)"));
    Assert.assertEquals("create table alter2 (a int, b int)",
        translator.translate("create table alter2(a int, b int) partitioned by (insertdate string)"));
    Assert.assertEquals("create table alter3_src ( col1 varchar(255) )",
        translator.translate("create table alter3_src ( col1 string ) stored as textfile "));
    Assert.assertEquals("create table alter3 ( col1 varchar(255) )",
        translator.translate("create table alter3 ( col1 string ) partitioned by (pcol1 string , " +
            "pcol2 string) stored as sequencefile"));
    Assert.assertEquals("create table ac.alter_char_1 (key varchar(255), value varchar(255))",
        translator.translate("create table ac.alter_char_1 (key string, value string)"));
    Assert.assertEquals("create table tst1 (key varchar(255), value varchar(255))",
        translator.translate("create table tst1(key string, value string) partitioned by (ds " +
            "string) clustered by (key) into 10 buckets"));
    Assert.assertEquals("create table over1k ( t smallint, si smallint, i int, b bigint, f real, " +
            "d double precision, bo boolean, s varchar(255), ts timestamp, dec decimal(4,2), bin " +
            "blob)",
        translator.translate("create table over1k( t tinyint, si smallint, i int, b bigint, f " +
            "float, d double, bo boolean, s string, ts timestamp, dec decimal(4,2), bin binary) " +
            "ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' STORED AS TEXTFILE"));
    Assert.assertEquals("create table if not exists loc_staging (state varchar(255),locid int,zip" +
            " bigint,year int )",
        translator.translate("create table if not exists loc_staging (state string,locid int,zip " +
            "bigint,year int ) row format delimited fields terminated by '|' stored as textfile"));
    Assert.assertEquals("create temporary table acid_dtt (a int, b varchar(128))",
        translator.translate("create temporary table acid_dtt(a int, b varchar(128)) clustered by" +
            " (a) into 2 buckets stored as orc TBLPROPERTIES ('transactional'='true')"));
    Assert.assertEquals("create table roottable (key varchar(255))",
        translator.translate("create external table roottable (key string) row format delimited " +
            "fields terminated by '\\t' stored as textfile"));
  }

  @Test
  public void dropTable() throws Exception {
    Assert.assertEquals("drop table t", translator.translate("drop table t"));
    Assert.assertEquals("drop table if exists t", translator.translate("drop table if exists t"));
    Assert.assertFalse(translator.isFailureOk());
    Assert.assertEquals("drop table db.t", translator.translate("drop table db.t"));
    Assert.assertEquals("drop table t", translator.translate("drop table t purge"));
  }

  @Test
  public void alterTable() throws Exception {
    Assert.assertEquals("alter table tab1 rename to tab2", translator.translate("alter table tab1 rename to tab2"));
    Assert.assertEquals("", translator.translate("alter table test set fileformat orc"));
    Assert.assertEquals("",
        translator.translate("alter table tst1 clustered by (key) into 8 buckets"));
    Assert.assertEquals("", translator.translate("alter table fact_daily skewed by (key, value) on (('484','val_484'),('238','val_238')) stored as DIRECTORIES"));
    Assert.assertEquals("", translator.translate("alter table skew_test.original3 not skewed"));
    Assert.assertEquals("", translator.translate("alter table stored_as_dirs_multiple not stored as DIRECTORIES"));
    Assert.assertEquals("", translator.translate("alter table T1 add partition (ds = 'today')"));
    Assert.assertEquals("", translator.translate("alter table temp add if not exists partition (p ='p1')"));

  }

  @Test
  public void selectSimple() throws Exception {
    Assert.assertEquals("select * from add_part_test",
        translator.translate("select * from add_part_test"));
    Assert.assertEquals("select key, value from dest1",
        translator.translate("select key, value from dest1"));
    Assert.assertEquals("select count(key) from src",
        translator.translate("select count(key) from src"));
    Assert.assertEquals("select count(key), sum(key) from src",
        translator.translate("select count(key), sum(key) from src"));
    Assert.assertEquals("select sum(sin(key)), sum(cos(value)) from src_rc_concatenate_test",
        translator.translate("select sum(sin(key)), sum(cos(value)) from src_rc_concatenate_test"));
    Assert.assertEquals("select cast(key as int) / cast(key as varchar(255)) from src",
        translator.translate("select cast(key as int) / cast(key as string) from src"));
    Assert.assertEquals("select 1", translator.translate("select 1"));
    Assert.assertEquals("select distinct l_partkey as p_partkey from lineitem",
        translator.translate("select distinct l_partkey as p_partkey from   Lineitem"));
    Assert.assertEquals("select all l_partkey as p_partkey from lineitem",
        translator.translate("select all l_partkey as p_partkey from   Lineitem"));
  }

  @Ignore
  public void selectInterval() throws Exception {
    thrown.expect(TranslationException.class);
    thrown.expectMessage("Could not translate interval, Hive SQL:");
    translator.translate(
        "select   interval '10-11' year to month,   interval '10' year,   interval '11' month from src limit 1");
  }

  @Ignore
  public void selectConstantCasts() throws Exception {
    Assert.assertEquals("select dateval - date '1999-06-07' from interval_arithmetic_1",
        translator.translate("select dateval - date '1999-06-07' from interval_arithmetic_1"));
    Assert.assertEquals("select dateval - date '1999-06-07' from interval_arithmetic_1",
        translator.translate("select dateval - date '1999-6-7' from interval_arithmetic_1"));
    Assert.assertEquals("select timestamp '1999-01-01 01:00:00' from interval_arithmetic_1",
        translator.translate("select timestamp '1999-01-01 01:00:00' from interval_arithmetic_1"));
    Assert.assertEquals("select timestamp '1999-01-01 01:00:00' from interval_arithmetic_1",
        translator.translate("select timestamp '1999-1-1 01:00:00' from interval_arithmetic_1"));
    Assert.assertEquals("select 101, -101, 100, -100, 100.00 from t",
        translator.translate("select 101Y, -101S, 100, -100L, 100.00BD from T"));

  }

  @Test
  public void selectJoin() throws Exception {
    Assert.assertEquals("select s.name, count(distinct registration) from studenttab10k s join " +
        "votertab10k v on (s.name = v.name) group by s.name",
        translator.translate("select s.name, count(distinct registration) from studenttab10k s " +
            "join votertab10k v on (s.name = v.name) group by s.name"));
    Assert.assertEquals("select count(*) from bucket_small a join bucket_big b on a.key = b.key",
        translator.translate("select /*+ mapjoin(a) */ count(*) FROM bucket_small a JOIN bucket_big b ON a.key = b.key"));
    Assert.assertEquals("select count(*) from tbl1 a left outer join tbl2 b on a.key = b.key",
        translator.translate("select count(*) FROM tbl1 a LEFT OUTER JOIN tbl2 b ON a.key = b.key"));

  }

  @Test
  public void selectFromSubquery() throws Exception {
    Assert.assertEquals("select a, b from t", translator.translate("select a, b from default.t"));
    Assert.assertEquals("select count(*) from (select a.key as key, a.value as val1, b.value " +
        "as val2 from tbl1 a join tbl2 b on a.key = b.key) subq1",
        translator.translate("select count(*) from (   select a.key as key, a.value as val1, b" +
            ".value as val2 from tbl1 a join tbl2 b on a.key = b.key ) subq1"));
    Assert.assertEquals("select count(*) from (select key, count(*) from (select a.key " +
        "as key, a.value as val1, b.value as val2 from tbl1 a join tbl2 b on a.key = b.key) " +
        "subq1 group by key) subq2",
        translator.translate("select count(*) from (   select key, count(*) from   (     select a" +
            ".key as key, a.value as val1, b.value as val2 from tbl1 a join tbl2 b on a.key = b" +
            ".key   ) subq1   group by key ) subq2"));
    Assert.assertEquals("select sum(subq.key) from (select a.key, a.value from " +
        "src a where a.key > 10) subq join src tab on (subq.key = tab.key and subq.key > 20 and " +
        "subq.value = tab.value) where tab.value < 200",
        translator.translate("SELECT sum(subq.key) FROM (select a.key, a.value " +
            "from src a where a.key > 10 ) subq JOIN src tab ON (subq.key = tab.key and subq.key " +
            "> 20 and subq.value = tab.value) where tab.value < 200"));
    Assert.assertEquals("select s1.k, count(*) from (select k from t) s1 join (select k from u) " +
            "on s1.k = s2.k s2 group by s1.k",
        translator.translate("select s1.k, count(*) from ( select k from t ) s1 join ( select k " +
            "from u ) on s1.k = s2.k s2 group by s1.k"));
    Assert.assertEquals("select src1.key, src1.cnt1, src2.cnt1 from (select key, count(*) as " +
        "cnt1 from (select a.key as key, a.value as val1, b.value as val2 from tbl1 a join" +
        " tbl2 b on a.key = b.key) subq1 group by key) src1 join (select key, count(*) as " +
        "cnt1 from (select a.key as key, a.value as val1, b.value as val2 from tbl1 a join" +
        " tbl2 b on a.key = b.key) subq2 group by key) src2 on src1.key = src2.key",
        translator.translate("select src1.key, src1.cnt1, src2.cnt1 from (   select key, count(*)" +
            " as cnt1 from   (     select a.key as key, a.value as val1, b.value as val2 from " +
            "tbl1 a join tbl2 b on a.key = b.key   ) subq1 group by key ) src1 join (   select " +
            "key, count(*) as cnt1 from   (     select a.key as key, a.value as val1, b.value as " +
            "val2 from tbl1 a join tbl2 b on a.key = b.key   ) subq2 group by key ) src2 on src1" +
            ".key = src2.key"));
  }

  @Test
  public void selectWhere() throws Exception {
    Assert.assertEquals("select * from alter5 where dt='a'",
        translator.translate("select * from alter5 where dt='a'"));
    Assert.assertEquals("select hr, c1, length(c1) from alter_char2 where hr = 1",
        translator.translate("select hr, c1, length(c1) from alter_char2 where hr = 1"));
    Assert.assertEquals("select key, value, count(*) from src_cbo b where b.key in (select key " +
            "from src_cbo where src_cbo.key > '8') group by key, value order by key",
        translator.translate("select key, value, count(*) from src_cbo b where b.key in ( select " +
            "key from src_cbo where src_cbo.key > '8' ) group by key, value  order by key"));
  }

  @Test
  public void selectGroupBy() throws Exception {
    Assert.assertEquals("select c1, count(*) from tmp1 group by c1",
        translator.translate("select c1, count(*) from tmp1 group by c1"));
    Assert.assertEquals("select k, count(*) from b group by k having count(*) > 100",
        translator.translate("select k, count(*) from b group by k having count(*) >   100"));
    Assert.assertEquals("select * from src_cbo b group by key, value having not exists (select a" +
            ".key from src_cbo a where b.value = a.value and a.key = b.key and a.value > 'val_12')",
        translator.translate("select * from src_cbo b group by key, value having not exists   ( " +
            "select a.key   from src_cbo a   where b.value = a.value  and a.key = b.key and a" +
            ".value > 'val_12'   )"));
    Assert.assertEquals("select key, value, count(*) from src_cbo b where b.key in (select key " +
            "from src_cbo where src_cbo.key > '8') group by key, value having count(*) in (select" +
            " count(*) from src_cbo s1 where s1.key > '9' group by s1.key) order by key",
        translator.translate("select key, value, count(*) from src_cbo b where b.key in (select " +
            "key from src_cbo where src_cbo.key > '8') group by key, value having count(*) in " +
            "(select count(*) from src_cbo s1 where s1.key > '9' group by s1.key ) order by key"));

  }

  @Test
  public void selectOrderBy() throws Exception {
    Assert.assertEquals("select a, b from acid_vectorized order by a, b",
        translator.translate("select a, b from acid_vectorized order by a, b"));
    Assert.assertEquals("select c1, count(*) from tmp1 group by c1 order by c1",
        translator.translate("select c1, count(*) from tmp1 group by c1 order by c1"));
  }

  @Test
  public void selectLimit() throws Exception {
    Assert.assertEquals("select key from src_autho_test order by key limit 20",
        translator.translate("select key from src_autho_test order by key limit 20"));
  }

  @Test
  public void selectUnion() throws Exception {
    Assert.assertEquals("select key, value from u1 union all select key, value from u2",
        translator.translate("select key, value from u1 union all select key, value FROM u2"));
    Assert.assertEquals("select key, value from u1 union distinct select key, value from u2",
        translator.translate("select key, value from u1 union distinct select key, value FROM u2"));
    Assert.assertEquals("select key, value from u1 union all select key, value from u2 union all select key as key, value from u",
        translator.translate("select key, value from u1 union all select key, value from u2 union all select key as key, value FROM u"));
    Assert.assertEquals("select key from src1 union select key2 from src2 order by key",
        translator.translate("select key from src1 union select key2 from src2 order BY key"));
    Assert.assertEquals("select key from src1 union select key2 from src2 order by key limit 5",
        translator.translate("select key from src1 union select key2 from src2 order BY key limit 5"));
  }

  @Test
  public void insert() throws Exception {
    Assert.assertEquals(
        "insert into acidjoin1 values ('aaa', 35), ('bbb', 32), ('ccc', 32), ('ddd', 35), ('eee', 32)",
        translator.translate(
            "insert into table acidjoin1 values ('aaa', 35), ('bbb', 32), ('ccc', 32), ('ddd', 35), ('eee', 32)"));
    Assert.assertEquals(
        "insert into acid_vectorized select cint, cstring1 from alltypesorc where cint is not null order by cint limit 10",
        translator.translate(
            "insert into table acid_vectorized select cint, cstring1 from alltypesorc where cint is not null order by cint limit 10"));
    Assert.assertEquals(
        "insert into ac.alter_char_1 select key, value from src order by key limit 5",
        translator.translate(
            "insert overwrite table ac.alter_char_1   select key, value from src order by key limit 5"));
    Assert.assertEquals("insert into acid values(\"foo\", \"bar\")",
        translator.translate(
            "insert into table acid partition(ds='2008-04-08') values(\"foo\", \"bar\")"));
    Assert.assertEquals("insert into acid select key,value,ds from srcpart",
        translator.translate(
            "insert into table acid partition(ds)  select key,value,ds from srcpart"));
    Assert.assertEquals("insert into tab_part select key,value from srcbucket_mapjoin_part",
        translator.translate(
            "insert overwrite table tab_part partition (ds='2008-04-08') select key,value from srcbucket_mapjoin_part"));
  }

  @Test
  public void update() throws Exception {
    Assert.assertEquals("update t_auth_up set j = 0 where i > 0",
        translator.translate("update t_auth_up set j = 0 where i > 0"));
    Assert.assertEquals("update acid set value = 'bar'",
        translator.translate("update acid set value = 'bar'"));
  }

  @Test
  public void delete() throws Exception {
    Assert.assertEquals("delete from acid_iud",
        translator.translate("delete from acid_iud"));
    Assert.assertEquals("delete from acid where key = 'foo' and ds='2008-04-08'",
        translator.translate("delete from acid where key = 'foo' and ds='2008-04-08'"));
  }

  @Test
  public void nullTranslator() throws Exception {
    Assert.assertEquals("", translator.translate("show tables"));
    Assert.assertEquals("", translator.translate("describe t"));
    Assert.assertEquals("", translator.translate("explain select * from t"));
    Assert.assertEquals("", translator.translate("analyze table src_rc_merge_test_stat compute statistics"));
    Assert.assertEquals("", translator.translate("grant select on table src_auth_tmp to user hive_test_user"));
    Assert.assertEquals("", translator.translate("revoke select on table src_autho_test from user hive_test_user"));
    Assert.assertEquals("", translator.translate("create index t1_index on table t1(a) as 'COMPACT' WITH DEFERRED REBUILD"));
    Assert.assertEquals("", translator.translate("alter index t1_index on t1 rebuild"));
    Assert.assertEquals("", translator.translate("drop index src_index_2 on src"));
    Assert.assertEquals("", translator.translate("create role role1"));
    Assert.assertEquals("", translator.translate("drop role sRc_roLE"));
    Assert.assertEquals("", translator.translate("set role ADMIN"));
    Assert.assertEquals("", translator.translate("alter database db_alter_onr set owner user user1"));
    Assert.assertEquals("", translator.translate("alter schema db_alter_onr set owner user user1"));
  }

  @Test
  public void createFunction() throws Exception {
    thrown.expect(TranslationException.class);
    thrown.expectMessage("Could not translate create function, Hive SQL:");
    translator.translate(
        "create function lookup as 'org.apache.hadoop.hive.ql.udf.UDFFileLookup' using file 'hdfs:///tmp/udf_using/sales.txt'");
  }

  @Test
  public void createTemporaryFunction() throws Exception {
    thrown.expect(TranslationException.class);
    thrown.expectMessage("Could not translate create function, Hive SQL:");
    translator.translate(
        "create temporary function udtfCount2 as 'org.apache.hadoop.hive.contrib.udtf.example.GenericUDTFCount2'");
  }

  @Test
  public void reloadFunction() throws Exception {
    thrown.expect(TranslationException.class);
    thrown.expectMessage("Could not translate reload function, Hive SQL:");
    translator.translate("reload function");
  }

  @Test
  public void dropFunction() throws Exception {
    thrown.expect(TranslationException.class);
    thrown.expectMessage("Could not translate drop function, Hive SQL:");
    translator.translate("drop function perm_fn");
  }

  @Test
  public void dropTemporaryFunction() throws Exception {
    thrown.expect(TranslationException.class);
    thrown.expectMessage("Could not translate drop function, Hive SQL:");
    translator.translate("drop temporary function matchpathtest");
  }

  @Test
  public void quoting() throws Exception {
    Assert.assertEquals("select * from t",
        translator.translate("select * from t"));
    Assert.assertEquals("select 'select from where' from t",
        translator.translate("select 'select from where' from T"));
    Assert.assertEquals("select \"select from where\" from \"table with a space\"",
        translator.translate("select \"select from where\" from `table with a space`"));
    Assert.assertEquals("select 'escaped ''quote' from t",
        translator.translate("select 'escaped ''quote' from t"));
    Assert.assertEquals("select 'ends on quote'",
        translator.translate("select 'ends on quote'"));
    Assert.assertEquals("select 'ends on escaped quote'''",
        translator.translate("select 'ends on escaped quote'''"));
  }

}
