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

public class TestDerbyTranslator {
  private DerbyStore store;
  private SQLTranslator translator;

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Before
  public void getTranslator() {
    store = new DerbyStore();
    translator = store.getTranslator();
  }

  @Test
  public void createDatabase() throws Exception {
    Assert.assertEquals("create schema add_part_test_db",
        translator.translate("CREATE DATABASE add_part_test_db"));
    Assert.assertFalse(translator.isFailureOk());
    Assert.assertEquals("create schema newdb",
        translator.translate("create database newDB location \"/tmp/\""));
    Assert.assertEquals("create schema dummydb",
        translator.translate("create database if not exists dummydb"));
    Assert.assertEquals("create schema test_db",
        translator.translate("CREATE DATABASE IF NOT EXISTS test_db COMMENT 'Hive test database'"));
    Assert.assertTrue(translator.isFailureOk());
    Assert.assertEquals("create schema test_db",
        translator.translate("CREATE DATABASE test_db COMMENT 'Hive test database'"));
    Assert.assertEquals("create schema db2",
        translator.translate("create database db2 with dbproperties (\n" +
            "'mapred.jobtracker.url'='http://my.jobtracker.com:53000',\n" +
            "'hive.warehouse.dir' = '/user/hive/warehouse',\n" +
            "'mapred.scratch.dir' = 'hdfs://tmp.dfs.com:50029/tmp')"));
    Assert.assertEquals("create schema test_db",
        translator.translate("create database test_db with dbproperties ('key1' = 'value1', 'key2' = 'value2')"));
    Assert.assertEquals("create schema jsondb1",
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
    Assert.assertEquals("create schema dummydb",
        translator.translate("create schema if not exists dummydb"));
    Assert.assertEquals("create schema test_db",
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
    Assert.assertEquals("create schema jsondb1",
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
    Assert.assertEquals("drop schema add_part_test_db restrict",
        translator.translate("DROP DATABASE add_part_test_db"));
    Assert.assertEquals("drop schema statsdb1 restrict",
        translator.translate("drop database if exists statsdb1"));
    Assert.assertEquals("drop schema to_drop_db1 restrict",
        translator.translate("DROP DATABASE to_drop_db1 CASCADE"));
    Assert.assertEquals("drop schema non_exists_db3 restrict",
        translator.translate("DROP DATABASE IF EXISTS non_exists_db3 RESTRICT"));
    Assert.assertEquals("drop schema to_drop_db4 restrict",
        translator.translate("DROP DATABASE to_drop_db4 RESTRICT"));

    Assert.assertEquals("drop schema add_part_test_db restrict",
        translator.translate("DROP SCHEMA add_part_test_db"));
    Assert.assertEquals("drop schema statsdb1 restrict",
        translator.translate("drop schema if exists statsdb1"));
    Assert.assertEquals("drop schema to_drop_db1 restrict",
        translator.translate("DROP SCHEMA to_drop_db1 CASCADE"));
    Assert.assertEquals("drop schema non_exists_db3 restrict",
        translator.translate("DROP SCHEMA IF EXISTS non_exists_db3 RESTRICT"));
    Assert.assertEquals("drop schema to_drop_db4 restrict",
        translator.translate("DROP SCHEMA to_drop_db4 RESTRICT"));
  }

  @Test
  public void createTableLike() throws Exception {
    Assert.assertEquals("create table alter3_like as select * from alter3",
        translator.translate("create table alter3_like like alter3"));
    Assert.assertEquals("create table emp_orc as select * from emp_staging",
        translator.translate("create table if not exists emp_orc like emp_staging"));
    Assert.assertTrue(translator.isFailureOk());
    Assert.assertEquals("create table source.srcpart as select * from default.srcpart",
        translator.translate("create table source.srcpart like default.srcpart;"));
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
    Assert.assertEquals("create table over1k ( t smallint, si smallint, i int, b bigint, f float, " +
            "d double, bo boolean, s varchar(255), ts timestamp, dec decimal(4,2), bin " +
            "blob)",
        translator.translate("create table over1k( t tinyint, si smallint, i int, b bigint, f " +
            "float, d double, bo boolean, s string, ts timestamp, dec decimal(4,2), bin binary) " +
            "ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' STORED AS TEXTFILE"));
    Assert.assertEquals("create table loc_staging (state varchar(255),locid int,zip bigint,year " +
            "int )",
        translator.translate("create table if not exists loc_staging (state string,locid int,zip " +
            "bigint,year int ) row format delimited fields terminated by '|' stored as textfile"));
    Assert.assertEquals("declare global temporary table acid_dtt (a int, b varchar(128))",
        translator.translate("create temporary table acid_dtt(a int, b varchar(128)) clustered by" +
            " (a) into 2 buckets stored as orc TBLPROPERTIES ('transactional'='true')"));
    Assert.assertEquals("create table roottable (key varchar(255))",
        translator.translate("create external table roottable (key string) row format delimited " +
            "fields terminated by '\\t' stored as textfile"));
  }

  @Test
  public void dropTable() throws Exception {
    Assert.assertEquals("drop table t", translator.translate("drop table t"));
    Assert.assertEquals("drop table t", translator.translate("drop table if exists t"));
    Assert.assertTrue(translator.isFailureOk());
    Assert.assertEquals("drop table db.t", translator.translate("drop table db.t"));
    Assert.assertEquals("drop table t", translator.translate("drop table t purge"));
  }

  @Test
  public void alterTable() throws Exception {
    thrown.expect(TranslationException.class);
    thrown.expectMessage("Could not translate alter table rename, Hive SQL:");
    translator.translate("alter table tab1 rename to tab2");
  }

  @Test
  public void selectLimit() throws Exception {
    Assert.assertEquals("select key from src_autho_test order by key",
        translator.translate("select key from src_autho_test order by key limit 20"));
    Assert.assertEquals(20, store.getLimit());
  }

  @Ignore
  public void constantCast() throws Exception {
    Assert.assertEquals("select dateval - '1999-06-07' from interval_arithmetic_1",
        translator.translate("select   dateval - date '1999-06-07' from interval_arithmetic_1"));
    Assert.assertEquals("select dateval - '1999-06-07' from interval_arithmetic_1",
        translator.translate("select   dateval - date '1999-6-7' from interval_arithmetic_1"));
    Assert.assertEquals("select '1999-01-01 01:00:00' from interval_arithmetic_1",
        translator.translate("select timestamp '1999-01-01 01:00:00' from interval_arithmetic_1"));
    Assert.assertEquals("select '1999-01-01 01:00:00' from interval_arithmetic_1",
        translator.translate("select timestamp '1999-1-1 01:00:00' from interval_arithmetic_1"));
  }
}
