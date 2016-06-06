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
package org.apache.hive.test.capybara.examples;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hive.test.capybara.IntegrationTest;
import org.apache.hive.test.capybara.TableTool;
import org.apache.hive.test.capybara.annotations.AcidOn;
import org.apache.hive.test.capybara.annotations.NoParquet;
import org.apache.hive.test.capybara.annotations.NoRcFile;
import org.apache.hive.test.capybara.annotations.NoSpark;
import org.apache.hive.test.capybara.annotations.NoTextFile;
import org.junit.Test;

@NoSpark // These tests don't make sense when Spark is the engine
public class ExampleTest extends IntegrationTest {
  static final private Logger LOG = LoggerFactory.getLogger(ExampleTest.class);

  @Test
  public void simple() throws Exception {
    TableTool.createAllTypes();

    /*runQuery("select cvarchar, cchar, cstring, cint, cbigint, csmallint, ctinyint, " +
        "cfloat, cdecimal, cdate, ctimestamp, cboolean from alltypes");*/
    runQuery("select cvarchar from alltypes");
    sortAndCompare();
  }

  @Test
  public void countStar() throws Exception {
    TableTool.createAllTypes();

    runQuery("select count(*) from alltypes");
    sortAndCompare();
  }

  @Test
  public void groupBy() throws Exception {
    TableTool.createCapySrcPart();

    runQuery("select k, count(*) as cnt from capysrcpart where value is not null group by k order" +
        " by cnt, k");
    compare();
  }

  @Test
  public void simpleJoin() throws Exception {
    TableTool.createPseudoTpch();

    runQuery("select p_name, avg(l_price) from ph_lineitem join ph_part on (l_partkey = " +
        "p_partkey) group by p_name order by p_name");
    compare();
  }

  // TODO
  // * Make it work with HS2
  // * Make it work with security
  // * Make it work with HBase metastore
  // * Make work for multi-user
  // * Should scale move to M on the cluster instead of K?
  // * Add qfile translator.
  // * Add default scale (-1) to DataGenerator.generateData so people can set pctNull without
  // messing with the scale.
  // * Make is so the user can change the package for tests created by UserQueryGenerator
  // * Split up the infra package into interface and impl, it's getting too big and confusing.
  // Move DataGenerator from top to interface.
  // * Make older version of Hive work as benchmark

  // TODO - needs tested
  // * Test ability to generate data in parallel (on cluster) for large data
  // * Test ability to compare data in tables, for ETL type queries

  // FIXME
  // * Make decimal work with default precision and scale
  // * Make binary work with Derby
  // * We don't properly drop old records in the testtables when we discover a wrong version of
  // the table.
  // * We don't do anything to assure that joined user tables generate records that will join.
  // This is somewhat hard in that Hive statistics don't help us, but we may want to at least
  // detect the join conditions and do something to make sure we don't get null results.  In
  // particular we could infer pk/fk relationships in star schemas.

  @AcidOn // Turn acid on for this test (ie set the appropriate config values)
  @NoParquet @NoRcFile @NoTextFile
  @Test
  public void updateAllNonPartitioned() throws Exception {
    TableTool.createAllTypes();

    // Run a query.  Complain if it fails.
    runQuery("drop table if exists acid_uanp");
    runQuery("create table acid_uanp(a int, b varchar(128)) clustered by (a) into 2 buckets " +
        "stored as orc TBLPROPERTIES ('transactional'='true')");
    runQuery("insert into acid_uanp select cint, cast(cstring as varchar(128)) " +
            "from alltypes where cint < 0");
    runQuery("select a,b from acid_uanp order by a");
    compare();  // compare the results of the previous query against the source of truth.
    runQuery("update acid_uanp set b = 'fred'");
    runQuery("select a,b from acid_uanp");
    sortAndCompare();
  }


}
