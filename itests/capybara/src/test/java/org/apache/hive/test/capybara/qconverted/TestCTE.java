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
import org.apache.hive.test.capybara.iface.TestTable;
import org.junit.Test;

public class TestCTE extends IntegrationTest {

  @Test
  public void singleCte() throws Exception {
    TableTool.createCapySrc();

    runQuery("with q1 as ( select k from capysrc) " +
        "select * from q1");
    sortAndCompare();
  }

  @Test
  public void multipleCte() throws Exception {
    TableTool.createPseudoTpch();

    runQuery("with q1 as (select l_lineitem, l_orderkey from ph_lineitem), " +
        "q2 as (select o_orderkey, o_custkey from o_custkey) " +
        "select l_lineitem, o_custkey from q1 join q2 on (q1.l_orderkey = q2.o_orderkey)");
    sortAndCompare();
  }

  @Test
  public void subquery() throws Exception {
    TableTool.createCapySrc();

    runQuery("with q1 as ( select k from capysrc ) " +
        "select * from (select k from q1) a");
    sortAndCompare();
  }

  @Test
  public void union() throws Exception {
    TableTool.createCapySrc();

    runQuery("with q1 as (select k from capysrc ), " +
        "q2 as (select k from capysrc s2 ) " +
        "select * from q1 union all select * from q2");
    sortAndCompare();
  }

  @Test
  public void insert() throws Exception {
    TableTool.createCapySrc();
    TestTable tt = TestTable.getBuilder("cte_insert")
        .addCol("k", "varchar(255)")
        .build();
    tt.create();

    runQuery("with q1 as ( select k from capysrc) " +
        "insert into cte_insert select * from q1");
    sortAndCompare();
  }

  @Test
  public void ctas() throws Exception {
    TableTool.createCapySrc();

    runQuery("create temporary table cte_ctas as " +
        "with q1 as ( select k from capysrc) " +
        "select * from q1");
    runQuery("select * from cte_ctas");
    sortAndCompare();
  }

  @Test
  public void view() throws Exception {
    TableTool.createCapySrc();

    try {
      runQuery("create view cte_view as " +
          "with q1 as ( select k from capysrc) " +
          "select * from q1");
      runQuery("select * from cte_view");
      sortAndCompare();
    } finally {
      runQuery("drop view cte_view");
    }
  }



}
