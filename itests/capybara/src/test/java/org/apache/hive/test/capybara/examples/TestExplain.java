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
package org.apache.hive.test.capybara.examples;

import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.exec.mr.MapRedTask;
import org.apache.hive.test.capybara.Explain;
import org.apache.hive.test.capybara.IntegrationTest;
import org.apache.hive.test.capybara.TableTool;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class TestExplain extends IntegrationTest {

  @Test
  public void limitPushdown() throws Exception {
    // Set configuration values for this test
    set("hive.explain.user", false);
    set("hive.limit.pushdown.memory.usage", 0.3f);
    set("hive.optimize.reducededuplication.min.reducer", 1);

    // TableTool provides a set of standard table for testing.  This particular one gets the
    // src table, which has two string fields, k and value.
    TableTool.createCapySrc();

    runQuery("select k,value from capysrc order by k limit 20");

    // Explain fetches an Explain object for the query, which contains the QueryPlan.  It also
    // includes tools for validating the tree.
    Explain explain = explain();

    // Expect that somewhere in the plan is a TezTask.  Fetch that task.  This will assert if it
    // can't find the task.
    MapRedTask mrTask = explain.expect(MapRedTask.class);
    // Expect that somewhere in the TezTask there's a limit operator.  Fetch that operator.  This
    // will assert if it can't find the operator.
    TableScanOperator scan = explain.expect(mrTask, TableScanOperator.class);
    Assert.assertNotNull(scan);
    compare();
  }

  @Test
  public void noRunQuery() throws Exception {
    // TableTool provides a set of standard table for testing.  This particular one gets the
    // src table, which has two string fields, k and value.
    TableTool.createCapySrc();

    Explain explain = explain("select k,value from capysrc order by k");

    // Expect that somewhere in the plan is a TezTask.  Fetch that task.  This will assert if it
    // can't find the task.
    MapRedTask mrTask = explain.expect(MapRedTask.class);
    // Expect that somewhere in the MapRedTask there's a limit operator.  Fetch that operator.  This
    // will assert if it can't find the operator.
    List<TableScanOperator> scans = explain.findAll(mrTask, TableScanOperator.class);
    Assert.assertEquals(1, scans.size());
  }
}
