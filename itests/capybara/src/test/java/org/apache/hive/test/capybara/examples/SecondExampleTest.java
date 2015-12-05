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

import org.apache.hive.test.capybara.IntegrationTest;
import org.apache.hive.test.capybara.TableTool;
import org.apache.hive.test.capybara.iface.TestTable;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * More example tests.
 */
public class SecondExampleTest extends IntegrationTest {
  static final private Logger LOG = LoggerFactory.getLogger(SecondExampleTest.class);

  @Test
  public void insert() throws Exception {
    TableTool.createAllTypes();

    TestTable target = TestTable.getBuilder("INSERT_EXAMPLE")
        .addCol("cstring", "varchar(120)")
        .addCol("cbool", "boolean")
        .addCol("clong", "bigint")
        .addCol("cfloat", "float")
        .addCol("cint", "int")
        .build();
    target.createTargetTable();
    runQuery("insert into INSERT_EXAMPLE select cvarchar, cboolean, cbigint, cfloat, cint " +
        "from alltypes");
    tableCompare(target);
  }
}
