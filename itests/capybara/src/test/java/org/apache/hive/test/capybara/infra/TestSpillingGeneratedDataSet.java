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

import org.apache.hive.test.capybara.iface.DataGenerator;
import org.apache.hive.test.capybara.IntegrationTest;
import org.apache.hive.test.capybara.iface.TestTable;
import org.junit.Test;

public class TestSpillingGeneratedDataSet extends IntegrationTest {

  @Test
  public void spill() throws Exception {

    System.setProperty(TestConf.SPILL_SIZE_PROPERTY, Integer.toString(1024 * 128));
    TestTable table = TestTable.getBuilder("spill")
        .addCol("c1", "varchar(120)")
        .addCol("c2", "int")
        .addCol("c3", "decimal(12,2)")
        .build();

    DataGenerator generator = new RandomDataGenerator(23);
    table.create();
    table.populate(generator, 500, null);

    runQuery("select count(*) from spill");
    compare();
  }

}
