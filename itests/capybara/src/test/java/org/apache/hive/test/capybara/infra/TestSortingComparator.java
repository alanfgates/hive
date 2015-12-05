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

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hive.test.capybara.DataGenerator;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.ListIterator;
import java.util.Random;

public class TestSortingComparator {

  private TestTable table;
  private List<String> rows;
  private List<FieldSchema> cols;

  @Before
  public void createTable() {
    final String tableName = "alltypes";


    cols = Arrays.asList(
        new FieldSchema("col_bi", "bigint", ""),
        new FieldSchema("col_i", "int", "")
    );
    table = TestTable.getBuilder(tableName).setCols(cols).build();

    rows = new ArrayList<>();
    rows.add("6,2");
    rows.add("6,3");
    rows.add("-6,37");
  }

  @Test
  public void allGood() throws SQLException, IOException {
    DataGenerator gen1 = new StaticDataGenerator(rows, ",");
    DataSet hive = gen1.generateData(table);
    DataGenerator gen2 = new StaticDataGenerator(rows, ",");
    DataSet bench = gen2.generateData(table);
    ResultComparator comparator = new SortingComparator();
    comparator.compare(hive, bench);
  }

  @Test
  public void allGoodOrderDifferent() throws SQLException, IOException {
    DataGenerator gen1 = new StaticDataGenerator(rows, ",");
    DataSet hive = gen1.generateData(table);
    List<String> differentOrder = new ArrayList<>();
    differentOrder.add(rows.get(2));
    differentOrder.add(rows.get(1));
    differentOrder.add(rows.get(0));
    DataGenerator gen2 = new StaticDataGenerator(differentOrder, ",");
    DataSet bench = gen2.generateData(table);
    ResultComparator comparator = new SortingComparator();
    comparator.compare(hive, bench);
  }

  @Test
  public void diffNumberColumns() throws Exception {

    TestTable diffTable = TestTable.getBuilder("difftable")
        .addCol("col_bi", "bigint")
        .build();

    List<String> diffRows = new ArrayList<>();
    diffRows.add("6");
    diffRows.add("7");
    diffRows.add("6");
    DataGenerator gen1 = new StaticDataGenerator(rows, ",");
    DataSet hive = gen1.generateData(table);
    DataGenerator gen2 = new StaticDataGenerator(diffRows, ",");
    DataSet bench = gen2.generateData(diffTable);
    ResultComparator comparator = new SortingComparator();
    boolean sawError = false;
    try {
      comparator.compare(hive, bench);
    } catch (AssertionError e) {
      Assert.assertEquals("Different number of columns expected:<1> but was:<2>", e.getMessage());
      sawError = true;
    }
    Assert.assertTrue(sawError);
  }

  @Test
  public void uncompatibleDataTypes() throws Exception {

    TestTable diffTable = TestTable.getBuilder("difftable")
        .addCol("col_bi", "bigint")
        .addCol("col_i", "string")
        .build();

    List<String> diffRows = new ArrayList<>();
    diffRows.add("6,mary had a little lamb");
    diffRows.add("-6022141300000000000,def");
    DataGenerator gen1 = new StaticDataGenerator(rows, ",");
    DataSet hive = gen1.generateData(table);
    DataGenerator gen2 = new StaticDataGenerator(diffRows, ",");
    DataSet bench = gen2.generateData(diffTable);
    ResultComparator comparator = new SortingComparator();
    boolean sawError = false;
    try {
      comparator.compare(hive, bench);
    } catch (AssertionError e) {
      Assert.assertEquals("Found discrepency in metadata at column 1", e.getMessage());
      sawError = true;
    }
    Assert.assertTrue(sawError);
  }

  @Test
  public void diffNull() throws Exception {
    List<String> diffRows = new ArrayList<>();
    diffRows.add("NULL,2");
    diffRows.add("-6,37");
    diffRows.add("6,3");
    DataGenerator gen1 = new StaticDataGenerator(rows, ",");
    DataSet hive = gen1.generateData(table);
    DataGenerator gen2 = new StaticDataGenerator(diffRows, ",");
    DataSet bench = gen2.generateData(table);
    ResultComparator comparator = new SortingComparator();
    boolean sawError = false;
    try {
      comparator.compare(hive, bench);
    } catch (AssertionError e) {
      Assert.assertEquals("Mismatch at row 1 hive row is <-6,37> bench row is <NULL,2>",
          e.getMessage());
      sawError = true;
    }
    Assert.assertTrue(sawError);
  }

  @Test
  public void diff() throws Exception {
    List<String> diffRows = new ArrayList<>();
    diffRows.add("-6,37");
    diffRows.add("6,3");
    diffRows.add("6,3");
    DataGenerator gen1 = new StaticDataGenerator(rows, ",");
    DataSet hive = gen1.generateData(table);
    DataGenerator gen2 = new StaticDataGenerator(diffRows, ",");
    DataSet bench = gen2.generateData(table);
    ResultComparator comparator = new SortingComparator();
    boolean sawError = false;
    try {
      comparator.compare(hive, bench);
    } catch (AssertionError e) {
      Assert.assertEquals("Mismatch at row 2 hive row is <6,2> bench row is <6,3>",
          e.getMessage());
      sawError = true;
    }
    Assert.assertTrue(sawError);
  }

  @Test
  public void big() throws Exception {
    List<String> spillRows = new ArrayList<>();
    Random rand = new Random(1);
    for (int i = 0; i < 937; i++) {
      spillRows.add(Long.toString(rand.nextInt(10)) + "," + Long.toString(rand.nextInt(10)));
    }
    List<String> reverseRows = new ArrayList<>();
    ListIterator<String> iter = spillRows.listIterator(spillRows.size());
    while (iter.hasPrevious()) reverseRows.add(iter.previous());

    assert spillRows.size() == reverseRows.size();

    DataGenerator gen1 = new StaticDataGenerator(spillRows, ",");
    DataSet hive = gen1.generateData(table);
    DataGenerator gen2 = new StaticDataGenerator(reverseRows, ",");
    DataSet bench = gen2.generateData(table);
    SortingComparator comparator = new SortingComparator();
    comparator.compare(hive, bench);
  }

  @Test
  public void setSchema() throws Exception {
    DataGenerator gen1 = new StaticDataGenerator(rows, ",");
    DataSet bench = gen1.generateData(table);

    List<String> cliRows = new ArrayList<>();
    cliRows.add("-6,37");
    cliRows.add("6,2");
    cliRows.add("6,3");

    DataSet hive = new StringDataSet(cliRows, ",", "NULL");
    hive.setSchema(cols);
    ResultComparator comparator = new SortingComparator();
    comparator.compare(hive, bench);
  }
}
