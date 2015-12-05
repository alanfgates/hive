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

import org.apache.hive.test.capybara.data.DataSet;
import org.apache.hive.test.capybara.iface.ResultComparator;
import org.apache.hive.test.capybara.iface.TestTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hive.test.capybara.iface.DataGenerator;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class TestNonSortingComparator {

  static final private Logger LOG = LoggerFactory.getLogger(TestNonSortingComparator.class.getName());

  private TestTable table;
  private List<String> rows;
  private List<FieldSchema> cols;

  @Before
  public void createTable() {
    final String tableName = "alltypes";


    cols = Arrays.asList(
        new FieldSchema("col_bi", "bigint", ""),
        new FieldSchema("col_i", "int", ""),
        new FieldSchema("col_si", "smallint", ""),
        new FieldSchema("col_ti", "tinyint", ""),
        new FieldSchema("col_bin", "binary", ""),
        new FieldSchema("col_bool", "boolean", ""),
        new FieldSchema("col_ch", "char(8)", ""),
        new FieldSchema("col_vc", "varchar(89)", ""),
        new FieldSchema("col_str", "string", ""),
        new FieldSchema("col_date", "date", ""),
        new FieldSchema("col_dec", "decimal(10,2)", ""),
        new FieldSchema("col_fl", "float", ""),
        new FieldSchema("col_dbl", "double", ""),
        new FieldSchema("col_tm", "timestamp", "")
    );

    table = TestTable.getBuilder(tableName).setCols(cols).build();

    rows = new ArrayList<>();
    rows.add("-6022141300000000000,-299792458,-1432,-7,def,false,joe,mary had a little " +
        "lamb,her fleece was white as snow,2015-08-04,371.89,-1.234,-6.0221413E-23,NULL");
    rows.add("6,2,1432,7,abc,true,NULL,mary had a little lamb,her" +
        " fleece was white as snow,2015-08-04,371.89,1.234,6.0221413E23,2015-08-04 17:16:32");
   }

  @Test
  public void allGood() throws SQLException, IOException {
    DataGenerator gen1 = new StaticDataGenerator(rows, ",");
    DataSet hive = gen1.generateData(table);
    DataGenerator gen2 = new StaticDataGenerator(rows, ",");
    DataSet bench = gen2.generateData(table);
    ResultComparator comparator = new NonSortingComparator();
    comparator.compare(hive, bench);

  }

  @Test
  public void hiveMoreRows() throws SQLException, IOException {
    DataGenerator gen1 = new StaticDataGenerator(rows, ",");
    DataSet hive = gen1.generateData(table);
    DataGenerator gen2 = new StaticDataGenerator(rows.subList(0, 1), ",");
    DataSet bench = gen2.generateData(table);
    ResultComparator comparator = new NonSortingComparator();
    try {
      comparator.compare(hive, bench);
      Assert.fail();
    } catch (AssertionError e) {
      Assert.assertEquals("Benchmark ran out of rows at 1 but hive still has rows", e.getMessage());
    }
  }

  @Test
  public void benchMoreRows() throws SQLException, IOException {
    DataGenerator gen1 = new StaticDataGenerator(rows.subList(0, 1), ",");
    DataSet hive = gen1.generateData(table);
    DataGenerator gen2 = new StaticDataGenerator(rows, ",");
    DataSet bench = gen2.generateData(table);
    ResultComparator comparator = new NonSortingComparator();
    try {
      comparator.compare(hive, bench);
      Assert.fail();
    } catch (AssertionError e) {
      Assert.assertEquals("Hive ran out of rows at 1 but benchmark still has rows", e.getMessage());
    }
  }

  @Test
  public void diffNumberColumns() throws Exception {

    TestTable diffTable = TestTable.getBuilder("difftable").addCol("col_bi", "bigint").build();

    List<String> diffRows = new ArrayList<>();
    diffRows.add("6");
    diffRows.add("7");
    DataGenerator gen1 = new StaticDataGenerator(rows, ",");
    DataSet hive = gen1.generateData(table);
    DataGenerator gen2 = new StaticDataGenerator(diffRows, ",");
    DataSet bench = gen2.generateData(diffTable);
    ResultComparator comparator = new NonSortingComparator();
    try {
      comparator.compare(hive, bench);
      Assert.fail();
    } catch (AssertionError e) {
      Assert.assertEquals("Different number of columns expected:<1> but was:<14>", e.getMessage());
    }
   }

  @Test
  public void uncompatibleDataTypes() throws Exception {

    TestTable diffTable = TestTable.getBuilder("difftable")
        .addCol("col_bi", "bigint")
        .addCol("col_i", "string")
        .addCol("col_si", "smallint")
        .addCol("col_ti", "tinyint")
        .addCol("col_bin", "binary")
        .addCol("col_bool", "boolean")
        .addCol("col_ch", "char(8)")
        .addCol("col_vc", "varchar(89)")
        .addCol("col_str", "string")
        .addCol("col_date", "date")
        .addCol("col_dec", "decimal(10,2)")
        .addCol("col_fl", "float")
        .addCol("col_dbl", "double")
        .addCol("col_tm", "timestamp")
        .build();

    List<String> diffRows = new ArrayList<>();
    diffRows.add("6,2,1432,7,abc,true,NULL,mary had a little lamb,her" +
        " fleece was white as snow,2015-08-04,371.89,1.234,6.0221413E23,2015-08-04 17:16:32");
    diffRows.add("-6022141300000000000,-299792458,-1432,-7,def,false,joe,mary had a little " +
        "lamb,her fleece was white as snow,2015-08-04,371.89,-1.234,-6.0221413E-23,NULL");
    DataGenerator gen1 = new StaticDataGenerator(rows, ",");
    DataSet hive = gen1.generateData(table);
    DataGenerator gen2 = new StaticDataGenerator(diffRows, ",");
    DataSet bench = gen2.generateData(diffTable);
    ResultComparator comparator = new NonSortingComparator();
    try {
      comparator.compare(hive, bench);
      Assert.fail();
    } catch (AssertionError e) {
      Assert.assertEquals("Found discrepency in metadata at column 1", e.getMessage());
    }
   }

  @Test
  public void diffNull() throws Exception {
    List<String> diffRows = new ArrayList<>();
    diffRows.add("6,2,1432,7,abc,true,NULL,mary had a little lamb,her" +
        " fleece was white as snow,2015-08-04,371.89,1.234,6.0221413E23,2015-08-04 17:16:32");
    diffRows.add("NULL,-299792458,-1432,-7,def,false,joe,mary had a little " +
        "lamb,her fleece was white as snow,2015-08-04,371.89,-1.234,-6.0221413E-23,NULL");
    DataGenerator gen1 = new StaticDataGenerator(rows, ",");
    DataSet hive = gen1.generateData(table);
    DataGenerator gen2 = new StaticDataGenerator(diffRows, ",");
    DataSet bench = gen2.generateData(table);
    ResultComparator comparator = new NonSortingComparator();
    try {
      comparator.compare(hive, bench);
      Assert.fail();
    } catch (AssertionError e) {
      Assert.assertEquals("Mismatch at row 1 hive row is <-6022141300000000000,-299792458,-1432," +
              "-7,dec,false,joe,mary had a little lamb,her fleece was white as snow,2015-08-04," +
              "371.89,-1.234,-6.0221413E-23,NULL> bench row is <NULL,-299792458,-1432," +
              "-7,dec,false,joe,mary had a little lamb,her fleece was white as snow,2015-08-04," +
              "371.89,-1.234,-6.0221413E-23,NULL>", e.getMessage());
    }
  }

  @Test
  public void diff() throws Exception {
    List<String> diffRows = new ArrayList<>();
    diffRows.add("6,2,1432,7,abc,true,NULL,mary had a little lamb,her" +
        " fleece was white as snow,2015-08-04,371.89,1.234,6.0221413E23,2015-08-04 17:16:32");
    diffRows.add("-6022141300000000000,-299792458,-1432,-7,def,false,joe,mary had a little " +
        "lamb,her fleece was white as snow,2015-08-04,371.89,1.234,-6.0221413E-23,NULL");
    DataGenerator gen1 = new StaticDataGenerator(rows, ",");
    DataSet hive = gen1.generateData(table);
    DataGenerator gen2 = new StaticDataGenerator(diffRows, ",");
    DataSet bench = gen2.generateData(table);
    ResultComparator comparator = new NonSortingComparator();
    try {
      comparator.compare(hive, bench);
      Assert.fail();
    } catch (AssertionError e) {
      Assert.assertEquals("Mismatch at row 1 hive row is <-6022141300000000000," +
          "-299792458,-1432,-7,dec,false,joe,mary had a little lamb,her fleece was white as snow," +
          "2015-08-04,371" +
          ".89,-1.234,-6.0221413E-23,NULL> bench row is <-6022141300000000000,-299792458,-1432," +
          "-7,dec,false,joe,mary had a little lamb,her fleece was white as snow,2015-08-04,371" +
          ".89,1.234,-6.0221413E-23,NULL>", e.getMessage());
    }
  }

  @Test
  public void cli() throws Exception {
    DataGenerator gen1 = new StaticDataGenerator(rows, ",");
    DataSet bench = gen1.generateData(table);

    List<String> cliRows = new ArrayList<>();
    cliRows.add("-6022141300000000000,-299792458,-1432,-7,def,false,joe,mary had a little " +
        "lamb,her fleece was white as snow,2015-08-04,371.89,-1.234,-6" +
        ".0221413E-23,NULL");
    cliRows.add("6,2,1432,7,abc,true,NULL,mary had a little " +
        "lamb,her fleece was white as snow,2015-08-04,371.89,1.234,6" +
        ".0221413E23,2015-08-04 17:16:32");

    DataSet hive = new StringDataSet(cols, cliRows, ",", "NULL");
    ResultComparator comparator = new NonSortingComparator();
    comparator.compare(hive, bench);
  }

  @Test
  public void setSchema() throws Exception {
    DataGenerator gen1 = new StaticDataGenerator(rows, ",");
    DataSet bench = gen1.generateData(table);

    List<String> cliRows = new ArrayList<>();
    cliRows.add("-6022141300000000000,-299792458,-1432,-7,def,false,joe,mary had a little " +
        "lamb,her fleece was white as snow,2015-08-04,371.89,-1.234,-6" +
        ".0221413E-23,NULL");
    cliRows.add("6,2,1432,7,abc,true,NULL,mary had a little " +
        "lamb,her fleece was white as snow,2015-08-04,371.89,1.234,6" +
        ".0221413E23,2015-08-04 17:16:32");

    DataSet hive = new StringDataSet(cliRows, ",", "NULL");
    hive.setSchema(cols);
    ResultComparator comparator = new NonSortingComparator();
    comparator.compare(hive, bench);
  }
}
