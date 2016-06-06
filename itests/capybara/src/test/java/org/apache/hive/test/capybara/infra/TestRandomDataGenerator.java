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

import org.apache.hive.test.capybara.data.Column;
import org.apache.hive.test.capybara.data.DataSet;
import org.apache.hive.test.capybara.data.Row;
import org.apache.hive.test.capybara.iface.TestTable;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hive.test.capybara.iface.DataGenerator;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

public class TestRandomDataGenerator {
  static final private Logger LOG = LoggerFactory.getLogger(TestRandomDataGenerator.class.getName());

  @Before
  public void setup() throws IOException {
    TestManager testMgr = TestManager.getTestManager();
    testMgr.getTestConf().getProperties().setProperty(TestConf.TEST_CLUSTER +
        TestConf.CLUSTER_CLUSTER_MANAGER, NullCluster.class.getName());
    testMgr.getTestConf().getProperties().setProperty(TestConf.BENCH_CLUSTER +
        TestConf.CLUSTER_CLUSTER_MANAGER, NullCluster.class.getName());
    testMgr.getTestClusterManager().setup(TestConf.TEST_CLUSTER);
    testMgr.getBenchmarkClusterManager().setup(TestConf.BENCH_CLUSTER);
  }

  @Test
  public void nonPartitionedAllTypes() throws IOException {
    List<FieldSchema> cols = new ArrayList<>();
    cols.add(new FieldSchema("col1", "bigint", ""));
    cols.add(new FieldSchema("col2", "int", ""));
    cols.add(new FieldSchema("col3", "smallint", ""));
    cols.add(new FieldSchema("col4", "tinyint", ""));
    cols.add(new FieldSchema("col5", "float", ""));
    cols.add(new FieldSchema("col6", "double", ""));
    cols.add(new FieldSchema("col7", "decimal(12,4)", ""));
    cols.add(new FieldSchema("col8", "date", ""));
    cols.add(new FieldSchema("col9", "timestamp", ""));
    cols.add(new FieldSchema("col10", "varchar(32)", ""));
    cols.add(new FieldSchema("col11", "char(9)", ""));
    cols.add(new FieldSchema("col12", "string", ""));
    cols.add(new FieldSchema("col13", "boolean", ""));
    cols.add(new FieldSchema("col14", "binary", ""));
    TestTable table = TestTable.getBuilder("t1").setCols(cols).build();

    RandomDataGenerator rand = new RandomDataGenerator(1);

    DataSet data = rand.generateData(table, 100);

    int rowCnt = 0;
    int[] nullsSeen = new int[cols.size()];
    Arrays.fill(nullsSeen, 0);
    Iterator<String> strIter = data.stringIterator(",", "NULL", "");
    for (Row row : data) {
      Assert.assertTrue(strIter.hasNext());
      LOG.debug("Row is " + strIter.next());
      // All the column sizes combined
      rowCnt++;
      Assert.assertEquals(cols.size(), row.size());

      // Check each of the columns to make sure we got something valid.  Many of tests are
      // bogus; they are there to make sure the compiler doesn't outsmart us and optimize out the
      // line.
      for (int i = 0; i < row.size(); i++) {
        if (row.get(i).isNull()) {
          nullsSeen[i]++;
        } else {
          switch (i) {
          case 0: Assert.assertTrue(row.get(i).asLong() >= Long.MIN_VALUE); break;

          case 1: Assert.assertTrue(row.get(i).asInt() >= Integer.MIN_VALUE); break;

          case 2: Assert.assertTrue(row.get(i).asShort() >= Short.MIN_VALUE); break;

          case 3: Assert.assertTrue(row.get(i).asByte() >= Byte.MIN_VALUE); break;

          case 4:
            // For strange reasons I can't fathom the float value >= min_value fails
            Assert.assertTrue(row.get(i).asFloat() != Float.NaN); break;

          case 5: Assert.assertTrue(row.get(i).asDouble() != Double.NaN); break;

          case 6:
            BigDecimal bd = row.get(i).asBigDecimal();
            // Precision might be as low as scale, since BigDecimal.precision returns real
            // precision for this instance.
            Assert.assertTrue(bd.precision() >= 4 && bd.precision() < 13);
            Assert.assertEquals(4, bd.scale());
            break;

          case 7:
            Assert.assertTrue(row.get(i).asDate().after(new Date(-100000000000000000L))); break;

          case 8:
            Assert.assertTrue(row.get(i).asTimestamp().after(new Timestamp(-10000000000000L)));
            break;

          case 9: Assert.assertTrue(row.get(i).asString().length() <= 32); break;

          case 10: Assert.assertTrue(row.get(i).asString().length() <= 9); break;

          case 11: Assert.assertTrue(row.get(i).asString().length() <= 20); break;

          case 12:
            Assert.assertTrue(row.get(i).asBoolean() || !row.get(i).asBoolean());
            break;

          case 13:
            Assert.assertTrue(row.get(i).asBytes().length <= 100); break;

          default: throw new RuntimeException("Too many columns");

          }
        }
      }
    }
    Assert.assertFalse(strIter.hasNext());

    long totalSize = data.lengthInBytes();
    Assert.assertTrue("Expected totalSize > 102400, but was " + totalSize, totalSize >= 102400);
    // For all rows we should have around 1% nulls
    for (int i = 0; i < cols.size(); i++) {
      LOG.debug("For column " + i + " nulls seen is " + nullsSeen[i] + " rowCnt is " + rowCnt);
      Assert.assertTrue(nullsSeen[i] > rowCnt * 0.001 && nullsSeen[i] < rowCnt * 0.019);
    }
  }

  @Test
  public void differentNumNulls() throws IOException {
    List<FieldSchema> cols = new ArrayList<>();
    cols.add(new FieldSchema("col1", "bigint", ""));
    cols.add(new FieldSchema("col2", "int", ""));
    TestTable table = TestTable.getBuilder("t2").setCols(cols).build();

    RandomDataGenerator rand = new RandomDataGenerator(2);

    DataSet data = rand.generateData(table, 100, new double[]{0.05, 0.02});

    int rowCnt = 0;
    int[] nullsSeen = new int[cols.size()];
    Arrays.fill(nullsSeen, 0);
    Iterator<String> strIter = data.stringIterator(",", "NULL", "");
    for (Row row : data) {
      LOG.debug("Row is " + strIter.next());
      rowCnt++;
      Assert.assertEquals(cols.size(), row.size());

      for (int i = 0; i < row.size(); i++) {
        if (row.get(i).isNull()) {
          nullsSeen[i]++;
        }
      }
    }

    for (int i = 0; i < cols.size(); i++) {
      LOG.debug("For column " + i + " nulls seen is " + nullsSeen[i] + " rowCnt is " + rowCnt);
    }
    Assert.assertTrue(nullsSeen[0] > rowCnt * 0.04 && nullsSeen[0] < rowCnt * 0.06);
    Assert.assertTrue(nullsSeen[1] > rowCnt * 0.01 && nullsSeen[1] < rowCnt * 0.03);
  }

  @Test
  public void partitions() throws IOException {
    List<FieldSchema> cols = new ArrayList<>();
    cols.add(new FieldSchema("col1", "varchar(3)", ""));
    cols.add(new FieldSchema("col2", "date", ""));
    List<FieldSchema> partCols = new ArrayList<>();
    partCols.add(new FieldSchema("p1", "smallint", ""));
    TestTable table = TestTable.getBuilder("t3")
        .setCols(cols)
        .setPartCols(partCols)
        .setNumParts(5)
        .build();

    RandomDataGenerator rand = new RandomDataGenerator(3);

    DataSet data = rand.generateData(table, 100);

    Set<Column> partValsSeen = new HashSet<>();
    Iterator<String> strIter = data.stringIterator(",", "NULL", "");
    for (Row row : data) {
      LOG.debug("Row is " + strIter.next());
      Assert.assertEquals(cols.size() + partCols.size(), row.size());

      partValsSeen.add(row.get(cols.size()));
    }

    Assert.assertEquals(5, partValsSeen.size());
  }

  @Test
  public void presetPartitions() throws IOException {
    List<FieldSchema> cols = new ArrayList<>();
    cols.add(new FieldSchema("col1", "varchar(3)", ""));
    cols.add(new FieldSchema("col2", "date", ""));
    List<FieldSchema> partCols = new ArrayList<>();
    partCols.add(new FieldSchema("p1", "smallint", ""));
    DataGenerator partValGenerator = new StaticDataGenerator(Arrays.asList("1", "2", "3"), ",");
    TestTable table = TestTable.getBuilder("t3")
        .setCols(cols)
        .setPartCols(partCols)
        .setPartValsGenerator(partValGenerator)
        .build();

    RandomDataGenerator rand = new RandomDataGenerator(3);

    DataSet data = rand.generateData(table, 100);

    SortedSet<Column> partValsSeen = new TreeSet<>();
    Iterator<String> strIter = data.stringIterator(",", "NULL", "");
    for (Row row : data) {
      LOG.debug("Row is " + strIter.next());
      Assert.assertEquals(cols.size() + partCols.size(), row.size());

      partValsSeen.add(row.get(cols.size()));
    }

    Assert.assertEquals(3, partValsSeen.size());
    Iterator<Column> iter = partValsSeen.iterator();
    short nextPartValExpected = 1;
    while (iter.hasNext()) {
      Assert.assertEquals(nextPartValExpected++, iter.next().asShort());
    }
  }

  @Test
  public void autoPartitionCount() throws IOException {
    List<FieldSchema> cols = new ArrayList<>();
    cols.add(new FieldSchema("col1", "varchar(3)", ""));
    cols.add(new FieldSchema("col2", "date", ""));
    List<FieldSchema> partCols = new ArrayList<>();
    partCols.add(new FieldSchema("p1", "date", ""));
    TestTable table = TestTable.getBuilder("t3")
        .setCols(cols)
        .setPartCols(partCols)
        .build();

    RandomDataGenerator rand = new RandomDataGenerator(4);

    DataSet data = rand.generateData(table, 100);

    Set<Comparable> partValsSeen = new HashSet<>();
    Iterator<String> strIter = data.stringIterator(",", "NULL", "");
    for (Row row : data) {
      LOG.debug("Row is " + strIter.next());
      Assert.assertEquals(cols.size() + partCols.size(), row.size());

      partValsSeen.add(row.get(cols.size()));
    }

    Assert.assertEquals(3, partValsSeen.size());
  }

  @Test
  public void primaryKey() throws IOException {
    List<FieldSchema> cols = new ArrayList<>();
    cols.add(new FieldSchema("col1", "varchar(3)", ""));
    cols.add(new FieldSchema("col2", "date", ""));
    List<FieldSchema> partCols = new ArrayList<>();
    partCols.add(new FieldSchema("p1", "smallint", ""));
    TestTable table = TestTable.getBuilder("t4")
        .setCols(cols)
        .setPartCols(partCols)
        .setPrimaryKey(new TestTable.PrimaryKey(0))
        .build();

    RandomDataGenerator rand = new RandomDataGenerator(4);

    DataSet data = rand.generateData(table, 100);

    int rowCnt = 0;
    Set<Column> pkSeen = new HashSet<>();
    Iterator<String> strIter = data.stringIterator(",", "NULL", "");
    for (Row row : data) {
      rowCnt++;
      LOG.debug("Row is " + strIter.next());
      Assert.assertEquals(cols.size() + partCols.size(), row.size());

      Assert.assertTrue(pkSeen.add(row.get(0)));
    }
    Assert.assertEquals(rowCnt, pkSeen.size());
  }

  @Test
  public void sequence() throws IOException {
    List<FieldSchema> cols = new ArrayList<>();
    cols.add(new FieldSchema("col1", "bigint", ""));
    cols.add(new FieldSchema("col2", "date", ""));
    List<FieldSchema> partCols = new ArrayList<>();
    partCols.add(new FieldSchema("p1", "smallint", ""));
    TestTable table = TestTable.getBuilder("t4")
        .setCols(cols)
        .setPartCols(partCols)
        .setPrimaryKey(new TestTable.Sequence(0))
        .build();

    RandomDataGenerator rand = new RandomDataGenerator(4);

    DataSet data = rand.generateData(table, 100);

    long nextSequence = 1;
    Iterator<String> strIter = data.stringIterator(",", "NULL", "");
    for (Row row : data) {
      LOG.debug("sequence row is " + strIter.next());
      Assert.assertEquals(cols.size() + partCols.size(), row.size());

      Assert.assertEquals(nextSequence++, row.get(0).asLong());
    }
  }

  @Test
  public void foreignKey() throws IOException {
    TestTable dim = TestTable.getBuilder("dim")
        .addCol("col1", "bigint")
        .addCol("col2", "date")
        .setPrimaryKey(new TestTable.Sequence(0))
        .build();

    RandomDataGenerator rand = new RandomDataGenerator(4);

    DataSet dimData = rand.generateData(dim, 5);

    int dimRowCnt = 0;
    for (Row dimRow : dimData) dimRowCnt++;
    LOG.debug("Number of dimension rows " + dimRowCnt);

    TestTable fact = TestTable.getBuilder("fact")
        .addCol("pk", "int")
        .addCol("fk", "bigint")
        .addForeignKey(new TestTable.ForeignKey(dimData, 0, 1))
        .build();

    DataSet factData = rand.generateData(fact, 100);

    Iterator<String> strIter = factData.stringIterator(",", "NULL", "");
    for (Row row : factData) {
      LOG.debug("Row is " + strIter.next());
      Assert.assertTrue(row.get(1).asLong() <= dimRowCnt);
    }
  }
}
