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
import org.apache.hive.test.capybara.data.Row;
import org.apache.hive.test.capybara.iface.TestTable;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class TestStatsDataGenerator {
  static final private Logger LOG = LoggerFactory.getLogger(TestStatsDataGenerator.class.getName());

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
  public void scale() {
    List<FieldSchema> cols = new ArrayList<>();
    cols.add(new FieldSchema("col1", "bigint", ""));
    TestTable table = TestTable.getBuilder("t1").setCols(cols).build();
    Map<String, StatsDataGenerator.ColStats> colStats = new HashMap<>();
    colStats.put("col1", new StatsDataGenerator.ColStats(cols.get(0).getName(),
        cols.get(0).getType(), -37, 37, 0, 0, 0, 0, 51, 16));
    StatsDataGenerator.TableStats tableStats = new StatsDataGenerator.TableStats(colStats, 0,
        table.getDbName(), table.getTableName(), 1024, 128);
    tableStats.scale(1);
    Assert.assertEquals(128, tableStats.numRows);
    Assert.assertEquals(0, tableStats.numPartitions);
    Assert.assertEquals(51, tableStats.colStats.get("col1").distinctCount);
    Assert.assertEquals(0.125, tableStats.colStats.get("col1").pctNull, 0.005);

    colStats = new HashMap<>();
    colStats.put("col1", new StatsDataGenerator.ColStats(cols.get(0).getName(),
        cols.get(0).getType(), -37, 37, 0, 0, 0, 0, 52, 4));
    tableStats = new StatsDataGenerator.TableStats(colStats, 0,
        table.getDbName(), table.getTableName(), 1024, 128);
    tableStats.scale(10);
    Assert.assertEquals(1280, tableStats.numRows);
    Assert.assertEquals(0, tableStats.numPartitions);
    Assert.assertEquals(520, tableStats.colStats.get("col1").distinctCount);
    Assert.assertEquals(0.03, tableStats.colStats.get("col1").pctNull, 0.005);

    colStats = new HashMap<>();
    colStats.put("col1", new StatsDataGenerator.ColStats(cols.get(0).getName(),
        cols.get(0).getType(), -37, 37, 0, 0, 0, 0, 51, 7));
    tableStats = new StatsDataGenerator.TableStats(colStats, 0,
        table.getDbName(), table.getTableName(), 4096, 512);
    tableStats.scale(1);
    Assert.assertEquals(128, tableStats.numRows);
    Assert.assertEquals(0, tableStats.numPartitions);
    Assert.assertEquals(12, tableStats.colStats.get("col1").distinctCount);
    Assert.assertEquals(0.015, tableStats.colStats.get("col1").pctNull, 0.005);

    // Test going from very large data to low scale, make sure we hit minimums.
    colStats = new HashMap<>();
    colStats.put("col1", new StatsDataGenerator.ColStats(cols.get(0).getName(),
        cols.get(0).getType(), -37, 37, 0, 0, 0, 0, 519, 367));
    tableStats = new StatsDataGenerator.TableStats(colStats, 0,
        table.getDbName(), table.getTableName(), 104857600000000L, 1310720L);
    tableStats.scale(1);
    Assert.assertEquals(100, tableStats.numRows);
    Assert.assertEquals(0, tableStats.numPartitions);
    Assert.assertEquals(10, tableStats.colStats.get("col1").distinctCount);
    Assert.assertEquals(0.0003, tableStats.colStats.get("col1").pctNull, 0.00005);

    // Make sure it works with partitions too
    table = TestTable.getBuilder("t1").setCols(cols).addPartCol("partcol", "string").build();
    colStats = new HashMap<>();
    colStats.put("col1", new StatsDataGenerator.ColStats(cols.get(0).getName(),
        cols.get(0).getType(), -37, 37, 0, 0, 0, 0, 59, 37));
    tableStats = new StatsDataGenerator.TableStats(colStats, 10,
        table.getDbName(), table.getTableName(), 1024, 128);
    tableStats.scale(1);
    Assert.assertEquals(10, tableStats.numPartitions);

    colStats = new HashMap<>();
    colStats.put("col1", new StatsDataGenerator.ColStats(cols.get(0).getName(),
        cols.get(0).getType(), -37, 37, 0, 0, 0, 0, 519, 367));
    tableStats = new StatsDataGenerator.TableStats(colStats, 10,
        table.getDbName(), table.getTableName(), 1024, 128);
    tableStats.scale(10);
    Assert.assertEquals(100, tableStats.numPartitions);

    colStats = new HashMap<>();
    colStats.put("col1", new StatsDataGenerator.ColStats(cols.get(0).getName(),
        cols.get(0).getType(), -37, 37, 0, 0, 0, 0, 519, 367));
    tableStats = new StatsDataGenerator.TableStats(colStats, 100,
        table.getDbName(), table.getTableName(), 10240, 1280);
    tableStats.scale(1);
    Assert.assertEquals(10, tableStats.numPartitions);

    colStats = new HashMap<>();
    colStats.put("col1", new StatsDataGenerator.ColStats(cols.get(0).getName(),
        cols.get(0).getType(), -37, 37, 0, 0, 0, 0, 519, 367));
    tableStats = new StatsDataGenerator.TableStats(colStats, 10,
        table.getDbName(), table.getTableName(), 1048576000000000L, 1310720);
    tableStats.scale(1);
    Assert.assertEquals(2, tableStats.numPartitions);
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
    cols.add(new FieldSchema("col7", "decimal(12,2)", ""));
    cols.add(new FieldSchema("col8", "date", ""));
    cols.add(new FieldSchema("col9", "timestamp", ""));
    cols.add(new FieldSchema("col10", "varchar(32)", ""));
    cols.add(new FieldSchema("col11", "char(9)", ""));
    cols.add(new FieldSchema("col12", "string", ""));
    cols.add(new FieldSchema("col13", "boolean", ""));
    cols.add(new FieldSchema("col14", "binary", ""));
    TestTable table = TestTable.getBuilder("t1").setCols(cols).build();

    Map<String, StatsDataGenerator.ColStats> colStats = new HashMap<>();
    long[] distinctCounts = new long[cols.size()];
    long[] nullCounts = new long[cols.size()];

    long minLong = -37L;
    long maxLong = 82387563739L;
    distinctCounts[0] = 317;
    nullCounts[0] = 53;
    colStats.put("col1", new StatsDataGenerator.ColStats(cols.get(0).getName(),
        cols.get(0).getType(), minLong, maxLong, 0, 0, 0, 0, distinctCounts[0], nullCounts[0]));

    int minInt = -98;
    int maxInt = 7619234;
    distinctCounts[1] = 127;
    nullCounts[1] = 47;
    colStats.put("col2", new StatsDataGenerator.ColStats(cols.get(1).getName(),
        cols.get(1).getType(), minInt, maxInt, 0, 0, 0, 0, distinctCounts[1], nullCounts[1]));


    short minShort = -988;
    short maxShort = 7134;
    distinctCounts[2] = 227;
    nullCounts[2] = 171;
    colStats.put("col3", new StatsDataGenerator.ColStats(cols.get(2).getName(),
        cols.get(2).getType(), minShort, maxShort, 0, 0, 0, 0, distinctCounts[2], nullCounts[2]));

    byte minByte = -88;
    byte maxByte = 34;
    distinctCounts[3] = 17;
    nullCounts[3] = 61;
    colStats.put("col4", new StatsDataGenerator.ColStats(cols.get(3).getName(),
        cols.get(3).getType(), minByte, maxByte, 0, 0, 0, 0, distinctCounts[3], nullCounts[3]));

    float minFloat = -9.098e8f;
    float maxFloat = 3.876363e4f;
    distinctCounts[4] = 135;
    nullCounts[4] = 198;
    colStats.put("col5", new StatsDataGenerator.ColStats(cols.get(4).getName(),
        cols.get(4).getType(), minFloat, maxFloat, 0, 0, 0, 0, distinctCounts[4], nullCounts[4]));

    double minDouble = -9.098e18;
    double maxDouble = 3.876363e14;
    distinctCounts[5] = 620;
    nullCounts[5] = 98;
    colStats.put("col6", new StatsDataGenerator.ColStats(cols.get(5).getName(),
        cols.get(5).getType(), minDouble, maxDouble, 0, 0, 0, 0, distinctCounts[5], nullCounts[5]));

    BigDecimal minDecimal = new BigDecimal("-1098098.23");
    BigDecimal maxDecimal = new BigDecimal("2983928293.12");
    distinctCounts[6] = 352;
    nullCounts[6] = 0;
    colStats.put("col7", new StatsDataGenerator.ColStats(cols.get(6).getName(),
        cols.get(6).getType(), minDecimal, maxDecimal, 0, 0, 0, 0, distinctCounts[6], nullCounts[6]));

    Date minDate = new Date(-1098098);
    Date maxDate = new Date(298392893);
    distinctCounts[7] = 52;
    nullCounts[7] = 1;
    colStats.put("col8", new StatsDataGenerator.ColStats(cols.get(7).getName(),
        cols.get(7).getType(), minDate, maxDate, 0, 0, 0, 0, distinctCounts[7], nullCounts[7]));

    Timestamp minTimestamp = new Timestamp(-198098);
    Timestamp maxTimestamp = new Timestamp(198392893);
    distinctCounts[8] = 152;
    nullCounts[8] = 10;
    colStats.put("col9", new StatsDataGenerator.ColStats(cols.get(8).getName(),
        cols.get(8).getType(), minTimestamp, maxTimestamp, 0, 0, 0, 0, distinctCounts[8], nullCounts[8]));

    long maxVarcharLen = 30;
    double avgVarcharLen = 25.0; // weight it toward one end to see if our average works.
    distinctCounts[9] = 122;
    nullCounts[9] = 10;
    colStats.put("col10", new StatsDataGenerator.ColStats(cols.get(9).getName(),
        cols.get(9).getType(), null, null, avgVarcharLen, maxVarcharLen, 0, 0, distinctCounts[9],
        nullCounts[9]));

    long maxCharLen = 7;
    double avgCharLen = 2.5; // weight it toward one end to see if our average works.
    distinctCounts[10] = 72;
    nullCounts[10] = 0;
    colStats.put("col11", new StatsDataGenerator.ColStats(cols.get(10).getName(),
        cols.get(10).getType(), null, null, avgCharLen, maxCharLen, 0, 0, distinctCounts[10],
        nullCounts[10]));

    long maxStringLen = 79;
    double avgStringLen = 42.15;
    distinctCounts[11] = 172;
    nullCounts[11] = 3;
    colStats.put("col12", new StatsDataGenerator.ColStats(cols.get(11).getName(),
        cols.get(11).getType(), null, null, avgStringLen, maxStringLen, 0, 0, distinctCounts[11],
        nullCounts[11]));

    long numTrues = 284;
    long numFalses = 400;
    nullCounts[12] = 100;
    colStats.put("col13", new StatsDataGenerator.ColStats(cols.get(12).getName(),
        cols.get(12).getType(), null, null, 0, 0, numFalses, numTrues, 0, nullCounts[12]));

    long maxBinaryLen = 179;
    double avgBinaryLen = 12.15; // weight it toward one end to see if our average works.
    nullCounts[13] = 3;
    colStats.put("col14", new StatsDataGenerator.ColStats(cols.get(13).getName(),
        cols.get(13).getType(), null, null, avgBinaryLen, maxBinaryLen, 0, 0, 0, nullCounts[13]));

    // Set the number of rows to match the passed in scale of 100 so our expected counts don't
    // change significantly.
    StatsDataGenerator.TableStats tableStats = new StatsDataGenerator.TableStats(colStats, 0,
        table.getDbName(), table.getTableName(), 102400, 784);

    StatsDataGenerator gen = new StatsDataGenerator(tableStats, 1);

    DataSet data = gen.generateData(table, 100);

    int rowCnt = 0;
    int[] nullsSeen = new int[cols.size()];
    Arrays.fill(nullsSeen, 0);
    Set<Long> distinctLongs = new HashSet<>();
    Set<Integer> distinctIntegers = new HashSet<>();
    Set<Short> distinctShorts = new HashSet<>();
    Set<Byte> distinctBytes = new HashSet<>();
    Set<Float> distinctFloats = new HashSet<>();
    Set<Double> distinctDoubles = new HashSet<>();
    Set<BigDecimal> distinctBigDecimals = new HashSet<>();
    Set<Date> distinctDates = new HashSet<>();
    Set<Timestamp> distinctTimestamps = new HashSet<>();
    Set<String> distinctVarchars = new HashSet<>();
    Set<String> distinctChars = new HashSet<>();
    Set<String> distinctStrings = new HashSet<>();
    long varcharLen, charLen, strLen, binLen, truesSeen, falsesSeen;
    varcharLen = charLen = strLen = binLen = truesSeen = falsesSeen = 0;

    Iterator<String> strIter = data.stringIterator(",", "NULL", "");
    for (Row row : data) {
      Assert.assertTrue(strIter.hasNext());
      LOG.debug("Row is " + strIter.next());
      // All the column sizes combined
      rowCnt++;
      Assert.assertEquals(cols.size(), row.size());

      for (int i = 0; i < row.size(); i++) {
        if (row.get(i).isNull()) {
          nullsSeen[i]++;
        } else {
          switch (i) {
          case 0:
            Assert.assertTrue("Expected long between " + minLong + " and " + maxLong + " got " +
                row.get(i).asLong(),
                row.get(i).asLong() >=  minLong && row.get(i).asLong() <= maxLong);
            distinctLongs.add(row.get(i).asLong());
            break;

          case 1:
            Assert.assertTrue(row.get(i).asInt() >= minInt && row.get(i).asInt() <= maxInt);
            distinctIntegers.add(row.get(i).asInt());
            break;

          case 2:
            Assert.assertTrue(row.get(i).asShort() >= minShort && row.get(i).asShort() <= maxShort);
            distinctShorts.add(row.get(i).asShort());
            break;

          case 3:
            Assert.assertTrue(row.get(i).asByte() >= minByte && row.get(i).asByte() <= maxByte);
            distinctBytes.add(row.get(i).asByte());
            break;

          case 4:
            Assert.assertTrue("Expected float between " + minFloat + " and " + maxFloat + " got " +
                row.get(i).asFloat(),
                row.get(i).asFloat() >= minFloat && row.get(i).asFloat() <= maxFloat);
            distinctFloats.add(row.get(i).asFloat());
            break;

          case 5:
            Assert.assertTrue(row.get(i).asDouble() >= minDouble && row.get(i).asDouble() <= maxDouble);
            distinctDoubles.add(row.get(i).asDouble());
            break;

          case 6:
            Assert.assertTrue("Expected big decimal between " + minDecimal + " and " + maxDecimal
                + " got " + row.get(i).asBigDecimal(),
                row.get(i).asBigDecimal().compareTo(minDecimal) >= 0 &&
                row.get(i).asBigDecimal().compareTo(maxDecimal) <= 0);
            distinctBigDecimals.add(row.get(i).asBigDecimal());
            break;

          case 7:
            Assert.assertTrue(row.get(i).asDate().compareTo(minDate) >= 0 &&
                row.get(i).asDate().compareTo(maxDate) <= 0);
            distinctDates.add(row.get(i).asDate());
            break;

          case 8:
            Assert.assertTrue(row.get(i).asTimestamp().compareTo(minTimestamp) >= 0 &&
                row.get(i).asTimestamp().compareTo(maxTimestamp) <= 0);
            distinctTimestamps.add(row.get(i).asTimestamp());
            break;

          case 9:
            Assert.assertTrue(row.get(i).asString().length() <= maxVarcharLen);
            distinctVarchars.add(row.get(i).asString());
            varcharLen += row.get(i).length();
            break;

          case 10:
            Assert.assertTrue(row.get(i).asString().length() <= maxCharLen);
            distinctChars.add(row.get(i).asString());
            charLen += row.get(i).length();
            break;

          case 11:
            Assert.assertTrue(row.get(i).asString().length() <= maxStringLen);
            distinctStrings.add(row.get(i).asString());
            strLen += row.get(i).length();
            break;

          case 12:
            if (row.get(i).asBoolean()) truesSeen++;
            else falsesSeen++;
            break;

          case 13:
            Assert.assertTrue(row.get(i).asBytes().length <= maxBinaryLen);
            binLen += row.get(i).length();
            break;

          default:
            throw new RuntimeException("Too many columns");

          }
        }
      }
    }
    Assert.assertFalse(strIter.hasNext());

    LOG.debug("Total generated rows is " + rowCnt);

    // Check each of the distinct values is near what we expect
    long range = (long)(distinctCounts[0] * 0.1);
    Assert.assertTrue("Distinct longs observed " + distinctLongs.size() + " expected " +
        distinctCounts[0], Math.abs(distinctLongs.size() - distinctCounts[0]) < range);
    range = (long)(distinctCounts[1] * 0.1);
    Assert.assertTrue("Distinct ints observed " + distinctIntegers.size() + " expected " +
        distinctCounts[1], Math.abs(distinctIntegers.size() - distinctCounts[1]) < range);
    range = (long)(distinctCounts[2] * 0.1);
    Assert.assertTrue("Distinct shorts observed " + distinctShorts.size() + " expected " +
        distinctCounts[2], Math.abs(distinctShorts.size() - distinctCounts[2]) < range);
    range = (long)(distinctCounts[3] * 0.2); // Seem to have greater margin of error on bytes,
    // not sure why.
    Assert.assertTrue("Distinct bytes observed " + distinctBytes.size() + " expected " +
        distinctCounts[3], Math.abs(distinctBytes.size() - distinctCounts[3]) < range);
    range = (long)(distinctCounts[4] * 0.1);
    Assert.assertTrue("Distinct floats observed " + distinctFloats.size() + " expected " +
        distinctCounts[4], Math.abs(distinctFloats.size() - distinctCounts[4]) < range);
    range = (long)(distinctCounts[5] * 0.1);
    Assert.assertTrue("Distinct doubles observed " + distinctDoubles.size() + " expected " +
        distinctCounts[5], Math.abs(distinctDoubles.size() - distinctCounts[5]) < range);
    range = (long)(distinctCounts[6] * 0.1);
    Assert.assertTrue("Distinct decimals observed " + distinctBigDecimals.size() + " expected " +
            distinctCounts[6], Math.abs(distinctBigDecimals.size() - distinctCounts[6]) < range);
    range = (long)(distinctCounts[7] * 0.1);
    Assert.assertTrue("Distinct dates observed " + distinctDates.size() + " expected " +
        distinctCounts[7], Math.abs(distinctDates.size() - distinctCounts[7]) < range);
    range = (long)(distinctCounts[8] * 0.1);
    Assert.assertTrue("Distinct timestamps observed " + distinctTimestamps.size() + " expected " +
        distinctCounts[8], Math.abs(distinctTimestamps.size() - distinctCounts[8]) < range);
    range = (long)(distinctCounts[9] * 0.1);
    Assert.assertTrue("Distinct varchars observed " + distinctVarchars.size() + " expected " +
        distinctCounts[9], Math.abs(distinctVarchars.size() - distinctCounts[9]) < range);
    range = (long)(distinctCounts[10] * 0.1);
    Assert.assertTrue("Distinct chars observed " + distinctChars.size() + " expected " +
        distinctCounts[10], Math.abs(distinctChars.size() - distinctCounts[10]) < range);
    range = (long)(distinctCounts[11] * 0.1);
    Assert.assertTrue("Distinct strings observed " + distinctStrings.size() + " expected " +
        distinctCounts[11], Math.abs(distinctStrings.size() - distinctCounts[11]) < range);

    // Check average lengths
    double dRange = avgVarcharLen * 0.2;
    Assert.assertEquals(avgVarcharLen, varcharLen / (double)rowCnt, dRange);
    dRange = avgCharLen * 0.25;
    Assert.assertEquals(avgCharLen, charLen / (double)rowCnt, dRange);
    dRange = avgStringLen * 0.2;
    Assert.assertEquals(avgStringLen, strLen / (double)rowCnt, dRange);
    dRange = avgBinaryLen * 0.2;
    Assert.assertEquals(avgBinaryLen, binLen / (double)rowCnt, dRange);

    long totalSize = data.lengthInBytes();
    LOG.debug("Total size is " + totalSize);
    Assert.assertTrue("Expected totalSize > 102400, but was " + totalSize, totalSize >= 102400);

    // Check numTrues and numFalses
    range = (long)(numTrues * 0.15);
    Assert.assertTrue("Expected truesSeen of " + numTrues + " got " + truesSeen,
        Math.abs(truesSeen - numTrues) < range);
    range = (long)(numFalses * 0.15);
    Assert.assertTrue("Expected falsesSeen of " + numFalses + " got " + falsesSeen,
        Math.abs(falsesSeen - numFalses) < range);


    //long totalSize = data.length();
    Assert.assertTrue("Expected totalSize > 102400, but was " + totalSize, totalSize >= 102400);
    for (int i = 0; i < cols.size(); i++) {
      range = (long)(nullCounts[i] * 0.2);
      Assert.assertTrue("For column " + i + " expected " + nullCounts[i] + " but got " +
              nullsSeen[i], Math.abs(nullsSeen[i] - nullCounts[i]) <= range ||
          Math.abs(nullsSeen[i] - nullCounts[i]) < 10); // cover very small cases
    }
  }

  @Ignore
  public void partitioned() throws Exception {
    Assert.fail();
  }
}

