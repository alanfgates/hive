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
package org.apache.hive.test.capybara.iface;

import org.apache.hive.test.capybara.data.ResultCode;
import org.apache.hive.test.capybara.data.Row;
import org.apache.hive.test.capybara.infra.ClusterDataGenerator;
import org.apache.hive.test.capybara.data.FetchResult;
import org.apache.hive.test.capybara.infra.HiveStore;
import org.apache.hive.test.capybara.infra.NonSortingComparator;
import org.apache.hive.test.capybara.infra.TestConf;
import org.apache.hive.test.capybara.infra.TestManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.NLineInputFormat;
import org.apache.hadoop.util.StringUtils;
import org.junit.Assert;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * A comparator that works on tables in the cluster (and the benchmark).  This is intended
 * for use when the tests are ETL operations that result in tables on the cluster rather than
 * select results.  If the table size is small then the results will be pulled back and compared
 * on the local machine.  If they are large then they'll be compared in the cluster.  Even in the
 * former case this isn't fast because the system has to run a query to determine result size.
 */
public class TableComparator {
  private static final Logger LOG = LoggerFactory.getLogger(ClusterDataGenerator.class);
  private static final String FIELD_SEP = ":";
  private static final String NTILE_COL = "capyntile";
  private static final String OK = "OK RICK";

  /**
   * Compare tables in Hive and the benchmark.  {@link java.lang.AssertionError} will be thrown
   * if the comparison fails.
   * @param hive HiveStore that contains the table.
   * @param bench Benchmark that contains the table.
   * @param table test table to compare.
   * @throws IOException
   * @throws SQLException
   */
  public void compare(HiveStore hive, BenchmarkDataStore bench, TestTable table)
      throws IOException, SQLException {
    if (!TestConf.onCluster()) {
      localCompare(hive, bench, table);
      return;
    }

    // Step 1, get an estimated size
    FetchResult cnt = hive.fetchData("select count(*) from " + table.getFullName());
    if (cnt.rc != ResultCode.SUCCESS) {
      throw new RuntimeException("Failed to count rows in " + table.getFullName());
    }
    cnt.data.setSchema(Arrays.asList(new FieldSchema("col1", "bigint", "")));
    Iterator<Row> iter = cnt.data.iterator();
    assert iter.hasNext();
    long rowCnt = iter.next().get(0).asLong();

    FetchResult lmt = hive.fetchData("select * from " + table.getFullName() + " limit 100");
    if (lmt.rc != ResultCode.SUCCESS) {
      throw new RuntimeException("Failed to fetch first 100 rows in " + table.getFullName());
    }
    lmt.data.setSchema(table.getCombinedSchema());
    Iterator<String> strIter = lmt.data.stringIterator(",", "N", "\"");
    int sum = 0;
    while (strIter.hasNext()) sum += strIter.next().length();
    int avg = sum / 100;

    long size = rowCnt * avg;
    int parallelism = (int) (2 * size * 1024 / TestConf.getClusterGenThreshold());

    // If parallelism turns out to be less than or equal to 1, do this locally as two separate selects.  Otherwise
    // we'll do it in the cluster
    if (parallelism <= 1) {
      localCompare(hive, bench, table);
    } else {
      clusterCompare(hive, bench, table, parallelism);
    }
  }

  private void localCompare(HiveStore hive, DataStore bench, TestTable table)
      throws IOException, SQLException {
    List<FieldSchema> allCols = table.getCombinedSchema();
    StringBuilder colList = new StringBuilder();
    boolean first = true;
    for (FieldSchema schema : allCols) {
      if (first) first = false;
      else colList.append(", ");
      colList.append(schema.getName());
    }
    StringBuilder sql = new StringBuilder("select ")
        .append(colList)
        .append(" from ")
        .append(table.getFullName())
        .append(" order by ")
        .append(colList);

    FetchResult hiveResult = hive.fetchData(sql.toString());
    Assert.assertEquals(ResultCode.SUCCESS, hiveResult.rc);
    FetchResult benchResult = bench.fetchData(sql.toString());
    Assert.assertEquals(ResultCode.SUCCESS, benchResult.rc);
    if (hiveResult.data.getSchema() == null) {
      hiveResult.data.setSchema(benchResult.data.getSchema());
    }
    ResultComparator comparator = new NonSortingComparator();
    comparator.compare(hiveResult.data, benchResult.data);
  }

  private void clusterCompare(HiveStore hive, BenchmarkDataStore bench, TestTable table,
                              int parallelism) throws IOException, SQLException {
    // Step 2 pick a set of columns to divide the table up by.  We want columns likely to have a
    // wide distribution of values.  Thus we'll avoid boolean and tinyint.  We will also avoid
    // floating point because who knows how that plays across different platforms.  Also, don't
    // use partition columns as those will tend to have a limited set of values.
    List<String> orderByCols = new ArrayList<>(3);

    for (int i = 0; i < table.getCols().size() && orderByCols.size() < 3; i++) {
      FieldSchema col = table.getCols().get(i);
      if (col.getType().equalsIgnoreCase("tinyint") ||
          col.getType().equalsIgnoreCase("float") ||
          col.getType().equalsIgnoreCase("double") ||
          col.getType().equalsIgnoreCase("boolean") ||
          col.getType().equalsIgnoreCase("binary")) {
        continue;
      }
      orderByCols.add(col.getName());
    }
    // If we found less than the desired number, oh well.  If we found none, then just take the
    // first column and hope for the best.
    if (orderByCols.size() == 0) orderByCols.add(table.getCols().get(0).getName());

    // Step 3, create a table similar to our existing table but partitioned by an ntile column.
    //  Use TestTable to do this rather than doing it directly as it already has facilities for
    // handling partitioning.
    List<FieldSchema> allCols = table.getCombinedSchema();
    String comparisonTableName = table.getTableName() + "_comparison";
    TestTable comparisonTable = TestTable.getBuilder(comparisonTableName)
        .setCols(allCols)
        .addPartCol(NTILE_COL, "int")
        .build();
    hive.forceCreateTable(comparisonTable);
    bench.forceCreateTable(comparisonTable);

    StringBuilder sql = new StringBuilder("insert into ")
        .append(comparisonTableName)
        .append(" partition (")
        .append(NTILE_COL)
        .append(") select ");
    for (FieldSchema col : allCols) {
      sql.append(col.getName())
          .append(", ");
    }
    sql.append("ntile(")
        .append(parallelism)
        .append(") over (order by ");
    boolean first = true;
    for (String orderByCol : orderByCols) {
      if (first) first = false;
      else sql.append(", ");
      sql.append(orderByCol);
    }
    sql.append(") from ")
        .append(table.getFullName());
    FetchResult hiveResult = hive.fetchData(sql.toString());
    Assert.assertEquals(ResultCode.SUCCESS, hiveResult.rc);
    FetchResult benchResult = bench.fetchData(sql.toString());
    Assert.assertEquals(ResultCode.SUCCESS, benchResult.rc);

    // Step 4, create an MR job that will do the comparison in pieces, with each task comparing
    // one partition.  The format of the file is one line per mapper each with four fields:
    // 1) which partition to compare;
    // 2) the class of HiveStore to use
    // 3) the class of DataStore to use for bench.
    // 4) the list of columns in the table
    // 5) the name of the table
    Configuration conf = TestManager.getTestManager().getConf();
    FileSystem fs = TestManager.getTestManager().getClusterManager().getFileSystem();
    Path inputFile = new Path(HiveStore.getDirForDumpFile());
    FSDataOutputStream out = fs.create(inputFile);
    for (int i = 0; i < parallelism; i++) {
      out.writeBytes(Integer.toString(i + 1));  // add 1 because SQL counts from 1
      out.writeBytes(FIELD_SEP);
      out.writeBytes(hive.getClass().getName());
      out.writeBytes(FIELD_SEP);
      out.writeBytes(bench.getClass().getName());
      out.writeBytes(FIELD_SEP);
      first = true;
      for (FieldSchema col : allCols) {
        if (first) first = false;
        else sql.append(", ");
        sql.append(col.getName());
      }
      out.writeBytes(FIELD_SEP);
      out.writeBytes(table.getFullName());
      out.writeBytes(System.getProperty("line.separator"));
    }
    out.close();

    Path outputFile = new Path(HiveStore.getDirForDumpFile());

    JobConf job = new JobConf(conf);
    job.setJobName("Capybara data compare for " + table.getTableName());
    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(Text.class);
    job.setJarByClass(TableComparator.class);
    job.setMapperClass(TableComparatorMapper.class);
    job.setNumReduceTasks(0);
    job.setInputFormat(NLineInputFormat.class);
    NLineInputFormat.addInputPath(job, inputFile);
    job.setOutputFormat(TextOutputFormat.class);
    TextOutputFormat.setOutputPath(job, outputFile);

    // Put the benchmark's JDBC driver jar into the cache so it will available on the cluster.
    String jar =
        bench.getDriverClass().getProtectionDomain().getCodeSource().getLocation().getPath();
    Path hdfsJar = new Path(HiveStore.getDirForDumpFile() + "/" + "jdbc_driver.jar");
    fs.copyFromLocalFile(new Path(jar), hdfsJar);
    // Add jar to distributed classPath
    DistributedCache.addFileToClassPath(hdfsJar, job);

    RunningJob runningJob = JobClient.runJob(job);
    LOG.debug("Submitted comparison job " + job.getJobName());
    runningJob.waitForCompletion();

    // Now, read the resulting output file and see how it went.
    FSDataInputStream input = fs.open(outputFile);
    BufferedReader in = new BufferedReader(new InputStreamReader(input));
    int linenum = 0;
    Map<Integer, String> errors = new HashMap<>();
    String line;
    while ((line = in.readLine()) != null) {
      linenum++;
      if (OK.equals(line)) continue;
      errors.put(linenum, line);
    }
    Assert.assertTrue(buildErrorMessage(errors), errors.isEmpty());
  }

  private String buildErrorMessage(Map<Integer, String> errors) {
    StringBuilder msg = new StringBuilder("Found errors: ");
    for (Map.Entry<Integer, String> error : errors.entrySet()) {
      msg.append("partition number ")
          .append(error.getKey())
          .append(" message: <")
          .append(error.getValue())
          .append(">")
          .append(System.getProperty("line.separator"));
    }
    return msg.toString();
  }

  static class TableComparatorMapper implements Mapper<LongWritable, Text, NullWritable, Text> {
    private JobConf job;

    @Override
    public void map(LongWritable longWritable, Text text,
                    OutputCollector<NullWritable, Text> outputCollector, Reporter reporter)
        throws IOException {
      String[] line = text.toString().split(FIELD_SEP);
      assert line.length == 5;

      // Construct the appropriate classes.
      try {
        Class hiveClass = Class.forName(line[1]);
        HiveStore hive = (HiveStore)hiveClass.newInstance();
        hive.setConf(job);

        Class benchClass = Class.forName(line[2]);
        DataStore bench = (DataStore)benchClass.newInstance();

        StringBuilder sql = new StringBuilder("select ")
            .append(line[3])
            .append(" from ")
            .append(line[4])
            .append(" where ")
            .append(NTILE_COL)
            .append(" = ")
            .append(line[0])
            .append(" order by ")
            .append(line[3]);

        FetchResult hiveResult = hive.fetchData(sql.toString());
        Assert.assertEquals(ResultCode.SUCCESS, hiveResult.rc);
        FetchResult benchResult = bench.fetchData(sql.toString());
        Assert.assertEquals(ResultCode.SUCCESS, benchResult.rc);
        if (hiveResult.data.getSchema() == null) {
          hiveResult.data.setSchema(benchResult.data.getSchema());
        }
        ResultComparator comparator = new NonSortingComparator();
        comparator.compare(hiveResult.data, benchResult.data);
        // If the comparator asserts, then the assertion will get recorded.
        outputCollector.collect(NullWritable.get(), new Text(OK));

      } catch (Throwable t) {
        outputCollector.collect(NullWritable.get(), new Text(StringUtils.stringifyException(t)));
      }

    }

    @Override
    public void close() throws IOException {

    }

    @Override
    public void configure(JobConf jobConf) {
      job = jobConf;
    }
  }
}
