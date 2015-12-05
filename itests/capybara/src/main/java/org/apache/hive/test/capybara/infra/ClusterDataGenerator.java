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

import org.apache.commons.codec.binary.Base64;
import org.apache.hive.test.capybara.data.DataSet;
import org.apache.hive.test.capybara.iface.TestTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.lib.NLineInputFormat;
import org.apache.hadoop.mapred.lib.NullOutputFormat;
import org.apache.hive.test.capybara.iface.DataGenerator;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * A class that takes a generator and makes many copies of it run on a cluster.
 */
public class ClusterDataGenerator extends DataGenerator {
  private static final Logger LOG = LoggerFactory.getLogger(ClusterDataGenerator.class);
  private static final String NUM_SPLITS = "hive.test.capybara.cluster.data.gen.numsplits";
  private DataGeneratorImpl wrapped;

  public ClusterDataGenerator(DataGeneratorImpl base) {
    wrapped = base;
  }

  @Override
  public DataSet generateData(TestTable table, int scale, double[] pctNulls) {
    LOG.debug("Generating data on cluster");
    if (table.getPartCols() != null) {
      // Decide on our part vals before we generate.  In the random case this prevents each
      // generator from generating different random values.  The number of discrete values should
      // be relatively small.
      table.setPartVals(wrapped.determinePartVals(table, scale, pctNulls));
    }

    try {
      Configuration conf = TestManager.getTestManager().getConf();
      FileSystem fs = TestManager.getTestManager().getClusterManager().getFileSystem();

      // If the table has one or more foreign keys don't try to encode that information in each
      // row. Write each foreign key into a separate file and add the file to the distributed cache.
      if (table.getForeignKeys() != null) {
        for (int i = 0; i < table.getForeignKeys().size(); i++) {
          TestTable.ForeignKey fk = table.getForeignKeys().get(i);
          Path fkPath = new Path(HiveStore.getDirForDumpFile());
          FSDataOutputStream out = fs.create(fkPath);
          // Write the index in the first line of the file.
          out.writeBytes(Integer.toString(i));
          out.writeBytes(System.getProperty("line.separator"));
          Iterator<String> iter = fk.targetTable.stringIterator("\u0001", "NULL", "\"");
          while (iter.hasNext()) {
            out.writeBytes(iter.next());
            out.writeBytes(System.getProperty("line.separator"));
          }
          out.close();
          fk.pkFile = fkPath;
          // TODO - not sure this is the correct way to get something in the DC.  Check once I
          // have network.
          DistributedCache.addCacheFile(fkPath.toUri(), conf);
        }
      }

      // Figure out how parallel to go.
      int parallelism = 2 * scale * 1024 / TestConf.getClusterGenThreshold();

    // The algorithm here is to write a line for each mapper we're going to start and then use
    // the NLineInputFormat to create one mapper per line.  This creates quite a lot of
    // duplication, but the entries are only a few K so it isn't that bad.  The exception is if
    // we have a foreign key it needs to be written into a side file and then put in the DC.
      String hiveDumpDir = HiveStore.getDirForDumpFile();
      Path inputFile = new Path(HiveStore.getDirForDumpFile());
      FSDataOutputStream out = fs.create(inputFile);
      for (int i = 0; i < parallelism; i++) {
        GeneratorInfo gi =
            new GeneratorInfo(scale / parallelism, pctNulls, table, wrapped, hiveDumpDir);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(gi);
        out.writeBytes(Base64.encodeBase64URLSafeString(baos.toByteArray()));
        out.writeBytes(System.getProperty("line.separator"));
      }
      out.close();

      // Now, construct an MR job to run this.
      JobConf job = new JobConf(conf);
      job.setJobName("Capybara data gen for " + table.getTableName());
      job.setOutputKeyClass(NullWritable.class);
      job.setOutputValueClass(NullWritable.class);
      job.setJarByClass(ClusterDataGenerator.class);
      job.setMapperClass(ClusterDataGeneratorMap.class);
      job.setNumReduceTasks(0);
      job.setInputFormat(NLineInputFormat.class);
      NLineInputFormat.addInputPath(job, inputFile);
      job.setOutputFormat(NullOutputFormat.class);

      RunningJob runningJob = JobClient.runJob(job);
      LOG.debug("Submitted generation job " + job.getJobName());
      runningJob.waitForCompletion();
      // Now, we write the value of the directory we dumped data to directly into the table ->
      // directory map for HiveStore to use in building temp tables.  This enables it to just
      // find the files we've created.
      DataSet clustered = new ClusteredDataSet(table.getCombinedSchema());
      clustered.setClusterLocation(new Path(hiveDumpDir));
      return clustered;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  static class ClusterDataGeneratorMap
      implements Mapper <LongWritable, Text, NullWritable, NullWritable> {
    private JobConf conf;

    @Override
    public void map(LongWritable key, Text value,
                    OutputCollector<NullWritable, NullWritable> outputCollector, Reporter reporter)
        throws IOException {
      try {
        FileSystem fs = FileSystem.get(conf);
        // Our info is in the Text value, reconstruct it from there.
        ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(
                Base64.decodeBase64(value.toString())));
        GeneratorInfo ginfo = (GeneratorInfo) ois.readObject();
        DataSet rows = ginfo.gen.generateData(ginfo.table, ginfo.scale, ginfo.pctNulls);

        // Deal with foreign key reconstruction if needed
        Path[] cached = DistributedCache.getLocalCacheFiles(conf);
        for (Path p : cached) {
          FSDataInputStream input = fs.open(cached[0]);
          BufferedReader in = new BufferedReader(new InputStreamReader(input));
          List<String> lines = new ArrayList<>();
          // The first line contains the foreign key entry number
          int entryNum = Integer.valueOf(in.readLine());
          String line;
          while ((line = in.readLine()) != null) lines.add(line);
          ginfo.table.getForeignKeys().get(entryNum).targetTable =
              new StringDataSet(ginfo.table.getCombinedSchema(), lines, "\u0001", "NULL");
          input.close();

        }

        // Now generate the data
        String filename = HiveStore.getFileForDump();
        Path file = new Path(ginfo.dumpDir, filename);
        FSDataOutputStream output = fs.create(file);
        HiveStore.writeToFile(output, rows);
      } catch (ClassNotFoundException e) {
        throw new IOException(e);
      }
    }

    @Override
    public void close() throws IOException {

    }

    @Override
    public void configure(JobConf jobConf) {
      conf = jobConf;
    }
  }

  static class GeneratorInfo implements Serializable {
    final int scale;
    final double[] pctNulls;
    final TestTable table;
    final DataGenerator gen;
    final String dumpDir;

    GeneratorInfo(int scale, double[] pctNulls, TestTable table, DataGenerator gen, String dumpDir) {
      this.scale = scale;
      this.pctNulls = pctNulls;
      this.table = table;
      this.gen = gen;
      this.dumpDir = dumpDir;
    }
  }
}
