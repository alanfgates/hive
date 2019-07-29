/*
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
package org.apache.hive.streaming;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.hadoop.hive.cli.CliSessionState;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.txn.TxnDbUtil;
import org.apache.hadoop.hive.ql.DriverFactory;
import org.apache.hadoop.hive.ql.IDriver;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class TestAvroWriter {
  private static final Logger LOG = LoggerFactory.getLogger(TestAvroWriter.class);
  private static final String dbName = "avrostreamingdb";

  private static HiveConf conf = null;
  private IDriver driver;
  private final IMetaStoreClient msClient;

  public TestAvroWriter() throws Exception {
    conf = new HiveConf(this.getClass());
    conf.set("fs.raw.impl", TestStreaming.RawFileSystem.class.getName());
    conf.setVar(HiveConf.ConfVars.HIVE_AUTHORIZATION_MANAGER,
        "org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactory");
    TxnDbUtil.setConfValues(conf);
    conf.setBoolVar(HiveConf.ConfVars.METASTORE_EXECUTE_SET_UGI, true);
    conf.setBoolVar(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY, true);


    //1) Start from a clean slate (metastore)
    TxnDbUtil.cleanDb(conf);
    TxnDbUtil.prepDb(conf);

    //2) obtain metastore clients
    msClient = new HiveMetaStoreClient(conf);
  }

  @Before
  public void setup() throws IOException {
    SessionState.start(new CliSessionState(conf));
    driver = DriverFactory.newDriver(conf);
    driver.setMaxRows(200002);//make sure Driver returns all results
    // drop and recreate the necessary databases and tables
    execSql("drop database if exists " + dbName + " cascade");
    execSql("create database " + dbName);
  }

  @After
  public void cleanup() {
    msClient.close();
    driver.close();
  }

  @Test
  public void withSchemaObject() throws StreamingException, IOException {
    Schema schema = SchemaBuilder.record("wsoasw")
                                   .namespace("org.apache.hive.streaming")
                                   .fields()
                                     .requiredString("field1")
                                     .requiredInt("field2")
                                 .endRecord();
    List<GenericRecord> records = new ArrayList<>();
    GenericRecordBuilder recordBuilder = new GenericRecordBuilder(schema);
    for (int i = 0; i < 10; i++) {
      recordBuilder.set("field1", Integer.toString(i));
      recordBuilder.set("field2", i);
      records.add(recordBuilder.build());
    }

    String tableName = "wso";
    String fullTableName = dbName + "." + tableName;

    execSql("drop table if exists " + fullTableName);
    execSql("create table " + fullTableName + " (f1 string, f2 int) stored as orc TBLPROPERTIES('transactional'='true')");

    RecordWriter writer = AvroWriter.newBuilder()
                                    .withSchema(schema)
                                    .build();

    HiveStreamingConnection conn = HiveStreamingConnection.newBuilder()
        .withDatabase(dbName)
        .withTable(tableName)
        .withTransactionBatchSize(5)
        .withRecordWriter(writer)
        .withHiveConf(conf)
        .connect();

    conn.beginTransaction();
    for (GenericRecord r : records) conn.write(r);
    conn.commitTransaction();
    conn.close();

    List<String> results = querySql("select f1, f2 from " + fullTableName);
    Assert.assertEquals(10, results.size());
    for (int i = 0; i < 10; i++) Assert.assertEquals(i + "\t" + i, results.get(i));
  }

  // test with schema object
  // test with schema string
  // test with different schema than in record
  // test with no schema
  // test with two schemas
  // test with all avro types
  // test with nullable unions

  private void execSql(String sql) throws IOException {
    runSql(sql, false);
  }

  private List<String> querySql(String sql) throws IOException {
    return runSql(sql, true);
  }

  private List<String> runSql(String sql, boolean expectResults) throws IOException {
    LOG.debug("Going to run: " + sql);
    CommandProcessorResponse cpr = driver.run(sql);
    if (cpr.getResponseCode() != 0) {
      throw new RuntimeException("Failed to run statement <" + sql + ">: " + cpr);
    }
    if (expectResults) {
      List<String> results = new ArrayList<>();
      driver.getResults(results);
      return results;
    } else {
      return Collections.emptyList();
    }
  }
}
