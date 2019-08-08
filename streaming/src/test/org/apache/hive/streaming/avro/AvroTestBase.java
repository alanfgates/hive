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
package org.apache.hive.streaming.avro;

import org.apache.hadoop.hive.cli.CliSessionState;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.txn.TxnDbUtil;
import org.apache.hadoop.hive.ql.DriverFactory;
import org.apache.hadoop.hive.ql.IDriver;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hive.streaming.TestStreaming;
import org.junit.After;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

class AvroTestBase {
  private static final Logger LOG = LoggerFactory.getLogger(AvroTestBase.class);
  protected static final String dbName = "avrostreamingdb";

  protected static HiveConf conf = null;
  protected IDriver driver;
  protected final IMetaStoreClient msClient;

  public AvroTestBase() throws Exception {
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

  protected void dropAndCreateTable(String fullTableName, String schema) throws IOException {
    dropAndCreateTable(fullTableName, schema, null);
  }

  protected void dropAndCreateTable(String fullTableName, String schema, String partCols) throws IOException {
    execSql("drop table if exists " + fullTableName);
    StringBuilder buf = new StringBuilder("create table ")
        .append(fullTableName)
        .append(" (")
        .append(schema)
        .append(")");
    if (partCols != null) {
      buf.append(" partitioned by (")
          .append(partCols)
          .append(')');

    }
    buf.append(" stored as orc TBLPROPERTIES('transactional'='true')");
    execSql(buf.toString());
  }

  protected void execSql(String sql) throws IOException {
    runSql(sql, false);
  }

  protected List<String> querySql(String sql) throws IOException {
    return runSql(sql, true);
  }

  protected List<String> runSql(String sql, boolean expectResults) throws IOException {
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
