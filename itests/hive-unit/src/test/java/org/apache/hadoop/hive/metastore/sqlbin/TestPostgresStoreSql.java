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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hadoop.hive.metastore.sqlbin;

import org.apache.hadoop.hive.cli.CliSessionState;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.ql.CommandNeedRetryException;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;
import org.apache.hadoop.hive.ql.security.SessionStateConfigUserAuthenticator;
import org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd
    .SQLStdHiveAuthorizerFactoryForTest;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class TestPostgresStoreSql {
  // If this property is defined, we'll know that the user intends to connect to an external
  // postgres, all tests will be ignored.
  private static PostgresStore store;
  // If you create any tables with partitions, you must add the corresponding partition table into
  // this list so that it gets dropped at test start time.
  private static List<String> tablesToDrop = new ArrayList<>();

  private static HiveConf conf;

  private Driver driver;

  @BeforeClass
  public static void connect() throws MetaException, SQLException {
    conf = new HiveConf();
    conf.setVar(HiveConf.ConfVars.DYNAMICPARTITIONINGMODE, "nonstrict");
    conf.setBoolVar(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY, false);
    // Setup so we can test SQL standard auth
    conf.setBoolVar(HiveConf.ConfVars.HIVE_TEST_AUTHORIZATION_SQLSTD_HS2_MODE, true);
    conf.setVar(HiveConf.ConfVars.HIVE_AUTHORIZATION_MANAGER,
        SQLStdHiveAuthorizerFactoryForTest.class.getName());
    conf.setVar(HiveConf.ConfVars.HIVE_AUTHENTICATOR_MANAGER,
        SessionStateConfigUserAuthenticator.class.getName());
    conf.setBoolVar(HiveConf.ConfVars.HIVE_AUTHORIZATION_ENABLED, true);
    conf.setVar(HiveConf.ConfVars.USERS_IN_ADMIN_ROLE, System.getProperty("user.name"));
    conf.setVar(HiveConf.ConfVars.HIVEMAPREDMODE,"nonstrict");
    store = PostgresKeyValue.connectForTest(conf, tablesToDrop);
  }

  @AfterClass
  public static void cleanup() throws SQLException {
    PostgresKeyValue.cleanupAfterTest(store, tablesToDrop);
  }

  @Before
  public void checkExternalPostgres() {
    Assume.assumeNotNull(System.getProperty(PostgresKeyValue.TEST_POSTGRES_JDBC));
    SessionState.start(new CliSessionState(conf));
    driver = new Driver(conf);
  }

  @Test
  public void query55() throws CommandNeedRetryException {
    String dbName = "query55_db";
    CommandProcessorResponse response = driver.run("drop database if exists " + dbName);
    Assert.assertEquals(response.getErrorMessage(), 0, response.getResponseCode());
    response = driver.run("create database if not exists " + dbName);
    Assert.assertEquals(response.getErrorMessage(), 0, response.getResponseCode());
  }

}
