/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.metastore.sqlbin;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.RawStore;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Collections;

public class TestPostgresStore {

  // If this property is defined, we'll know that the user intends to connect to an external
  // postgres, all tests will be ignored.
  private static final String POSTGRES_JDBC = "hive.test.posgres.jdbc";
  private static final String POSTGRES_USER = "hive.test.posgres.user";
  private static final String POSTGRES_PASSWD = "hive.test.posgres.password";
  private static RawStore store;

  @BeforeClass
  public static void connect() throws MetaException {
    String jdbc = System.getProperty(POSTGRES_JDBC);
    if (jdbc != null) {
      HiveConf conf = new HiveConf();
      conf.setVar(HiveConf.ConfVars.METASTORE_RAW_STORE_IMPL,
          PostgresStore.class.getCanonicalName());
      conf.setVar(HiveConf.ConfVars.METASTORECONNECTURLKEY, jdbc);
      String user = System.getProperty(POSTGRES_USER);
      if (user == null) user = "hive";
      conf.setVar(HiveConf.ConfVars.METASTORE_CONNECTION_USER_NAME, user);
      String passwd = System.getProperty(POSTGRES_PASSWD);
      if (passwd == null) passwd = "hive";
      conf.setVar(HiveConf.ConfVars.METASTOREPWD, passwd);

      store = new PostgresStore();
      store.setConf(conf);
    }

  }

  @Before
  public void checkExternalPostgres() {
    Assume.assumeNotNull(System.getProperty(POSTGRES_JDBC));
  }

  @Test
  public void dbs() throws InvalidObjectException, MetaException, NoSuchObjectException {
    Database db = new Database("dbtest1", "description", "file:/somewhere",
        Collections.<String, String>emptyMap());
    store.createDatabase(db);

    db = store.getDatabase("dbtest1");

    Assert.assertNotNull(db);
    Assert.assertEquals("dbtest1", db.getName());
    Assert.assertEquals("description", db.getDescription());
    Assert.assertEquals("file:/somewhere", db.getLocationUri());
    Assert.assertEquals(0, db.getParametersSize());
  }
}
