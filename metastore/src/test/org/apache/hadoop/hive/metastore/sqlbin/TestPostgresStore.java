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
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.*;
import org.junit.*;

import java.sql.SQLException;
import java.util.*;

public class TestPostgresStore {

  // If this property is defined, we'll know that the user intends to connect to an external
  // postgres, all tests will be ignored.
  private static final String POSTGRES_JDBC = "hive.test.posgres.jdbc";
  private static final String POSTGRES_USER = "hive.test.posgres.user";
  private static final String POSTGRES_PASSWD = "hive.test.posgres.password";
  private static PostgresStore store;
  // If you create any tables with partitions, you must add the corresponding partition table into
  // this list so that it gets dropped at test start time.
  private static List<String> tablesToDrop = new ArrayList<>();

  @BeforeClass
  public static void connect() throws MetaException, SQLException {
    tablesToDrop.add(PostgresKeyValue.DB_TABLE.getName());
    tablesToDrop.add(PostgresKeyValue.TABLE_TABLE.getName());
    tablesToDrop.add(PostgresKeyValue.FUNC_TABLE.getName());
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
      conf.set(PostgresKeyValue.CACHE_OFF, "true");

      store = new PostgresStore();
      store.setConf(conf);
    }

  }

  @AfterClass
  public static void cleanup() throws SQLException {
    PostgresKeyValue psql = store.connectionForTest();
    try {
      psql.begin();
      for (String table : tablesToDrop) {
        try {
          psql.dropPostgresTable(table);
        } catch (SQLException e) {
          // Ignore it, as it likely just means we haven't created the tables previously
        }
      }
    } finally {
      psql.commit();
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

  @Test
  public void tables() throws InvalidObjectException, MetaException {
    List<FieldSchema> cols = Collections.singletonList(
        new FieldSchema("a", "varchar(32)", "")
    );
    SerDeInfo serde = new SerDeInfo("serde", "serde", Collections.<String, String>emptyMap());
    StorageDescriptor sd = new StorageDescriptor(cols, "file:/tmp/tbl1", "inputformat",
        "outputformat", false, 0, serde, null, null, Collections.<String, String>emptyMap());
    Table table = new Table("tbl1", "default", "me", 1, 2, 3, sd, null,
        Collections.<String, String>emptyMap(), null, null, TableType.MANAGED_TABLE.name());
    store.createTable(table);

    table = store.getTable("default", "tbl1");

    // I assume thrift de/serialization works, so I'm not going to check every field.
    Assert.assertEquals("tbl1", table.getTableName());
    Assert.assertEquals("default", table.getDbName());
    Assert.assertEquals("me", table.getOwner());
    cols = table.getSd().getCols();
    Assert.assertEquals(1, cols.size());
    Assert.assertEquals("a", cols.get(0).getName());

  }

  @Test
  public void functions() throws InvalidObjectException, MetaException {
    Function count = new Function("count", "default", "o.a.h.count", "me", PrincipalType.ROLE, 1,
        FunctionType.JAVA, Collections.<ResourceUri>emptyList());
    Function sum = new Function("sum", "default", "o.a.h.count", "me", PrincipalType.ROLE, 1,
        FunctionType.JAVA, Collections.<ResourceUri>emptyList());
    store.createFunction(count);
    store.createFunction(sum);

    List<Function> funcs = store.getAllFunctions();
    Assert.assertEquals(2, funcs.size());
    Assert.assertTrue(
        (funcs.get(0).getFunctionName().equals("count") &&
            funcs.get(1).getFunctionName().equals("sum")) ||
        (funcs.get(0).getFunctionName().equals("sum") &&
            funcs.get(1).getFunctionName().equals("count")));
  }
}
