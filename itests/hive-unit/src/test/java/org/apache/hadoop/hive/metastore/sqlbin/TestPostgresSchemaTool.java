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
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Function;
import org.apache.hadoop.hive.metastore.api.FunctionType;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class TestPostgresSchemaTool extends TestPostgresStoreSql {

  private static PostgresStore store;
  // If you create any tables with partitions, you must add the corresponding partition table into
  // this list so that it gets dropped at test start time.
  private static List<String> tablesToDrop = new ArrayList<>();
  private PostgresSchemaTool tool;
  private ByteArrayOutputStream baos;

  @BeforeClass
  public static void createObjects() throws InvalidObjectException, MetaException {
    if (System.getProperty(PostgresKeyValue.TEST_POSTGRES_JDBC) != null) {
      store = PostgresKeyValue.connectForTest(new HiveConf(), tablesToDrop);
      assert store != null;
      String[] dbNames = {"mydb1", "mydb2"};
      String[] tableNames = {"mytable1", "mytable2"};
      String[] funcNames = {"myfunc1", "myfunc2"};
      String[] partVals = {"pval1", "pval2"};

      for (String dbName : dbNames) {
        Database db = new Database(dbName, "no description", "file:///tmp",
            Collections.<String, String>emptyMap());
        store.createDatabase(db);
      }

      StorageDescriptor sd = new StorageDescriptor(Arrays.asList(new FieldSchema("col1", "int",
          ""), new FieldSchema("col2", "varchar(32)", "")),
          "/tmp", null, null, false, 0, null, null, null, Collections.<String, String>emptyMap());
      for (String tableName : tableNames) {
        Table tab = new Table(tableName, dbNames[0], "me", 0, 0, 0, sd,
            Collections.singletonList(new FieldSchema("pcol1", "string", "")),
            Collections.<String, String>emptyMap(), null, null, null);
        store.createTable(tab);
        tablesToDrop.add(PostgresKeyValue.buildPartTableName(dbNames[0], tableName));
      }

      for (String funcName : funcNames) {
        Function function = new Function(funcName, dbNames[0], "Function", "me",
            PrincipalType.USER, 0, FunctionType.JAVA, null);
        store.createFunction(function);
      }

      for (String partVal : partVals) {
        Partition p = new Partition(Collections.singletonList(partVal), dbNames[0], tableNames[0], 0,

            0, sd, Collections.<String, String>emptyMap());
        store.addPartition(p);
      }
    }
  }

  @Before
  public void prepOutput() {
    Assume.assumeNotNull(System.getProperty(PostgresKeyValue.TEST_POSTGRES_JDBC));
    tool = new PostgresSchemaTool();
    baos = new ByteArrayOutputStream();
    tool.out = new PrintStream(baos);
  }

  @Test
  public void listDatabases() throws TException, UnsupportedEncodingException {
    tool.run(tool.parse("-d"));
    Assert.assertEquals("", baos.toString());
  }
}
