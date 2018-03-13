/*
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
package org.apache.hadoop.hive.metastore.client;

import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.MetaStoreTestUtils;
import org.apache.hadoop.hive.metastore.annotation.MetastoreCheckinTest;
import org.apache.hadoop.hive.metastore.api.Catalog;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.NotNullConstraintsRequest;
import org.apache.hadoop.hive.metastore.api.SQLNotNullConstraint;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.client.builder.CatalogBuilder;
import org.apache.hadoop.hive.metastore.client.builder.DatabaseBuilder;
import org.apache.hadoop.hive.metastore.client.builder.SQLNotNullConstraintBuilder;
import org.apache.hadoop.hive.metastore.client.builder.TableBuilder;
import org.apache.hadoop.hive.metastore.minihms.AbstractMetaStoreService;
import org.apache.thrift.TApplicationException;
import org.apache.thrift.TException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.hadoop.hive.metastore.Warehouse.DEFAULT_CATALOG_NAME;
import static org.apache.hadoop.hive.metastore.Warehouse.DEFAULT_DATABASE_NAME;

@RunWith(Parameterized.class)
@Category(MetastoreCheckinTest.class)
public class TestNotNullConstraint {
  // Needed until there is no junit release with @BeforeParam, @AfterParam (junit 4.13)
  // https://github.com/junit-team/junit4/commit/1bf8438b65858565dbb64736bfe13aae9cfc1b5a
  // Then we should remove our own copy
  private static Set<AbstractMetaStoreService> metaStoreServices = null;
  private static final String OTHER_DATABASE = "test_uc_other_database";
  private static final String OTHER_CATALOG = "test_uc_other_catalog";
  private static final String DATABASE_IN_OTHER_CATALOG = "test_uc_database_in_other_catalog";
  private final AbstractMetaStoreService metaStore;
  private IMetaStoreClient client;
  private Table[] testTables = new Table[3];
  private Database inOtherCatalog;

  @Parameterized.Parameters(name = "{0}")
  public static List<Object[]> getMetaStoreToTest() throws Exception {
    List<Object[]> result = MetaStoreFactoryForTests.getMetaStores();
    metaStoreServices = result.stream()
        .map(test -> (AbstractMetaStoreService)test[1])
        .collect(Collectors.toSet());
    return result;
  }

  public TestNotNullConstraint(String name, AbstractMetaStoreService metaStore) throws Exception {
    this.metaStore = metaStore;
    this.metaStore.start();
  }

  // Needed until there is no junit release with @BeforeParam, @AfterParam (junit 4.13)
  // https://github.com/junit-team/junit4/commit/1bf8438b65858565dbb64736bfe13aae9cfc1b5a
  // Then we should move this to @AfterParam
  @AfterClass
  public static void stopMetaStores() throws Exception {
    for(AbstractMetaStoreService metaStoreService : metaStoreServices) {
      metaStoreService.stop();
    }
  }

  @Before
  public void setUp() throws Exception {
    // Get new client
    client = metaStore.getClient();

    // Clean up the database
    client.dropDatabase(DEFAULT_CATALOG_NAME, OTHER_DATABASE, true, true, true);
    // Drop every table in the default database
    for(String tableName : client.getAllTables(DEFAULT_CATALOG_NAME, DEFAULT_DATABASE_NAME)) {
      client.dropTable(DEFAULT_CATALOG_NAME, DEFAULT_DATABASE_NAME, tableName, true, true, true);
    }

    client.dropDatabase(OTHER_CATALOG, DATABASE_IN_OTHER_CATALOG, true, true, true);
    try {
      client.dropCatalog(OTHER_CATALOG);
    } catch (NoSuchObjectException e) {
      // NOP
    }

    // Clean up trash
    metaStore.cleanWarehouseDirs();

    client.createDatabase(new DatabaseBuilder().setName(OTHER_DATABASE).build());

    Catalog cat = new CatalogBuilder()
        .setName(OTHER_CATALOG)
        .setLocation(MetaStoreTestUtils.getTestWarehouseDir(OTHER_CATALOG))
        .build();
    client.createCatalog(cat);

    // For this one don't specify a location to make sure it gets put in the catalog directory
    inOtherCatalog = new DatabaseBuilder()
        .setName(DATABASE_IN_OTHER_CATALOG)
        .setCatalogName(OTHER_CATALOG)
        .build();
    client.createDatabase(inOtherCatalog);

    testTables[0] =
        new TableBuilder()
            .setTableName("test_table_1")
            .addCol("col1", "int")
            .addCol("col2", "varchar(32)")
            .build();

    testTables[1] =
        new TableBuilder()
            .setDbName(OTHER_DATABASE)
            .setTableName("test_table_2")
            .addCol("col1", "int")
            .addCol("col2", "varchar(32)")
            .build();

    testTables[2] =
        new TableBuilder()
            .inDb(inOtherCatalog)
            .setTableName("test_table_3")
            .addCol("col1", "int")
            .addCol("col2", "varchar(32)")
            .build();

    // Create the tables in the MetaStore
    for(int i=0; i < testTables.length; i++) {
      client.createTable(testTables[i]);
    }

    // Reload tables from the MetaStore
    for(int i=0; i < testTables.length; i++) {
      testTables[i] = client.getTable(testTables[i].getCatName(), testTables[i].getDbName(),
          testTables[i].getTableName());
    }
  }

  @After
  public void tearDown() throws Exception {
    try {
      if (client != null) {
        client.close();
      }
    } finally {
      client = null;
    }
  }

  @Test
  public void createGetDrop() throws TException {
    Table table = testTables[0];
    // Make sure get on a table with no key returns empty list
    NotNullConstraintsRequest rqst =
        new NotNullConstraintsRequest(table.getCatName(), table.getDbName(), table.getTableName());
    List<SQLNotNullConstraint> fetched = client.getNotNullConstraints(rqst);
    Assert.assertTrue(fetched.isEmpty());

    // Single column unnamed primary key in default catalog and database
    List<SQLNotNullConstraint> nn = new SQLNotNullConstraintBuilder()
        .onTable(table)
        .addColumn("col1")
        .build();
    client.addNotNullConstraint(nn);

    rqst = new NotNullConstraintsRequest(table.getCatName(), table.getDbName(), table.getTableName());
    fetched = client.getNotNullConstraints(rqst);
    Assert.assertEquals(1, fetched.size());
    Assert.assertEquals(table.getDbName(), fetched.get(0).getTable_db());
    Assert.assertEquals(table.getTableName(), fetched.get(0).getTable_name());
    Assert.assertEquals("col1", fetched.get(0).getColumn_name());
    Assert.assertEquals(table.getTableName() + "_not_null_constraint", fetched.get(0).getNn_name());
    String table0PkName = fetched.get(0).getNn_name();
    Assert.assertTrue(fetched.get(0).isEnable_cstr());
    Assert.assertFalse(fetched.get(0).isValidate_cstr());
    Assert.assertFalse(fetched.get(0).isRely_cstr());
    Assert.assertEquals(table.getCatName(), fetched.get(0).getCatName());

    // Drop a primary key
    client.dropConstraint(table.getCatName(), table.getDbName(),
        table.getTableName(), table0PkName);
    rqst = new NotNullConstraintsRequest(table.getCatName(), table.getDbName(), table.getTableName());
    fetched = client.getNotNullConstraints(rqst);
    Assert.assertTrue(fetched.isEmpty());

    // Make sure I can add it back
    client.addNotNullConstraint(nn);
  }

  @Test
  public void inOtherCatalog() throws TException {
    String constraintName = "ocuc";
    // Table in non 'hive' catalog
    List<SQLNotNullConstraint> nn = new SQLNotNullConstraintBuilder()
        .onTable(testTables[2])
        .addColumn("col1")
        .setConstraintName(constraintName)
        .build();
    client.addNotNullConstraint(nn);

    NotNullConstraintsRequest rqst = new NotNullConstraintsRequest(testTables[2].getCatName(),
        testTables[2].getDbName(), testTables[2].getTableName());
    List<SQLNotNullConstraint> fetched = client.getNotNullConstraints(rqst);
    Assert.assertEquals(1, fetched.size());
    Assert.assertEquals(testTables[2].getDbName(), fetched.get(0).getTable_db());
    Assert.assertEquals(testTables[2].getTableName(), fetched.get(0).getTable_name());
    Assert.assertEquals("col1", fetched.get(0).getColumn_name());
    Assert.assertEquals(constraintName, fetched.get(0).getNn_name());
    Assert.assertTrue(fetched.get(0).isEnable_cstr());
    Assert.assertFalse(fetched.get(0).isValidate_cstr());
    Assert.assertFalse(fetched.get(0).isRely_cstr());
    Assert.assertEquals(testTables[2].getCatName(), fetched.get(0).getCatName());

    client.dropConstraint(testTables[2].getCatName(), testTables[2].getDbName(),
        testTables[2].getTableName(), constraintName);
    rqst = new NotNullConstraintsRequest(testTables[2].getCatName(), testTables[2].getDbName(),
        testTables[2].getTableName());
    fetched = client.getNotNullConstraints(rqst);
    Assert.assertTrue(fetched.isEmpty());
  }

  @Test
  public void createTableWithConstraintsPk() throws TException {
    String constraintName = "ctwcuc";
    Table table = new TableBuilder()
        .setTableName("table_with_constraints")
        .addCol("col1", "int")
        .addCol("col2", "varchar(32)")
        .build();

    List<SQLNotNullConstraint> nn = new SQLNotNullConstraintBuilder()
        .onTable(table)
        .addColumn("col1")
        .setConstraintName(constraintName)
        .build();

    client.createTableWithConstraints(table, null, null, null, nn, null);
    NotNullConstraintsRequest rqst = new NotNullConstraintsRequest(table.getCatName(),
        table.getDbName(), table.getTableName());
    List<SQLNotNullConstraint> fetched = client.getNotNullConstraints(rqst);
    Assert.assertEquals(1, fetched.size());
    Assert.assertEquals(table.getDbName(), fetched.get(0).getTable_db());
    Assert.assertEquals(table.getTableName(), fetched.get(0).getTable_name());
    Assert.assertEquals("col1", fetched.get(0).getColumn_name());
    Assert.assertEquals(constraintName, fetched.get(0).getNn_name());
    Assert.assertTrue(fetched.get(0).isEnable_cstr());
    Assert.assertFalse(fetched.get(0).isValidate_cstr());
    Assert.assertFalse(fetched.get(0).isRely_cstr());
    Assert.assertEquals(table.getCatName(), fetched.get(0).getCatName());

    client.dropConstraint(table.getCatName(), table.getDbName(), table.getTableName(), constraintName);
    rqst = new NotNullConstraintsRequest(table.getCatName(), table.getDbName(), table.getTableName());
    fetched = client.getNotNullConstraints(rqst);
    Assert.assertTrue(fetched.isEmpty());

  }

  @Test
  public void createTableWithConstraintsPkInOtherCatalog() throws TException {
    Table table = new TableBuilder()
        .setTableName("table_in_other_catalog_with_constraints")
        .inDb(inOtherCatalog)
        .addCol("col1", "int")
        .addCol("col2", "varchar(32)")
        .build();

    List<SQLNotNullConstraint> nn = new SQLNotNullConstraintBuilder()
        .onTable(table)
        .addColumn("col1")
        .build();

    client.createTableWithConstraints(table, null, null, null, nn, null);
    NotNullConstraintsRequest rqst = new NotNullConstraintsRequest(table.getCatName(),
        table.getDbName(), table.getTableName());
    List<SQLNotNullConstraint> fetched = client.getNotNullConstraints(rqst);
    Assert.assertEquals(1, fetched.size());
    Assert.assertEquals(table.getDbName(), fetched.get(0).getTable_db());
    Assert.assertEquals(table.getTableName(), fetched.get(0).getTable_name());
    Assert.assertEquals("col1", fetched.get(0).getColumn_name());
    Assert.assertEquals(table.getTableName() + "_not_null_constraint", fetched.get(0).getNn_name());
    String tablePkName = fetched.get(0).getNn_name();
    Assert.assertTrue(fetched.get(0).isEnable_cstr());
    Assert.assertFalse(fetched.get(0).isValidate_cstr());
    Assert.assertFalse(fetched.get(0).isRely_cstr());
    Assert.assertEquals(table.getCatName(), fetched.get(0).getCatName());

    client.dropConstraint(table.getCatName(), table.getDbName(), table.getTableName(), tablePkName);
    rqst = new NotNullConstraintsRequest(table.getCatName(), table.getDbName(), table.getTableName());
    fetched = client.getNotNullConstraints(rqst);
    Assert.assertTrue(fetched.isEmpty());
  }

  @Test
  public void doubleAddNotNullConstraint() throws TException {
    Table table = testTables[0];
    // Make sure get on a table with no key returns empty list
    NotNullConstraintsRequest rqst =
        new NotNullConstraintsRequest(table.getCatName(), table.getDbName(), table.getTableName());
    List<SQLNotNullConstraint> fetched = client.getNotNullConstraints(rqst);
    Assert.assertTrue(fetched.isEmpty());

    // Single column unnamed primary key in default catalog and database
    List<SQLNotNullConstraint> nn = new SQLNotNullConstraintBuilder()
        .onTable(table)
        .addColumn("col1")
        .build();
    client.addNotNullConstraint(nn);

    try {
      nn = new SQLNotNullConstraintBuilder()
          .onTable(table)
          .addColumn("col2")
          .build();
      client.addNotNullConstraint(nn);
      Assert.fail();
    } catch (InvalidObjectException|TApplicationException e) {
      // NOP
    }
  }

  @Test
  public void addNoSuchTable() throws TException {
    try {
      List<SQLNotNullConstraint> nn = new SQLNotNullConstraintBuilder()
          .setTableName("nosuch")
          .addColumn("col2")
          .build();
      client.addNotNullConstraint(nn);
      Assert.fail();
    } catch (InvalidObjectException |TApplicationException e) {
      // NOP
    }
  }

  @Test
  public void getNoSuchTable() throws TException {
    NotNullConstraintsRequest rqst =
        new NotNullConstraintsRequest(DEFAULT_CATALOG_NAME, DEFAULT_DATABASE_NAME, "nosuch");
    List<SQLNotNullConstraint> nn = client.getNotNullConstraints(rqst);
    Assert.assertTrue(nn.isEmpty());
  }

  @Test
  public void getNoSuchDb() throws TException {
    NotNullConstraintsRequest rqst =
        new NotNullConstraintsRequest(DEFAULT_CATALOG_NAME, "nosuch", testTables[0].getTableName());
    List<SQLNotNullConstraint> nn = client.getNotNullConstraints(rqst);
    Assert.assertTrue(nn.isEmpty());
  }

  @Test
  public void getNoSuchCatalog() throws TException {
    NotNullConstraintsRequest rqst =
        new NotNullConstraintsRequest("nosuch", testTables[0].getDbName(), testTables[0].getTableName());
    List<SQLNotNullConstraint> nn = client.getNotNullConstraints(rqst);
    Assert.assertTrue(nn.isEmpty());
  }
}

