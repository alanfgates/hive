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

package org.apache.hadoop.hive.metastore.client;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.MetaStoreTestUtils;
import org.apache.hadoop.hive.metastore.annotation.MetastoreCheckinTest;
import org.apache.hadoop.hive.metastore.api.Catalog;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.client.builder.CatalogBuilder;
import org.apache.hadoop.hive.metastore.client.builder.DatabaseBuilder;
import org.apache.hadoop.hive.metastore.client.builder.PartitionBuilder;
import org.apache.hadoop.hive.metastore.client.builder.TableBuilder;
import org.apache.hadoop.hive.metastore.minihms.AbstractMetaStoreService;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;

import com.google.common.collect.Lists;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static junit.framework.TestCase.assertNotNull;
import static junit.framework.TestCase.assertNull;
import static org.apache.hadoop.hive.metastore.Warehouse.DEFAULT_CATALOG_NAME;
import static org.apache.hadoop.hive.metastore.Warehouse.DEFAULT_DATABASE_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * API tests for HMS client's getPartitions methods.
 */
@RunWith(Parameterized.class)
@Category(MetastoreCheckinTest.class)
public class TestGetPartitions extends MetaStoreClientTest {
  private AbstractMetaStoreService metaStore;
  private IMetaStoreClient client;

  private static final String DB_NAME = "testpartdb";
  private static final String TABLE_NAME = "testparttable";

  public TestGetPartitions(String name, AbstractMetaStoreService metaStore) {
    this.metaStore = metaStore;
  }

  @Before
  public void setUp() throws Exception {
    // Get new client
    client = metaStore.getClient();

    // Clean up the database
    client.dropDatabase(DEFAULT_CATALOG_NAME, DB_NAME, true, true, true);
    createDB(DB_NAME);
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

  private void createDB(String dbName) throws TException {
    Database db = new DatabaseBuilder().
            setName(dbName).
            build();
    client.createDatabase(db);
  }


  private static Table createTestTable(IMetaStoreClient client, String dbName, String tableName,
                                       List<String> partCols, boolean setPartitionLevelPrivilages)
          throws TException {
    TableBuilder builder = new TableBuilder()
            .setDbName(dbName)
            .setTableName(tableName)
            .addCol("id", "int")
            .addCol("name", "string");

    partCols.forEach(col -> builder.addPartCol(col, "string"));
    Table table = builder.build();

    if (setPartitionLevelPrivilages) {
      table.putToParameters("PARTITION_LEVEL_PRIVILEGE", "true");
    }

    client.createTable(table);
    return table;
  }

  private static void addPartition(IMetaStoreClient client, Table table, List<String> values)
          throws TException {
    PartitionBuilder partitionBuilder = new PartitionBuilder().inTable(table);
    values.forEach(val -> partitionBuilder.addValue(val));
    client.add_partition(partitionBuilder.build());
  }

  private static void createTable3PartCols1PartGeneric(IMetaStoreClient client, boolean authOn)
          throws TException {
    Table t = createTestTable(client, DB_NAME, TABLE_NAME, Lists.newArrayList("yyyy", "mm",
            "dd"), authOn);
    addPartition(client, t, Lists.newArrayList("1997", "05", "16"));
  }

  private static void createTable3PartCols1Part(IMetaStoreClient client) throws TException {
    createTable3PartCols1PartGeneric(client, false);
  }

  private static void createTable3PartCols1PartAuthOn(IMetaStoreClient client) throws TException {
    createTable3PartCols1PartGeneric(client, true);
  }

  private static List<List<String>> createTable4PartColsParts(IMetaStoreClient client) throws
          Exception {
    Table t = createTestTable(client, DB_NAME, TABLE_NAME, Lists.newArrayList("yyyy", "mm", "dd"),
            false);
    List<List<String>> testValues = Lists.newArrayList(
            Lists.newArrayList("1999", "01", "02"),
            Lists.newArrayList("2009", "02", "10"),
            Lists.newArrayList("2017", "10", "26"),
            Lists.newArrayList("2017", "11", "27"));

    for(List<String> vals : testValues){
      addPartition(client, t, vals);
    }

    return testValues;
  }

  private static void assertAuthInfoReturned(String user, String group, Partition partition) {
    assertNotNull(partition.getPrivileges());
    assertEquals(Lists.newArrayList(),
            partition.getPrivileges().getUserPrivileges().get(user));
    assertEquals(Lists.newArrayList(),
            partition.getPrivileges().getGroupPrivileges().get(group));
    assertEquals(Lists.newArrayList(),
            partition.getPrivileges().getRolePrivileges().get("public"));
  }



  /**
   * Testing getPartition(String,String,String) ->
   *         get_partition_by_name(String,String,String).
   */
  @Test
  public void testGetPartition() throws Exception {
    createTable3PartCols1Part(client);
    Partition partition = client.getPartition(DEFAULT_CATALOG_NAME, DB_NAME, TABLE_NAME, "yyyy=1997/mm=05/dd=16");
    assertNotNull(partition);
    assertEquals(Lists.newArrayList("1997", "05", "16"), partition.getValues());
  }

  @Test(expected = NoSuchObjectException.class)
  public void testGetPartitionCaseSensitive() throws Exception {
    createTable3PartCols1Part(client);
    client.getPartition(DEFAULT_CATALOG_NAME, DB_NAME, TABLE_NAME, "YyYy=1997/mM=05/dD=16");
  }

  @Test(expected = NoSuchObjectException.class)
  public void testGetPartitionIncompletePartName() throws Exception {
    createTable3PartCols1Part(client);
    client.getPartition(DEFAULT_CATALOG_NAME, DB_NAME, TABLE_NAME, "yyyy=1997/mm=05");
  }

  @Test(expected = MetaException.class)
  public void testGetPartitionEmptyPartName() throws Exception {
    createTable3PartCols1Part(client);
    client.getPartition(DEFAULT_CATALOG_NAME, DB_NAME, TABLE_NAME, "");
  }

  @Test(expected = NoSuchObjectException.class)
  public void testGetPartitionNonexistingPart() throws Exception {
    createTable3PartCols1Part(client);
    client.getPartition(DEFAULT_CATALOG_NAME, DB_NAME, TABLE_NAME, "yyyy=1997/mm=05/dd=99");
  }

  @Test(expected = NoSuchObjectException.class)
  public void testGetPartitionNoDbName() throws Exception {
    createTable3PartCols1Part(client);
    client.getPartition(DEFAULT_CATALOG_NAME, "", TABLE_NAME, "yyyy=1997/mm=05/dd=16");
  }

  @Test(expected = NoSuchObjectException.class)
  public void testGetPartitionNoTblName() throws Exception {
    createTable3PartCols1Part(client);
    client.getPartition(DEFAULT_CATALOG_NAME, DB_NAME, "", "yyyy=1997/mm=05/dd=16");
  }

  @Test(expected = NoSuchObjectException.class)
  public void testGetPartitionNoTable() throws Exception {
    client.getPartition(DEFAULT_CATALOG_NAME, DB_NAME, TABLE_NAME, "yyyy=1997/mm=05/dd=16");
  }

  @Test(expected = NoSuchObjectException.class)
  public void testGetPartitionNoDb() throws Exception {
    client.dropDatabase(DEFAULT_CATALOG_NAME, DB_NAME);
    client.getPartition(DEFAULT_CATALOG_NAME, DB_NAME, TABLE_NAME, "yyyy=1997/mm=05/dd=16");
  }

  @Test(expected = MetaException.class)
  public void testGetPartitionNullDbName() throws Exception {
    createTable3PartCols1Part(client);
    client.getPartition(DEFAULT_CATALOG_NAME, null, TABLE_NAME, "yyyy=1997/mm=05/dd=16");
  }

  @Test(expected = MetaException.class)
  public void testGetPartitionNullTblName() throws Exception {
    createTable3PartCols1Part(client);
    client.getPartition(DEFAULT_CATALOG_NAME, DB_NAME, null, "yyyy=1997/mm=05/dd=16");
  }

  @Test(expected = MetaException.class)
  public void testGetPartitionNullPartName() throws Exception {
    createTable3PartCols1Part(client);
    client.getPartition(DEFAULT_CATALOG_NAME, DB_NAME, TABLE_NAME, (String)null);
  }



  /**
   * Testing getPartition(String,String,List(String)) ->
   *         get_partition(String,String,List(String)).
   */
  @Test
  public void testGetPartitionByValues() throws Exception {
    createTable3PartCols1Part(client);
    List<String> parts = Lists.newArrayList("1997", "05", "16");
    Partition partition = client.getPartition(DEFAULT_CATALOG_NAME, DB_NAME, TABLE_NAME, parts);
    assertNotNull(partition);
    assertEquals(parts, partition.getValues());
  }

  @Test(expected = NoSuchObjectException.class)
  public void testGetPartitionByValuesWrongPart() throws Exception {
    createTable3PartCols1Part(client);
    client.getPartition(DEFAULT_CATALOG_NAME, DB_NAME, TABLE_NAME, Lists.newArrayList("1997", "05", "66"));
  }

  @Test(expected = MetaException.class)
  public void testGetPartitionByValuesWrongNumOfPartVals() throws Exception {
    createTable3PartCols1Part(client);
    client.getPartition(DEFAULT_CATALOG_NAME, DB_NAME, TABLE_NAME, Lists.newArrayList("1997", "05"));
  }

  @Test(expected = MetaException.class)
  public void testGetPartitionByValuesEmptyPartVals() throws Exception {
    createTable3PartCols1Part(client);
    client.getPartition(DEFAULT_CATALOG_NAME, DB_NAME, TABLE_NAME, Lists.newArrayList());
  }

  @Test(expected = NoSuchObjectException.class)
  public void testGetPartitionByValuesNoDbName() throws Exception {
    createTable3PartCols1Part(client);
    client.getPartition(DEFAULT_CATALOG_NAME, "", TABLE_NAME, Lists.newArrayList("1997", "05", "16"));
  }

  @Test(expected = NoSuchObjectException.class)
  public void testGetPartitionByValuesNoTblName() throws Exception {
    createTable3PartCols1Part(client);
    client.getPartition(DEFAULT_CATALOG_NAME, DB_NAME, "", Lists.newArrayList("1997", "05", "16"));
  }

  @Test(expected = NoSuchObjectException.class)
  public void testGetPartitionByValuesNoTable() throws Exception {
    client.getPartition(DEFAULT_CATALOG_NAME, DB_NAME, TABLE_NAME, Lists.newArrayList("1997", "05", "16"));
  }

  @Test(expected = NoSuchObjectException.class)
  public void testGetPartitionByValuesNoDb() throws Exception {
    client.dropDatabase(DEFAULT_CATALOG_NAME, DB_NAME);
    client.getPartition(DEFAULT_CATALOG_NAME, DB_NAME, TABLE_NAME, Lists.newArrayList("1997", "05", "16"));
  }

  @Test(expected = MetaException.class)
  public void testGetPartitionByValuesNullDbName() throws Exception {
    createTable3PartCols1Part(client);
    client.getPartition(DEFAULT_CATALOG_NAME, null, TABLE_NAME, Lists.newArrayList("1997", "05", "16"));
  }

  @Test(expected = MetaException.class)
  public void testGetPartitionByValuesNullTblName() throws Exception {
    createTable3PartCols1Part(client);
    client.getPartition(DEFAULT_CATALOG_NAME, DB_NAME, null, Lists.newArrayList("1997", "05", "16"));
  }

  @Test(expected = MetaException.class)
  public void testGetPartitionByValuesNullValues() throws Exception {
    createTable3PartCols1Part(client);
    client.getPartition(DEFAULT_CATALOG_NAME, DB_NAME, TABLE_NAME, (List<String>)null);
  }



  /**
   * Testing getPartitionsByNames(String,String,List(String)) ->
   *         get_partitions_by_names(String,String,List(String)).
   */
  @Test
  public void testGetPartitionsByNames() throws Exception {
    List<List<String>> testValues = createTable4PartColsParts(client);

    //TODO: partition names in getPartitionsByNames are not case insensitive
    List<Partition> partitions = client.getPartitionsByNames(DEFAULT_CATALOG_NAME, DB_NAME, TABLE_NAME,
            Lists.newArrayList("yYYy=2017/MM=11/DD=27", "yYyY=1999/mM=01/dD=02"));
    assertEquals(0, partitions.size());

    partitions = client.getPartitionsByNames(DEFAULT_CATALOG_NAME, DB_NAME, TABLE_NAME,
            Lists.newArrayList("yyyy=2017/mm=11/dd=27", "yyyy=1999/mm=01/dd=02"));
    assertEquals(2, partitions.size());
    assertEquals(testValues.get(0), partitions.get(0).getValues());
    assertEquals(testValues.get(3), partitions.get(1).getValues());


    partitions = client.getPartitionsByNames(DEFAULT_CATALOG_NAME, DB_NAME, TABLE_NAME,
            Lists.newArrayList("yyyy=2017", "yyyy=1999/mm=01/dd=02"));
    assertEquals(testValues.get(0), partitions.get(0).getValues());
  }

  @Test
  public void testGetPartitionsByNamesEmptyParts() throws Exception {
    List<List<String>> testValues = createTable4PartColsParts(client);

    List<Partition> partitions = client.getPartitionsByNames(DEFAULT_CATALOG_NAME, DB_NAME, TABLE_NAME,
            Lists.newArrayList("", ""));
    assertEquals(0, partitions.size());

    partitions = client.getPartitionsByNames(DEFAULT_CATALOG_NAME, DB_NAME, TABLE_NAME,
            Lists.newArrayList());
    assertEquals(0, partitions.size());
  }

  @Test(expected = NoSuchObjectException.class)
  public void testGetPartitionsByNamesNoDbName() throws Exception {
    createTable3PartCols1Part(client);
    client.getPartitionsByNames(DEFAULT_CATALOG_NAME, "", TABLE_NAME, Lists.newArrayList("yyyy=2000/mm=01/dd=02"));
  }

  @Test(expected = NoSuchObjectException.class)
  public void testGetPartitionsByNamesNoTblName() throws Exception {
    createTable3PartCols1Part(client);
    client.getPartitionsByNames(DEFAULT_CATALOG_NAME, DB_NAME, "", Lists.newArrayList("yyyy=2000/mm=01/dd=02"));
  }

  @Test(expected = NoSuchObjectException.class)
  public void testGetPartitionsByNamesNoTable() throws Exception {
    client.getPartitionsByNames(DEFAULT_CATALOG_NAME, DB_NAME, TABLE_NAME, Lists.newArrayList("yyyy=2000/mm=01/dd=02"));
  }

  @Test(expected = NoSuchObjectException.class)
  public void testGetPartitionsByNamesNoDb() throws Exception {
    client.dropDatabase(DEFAULT_CATALOG_NAME, DB_NAME);
    client.getPartitionsByNames(DEFAULT_CATALOG_NAME, DB_NAME, TABLE_NAME, Lists.newArrayList("yyyy=2000/mm=01/dd=02"));
  }

  @Test
  public void testGetPartitionsByNamesNullDbName() throws Exception {
    try {
      createTable3PartCols1Part(client);
      client.getPartitionsByNames(DEFAULT_CATALOG_NAME, null, TABLE_NAME, Lists.newArrayList("yyyy=2000/mm=01/dd=02"));
      fail("Should have thrown exception");
    } catch (NullPointerException | TTransportException e) {
      //TODO: should not throw different exceptions for different HMS deployment types
    }
  }

  @Test
  public void testGetPartitionsByNamesNullTblName() throws Exception {
    try {
      createTable3PartCols1Part(client);
      client.getPartitionsByNames(DEFAULT_CATALOG_NAME, DB_NAME, null, Lists.newArrayList("yyyy=2000/mm=01/dd=02"));
      fail("Should have thrown exception");
    } catch (NullPointerException | TTransportException e) {
      //TODO: should not throw different exceptions for different HMS deployment types
    }
  }

  @Test(expected = MetaException.class)
  public void testGetPartitionsByNamesNullNames() throws Exception {
    createTable3PartCols1Part(client);
    client.getPartitionsByNames(DEFAULT_CATALOG_NAME, DB_NAME, TABLE_NAME, (List<String>)null);
  }



  /**
   * Testing getPartitionWithAuthInfo(String,String,List(String),String,List(String)) ->
   *         get_partition_with_auth(String,String,List(String),String,List(String)).
   */
  @Test
  public void testGetPartitionWithAuthInfoNoPrivilagesSet() throws Exception {
    createTable3PartCols1Part(client);
    Partition partition = client.getPartitionWithAuthInfo(DEFAULT_CATALOG_NAME, DB_NAME, TABLE_NAME, Lists.newArrayList(
            "1997", "05", "16"), "", Lists.newArrayList());
    assertNotNull(partition);
    assertNull(partition.getPrivileges());
  }

  @Test
  public void testGetPartitionWithAuthInfo() throws Exception {
    createTable3PartCols1PartAuthOn(client);
    Partition partition = client.getPartitionWithAuthInfo(DEFAULT_CATALOG_NAME, DB_NAME, TABLE_NAME,
            Lists.newArrayList("1997", "05", "16"), "user0", Lists.newArrayList("group0"));
    assertNotNull(partition);
    assertAuthInfoReturned("user0", "group0", partition);
  }

  @Test
  public void testGetPartitionWithAuthInfoEmptyUserGroup() throws Exception {
    createTable3PartCols1PartAuthOn(client);
    Partition partition = client.getPartitionWithAuthInfo(DEFAULT_CATALOG_NAME, DB_NAME, TABLE_NAME,
            Lists.newArrayList("1997", "05", "16"), "", Lists.newArrayList(""));
    assertNotNull(partition);
    assertAuthInfoReturned("", "", partition);
  }

  @Test(expected = NoSuchObjectException.class)
  public void testGetPartitionWithAuthInfoNoDbName() throws Exception {
    createTable3PartCols1PartAuthOn(client);
    client.getPartitionWithAuthInfo(DEFAULT_CATALOG_NAME, "", TABLE_NAME,
            Lists.newArrayList("1997", "05", "16"), "user0", Lists.newArrayList("group0"));
  }

  @Test(expected = NoSuchObjectException.class)
  public void testGetPartitionWithAuthInfoNoTblName() throws Exception {
    createTable3PartCols1PartAuthOn(client);
    client.getPartitionWithAuthInfo(DEFAULT_CATALOG_NAME, DB_NAME, "",
            Lists.newArrayList("1997", "05", "16"), "user0", Lists.newArrayList("group0"));
  }

  @Test(expected = NoSuchObjectException.class)
  public void testGetPartitionWithAuthInfoNoSuchPart() throws Exception {
    createTable3PartCols1PartAuthOn(client);
    client.getPartitionWithAuthInfo(DEFAULT_CATALOG_NAME, DB_NAME, TABLE_NAME,
            Lists.newArrayList("1997", "05", "66"), "user0", Lists.newArrayList("group0"));
  }

  @Test(expected = MetaException.class)
  public void testGetPartitionWithAuthInfoWrongNumOfPartVals() throws Exception {
    createTable3PartCols1PartAuthOn(client);
    client.getPartitionWithAuthInfo(DEFAULT_CATALOG_NAME, DB_NAME, TABLE_NAME,
            Lists.newArrayList("1997", "05"), "user0", Lists.newArrayList("group0"));
  }

  @Test
  public void testGetPartitionWithAuthInfoNullDbName() throws Exception {
    try {
      createTable3PartCols1PartAuthOn(client);
      client.getPartitionWithAuthInfo(DEFAULT_CATALOG_NAME, null, TABLE_NAME,
              Lists.newArrayList("1997", "05", "16"), "user0", Lists.newArrayList("group0"));
      fail("Should have thrown exception");
    } catch (NullPointerException | TTransportException e) {
      //TODO: should not throw different exceptions for different HMS deployment types
    }
  }

  @Test
  public void testGetPartitionWithAuthInfoNullTblName() throws Exception {
    try {
      createTable3PartCols1PartAuthOn(client);
      client.getPartitionWithAuthInfo(DEFAULT_CATALOG_NAME, DB_NAME, null,
              Lists.newArrayList("1997", "05", "16"), "user0", Lists.newArrayList("group0"));
      fail("Should have thrown exception");
    } catch (NullPointerException | TTransportException e) {
      //TODO: should not throw different exceptions for different HMS deployment types
    }
  }

  @Test(expected = MetaException.class)
  public void testGetPartitionWithAuthInfoNullValues() throws Exception {
    createTable3PartCols1PartAuthOn(client);
    client.getPartitionWithAuthInfo(DEFAULT_CATALOG_NAME, DB_NAME, TABLE_NAME,
            null, "user0", Lists.newArrayList("group0"));
  }

  @Test(expected = NoSuchObjectException.class)
  public void testGetPartitionWithAuthInfoNullUser() throws Exception {
    createTable3PartCols1PartAuthOn(client);
    client.getPartitionWithAuthInfo(DEFAULT_CATALOG_NAME, DB_NAME, "",
            Lists.newArrayList("1997", "05", "16"), null, Lists.newArrayList("group0"));
  }

  @Test
  public void testGetPartitionWithAuthInfoNullGroups() throws Exception {
    createTable3PartCols1PartAuthOn(client);
    client.getPartitionWithAuthInfo(DEFAULT_CATALOG_NAME, DB_NAME, TABLE_NAME,
            Lists.newArrayList("1997", "05", "16"), "user0", null);
  }

  @Test
  public void otherCatalog() throws TException {
    String catName = "get_partition_catalog";
    Catalog cat = new CatalogBuilder()
        .setName(catName)
        .setLocation(MetaStoreTestUtils.getTestWarehouseDir(catName))
        .build();
    client.createCatalog(cat);

    String dbName = "get_partition_database_in_other_catalog";
    Database db = new DatabaseBuilder()
        .setName(dbName)
        .setCatalogName(catName)
        .build();
    client.createDatabase(db);

    String tableName = "table_in_other_catalog";
    Table table = new TableBuilder()
        .inDb(db)
        .setTableName(tableName)
        .addCol("id", "int")
        .addCol("name", "string")
        .addPartCol("partcol", "string")
        .addTableParam("PARTITION_LEVEL_PRIVILEGE", "true")
        .build();
    client.createTable(table);

    Partition[] parts = new Partition[5];
    for (int i = 0; i < parts.length; i++) {
      parts[i] = new PartitionBuilder()
          .inTable(table)
          .addValue("a" + i)
          .build();
    }
    client.add_partitions(Arrays.asList(parts));

    Partition fetched = client.getPartition(catName, dbName, tableName,
        Collections.singletonList("a0"));
    Assert.assertEquals(catName, fetched.getCatName());
    Assert.assertEquals("a0", fetched.getValues().get(0));

    fetched = client.getPartition(catName, dbName, tableName, "partcol=a0");
    Assert.assertEquals(catName, fetched.getCatName());
    Assert.assertEquals("a0", fetched.getValues().get(0));

    // TODO CAT - withAuthInfo likely won't work until I've done privileges

    List<Partition> fetchedParts = client.getPartitionsByNames(catName, dbName, tableName,
        Arrays.asList("partcol=a0", "partcol=a1"));
    Assert.assertEquals(2, fetchedParts.size());
    Set<String> vals = new HashSet<>(fetchedParts.size());
    for (Partition part : fetchedParts) vals.add(part.getValues().get(0));
    Assert.assertTrue(vals.contains("a0"));
    Assert.assertTrue(vals.contains("a1"));

  }

  @SuppressWarnings("deprecation")
  @Test
  public void deprecatedCalls() throws TException {
    String tableName = "table_deprecated";
    Table table = new TableBuilder()
        .setTableName(tableName)
        .addCol("id", "int")
        .addCol("name", "string")
        .addPartCol("partcol", "string")
        .addTableParam("PARTITION_LEVEL_PRIVILEGE", "true")
        .build();
    client.createTable(table);

    Partition[] parts = new Partition[5];
    for (int i = 0; i < parts.length; i++) {
      parts[i] = new PartitionBuilder()
          .inTable(table)
          .addValue("a" + i)
          .build();
    }
    client.add_partitions(Arrays.asList(parts));

    Partition fetched = client.getPartition(DEFAULT_DATABASE_NAME, tableName,
        Collections.singletonList("a0"));
    Assert.assertEquals(DEFAULT_CATALOG_NAME, fetched.getCatName());
    Assert.assertEquals("a0", fetched.getValues().get(0));

    fetched = client.getPartition(DEFAULT_DATABASE_NAME, tableName, "partcol=a0");
    Assert.assertEquals(DEFAULT_CATALOG_NAME, fetched.getCatName());
    Assert.assertEquals("a0", fetched.getValues().get(0));

    createTable3PartCols1PartAuthOn(client);
    Partition partition = client.getPartitionWithAuthInfo(DB_NAME, TABLE_NAME,
        Lists.newArrayList("1997", "05", "16"), "user0", Lists.newArrayList("group0"));
    assertNotNull(partition);
    assertAuthInfoReturned("user0", "group0", partition);

    List<Partition> fetchedParts = client.getPartitionsByNames(DEFAULT_DATABASE_NAME, tableName,
        Arrays.asList("partcol=a0", "partcol=a1"));
    Assert.assertEquals(2, fetchedParts.size());
    Set<String> vals = new HashSet<>(fetchedParts.size());
    for (Partition part : fetchedParts) vals.add(part.getValues().get(0));
    Assert.assertTrue(vals.contains("a0"));
    Assert.assertTrue(vals.contains("a1"));
  }

  @Test(expected = NoSuchObjectException.class)
  public void getPartitionBogusCatalog() throws TException {
    createTable3PartCols1Part(client);
    client.getPartition("bogus", DB_NAME, TABLE_NAME, Lists.newArrayList("1997", "05", "16"));
  }

  @Test(expected = NoSuchObjectException.class)
  public void getPartitionByNameBogusCatalog() throws TException {
    createTable3PartCols1Part(client);
    client.getPartition("bogus", DB_NAME, TABLE_NAME, "yyyy=1997/mm=05/dd=16");
  }

  @Test(expected = NoSuchObjectException.class)
  public void getPartitionWithAuthBogusCatalog() throws TException {
    createTable3PartCols1PartAuthOn(client);
    client.getPartitionWithAuthInfo("bogus", DB_NAME, TABLE_NAME,
        Lists.newArrayList("1997", "05", "16"), "user0", Lists.newArrayList("group0"));
  }

  @Test(expected = NoSuchObjectException.class)
  public void getPartitionsByNamesBogusCatalog() throws TException {
    createTable3PartCols1Part(client);
    client.getPartitionsByNames("bogus", DB_NAME, TABLE_NAME,
        Collections.singletonList("yyyy=1997/mm=05/dd=16"));
  }



}
