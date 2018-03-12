package org.apache.hadoop.hive.metastore.client;

import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.MetaStoreTestUtils;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.annotation.MetastoreCheckinTest;
import org.apache.hadoop.hive.metastore.api.Catalog;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.client.builder.CatalogBuilder;
import org.apache.hadoop.hive.metastore.client.builder.DatabaseBuilder;
import org.apache.hadoop.hive.metastore.minihms.AbstractMetaStoreService;
import org.apache.thrift.TException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

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
@RunWith(Parameterized.class)
@Category(MetastoreCheckinTest.class)
public class TestCatalogs {
  private static final Logger LOG = LoggerFactory.getLogger(TestCatalogs.class);
  // Needed until there is no junit release with @BeforeParam, @AfterParam (junit 4.13)
  // https://github.com/junit-team/junit4/commit/1bf8438b65858565dbb64736bfe13aae9cfc1b5a
  // Then we should remove our own copy
  private static Set<AbstractMetaStoreService> metaStoreServices = null;
  private final AbstractMetaStoreService metaStore;
  private IMetaStoreClient client;

  @Parameterized.Parameters(name = "{0}")
  public static List<Object[]> getMetaStoreToTest() throws Exception {
    List<Object[]> result = MetaStoreFactoryForTests.getMetaStores();
    metaStoreServices = result.stream()
        .map(test -> (AbstractMetaStoreService)test[1])
        .collect(Collectors.toSet());
    return result;
  }

  public TestCatalogs(String name, AbstractMetaStoreService metaStore) throws Exception {
    this.metaStore = metaStore;
    this.metaStore.start();
  }

  // Needed until there is no junit release with @BeforeParam, @AfterParam (junit 4.13)
  // https://github.com/junit-team/junit4/commit/1bf8438b65858565dbb64736bfe13aae9cfc1b5a
  // Then we should move this to @AfterParam
  @AfterClass
  public static void stopMetaStores() throws Exception {
    for(AbstractMetaStoreService metaStoreService : metaStoreServices) {
      try {
        metaStoreService.stop();
      } catch(Exception e) {
        // Catch the exceptions, so every other metastore could be stopped as well
        // Log it, so at least there is a slight possibility we find out about this :)
        LOG.error("Error stopping MetaStoreService", e);
      }
    }
  }

  @Before
  public void setUp() throws Exception {
    // Get new client
    client = metaStore.getClient();

  }

  @After
  public void tearDown() throws Exception {
    // Drop any left over catalogs
    List<String> catalogs = client.getCatalogs();
    for (String catName : catalogs) {
      if (!catName.equalsIgnoreCase(Warehouse.DEFAULT_CATALOG_NAME)) {
        // First drop any databases in catalog
        List<String> databases = client.getAllDatabases(catName);
        for (String db : databases) {
          client.dropDatabase(catName, db, true, false, false);
        }
        client.dropCatalog(catName);
      } else {
        List<String> databases = client.getAllDatabases(catName);
        for (String db : databases) {
          if (!db.equalsIgnoreCase(Warehouse.DEFAULT_DATABASE_NAME)) {
            client.dropDatabase(catName, db, true, false, false);
          }
        }

      }
    }
    try {
      if (client != null) {
        client.close();
      }
    } finally {
      client = null;
    }
  }

  @Test
  public void catalogOperations() throws TException {
    String[] catNames = {"cat1", "cat2", "ADifferentName"};
    String[] description = {"a description", "super descriptive", null};
    String[] location = {MetaStoreTestUtils.getTestWarehouseDir("cat1"),
                         MetaStoreTestUtils.getTestWarehouseDir("cat2"),
                         MetaStoreTestUtils.getTestWarehouseDir("different")};

    for (int i = 0; i < catNames.length; i++) {
      Catalog cat = new CatalogBuilder()
          .setName(catNames[i])
          .setLocation(location[i])
          .setDescription(description[i])
          .build();
      client.createCatalog(cat);
      File dir = new File(cat.getLocationUri());
      Assert.assertTrue(dir.exists() && dir.isDirectory());
    }

    for (int i = 0; i < catNames.length; i++) {
      Catalog cat = client.getCatalog(catNames[i]);
      Assert.assertTrue(catNames[i].equalsIgnoreCase(cat.getName()));
      Assert.assertEquals(description[i], cat.getDescription());
      Assert.assertEquals(location[i], cat.getLocationUri());
      File dir = new File(cat.getLocationUri());
      Assert.assertTrue(dir.exists() && dir.isDirectory());
    }

    List<String> catalogs = client.getCatalogs();
    Assert.assertEquals(4, catalogs.size());
    catalogs.sort(Comparator.naturalOrder());
    List<String> expected = new ArrayList<>(catNames.length + 1);
    expected.add(Warehouse.DEFAULT_CATALOG_NAME);
    expected.addAll(Arrays.asList(catNames));
    expected.sort(Comparator.naturalOrder());
    for (int i = 0; i < catalogs.size(); i++) {
      Assert.assertTrue("Expected " + expected.get(i) + " actual " + catalogs.get(i),
          catalogs.get(i).equalsIgnoreCase(expected.get(i)));
    }

    for (int i = 0; i < catNames.length; i++) {
      client.dropCatalog(catNames[i]);
      File dir = new File(location[i]);
      Assert.assertFalse(dir.exists());
    }

    catalogs = client.getCatalogs();
    Assert.assertEquals(1, catalogs.size());
    Assert.assertTrue(catalogs.get(0).equalsIgnoreCase(Warehouse.DEFAULT_CATALOG_NAME));
  }

  @Test(expected = NoSuchObjectException.class)
  public void getNonExistentCatalog() throws TException {
    client.getCatalog("noSuchCatalog");
  }

  @Test(expected = MetaException.class)
  public void createCatalogWithBadLocation() throws TException {
    Catalog cat = new CatalogBuilder()
        .setName("goodluck")
        .setLocation("/nosuchdir/nosuch")
        .build();
    client.createCatalog(cat);
  }

  @Test(expected = NoSuchObjectException.class)
  public void dropNonExistentCatalog() throws TException {
    client.dropCatalog("noSuchCatalog");
  }

  @Test(expected = MetaException.class)
  public void dropHiveCatalog() throws TException {
    client.dropCatalog(Warehouse.DEFAULT_CATALOG_NAME);
  }

  @Test(expected = InvalidOperationException.class)
  public void dropNonEmptyCatalog() throws TException {
    String catName = "toBeDropped";
    Catalog cat = new CatalogBuilder()
        .setName(catName)
        .setLocation(MetaStoreTestUtils.getTestWarehouseDir(catName))
        .build();
    client.createCatalog(cat);

    String dbName = "dontDropMe";
    Database db = new DatabaseBuilder()
        .setName(dbName)
        .setCatalogName(catName)
        .build();
    client.createDatabase(db);

    client.dropCatalog(catName);
  }
}
