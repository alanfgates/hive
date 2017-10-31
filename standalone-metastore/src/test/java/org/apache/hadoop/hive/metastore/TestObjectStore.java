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
package org.apache.hadoop.hive.metastore;


import com.codahale.metrics.Counter;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import org.apache.hadoop.hive.metastore.ObjectStore.RetryingExecutor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.CurrentNotificationEventId;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.FileMetadataExprType;
import org.apache.hadoop.hive.metastore.api.Function;
import org.apache.hadoop.hive.metastore.api.ISchema;
import org.apache.hadoop.hive.metastore.api.Index;
import org.apache.hadoop.hive.metastore.api.InvalidInputException;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.NotificationEvent;
import org.apache.hadoop.hive.metastore.api.NotificationEventRequest;
import org.apache.hadoop.hive.metastore.api.NotificationEventResponse;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.Role;
import org.apache.hadoop.hive.metastore.api.SQLForeignKey;
import org.apache.hadoop.hive.metastore.api.SQLPrimaryKey;
import org.apache.hadoop.hive.metastore.api.SchemaCompatibility;
import org.apache.hadoop.hive.metastore.api.SchemaType;
import org.apache.hadoop.hive.metastore.api.SchemaValidation;
import org.apache.hadoop.hive.metastore.api.SchemaVersion;
import org.apache.hadoop.hive.metastore.api.SchemaVersionState;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.client.builder.ISchemaBuilder;
import org.apache.hadoop.hive.metastore.client.builder.SchemaVersionBuilder;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.messaging.EventMessage;
import org.apache.hadoop.hive.metastore.metrics.Metrics;
import org.apache.hadoop.hive.metastore.metrics.MetricsConstants;
import org.apache.hadoop.hive.metastore.model.MNotificationLog;
import org.apache.hadoop.hive.metastore.model.MNotificationNextId;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.thrift.TException;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jdo.Query;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class TestObjectStore {
  private ObjectStore objectStore = null;

  private static final String DB1 = "testobjectstoredb1";
  private static final String DB2 = "testobjectstoredb2";
  private static final String TABLE1 = "testobjectstoretable1";
  private static final String KEY1 = "testobjectstorekey1";
  private static final String KEY2 = "testobjectstorekey2";
  private static final String OWNER = "testobjectstoreowner";
  private static final String USER1 = "testobjectstoreuser1";
  private static final String ROLE1 = "testobjectstorerole1";
  private static final String ROLE2 = "testobjectstorerole2";
  private static final Logger LOG = LoggerFactory.getLogger(TestObjectStore.class.getName());

  private static final class LongSupplier implements Supplier<Long> {
    public long value = 0;

    @Override
    public Long get() {
      return value;
    }
  }

  public static class MockPartitionExpressionProxy implements PartitionExpressionProxy {
    @Override
    public String convertExprToFilter(byte[] expr) throws MetaException {
      return null;
    }

    @Override
    public boolean filterPartitionsByExpr(List<FieldSchema> partColumns,
                                          byte[] expr, String defaultPartitionName,
                                          List<String> partitionNames)
        throws MetaException {
      return false;
    }

    @Override
    public FileMetadataExprType getMetadataType(String inputFormat) {
      return null;
    }

    @Override
    public SearchArgument createSarg(byte[] expr) {
      return null;
    }

    @Override
    public FileFormatProxy getFileFormatProxy(FileMetadataExprType type) {
      return null;
    }
  }

  @Before
  public void setUp() throws Exception {
    Configuration conf = MetastoreConf.newMetastoreConf();
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.EXPRESSION_PROXY_CLASS,
        MockPartitionExpressionProxy.class.getName());

    objectStore = new ObjectStore();
    objectStore.setConf(conf);
    dropAllStoreObjects(objectStore);
  }

  /**
   * Test database operations
   */
  @Test
  public void testDatabaseOps() throws MetaException, InvalidObjectException,
      NoSuchObjectException {
    Database db1 = new Database(DB1, "description", "locationurl", null);
    Database db2 = new Database(DB2, "description", "locationurl", null);
    objectStore.createDatabase(db1);
    objectStore.createDatabase(db2);

    List<String> databases = objectStore.getAllDatabases();
    LOG.info("databases: " + databases);
    Assert.assertEquals(2, databases.size());
    Assert.assertEquals(DB1, databases.get(0));
    Assert.assertEquals(DB2, databases.get(1));

    objectStore.dropDatabase(DB1);
    databases = objectStore.getAllDatabases();
    Assert.assertEquals(1, databases.size());
    Assert.assertEquals(DB2, databases.get(0));

    objectStore.dropDatabase(DB2);
  }

  /**
   * Test table operations
   */
  @Test
  public void testTableOps() throws MetaException, InvalidObjectException, NoSuchObjectException,
      InvalidInputException {
    Database db1 = new Database(DB1, "description", "locationurl", null);
    objectStore.createDatabase(db1);
    StorageDescriptor sd1 =
        new StorageDescriptor(ImmutableList.of(new FieldSchema("pk_col", "double", null)),
            "location", null, null, false, 0, new SerDeInfo("SerDeName", "serializationLib", null),
            null, null, null);
    HashMap<String, String> params = new HashMap<>();
    params.put("EXTERNAL", "false");
    Table tbl1 =
        new Table(TABLE1, DB1, "owner", 1, 2, 3, sd1, null, params, null, null, "MANAGED_TABLE");
    objectStore.createTable(tbl1);

    List<String> tables = objectStore.getAllTables(DB1);
    Assert.assertEquals(1, tables.size());
    Assert.assertEquals(TABLE1, tables.get(0));

    StorageDescriptor sd2 =
        new StorageDescriptor(ImmutableList.of(new FieldSchema("fk_col", "double", null)),
            "location", null, null, false, 0, new SerDeInfo("SerDeName", "serializationLib", null),
            null, null, null);
    Table newTbl1 = new Table("new" + TABLE1, DB1, "owner", 1, 2, 3, sd2, null, params, null, null,
        "MANAGED_TABLE");
    objectStore.alterTable(DB1, TABLE1, newTbl1);
    tables = objectStore.getTables(DB1, "new*");
    Assert.assertEquals(1, tables.size());
    Assert.assertEquals("new" + TABLE1, tables.get(0));

    objectStore.createTable(tbl1);
    tables = objectStore.getAllTables(DB1);
    Assert.assertEquals(2, tables.size());

    List<SQLForeignKey> foreignKeys = objectStore.getForeignKeys(DB1, TABLE1, null, null);
    Assert.assertEquals(0, foreignKeys.size());

    SQLPrimaryKey pk = new SQLPrimaryKey(DB1, TABLE1, "pk_col", 1,
        "pk_const_1", false, false, false);
    objectStore.addPrimaryKeys(ImmutableList.of(pk));
    SQLForeignKey fk = new SQLForeignKey(DB1, TABLE1, "pk_col",
        DB1, "new" + TABLE1, "fk_col", 1,
        0, 0, "fk_const_1", "pk_const_1", false, false, false);
    objectStore.addForeignKeys(ImmutableList.of(fk));

    // Retrieve from PK side
    foreignKeys = objectStore.getForeignKeys(null, null, DB1, "new" + TABLE1);
    Assert.assertEquals(1, foreignKeys.size());

    List<SQLForeignKey> fks = objectStore.getForeignKeys(null, null, DB1, "new" + TABLE1);
    if (fks != null) {
      for (SQLForeignKey fkcol : fks) {
        objectStore.dropConstraint(fkcol.getFktable_db(), fkcol.getFktable_name(),
            fkcol.getFk_name());
      }
    }
    // Retrieve from FK side
    foreignKeys = objectStore.getForeignKeys(DB1, TABLE1, null, null);
    Assert.assertEquals(0, foreignKeys.size());
    // Retrieve from PK side
    foreignKeys = objectStore.getForeignKeys(null, null, DB1, "new" + TABLE1);
    Assert.assertEquals(0, foreignKeys.size());

    objectStore.dropTable(DB1, TABLE1);
    tables = objectStore.getAllTables(DB1);
    Assert.assertEquals(1, tables.size());

    objectStore.dropTable(DB1, "new" + TABLE1);
    tables = objectStore.getAllTables(DB1);
    Assert.assertEquals(0, tables.size());

    objectStore.dropDatabase(DB1);
  }

  private StorageDescriptor createFakeSd(String location) {
    return new StorageDescriptor(null, location, null, null, false, 0,
        new SerDeInfo("SerDeName", "serializationLib", null), null, null, null);
  }


  /**
   * Tests partition operations
   */
  @Test
  public void testPartitionOps() throws MetaException, InvalidObjectException,
      NoSuchObjectException, InvalidInputException {
    Database db1 = new Database(DB1, "description", "locationurl", null);
    objectStore.createDatabase(db1);
    StorageDescriptor sd = createFakeSd("location");
    HashMap<String, String> tableParams = new HashMap<>();
    tableParams.put("EXTERNAL", "false");
    FieldSchema partitionKey1 = new FieldSchema("Country", ColumnType.STRING_TYPE_NAME, "");
    FieldSchema partitionKey2 = new FieldSchema("State", ColumnType.STRING_TYPE_NAME, "");
    Table tbl1 =
        new Table(TABLE1, DB1, "owner", 1, 2, 3, sd, Arrays.asList(partitionKey1, partitionKey2),
            tableParams, null, null, "MANAGED_TABLE");
    objectStore.createTable(tbl1);
    HashMap<String, String> partitionParams = new HashMap<>();
    partitionParams.put("PARTITION_LEVEL_PRIVILEGE", "true");
    List<String> value1 = Arrays.asList("US", "CA");
    Partition part1 = new Partition(value1, DB1, TABLE1, 111, 111, sd, partitionParams);
    objectStore.addPartition(part1);
    List<String> value2 = Arrays.asList("US", "MA");
    Partition part2 = new Partition(value2, DB1, TABLE1, 222, 222, sd, partitionParams);
    objectStore.addPartition(part2);

    Deadline.startTimer("getPartition");
    List<Partition> partitions = objectStore.getPartitions(DB1, TABLE1, 10);
    Assert.assertEquals(2, partitions.size());
    Assert.assertEquals(111, partitions.get(0).getCreateTime());
    Assert.assertEquals(222, partitions.get(1).getCreateTime());

    int numPartitions = objectStore.getNumPartitionsByFilter(DB1, TABLE1, "");
    Assert.assertEquals(partitions.size(), numPartitions);

    numPartitions = objectStore.getNumPartitionsByFilter(DB1, TABLE1, "country = \"US\"");
    Assert.assertEquals(2, numPartitions);

    objectStore.dropPartition(DB1, TABLE1, value1);
    partitions = objectStore.getPartitions(DB1, TABLE1, 10);
    Assert.assertEquals(1, partitions.size());
    Assert.assertEquals(222, partitions.get(0).getCreateTime());

    objectStore.dropPartition(DB1, TABLE1, value2);
    objectStore.dropTable(DB1, TABLE1);
    objectStore.dropDatabase(DB1);
  }

  /**
   * Test master keys operation
   */
  @Test
  public void testMasterKeyOps() throws MetaException, NoSuchObjectException {
    int id1 = objectStore.addMasterKey(KEY1);
    int id2 = objectStore.addMasterKey(KEY2);

    String[] keys = objectStore.getMasterKeys();
    Assert.assertEquals(2, keys.length);
    Assert.assertEquals(KEY1, keys[0]);
    Assert.assertEquals(KEY2, keys[1]);

    objectStore.updateMasterKey(id1, "new" + KEY1);
    objectStore.updateMasterKey(id2, "new" + KEY2);
    keys = objectStore.getMasterKeys();
    Assert.assertEquals(2, keys.length);
    Assert.assertEquals("new" + KEY1, keys[0]);
    Assert.assertEquals("new" + KEY2, keys[1]);

    objectStore.removeMasterKey(id1);
    keys = objectStore.getMasterKeys();
    Assert.assertEquals(1, keys.length);
    Assert.assertEquals("new" + KEY2, keys[0]);

    objectStore.removeMasterKey(id2);
  }

  /**
   * Test role operation
   */
  @Test
  public void testRoleOps() throws InvalidObjectException, MetaException, NoSuchObjectException {
    objectStore.addRole(ROLE1, OWNER);
    objectStore.addRole(ROLE2, OWNER);
    List<String> roles = objectStore.listRoleNames();
    Assert.assertEquals(2, roles.size());
    Assert.assertEquals(ROLE2, roles.get(1));
    Role role1 = objectStore.getRole(ROLE1);
    Assert.assertEquals(OWNER, role1.getOwnerName());
    objectStore.grantRole(role1, USER1, PrincipalType.USER, OWNER, PrincipalType.ROLE, true);
    objectStore.revokeRole(role1, USER1, PrincipalType.USER, false);
    objectStore.removeRole(ROLE1);
  }

  @Test
  public void testDirectSqlErrorMetrics() throws Exception {
    Configuration conf = MetastoreConf.newMetastoreConf();
    MetastoreConf.setBoolVar(conf, MetastoreConf.ConfVars.METRICS_ENABLED, true);
    Metrics.initialize(conf);
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.HIVE_CODAHALE_METRICS_REPORTER_CLASSES,
        "org.apache.hadoop.hive.common.metrics.metrics2.JsonFileMetricsReporter, " +
            "org.apache.hadoop.hive.common.metrics.metrics2.JmxMetricsReporter"
    );

    // recall setup so that we get an object store with the metrics initalized
    setUp();
    Counter directSqlErrors =
        Metrics.getRegistry().getCounters().get(MetricsConstants.DIRECTSQL_ERRORS);

    objectStore.new GetDbHelper("foo", true, true) {
      @Override
      protected Database getSqlResult(ObjectStore.GetHelper<Database> ctx) throws MetaException {
        return null;
      }

      @Override
      protected Database getJdoResult(ObjectStore.GetHelper<Database> ctx) throws MetaException,
          NoSuchObjectException {
        return null;
      }
    }.run(false);

    Assert.assertEquals(0, directSqlErrors.getCount());

    objectStore.new GetDbHelper("foo", true, true) {
      @Override
      protected Database getSqlResult(ObjectStore.GetHelper<Database> ctx) throws MetaException {
        throw new RuntimeException();
      }

      @Override
      protected Database getJdoResult(ObjectStore.GetHelper<Database> ctx) throws MetaException,
          NoSuchObjectException {
        return null;
      }
    }.run(false);

    Assert.assertEquals(1, directSqlErrors.getCount());
  }

  private static void dropAllStoreObjects(RawStore store)
      throws MetaException, InvalidObjectException, InvalidInputException {
    try {
      Deadline.registerIfNot(100000);
      List<Function> functions = store.getAllFunctions();
      for (Function func : functions) {
        store.dropFunction(func.getDbName(), func.getFunctionName());
      }
      List<String> dbs = store.getAllDatabases();
      for (String db : dbs) {
        List<String> tbls = store.getAllTables(db);
        for (String tbl : tbls) {
          List<Index> indexes = store.getIndexes(db, tbl, 100);
          for (Index index : indexes) {
            store.dropIndex(db, tbl, index.getIndexName());
          }
        }
        for (String tbl : tbls) {
          Deadline.startTimer("getPartition");
          List<Partition> parts = store.getPartitions(db, tbl, 100);
          for (Partition part : parts) {
            store.dropPartition(db, tbl, part.getValues());
          }
          // Find any constraints and drop them
          Set<String> constraints = new HashSet<>();
          List<SQLPrimaryKey> pk = store.getPrimaryKeys(db, tbl);
          if (pk != null) {
            for (SQLPrimaryKey pkcol : pk) {
              constraints.add(pkcol.getPk_name());
            }
          }
          List<SQLForeignKey> fks = store.getForeignKeys(null, null, db, tbl);
          if (fks != null) {
            for (SQLForeignKey fkcol : fks) {
              constraints.add(fkcol.getFk_name());
            }
          }
          for (String constraint : constraints) {
            store.dropConstraint(db, tbl, constraint);
          }
          store.dropTable(db, tbl);
        }
        store.dropDatabase(db);
      }
      List<String> roles = store.listRoleNames();
      for (String role : roles) {
        store.removeRole(role);
      }
    } catch (NoSuchObjectException e) {
    }
  }

  @Test
  public void testQueryCloseOnError() throws Exception {
    ObjectStore spy = Mockito.spy(objectStore);
    spy.getAllDatabases();
    spy.getAllFunctions();
    spy.getAllTables(DB1);
    spy.getPartitionCount();
    Mockito.verify(spy, Mockito.times(3))
        .rollbackAndCleanup(Mockito.anyBoolean(), Mockito.<Query>anyObject());
  }

  @Test
  public void testRetryingExecutorSleep() throws Exception {
    RetryingExecutor re = new ObjectStore.RetryingExecutor(MetastoreConf.newMetastoreConf(), null);
    Assert.assertTrue("invalid sleep value", re.getSleepInterval() >= 0);
  }

  @Ignore // See comment in ObjectStore.getDataSourceProps
  @Test
  public void testNonConfDatanucleusValueSet() {
    String key = "datanucleus.no.such.key";
    String value = "test_value";
    String key1 = "blabla.no.such.key";
    String value1 = "another_value";
    Assume.assumeTrue(System.getProperty(key) == null);
    Configuration localConf = MetastoreConf.newMetastoreConf();
    MetastoreConf.setVar(localConf, MetastoreConf.ConfVars.EXPRESSION_PROXY_CLASS,
        MockPartitionExpressionProxy.class.getName());
    localConf.set(key, value);
    localConf.set(key1, value1);
    objectStore = new ObjectStore();
    objectStore.setConf(localConf);
    Assert.assertEquals(value, objectStore.getProp().getProperty(key));
    Assert.assertNull(objectStore.getProp().getProperty(key1));
  }

  /**
   * Test notification operations
   */
  // TODO MS-SPLIT uncomment once we move EventMessage over
  @Test
  public void testNotificationOps() throws InterruptedException {
    final int NO_EVENT_ID = 0;
    final int FIRST_EVENT_ID = 1;
    final int SECOND_EVENT_ID = 2;

    NotificationEvent event =
        new NotificationEvent(0, 0, EventMessage.EventType.CREATE_DATABASE.toString(), "");
    NotificationEventResponse eventResponse;
    CurrentNotificationEventId eventId;

    // Verify that there is no notifications available yet
    eventId = objectStore.getCurrentNotificationEventId();
    Assert.assertEquals(NO_EVENT_ID, eventId.getEventId());

    // Verify that addNotificationEvent() updates the NotificationEvent with the new event ID
    objectStore.addNotificationEvent(event);
    Assert.assertEquals(FIRST_EVENT_ID, event.getEventId());
    objectStore.addNotificationEvent(event);
    Assert.assertEquals(SECOND_EVENT_ID, event.getEventId());

    // Verify that objectStore fetches the latest notification event ID
    eventId = objectStore.getCurrentNotificationEventId();
    Assert.assertEquals(SECOND_EVENT_ID, eventId.getEventId());

    // Verify that getNextNotification() returns all events
    eventResponse = objectStore.getNextNotification(new NotificationEventRequest());
    Assert.assertEquals(2, eventResponse.getEventsSize());
    Assert.assertEquals(FIRST_EVENT_ID, eventResponse.getEvents().get(0).getEventId());
    Assert.assertEquals(SECOND_EVENT_ID, eventResponse.getEvents().get(1).getEventId());

    // Verify that getNextNotification(last) returns events after a specified event
    eventResponse = objectStore.getNextNotification(new NotificationEventRequest(FIRST_EVENT_ID));
    Assert.assertEquals(1, eventResponse.getEventsSize());
    Assert.assertEquals(SECOND_EVENT_ID, eventResponse.getEvents().get(0).getEventId());

    // Verify that getNextNotification(last) returns zero events if there are no more notifications available
    eventResponse = objectStore.getNextNotification(new NotificationEventRequest(SECOND_EVENT_ID));
    Assert.assertEquals(0, eventResponse.getEventsSize());

    // Verify that cleanNotificationEvents() cleans up all old notifications
    Thread.sleep(1);
    objectStore.cleanNotificationEvents(1);
    eventResponse = objectStore.getNextNotification(new NotificationEventRequest());
    Assert.assertEquals(0, eventResponse.getEventsSize());
  }

  @Ignore(
      "This test is here to allow testing with other databases like mysql / postgres etc\n"
          + " with  user changes to the code. This cannot be run on apache derby because of\n"
          + " https://db.apache.org/derby/docs/10.10/devguide/cdevconcepts842385.html"
  )
  @Test
  public void testConcurrentAddNotifications() throws ExecutionException, InterruptedException {

    final int NUM_THREADS = 10;
    CyclicBarrier cyclicBarrier = new CyclicBarrier(NUM_THREADS,
        () -> LoggerFactory.getLogger("test")
            .debug(NUM_THREADS + " threads going to add notification"));

    Configuration conf = MetastoreConf.newMetastoreConf();
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.EXPRESSION_PROXY_CLASS,
        MockPartitionExpressionProxy.class.getName());
    /*
       Below are the properties that need to be set based on what database this test is going to be run
     */

//    conf.setVar(HiveConf.ConfVars.METASTORE_CONNECTION_DRIVER, "com.mysql.jdbc.Driver");
//    conf.setVar(HiveConf.ConfVars.METASTORECONNECTURLKEY,
//        "jdbc:mysql://localhost:3306/metastore_db");
//    conf.setVar(HiveConf.ConfVars.METASTORE_CONNECTION_USER_NAME, "");
//    conf.setVar(HiveConf.ConfVars.METASTOREPWD, "");

    /*
     we have to  add this one manually as for tests the db is initialized via the metastoreDiretSQL
     and we don't run the schema creation sql that includes the an insert for notification_sequence
     which can be locked. the entry in notification_sequence happens via notification_event insertion.
    */
    objectStore.getPersistenceManager().newQuery(MNotificationLog.class, "eventType==''").execute();
    objectStore.getPersistenceManager().newQuery(MNotificationNextId.class, "nextEventId==-1").execute();

    objectStore.addNotificationEvent(
        new NotificationEvent(0, 0,
            EventMessage.EventType.CREATE_DATABASE.toString(),
            "CREATE DATABASE DB initial"));

    ExecutorService executorService = Executors.newFixedThreadPool(NUM_THREADS);
    for (int i = 0; i < NUM_THREADS; i++) {
      final int n = i;

      executorService.execute(
          () -> {
            ObjectStore store = new ObjectStore();
            store.setConf(conf);

            String eventType = EventMessage.EventType.CREATE_DATABASE.toString();
            NotificationEvent dbEvent =
                new NotificationEvent(0, 0, eventType,
                    "CREATE DATABASE DB" + n);
            System.out.println("ADDING NOTIFICATION");

            try {
              cyclicBarrier.await();
            } catch (InterruptedException | BrokenBarrierException e) {
              throw new RuntimeException(e);
            }
            store.addNotificationEvent(dbEvent);
            System.out.println("FINISH NOTIFICATION");
          });
    }
    executorService.shutdown();
    Assert.assertTrue(executorService.awaitTermination(15, TimeUnit.SECONDS));

    // we have to setup this again as the underlying PMF keeps getting reinitialized with original
    // reference closed
    ObjectStore store = new ObjectStore();
    store.setConf(conf);

    NotificationEventResponse eventResponse = store.getNextNotification(
        new NotificationEventRequest());
    Assert.assertEquals(NUM_THREADS + 1, eventResponse.getEventsSize());
    long previousId = 0;
    for (NotificationEvent event : eventResponse.getEvents()) {
      Assert.assertTrue("previous:" + previousId + " current:" + event.getEventId(),
          previousId < event.getEventId());
      Assert.assertTrue(previousId + 1 == event.getEventId());
      previousId = event.getEventId();
    }
  }

  @Test
  public void iSchema() throws MetaException, AlreadyExistsException, NoSuchObjectException {
    ISchema schema = objectStore.getISchema("no.such.schema");
    Assert.assertNull(schema);

    String schemaName = "schema1";
    String schemaGroup = "group1";
    String description = "This is a description";
    schema = new ISchemaBuilder()
        .setSchemaType(SchemaType.AVRO)
        .setName(schemaName)
        .setCompatibility(SchemaCompatibility.FORWARD)
        .setValidationLevel(SchemaValidation.LATEST)
        .setCanEvolve(false)
        .setSchemaGroup(schemaGroup)
        .setDescription(description)
        .build();
    objectStore.createISchema(schema);

    schema = objectStore.getISchema(schemaName);
    Assert.assertNotNull(schema);

    Assert.assertEquals(SchemaType.AVRO, schema.getSchemaType());
    Assert.assertEquals(schemaName, schema.getName());
    Assert.assertEquals(SchemaCompatibility.FORWARD, schema.getCompatibility());
    Assert.assertEquals(SchemaValidation.LATEST, schema.getValidationLevel());
    Assert.assertFalse(schema.isCanEvolve());
    Assert.assertEquals(schemaGroup, schema.getSchemaGroup());
    Assert.assertEquals(description, schema.getDescription());

    schemaGroup = "new group";
    description = "new description";
    schema.setCompatibility(SchemaCompatibility.BOTH);
    schema.setValidationLevel(SchemaValidation.ALL);
    schema.setCanEvolve(true);
    schema.setSchemaGroup(schemaGroup);
    schema.setDescription(description);
    objectStore.alterISchema(schemaName, schema);

    schema = objectStore.getISchema(schemaName);
    Assert.assertNotNull(schema);

    Assert.assertEquals(SchemaType.AVRO, schema.getSchemaType());
    Assert.assertEquals(schemaName, schema.getName());
    Assert.assertEquals(SchemaCompatibility.BOTH, schema.getCompatibility());
    Assert.assertEquals(SchemaValidation.ALL, schema.getValidationLevel());
    Assert.assertTrue(schema.isCanEvolve());
    Assert.assertEquals(schemaGroup, schema.getSchemaGroup());
    Assert.assertEquals(description, schema.getDescription());

    objectStore.dropISchema(schemaName);
    schema = objectStore.getISchema(schemaName);
    Assert.assertNull(schema);
  }

  @Test(expected = AlreadyExistsException.class)
  public void schemaAlreadyExists() throws MetaException, AlreadyExistsException {
    String schemaName = "schema2";
    ISchema schema = new ISchemaBuilder()
        .setSchemaType(SchemaType.HIVE)
        .setName(schemaName)
        .build();
    objectStore.createISchema(schema);

    schema = objectStore.getISchema(schemaName);
    Assert.assertNotNull(schema);

    Assert.assertEquals(SchemaType.HIVE, schema.getSchemaType());
    Assert.assertEquals(schemaName, schema.getName());
    Assert.assertEquals(SchemaCompatibility.BACKWARD, schema.getCompatibility());
    Assert.assertEquals(SchemaValidation.ALL, schema.getValidationLevel());
    Assert.assertTrue(schema.isCanEvolve());

    // This second attempt to create it should throw
    objectStore.createISchema(schema);
  }

  @Test(expected = NoSuchObjectException.class)
  public void alterNonExistentSchema() throws MetaException, NoSuchObjectException {
    String schemaName = "noSuchSchema";
    ISchema schema = new ISchemaBuilder()
        .setSchemaType(SchemaType.HIVE)
        .setName(schemaName)
        .setDescription("a new description")
        .build();
    objectStore.alterISchema(schemaName, schema);
  }

  @Test(expected = NoSuchObjectException.class)
  public void dropNonExistentSchema() throws MetaException, NoSuchObjectException {
    objectStore.dropISchema("no_such_schema");
  }

  @Test(expected = NoSuchObjectException.class)
  public void createVersionOfNonExistentSchema() throws MetaException, AlreadyExistsException,
      NoSuchObjectException {
    SchemaVersion schemaVersion = new SchemaVersionBuilder()
        .setSchemaName("noSchemaOfThisNameExists")
        .setVersion(1)
        .addCol("a", ColumnType.STRING_TYPE_NAME)
        .build();
    objectStore.addSchemaVersion(schemaVersion);
  }

  @Test
  public void addSchemaVersion() throws MetaException, AlreadyExistsException,
      NoSuchObjectException {
    String schemaName = "schema37";
    int version = 1;
    SchemaVersion schemaVersion = objectStore.getSchemaVersion(schemaName, version);
    Assert.assertNull(schemaVersion);

    ISchema schema = new ISchemaBuilder()
        .setSchemaType(SchemaType.AVRO)
        .setName(schemaName)
        .build();
    objectStore.createISchema(schema);

    String description = "very descriptive";
    String schemaText = "this should look like json, but oh well";
    String fingerprint = "this should be an md5 string";
    String versionName = "why would I name a version?";
    long creationTime = 10;
    String serdeName = "serde_for_schema37";
    String serializer = "org.apache.hadoop.hive.metastore.test.Serializer";
    String deserializer = "org.apache.hadoop.hive.metastore.test.Deserializer";
    String serdeDescription = "how do you describe a serde?";
    schemaVersion = new SchemaVersionBuilder()
        .setSchemaName(schemaName)
        .setVersion(version)
        .addCol("a", ColumnType.INT_TYPE_NAME)
        .addCol("b", ColumnType.FLOAT_TYPE_NAME)
        .setCreatedAt(creationTime)
        .setState(SchemaVersionState.INITIATED)
        .setDescription(description)
        .setSchemaText(schemaText)
        .setFingerprint(fingerprint)
        .setName(versionName)
        .setSerdeName(serdeName)
        .setSerdeSerializerClass(serializer)
        .setSerdeDeserializerClass(deserializer)
        .setSerdeDescription(serdeDescription)
        .build();
    objectStore.addSchemaVersion(schemaVersion);

    schemaVersion = objectStore.getSchemaVersion(schemaName, version);
    Assert.assertNotNull(schemaVersion);
    Assert.assertEquals(schemaName, schemaVersion.getSchemaName());
    Assert.assertEquals(version, schemaVersion.getVersion());
    Assert.assertEquals(creationTime, schemaVersion.getCreatedAt());
    Assert.assertEquals(SchemaVersionState.INITIATED, schemaVersion.getState());
    Assert.assertEquals(description, schemaVersion.getDescription());
    Assert.assertEquals(schemaText, schemaVersion.getSchemaText());
    Assert.assertEquals(fingerprint, schemaVersion.getFingerprint());
    Assert.assertEquals(versionName, schemaVersion.getName());
    Assert.assertEquals(serdeName, schemaVersion.getSerDe().getName());
    Assert.assertEquals(serializer, schemaVersion.getSerDe().getSerializerClass());
    Assert.assertEquals(deserializer, schemaVersion.getSerDe().getDeserializerClass());
    Assert.assertEquals(serdeDescription, schemaVersion.getSerDe().getDescription());
    Assert.assertEquals(2, schemaVersion.getColsSize());
    List<FieldSchema> cols = schemaVersion.getCols();
    Collections.sort(cols);
    Assert.assertEquals("a", cols.get(0).getName());
    Assert.assertEquals(ColumnType.INT_TYPE_NAME, cols.get(0).getType());
    Assert.assertEquals("b", cols.get(1).getName());
    Assert.assertEquals(ColumnType.FLOAT_TYPE_NAME, cols.get(1).getType());

    objectStore.dropSchemaVersion(schemaName, version);
    schemaVersion = objectStore.getSchemaVersion(schemaName, version);
    Assert.assertNull(schemaVersion);
  }

  // Test that adding multiple versions of the same schema
  @Test
  public void multipleSchemaVersions() throws MetaException, AlreadyExistsException,
      NoSuchObjectException {
    String schemaName = "schema195";

    ISchema schema = new ISchemaBuilder()
        .setSchemaType(SchemaType.AVRO)
        .setName(schemaName)
        .build();
    objectStore.createISchema(schema);
    SchemaVersion schemaVersion = new SchemaVersionBuilder()
        .setSchemaName(schemaName)
        .setVersion(1)
        .addCol("a", ColumnType.BIGINT_TYPE_NAME)
        .build();
    objectStore.addSchemaVersion(schemaVersion);

    schemaVersion = new SchemaVersionBuilder()
        .setSchemaName(schemaName)
        .setVersion(2)
        .addCol("a", ColumnType.BIGINT_TYPE_NAME)
        .addCol("b", ColumnType.DATE_TYPE_NAME)
        .build();
    objectStore.addSchemaVersion(schemaVersion);

    schemaVersion = new SchemaVersionBuilder()
        .setSchemaName(schemaName)
        .setVersion(3)
        .addCol("a", ColumnType.BIGINT_TYPE_NAME)
        .addCol("b", ColumnType.DATE_TYPE_NAME)
        .addCol("c", ColumnType.TIMESTAMP_TYPE_NAME)
        .build();
    objectStore.addSchemaVersion(schemaVersion);

    schemaVersion = objectStore.getLatestSchemaVersion(schemaName);
    Assert.assertEquals(3, schemaVersion.getVersion());
    Assert.assertEquals(3, schemaVersion.getColsSize());
    List<FieldSchema> cols = schemaVersion.getCols();
    Collections.sort(cols);
    Assert.assertEquals("a", cols.get(0).getName());
    Assert.assertEquals("b", cols.get(1).getName());
    Assert.assertEquals("c", cols.get(2).getName());
    Assert.assertEquals(ColumnType.BIGINT_TYPE_NAME, cols.get(0).getType());
    Assert.assertEquals(ColumnType.DATE_TYPE_NAME, cols.get(1).getType());
    Assert.assertEquals(ColumnType.TIMESTAMP_TYPE_NAME, cols.get(2).getType());

    schemaVersion = objectStore.getLatestSchemaVersion("no.such.schema.with.this.name");
    Assert.assertNull(schemaVersion);

    List<SchemaVersion> versions =
        objectStore.getAllSchemaVersion("there.really.isnt.a.schema.named.this");
    Assert.assertEquals(0, versions.size());

    versions = objectStore.getAllSchemaVersion(schemaName);
    Assert.assertEquals(3, versions.size());
    versions.sort((o1, o2) -> Integer.compare(o1.getVersion(), o2.getVersion()));
    Assert.assertEquals(1, versions.get(0).getVersion());
    Assert.assertEquals(1, versions.get(0).getColsSize());
    Assert.assertEquals(ColumnType.BIGINT_TYPE_NAME, versions.get(0).getCols().get(0).getType());

    Assert.assertEquals(2, versions.get(1).getVersion());
    Assert.assertEquals(2, versions.get(1).getColsSize());
    cols = versions.get(1).getCols();
    Collections.sort(cols);
    Assert.assertEquals("a", cols.get(0).getName());
    Assert.assertEquals("b", cols.get(1).getName());
    Assert.assertEquals(ColumnType.BIGINT_TYPE_NAME, cols.get(0).getType());
    Assert.assertEquals(ColumnType.DATE_TYPE_NAME, cols.get(1).getType());

    Assert.assertEquals(3, versions.get(2).getVersion());
    Assert.assertEquals(3, versions.get(2).getColsSize());
    cols = versions.get(2).getCols();
    Collections.sort(cols);
    Assert.assertEquals("a", cols.get(0).getName());
    Assert.assertEquals("b", cols.get(1).getName());
    Assert.assertEquals("c", cols.get(2).getName());
    Assert.assertEquals(ColumnType.BIGINT_TYPE_NAME, cols.get(0).getType());
    Assert.assertEquals(ColumnType.DATE_TYPE_NAME, cols.get(1).getType());
    Assert.assertEquals(ColumnType.TIMESTAMP_TYPE_NAME, cols.get(2).getType());
  }

  @Test(expected = AlreadyExistsException.class)
  public void addDuplicateSchemaVersion() throws MetaException, AlreadyExistsException,
      NoSuchObjectException {
    String schemaName = "schema1234";
    int version = 1;
    SchemaVersion schemaVersion = objectStore.getSchemaVersion(schemaName, version);
    Assert.assertNull(schemaVersion);

    ISchema schema = new ISchemaBuilder()
        .setSchemaType(SchemaType.AVRO)
        .setName(schemaName)
        .build();
    objectStore.createISchema(schema);

    schemaVersion = new SchemaVersionBuilder()
        .setSchemaName(schemaName)
        .setVersion(version)
        .addCol("a", ColumnType.INT_TYPE_NAME)
        .addCol("b", ColumnType.FLOAT_TYPE_NAME)
        .build();
    objectStore.addSchemaVersion(schemaVersion);

    objectStore.addSchemaVersion(schemaVersion);
  }

  @Test
  public void alterSchemaVersion() throws MetaException, AlreadyExistsException,
      NoSuchObjectException {
    String schemaName = "schema371234";
    int version = 1;
    SchemaVersion schemaVersion = objectStore.getSchemaVersion(schemaName, version);
    Assert.assertNull(schemaVersion);

    ISchema schema = new ISchemaBuilder()
        .setSchemaType(SchemaType.AVRO)
        .setName(schemaName)
        .build();
    objectStore.createISchema(schema);

    schemaVersion = new SchemaVersionBuilder()
        .setSchemaName(schemaName)
        .setVersion(version)
        .addCol("a", ColumnType.INT_TYPE_NAME)
        .addCol("b", ColumnType.FLOAT_TYPE_NAME)
        .setState(SchemaVersionState.INITIATED)
        .build();
    objectStore.addSchemaVersion(schemaVersion);

    schemaVersion = objectStore.getSchemaVersion(schemaName, version);
    Assert.assertNotNull(schemaVersion);
    Assert.assertEquals(schemaName, schemaVersion.getSchemaName());
    Assert.assertEquals(version, schemaVersion.getVersion());
    Assert.assertEquals(SchemaVersionState.INITIATED, schemaVersion.getState());

    schemaVersion.setState(SchemaVersionState.REVIEWED);
    String serdeName = "serde for " + schemaName;
    SerDeInfo serde = new SerDeInfo(serdeName, "", Collections.emptyMap());
    String serializer = "org.apache.hadoop.hive.metastore.test.Serializer";
    String deserializer = "org.apache.hadoop.hive.metastore.test.Deserializer";
    serde.setSerializerClass(serializer);
    serde.setDeserializerClass(deserializer);
    schemaVersion.setSerDe(serde);
    objectStore.alterSchemaVersion(schemaName, version, schemaVersion);

    schemaVersion = objectStore.getSchemaVersion(schemaName, version);
    Assert.assertNotNull(schemaVersion);
    Assert.assertEquals(schemaName, schemaVersion.getSchemaName());
    Assert.assertEquals(version, schemaVersion.getVersion());
    Assert.assertEquals(SchemaVersionState.REVIEWED, schemaVersion.getState());
    Assert.assertEquals(serdeName, schemaVersion.getSerDe().getName());
    Assert.assertEquals(serializer, schemaVersion.getSerDe().getSerializerClass());
    Assert.assertEquals(deserializer, schemaVersion.getSerDe().getDeserializerClass());
  }

  @Test(expected = NoSuchObjectException.class)
  public void alterNonExistentSchemaVersion() throws MetaException, AlreadyExistsException,
      NoSuchObjectException {
    String schemaName = "schema3723asdflj";
    int version = 37;
    SchemaVersion schemaVersion = new SchemaVersionBuilder()
        .setSchemaName(schemaName)
        .setVersion(version)
        .addCol("a", ColumnType.INT_TYPE_NAME)
        .addCol("b", ColumnType.FLOAT_TYPE_NAME)
        .setState(SchemaVersionState.INITIATED)
        .build();
    objectStore.alterSchemaVersion(schemaName, version, schemaVersion);
  }

  @Test(expected = NoSuchObjectException.class)
  public void dropNonExistentSchemaVersion() throws NoSuchObjectException, MetaException {
    objectStore.dropSchemaVersion("ther is no schema named this", 23);
  }

  @Test
  public void schemaQuery() throws AlreadyExistsException, MetaException, NoSuchObjectException {
    String schemaName1 = "a_schema1";
    ISchema schema1 = new ISchemaBuilder()
        .setSchemaType(SchemaType.AVRO)
        .setName(schemaName1)
        .build();
    objectStore.createISchema(schema1);

    String schemaName2 = "a_schema2";
    ISchema schema2 = new ISchemaBuilder()
        .setSchemaType(SchemaType.AVRO)
        .setName(schemaName2)
        .build();
    objectStore.createISchema(schema2);

    SchemaVersion schemaVersion1_1 = new SchemaVersionBuilder()
        .setSchemaName(schemaName1)
        .setVersion(1)
        .addCol("alpha", ColumnType.BIGINT_TYPE_NAME)
        .addCol("beta", ColumnType.DATE_TYPE_NAME)
        .build();
    objectStore.addSchemaVersion(schemaVersion1_1);

    SchemaVersion schemaVersion1_2 = new SchemaVersionBuilder()
        .setSchemaName(schemaName1)
        .setVersion(2)
        .addCol("alpha", ColumnType.BIGINT_TYPE_NAME)
        .addCol("beta", ColumnType.DATE_TYPE_NAME)
        .addCol("gamma", ColumnType.BIGINT_TYPE_NAME, "namespace=x")
        .build();
    objectStore.addSchemaVersion(schemaVersion1_2);

    SchemaVersion schemaVersion2_1 = new SchemaVersionBuilder()
        .setSchemaName(schemaName2)
        .setVersion(1)
        .addCol("ALPHA", ColumnType.SMALLINT_TYPE_NAME)
        .addCol("delta", ColumnType.DOUBLE_TYPE_NAME)
        .build();
    objectStore.addSchemaVersion(schemaVersion2_1);

    SchemaVersion schemaVersion2_2 = new SchemaVersionBuilder()
        .setSchemaName(schemaName2)
        .setVersion(2)
        .addCol("ALPHA", ColumnType.SMALLINT_TYPE_NAME)
        .addCol("delta", ColumnType.DOUBLE_TYPE_NAME)
        .addCol("epsilon", ColumnType.STRING_TYPE_NAME, "namespace=x")
        .build();
    objectStore.addSchemaVersion(schemaVersion2_2);

    // Query that should return nothing
    List<SchemaVersion> results = objectStore.getSchemaVersionsByColumns("x", "y", "z");
    Assert.assertEquals(0, results.size());

    // Query that should fetch one column
    results = objectStore.getSchemaVersionsByColumns("gamma", null, null);
    Assert.assertEquals(1, results.size());
    Assert.assertEquals(schemaName1, results.get(0).getSchemaName());
    Assert.assertEquals(2, results.get(0).getVersion());

    // fetch 2 in same schema
    results = objectStore.getSchemaVersionsByColumns("beta", null, null);
    Assert.assertEquals(2, results.size());
    Collections.sort(results);
    Assert.assertEquals(schemaName1, results.get(0).getSchemaName());
    Assert.assertEquals(1, results.get(0).getVersion());
    Assert.assertEquals(schemaName1, results.get(1).getSchemaName());
    Assert.assertEquals(2, results.get(1).getVersion());

    // fetch across schemas
    results = objectStore.getSchemaVersionsByColumns("alpha", null, null);
    Assert.assertEquals(4, results.size());
    Collections.sort(results);
    Assert.assertEquals(schemaName1, results.get(0).getSchemaName());
    Assert.assertEquals(1, results.get(0).getVersion());
    Assert.assertEquals(schemaName1, results.get(1).getSchemaName());
    Assert.assertEquals(2, results.get(1).getVersion());
    Assert.assertEquals(schemaName2, results.get(2).getSchemaName());
    Assert.assertEquals(1, results.get(2).getVersion());
    Assert.assertEquals(schemaName2, results.get(3).getSchemaName());
    Assert.assertEquals(2, results.get(3).getVersion());

    // fetch by namespace
    results = objectStore.getSchemaVersionsByColumns(null, "namespace=x", null);
    Assert.assertEquals(2, results.size());
    Collections.sort(results);
    Assert.assertEquals(schemaName1, results.get(0).getSchemaName());
    Assert.assertEquals(2, results.get(0).getVersion());
    Assert.assertEquals(schemaName2, results.get(1).getSchemaName());
    Assert.assertEquals(2, results.get(1).getVersion());

    // fetch by name and type
    results = objectStore.getSchemaVersionsByColumns("alpha", null, ColumnType.SMALLINT_TYPE_NAME);
    Assert.assertEquals(2, results.size());
    Collections.sort(results);
    Assert.assertEquals(schemaName2, results.get(0).getSchemaName());
    Assert.assertEquals(1, results.get(0).getVersion());
    Assert.assertEquals(schemaName2, results.get(1).getSchemaName());
    Assert.assertEquals(2, results.get(1).getVersion());

    // Make sure matching name but wrong type doesn't return
    results = objectStore.getSchemaVersionsByColumns("alpha", null, ColumnType.STRING_TYPE_NAME); Assert.assertEquals(0, results.size());
  }

  @Test(expected = MetaException.class)
  public void schemaVersionQueryNoNameOrNamespace() throws MetaException {
    objectStore.getSchemaVersionsByColumns(null, null, ColumnType.STRING_TYPE_NAME);
  }
}

