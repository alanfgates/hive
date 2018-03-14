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
package org.apache.hadoop.hive.metastore.cache;


import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.metastore.Deadline;
import org.apache.hadoop.hive.metastore.FileMetadataHandler;
import org.apache.hadoop.hive.metastore.ObjectStore;
import org.apache.hadoop.hive.metastore.PartFilterExprUtil;
import org.apache.hadoop.hive.metastore.PartitionExpressionProxy;
import org.apache.hadoop.hive.metastore.RawStore;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.AggrStats;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.Catalog;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsDesc;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.CreationMetadata;
import org.apache.hadoop.hive.metastore.api.CurrentNotificationEventId;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.FileMetadataExprType;
import org.apache.hadoop.hive.metastore.api.Function;
import org.apache.hadoop.hive.metastore.api.HiveObjectPrivilege;
import org.apache.hadoop.hive.metastore.api.ISchema;
import org.apache.hadoop.hive.metastore.api.ISchemaName;
import org.apache.hadoop.hive.metastore.api.InvalidInputException;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.InvalidPartitionException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.NotificationEvent;
import org.apache.hadoop.hive.metastore.api.NotificationEventRequest;
import org.apache.hadoop.hive.metastore.api.NotificationEventResponse;
import org.apache.hadoop.hive.metastore.api.NotificationEventsCountRequest;
import org.apache.hadoop.hive.metastore.api.NotificationEventsCountResponse;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PartitionEventType;
import org.apache.hadoop.hive.metastore.api.PartitionValuesResponse;
import org.apache.hadoop.hive.metastore.api.PrincipalPrivilegeSet;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.PrivilegeBag;
import org.apache.hadoop.hive.metastore.api.WMNullablePool;
import org.apache.hadoop.hive.metastore.api.WMNullableResourcePlan;
import org.apache.hadoop.hive.metastore.api.WMResourcePlan;
import org.apache.hadoop.hive.metastore.api.WMTrigger;
import org.apache.hadoop.hive.metastore.api.WMValidateResourcePlanResponse;
import org.apache.hadoop.hive.metastore.cache.SharedCache.StatsType;
import org.apache.hadoop.hive.metastore.columnstats.aggr.ColumnStatsAggregator;
import org.apache.hadoop.hive.metastore.columnstats.aggr.ColumnStatsAggregatorFactory;
import org.apache.hadoop.hive.metastore.api.Role;
import org.apache.hadoop.hive.metastore.api.RolePrincipalGrant;
import org.apache.hadoop.hive.metastore.api.SQLDefaultConstraint;
import org.apache.hadoop.hive.metastore.api.SQLForeignKey;
import org.apache.hadoop.hive.metastore.api.SQLNotNullConstraint;
import org.apache.hadoop.hive.metastore.api.SQLPrimaryKey;
import org.apache.hadoop.hive.metastore.api.SQLUniqueConstraint;
import org.apache.hadoop.hive.metastore.api.SchemaVersion;
import org.apache.hadoop.hive.metastore.api.SchemaVersionDescriptor;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.TableMeta;
import org.apache.hadoop.hive.metastore.api.Type;
import org.apache.hadoop.hive.metastore.api.UnknownDBException;
import org.apache.hadoop.hive.metastore.api.UnknownPartitionException;
import org.apache.hadoop.hive.metastore.api.UnknownTableException;
import org.apache.hadoop.hive.metastore.api.WMFullResourcePlan;
import org.apache.hadoop.hive.metastore.api.WMMapping;
import org.apache.hadoop.hive.metastore.api.WMPool;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars;
import org.apache.hadoop.hive.metastore.partition.spec.PartitionSpecProxy;
import org.apache.hadoop.hive.metastore.utils.FileUtils;
import org.apache.hadoop.hive.metastore.utils.JavaUtils;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils.ColStatsObjWithSourceInfo;
import org.apache.hadoop.hive.metastore.utils.StringUtils;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

import static org.apache.hadoop.hive.metastore.Warehouse.DEFAULT_CATALOG_NAME;
import static org.apache.hadoop.hive.metastore.utils.StringUtils.normalizeIdentifier;

// TODO filter->expr
// TODO functionCache
// TODO constraintCache
// TODO need sd nested copy?
// TODO String intern
// TODO restructure HBaseStore
// TODO monitor event queue
// TODO initial load slow?
// TODO size estimation
// TODO factor in extrapolation logic (using partitions found) during aggregate stats calculation

public class CachedStore implements RawStore, Configurable {
  private static ScheduledExecutorService cacheUpdateMaster = null;
  private static ReentrantReadWriteLock databaseCacheLock = new ReentrantReadWriteLock(true);
  private static AtomicBoolean isDatabaseCacheDirty = new AtomicBoolean(false);
  private static ReentrantReadWriteLock catalogCacheLock = new ReentrantReadWriteLock(true);
  private static ReentrantReadWriteLock tableCacheLock = new ReentrantReadWriteLock(true);
  private static AtomicBoolean isTableCacheDirty = new AtomicBoolean(false);
  private static ReentrantReadWriteLock partitionCacheLock = new ReentrantReadWriteLock(true);
  private static AtomicBoolean isPartitionCacheDirty = new AtomicBoolean(false);
  private static ReentrantReadWriteLock tableColStatsCacheLock = new ReentrantReadWriteLock(true);
  private static AtomicBoolean isTableColStatsCacheDirty = new AtomicBoolean(false);
  private static ReentrantReadWriteLock partitionColStatsCacheLock = new ReentrantReadWriteLock(
      true);
  private static ReentrantReadWriteLock partitionAggrColStatsCacheLock =
      new ReentrantReadWriteLock(true);
  private static AtomicBoolean isPartitionAggrColStatsCacheDirty = new AtomicBoolean(false);
  private static AtomicBoolean isPartitionColStatsCacheDirty = new AtomicBoolean(false);
  private static List<Pattern> whitelistPatterns = null;
  private static List<Pattern> blacklistPatterns = null;
  private RawStore rawStore = null;
  private Configuration conf;
  private PartitionExpressionProxy expressionProxy = null;
  // Default value set to 100 milliseconds for test purpose
  private static long cacheRefreshPeriod = 100;

  /** A wrapper over SharedCache. Allows one to get SharedCache safely; should be merged
   *  into SharedCache itself (see the TODO on the class). */
  private static final SharedCacheWrapper sharedCacheWrapper = new SharedCacheWrapper();

  static final private Logger LOG = LoggerFactory.getLogger(CachedStore.class.getName());

  static class TableWrapper {
    Table t;
    String location;
    Map<String, String> parameters;
    byte[] sdHash;
    TableWrapper(Table t, byte[] sdHash, String location, Map<String, String> parameters) {
      this.t = t;
      this.sdHash = sdHash;
      this.location = location;
      this.parameters = parameters;
    }
    public Table getTable() {
      return t;
    }
    public byte[] getSdHash() {
      return sdHash;
    }
    public String getLocation() {
      return location;
    }
    public Map<String, String> getParameters() {
      return parameters;
    }
  }

  static class PartitionWrapper {
    Partition p;
    String location;
    Map<String, String> parameters;
    byte[] sdHash;
    PartitionWrapper(Partition p, byte[] sdHash, String location, Map<String, String> parameters) {
      this.p = p;
      this.sdHash = sdHash;
      this.location = location;
      this.parameters = parameters;
    }
    public Partition getPartition() {
      return p;
    }
    public byte[] getSdHash() {
      return sdHash;
    }
    public String getLocation() {
      return location;
    }
    public Map<String, String> getParameters() {
      return parameters;
    }
  }

  static class StorageDescriptorWrapper {
    StorageDescriptor sd;
    int refCount = 0;
    StorageDescriptorWrapper(StorageDescriptor sd, int refCount) {
      this.sd = sd;
      this.refCount = refCount;
    }
    public StorageDescriptor getSd() {
      return sd;
    }
    public int getRefCount() {
      return refCount;
    }
  }

  public CachedStore() {
  }

  public static void initSharedCacheAsync(Configuration conf) {
    String clazzName = null;
    boolean isEnabled = false;
    try {
      clazzName = MetastoreConf.getVar(conf, MetastoreConf.ConfVars.RAW_STORE_IMPL);
      isEnabled = JavaUtils.getClass(clazzName, RawStore.class).isAssignableFrom(CachedStore.class);
    } catch (MetaException e) {
      LOG.error("Cannot instantiate metastore class", e);
    }
    if (!isEnabled) {
      LOG.debug("CachedStore is not enabled; using " + clazzName);
      return;
    }
    sharedCacheWrapper.startInit(conf);
  }

  @Override
  public void setConf(Configuration conf) {
    String rawStoreClassName = MetastoreConf.getVar(conf, ConfVars.CACHED_RAW_STORE_IMPL,
        ObjectStore.class.getName());
    if (rawStore == null) {
      try {
        rawStore = (JavaUtils.getClass(rawStoreClassName, RawStore.class)).newInstance();
      } catch (Exception e) {
        throw new RuntimeException("Cannot instantiate " + rawStoreClassName, e);
      }
    }
    rawStore.setConf(conf);
    Configuration oldConf = this.conf;
    this.conf = conf;
    if (expressionProxy != null && conf != oldConf) {
      LOG.warn("Unexpected setConf when we were already configured");
    }
    if (expressionProxy == null || conf != oldConf) {
      expressionProxy = PartFilterExprUtil.createExpressionProxy(conf);
    }
    initBlackListWhiteList(conf);
  }

  @VisibleForTesting
  static void prewarm(RawStore rawStore) throws Exception {
    // Prevents throwing exceptions in our raw store calls since we're not using RawStoreProxy
    Deadline.registerIfNot(1000000);
    Collection<String> catalogsToCache = catalogsToCache(rawStore);
    LOG.info("Going to cache catalogs: " +
        org.apache.commons.lang.StringUtils.join(catalogsToCache, ", "));
    for (String catName : catalogsToCache) {
      SharedCache sharedCache = sharedCacheWrapper.getUnsafe();
      sharedCache.addCatalogToCache(rawStore.getCatalog(catName));
      List<String> dbNames = rawStore.getAllDatabases(catName);
      LOG.info("Number of databases to prewarm in catalog " + catName + ": " + dbNames.size());
      for (int i = 0; i < dbNames.size(); i++) {
        String dbName = normalizeIdentifier(dbNames.get(i));
        // Cache partition column stats
        Deadline.startTimer("getColStatsForDatabase");
        List<ColStatsObjWithSourceInfo> colStatsForDB =
            rawStore.getPartitionColStatsForDatabase(catName, dbName);
        Deadline.stopTimer();
        if (colStatsForDB != null) {
          sharedCache.addPartitionColStatsToCache(colStatsForDB);
        }
        LOG.info("Caching database: {}. Cached {} / {} databases so far.", dbName, i,
            dbNames.size());
        Database db = rawStore.getDatabase(catName, dbName);
        sharedCache.addDatabaseToCache(dbName, db);
        List<String> tblNames = rawStore.getAllTables(catName, dbName);
        LOG.debug("Tables in database: {} : {}", dbName, tblNames);
        for (int j = 0; j < tblNames.size(); j++) {
          String tblName = normalizeIdentifier(tblNames.get(j));
          if (!shouldCacheTable(catName, dbName, tblName)) {
            LOG.info("Not caching database: {}'s table: {}", dbName, tblName);
            continue;
          }
          LOG.info("Caching database: {}'s table: {}. Cached {} / {}  tables so far.", dbName,
              tblName, j, tblNames.size());
          Table table = rawStore.getTable(catName, dbName, tblName);
          // It is possible the table is deleted during fetching tables of the database,
          // in that case, continue with the next table
          if (table == null) {
            continue;
          }
          sharedCache.addTableToCache(catName, dbName, tblName, table);
          if (table.getPartitionKeys() != null && table.getPartitionKeys().size() > 0) {
            Deadline.startTimer("getPartitions");
            List<Partition> partitions =
                rawStore.getPartitions(catName, dbName, tblName, Integer.MAX_VALUE);
            Deadline.stopTimer();
            for (Partition partition : partitions) {
              sharedCache.addPartitionToCache(catName, dbName, tblName, partition);
            }
          }
          // Cache table column stats
          List<String> colNames = MetaStoreUtils.getColumnNamesForTable(table);
          Deadline.startTimer("getTableColumnStatistics");
          ColumnStatistics tableColStats =
              rawStore.getTableColumnStatistics(catName, dbName, tblName, colNames);
          Deadline.stopTimer();
          if ((tableColStats != null) && (tableColStats.getStatsObjSize() > 0)) {
            sharedCache.addTableColStatsToCache(catName, dbName, tblName, tableColStats.getStatsObj());
          }
          // Cache aggregate stats for all partitions of a table and for all but default partition
          List<String> partNames = rawStore.listPartitionNames(catName, dbName, tblName, (short) -1);
          if ((partNames != null) && (partNames.size() > 0)) {
            AggrStats aggrStatsAllPartitions =
                rawStore.get_aggr_stats_for(catName, dbName, tblName, partNames, colNames);
            // Remove default partition from partition names and get aggregate
            // stats again
            List<FieldSchema> partKeys = table.getPartitionKeys();
            String defaultPartitionValue =
                MetastoreConf.getVar(rawStore.getConf(), ConfVars.DEFAULTPARTITIONNAME);
            List<String> partCols = new ArrayList<>();
            List<String> partVals = new ArrayList<>();
            for (FieldSchema fs : partKeys) {
              partCols.add(fs.getName());
              partVals.add(defaultPartitionValue);
            }
            String defaultPartitionName = FileUtils.makePartName(partCols, partVals);
            partNames.remove(defaultPartitionName);
            AggrStats aggrStatsAllButDefaultPartition =
                rawStore.get_aggr_stats_for(catName, dbName, tblName, partNames, colNames);
            sharedCache.addAggregateStatsToCache(catName, dbName, tblName, aggrStatsAllPartitions,
                aggrStatsAllButDefaultPartition);
          }
        }
      }
    }
    // Notify all blocked threads that prewarm is complete now
    sharedCacheWrapper.notifyAllBlocked();
  }

  @VisibleForTesting
  static SharedCache getSharedCache() throws MetaException {
    return sharedCacheWrapper.get();
  }

  private static void initBlackListWhiteList(Configuration conf) {
    if (whitelistPatterns == null || blacklistPatterns == null) {
      whitelistPatterns = createPatterns(MetastoreConf.getAsString(conf,
          MetastoreConf.ConfVars.CACHED_RAW_STORE_CACHED_OBJECTS_WHITELIST));
      blacklistPatterns = createPatterns(MetastoreConf.getAsString(conf,
          MetastoreConf.ConfVars.CACHED_RAW_STORE_CACHED_OBJECTS_BLACKLIST));
      // The last specified blacklist pattern gets precedence
      Collections.reverse(blacklistPatterns);
    }
  }

  private static Collection<String> catalogsToCache(RawStore rs) throws MetaException {
    Collection<String> confValue =
        MetastoreConf.getStringCollection(rs.getConf(), ConfVars.CATALOGS_TO_CACHE);
    if (confValue == null || confValue.isEmpty() ||
        (confValue.size() == 1 && confValue.contains(""))) {
      return rs.getCatalogs();
    } else {
      return confValue;
    }
  }

  @VisibleForTesting
  synchronized static void startCacheUpdateService(Configuration conf) {
    if (cacheUpdateMaster == null) {
      initBlackListWhiteList(conf);
      if (!MetastoreConf.getBoolVar(conf, ConfVars.HIVE_IN_TEST)) {
        cacheRefreshPeriod = MetastoreConf.getTimeVar(conf,
            ConfVars.CACHED_RAW_STORE_CACHE_UPDATE_FREQUENCY, TimeUnit.MILLISECONDS);
      }
      LOG.info("CachedStore: starting cache update service (run every {} ms", cacheRefreshPeriod);
      cacheUpdateMaster = Executors.newScheduledThreadPool(1, new ThreadFactory() {
        @Override
        public Thread newThread(Runnable r) {
          Thread t = Executors.defaultThreadFactory().newThread(r);
          t.setName("CachedStore-CacheUpdateService: Thread-" + t.getId());
          t.setDaemon(true);
          return t;
        }
      });
      cacheUpdateMaster.scheduleAtFixedRate(new CacheUpdateMasterWork(conf), 0, cacheRefreshPeriod,
          TimeUnit.MILLISECONDS);
    }
  }

  @VisibleForTesting
  synchronized static boolean stopCacheUpdateService(long timeout) {
    boolean tasksStoppedBeforeShutdown = false;
    if (cacheUpdateMaster != null) {
      LOG.info("CachedStore: shutting down cache update service");
      try {
        tasksStoppedBeforeShutdown =
            cacheUpdateMaster.awaitTermination(timeout, TimeUnit.MILLISECONDS);
      } catch (InterruptedException e) {
        LOG.info("CachedStore: cache update service was interrupted while waiting for tasks to "
            + "complete before shutting down. Will make a hard stop now.");
      }
      cacheUpdateMaster.shutdownNow();
      cacheUpdateMaster = null;
    }
    return tasksStoppedBeforeShutdown;
  }

  @VisibleForTesting
  static void setCacheRefreshPeriod(long time) {
    cacheRefreshPeriod = time;
  }

  static class CacheUpdateMasterWork implements Runnable {
    private boolean isFirstRun = true;
    private final RawStore rawStore;

    public CacheUpdateMasterWork(Configuration conf) {
      String rawStoreClassName = MetastoreConf.getVar(conf, ConfVars.CACHED_RAW_STORE_IMPL,
          ObjectStore.class.getName());
      try {
        rawStore = JavaUtils.getClass(rawStoreClassName, RawStore.class).newInstance();
        rawStore.setConf(conf);
      } catch (InstantiationException | IllegalAccessException | MetaException e) {
        // MetaException here really means ClassNotFound (see the utility method).
        // So, if any of these happen, that means we can never succeed.
        sharedCacheWrapper.updateInitState(e, true);
        throw new RuntimeException("Cannot instantiate " + rawStoreClassName, e);
      }
    }

    @Override
    public void run() {
      if (isFirstRun) {
        while (isFirstRun) {
          try {
            long startTime = System.nanoTime();
            LOG.info("Prewarming CachedStore");
            prewarm(rawStore);
            LOG.info("CachedStore initialized");
            long endTime = System.nanoTime();
            LOG.info("Time taken in prewarming = " + (endTime - startTime) / 1000000 + "ms");
          } catch (Exception e) {
            LOG.error("Prewarm failure", e);
            sharedCacheWrapper.updateInitState(e, false);
            return;
          }
          sharedCacheWrapper.updateInitState(null, false);
          isFirstRun = false;
        }
      } else {
        // TODO: prewarm and update can probably be merged.
        update();
      }
    }

    public void update() {
      Deadline.registerIfNot(1000000);
      LOG.debug("CachedStore: updating cached objects");
      try {
        for (String catName : catalogsToCache(rawStore)) {
          List<String> dbNames = rawStore.getAllDatabases(catName);
          if (dbNames != null) {
            // Update the database in cache
            updateDatabases(rawStore, catName, dbNames);
            for (String dbName : dbNames) {
              updateDatabasePartitionColStats(rawStore, catName, dbName);
              // Update the tables in cache
              updateTables(rawStore, catName, dbName);
              List<String> tblNames = getAllTablesInternal(catName, dbName,
                  sharedCacheWrapper.getUnsafe());
              for (String tblName : tblNames) {
                if (!shouldCacheTable(catName, dbName, tblName)) {
                  continue;
                }
                // Update the partitions for a table in cache
                updateTablePartitions(rawStore, catName, dbName, tblName);
                // Update the table column stats for a table in cache
                updateTableColStats(rawStore, catName, dbName, tblName);
                // Update aggregate column stats cache
                updateAggregateStatsCache(rawStore, catName, dbName, tblName);
              }
            }
          }
        }
      } catch (Exception e) {
        LOG.error("Updating CachedStore: error happen when refresh; ignoring", e);
      }
    }

    private void updateDatabasePartitionColStats(RawStore rawStore, String catName, String dbName) {
      try {
        Deadline.startTimer("getColStatsForDatabasePartitions");
        List<ColStatsObjWithSourceInfo> colStatsForDB =
            rawStore.getPartitionColStatsForDatabase(catName, dbName);
        Deadline.stopTimer();
        if (colStatsForDB != null) {
          if (partitionColStatsCacheLock.writeLock().tryLock()) {
            // Skip background updates if we detect change
            if (isPartitionColStatsCacheDirty.compareAndSet(true, false)) {
              LOG.debug("Skipping partition column stats cache update; the partition column stats "
                  + "list we have is dirty.");
              return;
            }
            sharedCacheWrapper.getUnsafe() .refreshPartitionColStats(normalizeIdentifier(catName),
                    normalizeIdentifier(dbName), colStatsForDB);
          }
        }
      } catch (MetaException | NoSuchObjectException e) {
        LOG.info("Updating CachedStore: unable to read partitions column stats of database: {}",
            dbName, e);
      } finally {
        if (partitionColStatsCacheLock.isWriteLockedByCurrentThread()) {
          partitionColStatsCacheLock.writeLock().unlock();
        }
      }
    }

    // Update cached aggregate stats for all partitions of a table and for all
    // but default partition
    private void updateAggregateStatsCache(RawStore rawStore, String catName, String dbName,
                                           String tblName) {
      try {
        Table table = rawStore.getTable(catName, dbName, tblName);
        List<String> partNames = rawStore.listPartitionNames(catName, dbName, tblName, (short) -1);
        List<String> colNames = MetaStoreUtils.getColumnNamesForTable(table);
        if ((partNames != null) && (partNames.size() > 0)) {
          Deadline.startTimer("getAggregareStatsForAllPartitions");
          AggrStats aggrStatsAllPartitions =
              rawStore.get_aggr_stats_for(catName, dbName, tblName, partNames, colNames);
          Deadline.stopTimer();
          // Remove default partition from partition names and get aggregate stats again
          List<FieldSchema> partKeys = table.getPartitionKeys();
          String defaultPartitionValue =
              MetastoreConf.getVar(rawStore.getConf(), ConfVars.DEFAULTPARTITIONNAME);
          List<String> partCols = new ArrayList<>();
          List<String> partVals = new ArrayList<>();
          for (FieldSchema fs : partKeys) {
            partCols.add(fs.getName());
            partVals.add(defaultPartitionValue);
          }
          String defaultPartitionName = FileUtils.makePartName(partCols, partVals);
          partNames.remove(defaultPartitionName);
          Deadline.startTimer("getAggregareStatsForAllPartitionsExceptDefault");
          AggrStats aggrStatsAllButDefaultPartition =
              rawStore.get_aggr_stats_for(catName, dbName, tblName, partNames, colNames);
          Deadline.stopTimer();
          if ((aggrStatsAllPartitions != null) && (aggrStatsAllButDefaultPartition != null)) {
            if (partitionAggrColStatsCacheLock.writeLock().tryLock()) {
              // Skip background updates if we detect change
              if (isPartitionAggrColStatsCacheDirty.compareAndSet(true, false)) {
                LOG.debug(
                    "Skipping aggregate column stats cache update; the aggregate column stats we "
                        + "have is dirty.");
                return;
              }
              sharedCacheWrapper.getUnsafe().refreshAggregateStatsCache(
                  normalizeIdentifier(catName), normalizeIdentifier(dbName),
                  normalizeIdentifier(tblName), aggrStatsAllPartitions,
                  aggrStatsAllButDefaultPartition);
            }
          }
        }
      } catch (MetaException | NoSuchObjectException e) {
        LOG.info("Updating CachedStore: unable to read aggregate column stats of table: " + tblName,
            e);
      } finally {
        if (partitionAggrColStatsCacheLock.isWriteLockedByCurrentThread()) {
          partitionAggrColStatsCacheLock.writeLock().unlock();
        }
      }
    }

    private void updateDatabases(RawStore rawStore, String catName, List<String> dbNames) {
      // Prepare the list of databases
      List<Database> databases = new ArrayList<>();
      for (String dbName : dbNames) {
        Database db;
        try {
          db = rawStore.getDatabase(catName, dbName);
          databases.add(db);
        } catch (NoSuchObjectException e) {
          LOG.info("Updating CachedStore: database - " + catName + "." + dbName
              + " does not exist.", e);
        }
      }
      // Update the cached database objects
      try {
        if (databaseCacheLock.writeLock().tryLock()) {
          // Skip background updates if we detect change
          if (isDatabaseCacheDirty.compareAndSet(true, false)) {
            LOG.debug("Skipping database cache update; the database list we have is dirty.");
            return;
          }
          sharedCacheWrapper.getUnsafe().refreshDatabases(databases);
        }
      } finally {
        if (databaseCacheLock.isWriteLockedByCurrentThread()) {
          databaseCacheLock.writeLock().unlock();
        }
      }
    }

    // Update the cached table objects
    private void updateTables(RawStore rawStore, String catName, String dbName) {
      List<Table> tables = new ArrayList<>();
      try {
        List<String> tblNames = rawStore.getAllTables(catName, dbName);
        for (String tblName : tblNames) {
          if (!shouldCacheTable(catName, dbName, tblName)) {
            continue;
          }
          Table table =
              rawStore.getTable(normalizeIdentifier(catName),
                  normalizeIdentifier(dbName),
                  normalizeIdentifier(tblName));
          tables.add(table);
        }
        if (tableCacheLock.writeLock().tryLock()) {
          // Skip background updates if we detect change
          if (isTableCacheDirty.compareAndSet(true, false)) {
            LOG.debug("Skipping table cache update; the table list we have is dirty.");
            return;
          }
          sharedCacheWrapper.getUnsafe().refreshTables(catName, dbName, tables);
        }
      } catch (MetaException e) {
        LOG.info("Updating CachedStore: unable to read tables for database - " + dbName, e);
      } finally {
        if (tableCacheLock.isWriteLockedByCurrentThread()) {
          tableCacheLock.writeLock().unlock();
        }
      }
    }

    // Update the cached partition objects for a table
    private void updateTablePartitions(RawStore rawStore, String catName, String dbName,
                                       String tblName) {
      try {
        Deadline.startTimer("getPartitions");
        List<Partition> partitions = rawStore.getPartitions(catName, dbName, tblName, Integer.MAX_VALUE);
        Deadline.stopTimer();
        if (partitionCacheLock.writeLock().tryLock()) {
          // Skip background updates if we detect change
          if (isPartitionCacheDirty.compareAndSet(true, false)) {
            LOG.debug("Skipping partition cache update; the partition list we have is dirty.");
            return;
          }
          sharedCacheWrapper.getUnsafe().refreshPartitions(normalizeIdentifier(catName),
              normalizeIdentifier(dbName),
              normalizeIdentifier(tblName), partitions);
        }
      } catch (MetaException | NoSuchObjectException e) {
        LOG.info("Updating CachedStore: unable to read partitions of table: " + tblName, e);
      } finally {
        if (partitionCacheLock.isWriteLockedByCurrentThread()) {
          partitionCacheLock.writeLock().unlock();
        }
      }
    }

    // Update the cached col stats for this table
    private void updateTableColStats(RawStore rawStore, String catName, String dbName,
                                     String tblName) {
      try {
        Table table = rawStore.getTable(catName, dbName, tblName);
        List<String> colNames = MetaStoreUtils.getColumnNamesForTable(table);
        Deadline.startTimer("getTableColumnStatistics");
        ColumnStatistics tableColStats =
            rawStore.getTableColumnStatistics(catName, dbName, tblName, colNames);
        Deadline.stopTimer();
        if (tableColStats != null) {
          if (tableColStatsCacheLock.writeLock().tryLock()) {
            // Skip background updates if we detect change
            if (isTableColStatsCacheDirty.compareAndSet(true, false)) {
              LOG.debug("Skipping table column stats cache update; the table column stats list we "
                  + "have is dirty.");
              return;
            }
            sharedCacheWrapper.getUnsafe().refreshTableColStats(normalizeIdentifier(catName),
                normalizeIdentifier(dbName), normalizeIdentifier(tblName),
                tableColStats.getStatsObj());
          }
        }
      } catch (MetaException | NoSuchObjectException e) {
        LOG.info("Updating CachedStore: unable to read table column stats of table: " + tblName, e);
      } finally {
        if (tableColStatsCacheLock.isWriteLockedByCurrentThread()) {
          tableColStatsCacheLock.writeLock().unlock();
        }
      }
    }
  }

  @Override
  public Configuration getConf() {
    return rawStore.getConf();
  }

  @Override
  public void shutdown() {
    rawStore.shutdown();
  }

  @Override
  public boolean openTransaction() {
    return rawStore.openTransaction();
  }

  @Override
  public boolean commitTransaction() {
    return rawStore.commitTransaction();
  }

  @Override
  public boolean isActiveTransaction() {
    return rawStore.isActiveTransaction();
  }

  @Override
  public void rollbackTransaction() {
    rawStore.rollbackTransaction();
  }

  @Override
  public void createCatalog(Catalog cat) throws MetaException {
    rawStore.createCatalog(cat);
    SharedCache sharedCache = sharedCacheWrapper.get();
    if (sharedCache != null) {
      catalogCacheLock.readLock().lock();
      try (CloseableLock ignored = new CloseableLock(catalogCacheLock.readLock())) {
        sharedCache.addCatalogToCache(cat);
      }
    }
  }

  @Override
  public void alterCatalog(String catName, Catalog cat) throws MetaException,
      InvalidOperationException {
    rawStore.alterCatalog(catName, cat);
    SharedCache sharedCache = sharedCacheWrapper.get();
    if (sharedCache != null) {
      catalogCacheLock.readLock().lock();
      try (CloseableLock ignored = new CloseableLock(catalogCacheLock.readLock())) {
        sharedCache.alterCatalogInCache(normalizeIdentifier(catName), cat);
      }
    }
  }

  @Override
  public Catalog getCatalog(String catalogName) throws NoSuchObjectException, MetaException {
    if (!sharedCacheWrapper.isInitialized()) return rawStore.getCatalog(catalogName);

    Catalog cat =
        sharedCacheWrapper.get().getCatalogFromCache(normalizeIdentifier(catalogName));
    if (cat == null) throw new NoSuchObjectException("No catalog " + catalogName);
    return cat;
  }

  @Override
  public List<String> getCatalogs() throws MetaException {
    return (sharedCacheWrapper.isInitialized()) ? sharedCacheWrapper.get().listCachedCatalogs() :
      rawStore.getCatalogs();
  }

  @Override
  public void dropCatalog(String catalogName) throws NoSuchObjectException, MetaException {
    rawStore.dropCatalog(catalogName);
    SharedCache sharedCache = sharedCacheWrapper.get();
    if (sharedCache != null) {
      catalogCacheLock.readLock().lock();
      try (CloseableLock ignored = new CloseableLock(catalogCacheLock.readLock())){
        sharedCache.removeCatalogFromCache(normalizeIdentifier(catalogName));
      }
    }
  }

  @Override
  public void createDatabase(Database db) throws InvalidObjectException, MetaException {
    rawStore.createDatabase(db);
    SharedCache sharedCache = sharedCacheWrapper.get();
    if (sharedCache == null) {
      return;
    }
    try {
      // Wait if background cache update is happening
      databaseCacheLock.readLock().lock();
      isDatabaseCacheDirty.set(true);
      sharedCache.addDatabaseToCache(normalizeIdentifier(db.getName()), db.deepCopy());
    } finally {
      databaseCacheLock.readLock().unlock();
    }
  }

  @Override
  public Database getDatabase(String catName, String dbName) throws NoSuchObjectException {
    SharedCache sharedCache;

    if (!sharedCacheWrapper.isInitialized()) {
      return rawStore.getDatabase(catName, dbName);
    }

    try {
      sharedCache = sharedCacheWrapper.get();
    } catch (MetaException e) {
      throw new RuntimeException(e); // TODO: why doesn't getDatabase throw MetaEx?
    }
    Database db = sharedCache.getDatabaseFromCache(normalizeIdentifier(catName),
        normalizeIdentifier(dbName));
    if (db == null) {
      throw new NoSuchObjectException();
    }
    return db;
  }

  @Override
  public boolean dropDatabase(String catName, String dbname) throws NoSuchObjectException, MetaException {
    boolean succ = rawStore.dropDatabase(catName, dbname);
    if (succ) {
      SharedCache sharedCache = sharedCacheWrapper.get();
      if (sharedCache == null) {
        return succ;
      }
      try {
        // Wait if background cache update is happening
        databaseCacheLock.readLock().lock();
        isDatabaseCacheDirty.set(true);
        sharedCache.removeDatabaseFromCache(normalizeIdentifier(catName),
            normalizeIdentifier(dbname));
      } finally {
        databaseCacheLock.readLock().unlock();
      }
    }
    return succ;
  }

  @Override
  public boolean alterDatabase(String catName, String dbName, Database db) throws NoSuchObjectException,
      MetaException {
    boolean succ = rawStore.alterDatabase(catName, dbName, db);
    if (succ) {
      SharedCache sharedCache = sharedCacheWrapper.get();
      if (sharedCache == null) {
        return succ;
      }
      try {
        // Wait if background cache update is happening
        databaseCacheLock.readLock().lock();
        isDatabaseCacheDirty.set(true);
        sharedCache.alterDatabaseInCache(normalizeIdentifier(catName),
            normalizeIdentifier(dbName), db);
      } finally {
        databaseCacheLock.readLock().unlock();
      }
    }
    return succ;
  }

  @Override
  public List<String> getDatabases(String catName, String pattern) throws MetaException {
    if (!sharedCacheWrapper.isInitialized()) {
      return rawStore.getDatabases(catName, pattern);
    }
    catName = normalizeIdentifier(catName);
    SharedCache sharedCache = sharedCacheWrapper.get();
    List<String> results = new ArrayList<>();
    for (String dbName : sharedCache.listCachedDatabases(catName)) {
      dbName = normalizeIdentifier(dbName);
      if (CacheUtils.matches(dbName, pattern)) {
        results.add(dbName);
      }
    }
    return results;
  }

  @Override
  public List<String> getAllDatabases(String catName) throws MetaException {
    if (!sharedCacheWrapper.isInitialized()) {
      return rawStore.getAllDatabases(catName);
    }
    SharedCache sharedCache = sharedCacheWrapper.get();
    return sharedCache.listCachedDatabases(catName);
  }

  @Override
  public boolean createType(Type type) {
    return rawStore.createType(type);
  }

  @Override
  public Type getType(String typeName) {
    return rawStore.getType(typeName);
  }

  @Override
  public boolean dropType(String typeName) {
    return rawStore.dropType(typeName);
  }

  private void validateTableType(Table tbl) {
    // If the table has property EXTERNAL set, update table type
    // accordingly
    String tableType = tbl.getTableType();
    boolean isExternal = Boolean.parseBoolean(tbl.getParameters().get("EXTERNAL"));
    if (TableType.MANAGED_TABLE.toString().equals(tableType)) {
      if (isExternal) {
        tableType = TableType.EXTERNAL_TABLE.toString();
      }
    }
    if (TableType.EXTERNAL_TABLE.toString().equals(tableType)) {
      if (!isExternal) {
        tableType = TableType.MANAGED_TABLE.toString();
      }
    }
    tbl.setTableType(tableType);
  }

  @Override
  public void createTable(Table tbl) throws InvalidObjectException, MetaException {
    rawStore.createTable(tbl);
    String catName = normalizeIdentifier(tbl.getCatName());
    String dbName = normalizeIdentifier(tbl.getDbName());
    String tblName = normalizeIdentifier(tbl.getTableName());
    if (!shouldCacheTable(catName, dbName, tblName)) {
      return;
    }
    SharedCache sharedCache = sharedCacheWrapper.get();
    if (sharedCache == null) {
      return;
    }
    validateTableType(tbl);
    try {
      // Wait if background cache update is happening
      tableCacheLock.readLock().lock();
      isTableCacheDirty.set(true);
      sharedCache.addTableToCache(catName, dbName, tblName, tbl);
    } finally {
      tableCacheLock.readLock().unlock();
    }
  }

  @Override
  public boolean dropTable(String catName, String dbName, String tblName) throws MetaException,
      NoSuchObjectException, InvalidObjectException, InvalidInputException {
    boolean succ = rawStore.dropTable(catName, dbName, tblName);
    if (succ) {
      catName = normalizeIdentifier(catName);
      dbName = normalizeIdentifier(dbName);
      tblName = normalizeIdentifier(tblName);
      if (!shouldCacheTable(catName, dbName, tblName)) {
        return succ;
      }
      SharedCache sharedCache = sharedCacheWrapper.get();
      if (sharedCache == null) {
        return succ;
      }
      // Remove table
      try {
        // Wait if background table cache update is happening
        tableCacheLock.readLock().lock();
        isTableCacheDirty.set(true);
        sharedCache.removeTableFromCache(catName, dbName, tblName);
      } finally {
        tableCacheLock.readLock().unlock();
      }
      // Remove table col stats
      try {
        // Wait if background table col stats cache update is happening
        tableColStatsCacheLock.readLock().lock();
        isTableColStatsCacheDirty.set(true);
        sharedCache.removeTableColStatsFromCache(catName, dbName, tblName);
      } finally {
        tableColStatsCacheLock.readLock().unlock();
      }
    }
    return succ;
  }

  @Override
  public Table getTable(String catName, String dbName, String tblName) throws MetaException {
    catName = normalizeIdentifier(catName);
    dbName = normalizeIdentifier(dbName);
    tblName = normalizeIdentifier(tblName);
    if (!sharedCacheWrapper.isInitialized() || !shouldCacheTable(catName, dbName, tblName)) {
      return rawStore.getTable(catName, dbName, tblName);
    }
    SharedCache sharedCache = sharedCacheWrapper.get();
    Table tbl = sharedCache.getTableFromCache(catName, dbName, tblName);
    if (tbl != null) {
      tbl.unsetPrivileges();
      tbl.setRewriteEnabled(tbl.isRewriteEnabled());
    }
    return tbl;
  }

  @Override
  public boolean addPartition(Partition part) throws InvalidObjectException, MetaException {
    boolean succ = rawStore.addPartition(part);
    if (succ) {
      String dbName = normalizeIdentifier(part.getDbName());
      String tblName = normalizeIdentifier(part.getTableName());
      String catName = part.isSetCatName() ? normalizeIdentifier(part.getCatName()) : DEFAULT_CATALOG_NAME;
      if (!shouldCacheTable(catName, dbName, tblName)) {
        return succ;
      }
      SharedCache sharedCache = sharedCacheWrapper.get();
      if (sharedCache == null) {
        return succ;
      }
      try {
        // Wait if background cache update is happening
        partitionCacheLock.readLock().lock();
        isPartitionCacheDirty.set(true);
        sharedCache.addPartitionToCache(catName, dbName, tblName, part);
      } finally {
        partitionCacheLock.readLock().unlock();
      }
      // Remove aggregate partition col stats for this table
      try {
        // Wait if background cache update is happening
        partitionAggrColStatsCacheLock.readLock().lock();
        isPartitionAggrColStatsCacheDirty.set(true);
        sharedCache.removeAggrPartitionColStatsFromCache(catName, dbName, tblName);
      } finally {
        partitionAggrColStatsCacheLock.readLock().unlock();
      }
    }
    return succ;
  }

  @Override
  public boolean addPartitions(String catName, String dbName, String tblName, List<Partition> parts)
      throws InvalidObjectException, MetaException {
    boolean succ = rawStore.addPartitions(catName, dbName, tblName, parts);
    if (succ) {
      catName = normalizeIdentifier(catName);
      dbName = normalizeIdentifier(dbName);
      tblName = normalizeIdentifier(tblName);
      if (!shouldCacheTable(catName, dbName, tblName)) {
        return succ;
      }
      SharedCache sharedCache = sharedCacheWrapper.get();
      if (sharedCache == null) {
        return succ;
      }
      try {
        // Wait if background cache update is happening
        partitionCacheLock.readLock().lock();
        isPartitionCacheDirty.set(true);
        for (Partition part : parts) {
          sharedCache.addPartitionToCache(catName, dbName, tblName, part);
        }
      } finally {
        partitionCacheLock.readLock().unlock();
      }
      // Remove aggregate partition col stats for this table
      try {
        // Wait if background cache update is happening
        partitionAggrColStatsCacheLock.readLock().lock();
        isPartitionAggrColStatsCacheDirty.set(true);
        sharedCache.removeAggrPartitionColStatsFromCache(catName, dbName, tblName);
      } finally {
        partitionAggrColStatsCacheLock.readLock().unlock();
      }
    }
    return succ;
  }

  @Override
  public boolean addPartitions(String catName, String dbName, String tblName, PartitionSpecProxy partitionSpec,
      boolean ifNotExists) throws InvalidObjectException, MetaException {
    boolean succ = rawStore.addPartitions(catName, dbName, tblName, partitionSpec, ifNotExists);
    if (succ) {
      catName = normalizeIdentifier(catName);
      dbName = normalizeIdentifier(dbName);
      tblName = normalizeIdentifier(tblName);
      if (!shouldCacheTable(catName, dbName, tblName)) {
        return succ;
      }
      SharedCache sharedCache = sharedCacheWrapper.get();
      if (sharedCache == null) {
        return succ;
      }
      try {
        // Wait if background cache update is happening
        partitionCacheLock.readLock().lock();
        isPartitionCacheDirty.set(true);
        PartitionSpecProxy.PartitionIterator iterator = partitionSpec.getPartitionIterator();
        while (iterator.hasNext()) {
          Partition part = iterator.next();
          sharedCache.addPartitionToCache(catName, dbName, tblName, part);
        }
      } finally {
        partitionCacheLock.readLock().unlock();
      }
      // Remove aggregate partition col stats for this table
      try {
        // Wait if background cache update is happening
        partitionAggrColStatsCacheLock.readLock().lock();
        isPartitionAggrColStatsCacheDirty.set(true);
        sharedCache.removeAggrPartitionColStatsFromCache(catName, dbName, tblName);
      } finally {
        partitionAggrColStatsCacheLock.readLock().unlock();
      }
    }
    return succ;
  }

  @Override
  public Partition getPartition(String catName, String dbName, String tblName, List<String> part_vals)
      throws MetaException, NoSuchObjectException {
    dbName = normalizeIdentifier(dbName);
    tblName = normalizeIdentifier(tblName);

    if (!sharedCacheWrapper.isInitialized() || !shouldCacheTable(catName, dbName, tblName)) {
      return rawStore.getPartition(catName, dbName, tblName, part_vals);
    }
    SharedCache sharedCache = sharedCacheWrapper.get();
    Partition part = sharedCache.getPartitionFromCache(catName, dbName, tblName, part_vals);
    if (part == null) {
      // TODO Manage privileges
      throw new NoSuchObjectException("partition values=" + part_vals.toString());
    }
    return part;
  }

  @Override
  public boolean doesPartitionExist(String catName, String dbName, String tblName,
      List<String> part_vals) throws MetaException, NoSuchObjectException {
    dbName = normalizeIdentifier(dbName);
    tblName = normalizeIdentifier(tblName);
    if (!sharedCacheWrapper.isInitialized() || !shouldCacheTable(catName, dbName, tblName)) {
      return rawStore.doesPartitionExist(catName, dbName, tblName, part_vals);
    }
    SharedCache sharedCache = sharedCacheWrapper.get();
    return sharedCache.existPartitionFromCache(catName, dbName, tblName, part_vals);
  }

  @Override
  public boolean dropPartition(String catName, String dbName, String tblName, List<String> part_vals)
      throws MetaException, NoSuchObjectException, InvalidObjectException, InvalidInputException {
    boolean succ = rawStore.dropPartition(catName, dbName, tblName, part_vals);
    if (succ) {
      dbName = normalizeIdentifier(dbName);
      tblName = normalizeIdentifier(tblName);
      if (!shouldCacheTable(catName, dbName, tblName)) {
        return succ;
      }
      SharedCache sharedCache = sharedCacheWrapper.get();
      if (sharedCache == null) {
        return succ;
      }
      // Remove partition
      try {
        // Wait if background cache update is happening
        partitionCacheLock.readLock().lock();
        isPartitionCacheDirty.set(true);
        sharedCache.removePartitionFromCache(catName, dbName, tblName, part_vals);
      } finally {
        partitionCacheLock.readLock().unlock();
      }
      // Remove partition col stats
      try {
        // Wait if background cache update is happening
        partitionColStatsCacheLock.readLock().lock();
        isPartitionColStatsCacheDirty.set(true);
        sharedCache.removePartitionColStatsFromCache(catName, dbName, tblName, part_vals);
      } finally {
        partitionColStatsCacheLock.readLock().unlock();
      }
      // Remove aggregate partition col stats for this table
      try {
        // Wait if background cache update is happening
        partitionAggrColStatsCacheLock.readLock().lock();
        isPartitionAggrColStatsCacheDirty.set(true);
        sharedCache.removeAggrPartitionColStatsFromCache(catName, dbName, tblName);
      } finally {
        partitionAggrColStatsCacheLock.readLock().unlock();
      }
    }
    return succ;
  }

  @Override
  public List<Partition> getPartitions(String catName, String dbName, String tblName, int max)
      throws MetaException, NoSuchObjectException {
    dbName = normalizeIdentifier(dbName);
    tblName = normalizeIdentifier(tblName);
    if (!sharedCacheWrapper.isInitialized() || !shouldCacheTable(catName, dbName, tblName)) {
      return rawStore.getPartitions(catName, dbName, tblName, max);
    }
    SharedCache sharedCache = sharedCacheWrapper.get();
    List<Partition> parts = sharedCache.listCachedPartitions(catName, dbName, tblName, max);
    return parts;
  }

  @Override
  public void alterTable(String catName, String dbName, String tblName, Table newTable)
      throws InvalidObjectException, MetaException {
    rawStore.alterTable(catName, dbName, tblName, newTable);
    dbName = normalizeIdentifier(dbName);
    tblName = normalizeIdentifier(tblName);
    String newTblName = normalizeIdentifier(newTable.getTableName());
    if (!shouldCacheTable(catName, dbName, tblName) &&
        !shouldCacheTable(catName, dbName, newTblName)) {
      return;
    }
    SharedCache sharedCache = sharedCacheWrapper.get();
    if (sharedCache == null) {
      return;
    }

    if (shouldCacheTable(catName, dbName, newTblName)) {
      validateTableType(newTable);
      // Update table cache
      try {
        // Wait if background cache update is happening
        tableCacheLock.readLock().lock();
        isTableCacheDirty.set(true);
        sharedCache.alterTableInCache(catName, normalizeIdentifier(dbName),
            normalizeIdentifier(tblName), newTable);
      } finally {
        tableCacheLock.readLock().unlock();
      }
      // Update partition cache (key might have changed since table name is a
      // component of key)
      try {
        // Wait if background cache update is happening
        partitionCacheLock.readLock().lock();
        isPartitionCacheDirty.set(true);
        sharedCache.alterTableInPartitionCache(normalizeIdentifier(catName), normalizeIdentifier(dbName),
            normalizeIdentifier(tblName), newTable);
      } finally {
        partitionCacheLock.readLock().unlock();
      }
    } else {
      // Remove the table and its cached partitions, stats etc,
      // since it does not pass the whitelist/blacklist filter.
      // Remove table
      try {
        // Wait if background cache update is happening
        tableCacheLock.readLock().lock();
        isTableCacheDirty.set(true);
        sharedCache.removeTableFromCache(catName, dbName, tblName);
      } finally {
        tableCacheLock.readLock().unlock();
      }
      // Remove partitions
      try {
        // Wait if background cache update is happening
        partitionCacheLock.readLock().lock();
        isPartitionCacheDirty.set(true);
        sharedCache.removePartitionsFromCache(catName, dbName, tblName);
      } finally {
        partitionCacheLock.readLock().unlock();
      }
      // Remove partition col stats
      try {
        // Wait if background cache update is happening
        partitionColStatsCacheLock.readLock().lock();
        isPartitionColStatsCacheDirty.set(true);
        sharedCache.removePartitionColStatsFromCache(dbName, tblName);
      } finally {
        partitionColStatsCacheLock.readLock().unlock();
      }
      // Update aggregate partition col stats keys wherever applicable
      try {
        // Wait if background cache update is happening
        partitionAggrColStatsCacheLock.readLock().lock();
        isPartitionAggrColStatsCacheDirty.set(true);
        sharedCache.alterTableInAggrPartitionColStatsCache(catName, dbName, tblName, newTable);
      } finally {
        partitionAggrColStatsCacheLock.readLock().unlock();
      }
    }
  }

  @Override
  public void updateCreationMetadata(String catName, String dbname, String tablename, CreationMetadata cm)
      throws MetaException {
    rawStore.updateCreationMetadata(catName, dbname, tablename, cm);
  }

  public List<String> getTables(String catName, String dbName, String pattern)
      throws MetaException {
    if (!isBlacklistWhitelistEmpty(conf) || !sharedCacheWrapper.isInitialized()) {
      return rawStore.getTables(catName, dbName, pattern);
    }
    SharedCache sharedCache = sharedCacheWrapper.get();
    List<String> tableNames = new ArrayList<>();
    for (Table table : sharedCache.listCachedTables(normalizeIdentifier(catName),
        StringUtils .normalizeIdentifier (dbName))) {
      if (CacheUtils.matches(table.getTableName(), pattern)) {
        tableNames.add(table.getTableName());
      }
    }
    return tableNames;
  }

  @Override
  public List<String> getTables(String catName, String dbName, String pattern, TableType tableType)
      throws MetaException {
    if (!isBlacklistWhitelistEmpty(conf) || !sharedCacheWrapper.isInitialized()) {
      return rawStore.getTables(catName, dbName, pattern, tableType);
    }
    SharedCache sharedCache = sharedCacheWrapper.get();
    List<String> tableNames = new ArrayList<>();
    for (Table table : sharedCache.listCachedTables(normalizeIdentifier(catName),
        normalizeIdentifier(dbName))) {
      if (CacheUtils.matches(table.getTableName(), pattern) &&
          table.getTableType().equals(tableType.toString())) {
        tableNames.add(table.getTableName());
      }
    }
    return tableNames;
  }

  @Override
  public List<String> getMaterializedViewsForRewriting(String catName, String dbName)
      throws MetaException, NoSuchObjectException {
    return rawStore.getMaterializedViewsForRewriting(catName, dbName);
  }

  @Override
  public List<TableMeta> getTableMeta(String catName, String dbNames, String tableNames,
                                      List<String> tableTypes) throws MetaException {
    // TODO Check if all required tables are allowed, if so, get it from cache
    if (!isBlacklistWhitelistEmpty(conf) || !sharedCacheWrapper.isInitialized()) {
      return rawStore.getTableMeta(catName, dbNames, tableNames, tableTypes);
    }
    SharedCache sharedCache = sharedCacheWrapper.get();
    return sharedCache.getTableMeta(normalizeIdentifier(catName),
        normalizeIdentifier(dbNames),
        normalizeIdentifier(tableNames), tableTypes);
  }

  @Override
  public List<Table> getTableObjectsByName(String catName, String dbName, List<String> tblNames)
      throws MetaException, UnknownDBException {
    dbName = normalizeIdentifier(dbName);
    catName = normalizeIdentifier(catName);
    boolean missSomeInCache = false;
    for (String tblName : tblNames) {
      tblName = normalizeIdentifier(tblName);
      if (!shouldCacheTable(catName, dbName, tblName)) {
        missSomeInCache = true;
        break;
      }
    }
    if (!sharedCacheWrapper.isInitialized() || missSomeInCache) {
      return rawStore.getTableObjectsByName(catName, dbName, tblNames);
    }
    SharedCache sharedCache = sharedCacheWrapper.get();
    List<Table> tables = new ArrayList<>();
    for (String tblName : tblNames) {
      tblName = normalizeIdentifier(tblName);
      Table tbl = sharedCache.getTableFromCache(catName, dbName, tblName);
      if (tbl == null) {
        tbl = rawStore.getTable(catName, dbName, tblName);
      }
      tables.add(tbl);
    }
    return tables;
  }

  @Override
  public List<String> getAllTables(String catName, String dbName) throws MetaException {
    if (!isBlacklistWhitelistEmpty(conf) || !sharedCacheWrapper.isInitialized()) {
      return rawStore.getAllTables(catName, dbName);
    }
    SharedCache sharedCache = sharedCacheWrapper.get();
    return getAllTablesInternal(catName, dbName, sharedCache);
  }

  private static List<String> getAllTablesInternal(String catName, String dbName,
                                                   SharedCache sharedCache) {
    List<String> tblNames = new ArrayList<>();
    for (Table tbl : sharedCache.listCachedTables(normalizeIdentifier(catName),
        normalizeIdentifier(dbName))) {
      tblNames.add(normalizeIdentifier(tbl.getTableName()));
    }
    return tblNames;
  }

  @Override
  public List<String> listTableNamesByFilter(String catName, String dbName, String filter,
                                             short max_tables)
      throws MetaException, UnknownDBException {
    if (!isBlacklistWhitelistEmpty(conf) || !sharedCacheWrapper.isInitialized()) {
      return rawStore.listTableNamesByFilter(catName, dbName, filter, max_tables);
    }
    SharedCache sharedCache = sharedCacheWrapper.get();
    List<String> tableNames = new ArrayList<>();
    int count = 0;
    for (Table table : sharedCache.listCachedTables(catName, normalizeIdentifier(dbName))) {
      if (CacheUtils.matches(table.getTableName(), filter)
          && (max_tables == -1 || count < max_tables)) {
        tableNames.add(table.getTableName());
        count++;
      }
    }
    return tableNames;
  }

  @Override
  public List<String> listPartitionNames(String catName, String dbName, String tblName,
      short max_parts) throws MetaException {
    dbName = normalizeIdentifier(dbName);
    tblName = normalizeIdentifier(tblName);
    if (!sharedCacheWrapper.isInitialized() || !shouldCacheTable(catName, dbName, tblName)) {
      return rawStore.listPartitionNames(catName, dbName, tblName, max_parts);
    }
    SharedCache sharedCache = sharedCacheWrapper.get();
    List<String> partitionNames = new ArrayList<>();
    Table t = sharedCache.getTableFromCache(catName, dbName, tblName);
    int count = 0;
    for (Partition part : sharedCache.listCachedPartitions(catName, dbName, tblName, max_parts)) {
      if (max_parts == -1 || count < max_parts) {
        partitionNames.add(Warehouse.makePartName(t.getPartitionKeys(), part.getValues()));
      }
    }
    return partitionNames;
  }

  @Override
  public PartitionValuesResponse listPartitionValues(String catName, String db_name, String tbl_name,
      List<FieldSchema> cols, boolean applyDistinct, String filter, boolean ascending,
      List<FieldSchema> order, long maxParts) throws MetaException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void alterPartition(String catName, String dbName, String tblName, List<String> partVals,
                             Partition newPart) throws InvalidObjectException, MetaException {
    rawStore.alterPartition(catName, dbName, tblName, partVals, newPart);
    catName = normalizeIdentifier(catName);
    dbName = normalizeIdentifier(dbName);
    tblName = normalizeIdentifier(tblName);
    if (!shouldCacheTable(catName, dbName, tblName)) {
      return;
    }
    SharedCache sharedCache = sharedCacheWrapper.get();
    if (sharedCache == null) {
      return;
    }
    // Update partition cache
    try {
      // Wait if background cache update is happening
      partitionCacheLock.readLock().lock();
      isPartitionCacheDirty.set(true);
      sharedCache.alterPartitionInCache(catName, dbName, tblName, partVals, newPart);
    } finally {
      partitionCacheLock.readLock().unlock();
    }
    // Update partition column stats cache
    try {
      // Wait if background cache update is happening
      partitionColStatsCacheLock.readLock().lock();
      isPartitionColStatsCacheDirty.set(true);
      sharedCache.alterPartitionInColStatsCache(catName, dbName, tblName, partVals, newPart);
    } finally {
      partitionColStatsCacheLock.readLock().unlock();
    }
    // Remove aggregate partition col stats for this table
    try {
      // Wait if background cache update is happening
      partitionAggrColStatsCacheLock.readLock().lock();
      isPartitionAggrColStatsCacheDirty.set(true);
      sharedCache.removeAggrPartitionColStatsFromCache(catName, dbName, tblName);
    } finally {
      partitionAggrColStatsCacheLock.readLock().unlock();
    }
  }

  @Override
  public void alterPartitions(String catName, String dbName, String tblName,
                              List<List<String>> partValsList, List<Partition> newParts)
      throws InvalidObjectException, MetaException {
    rawStore.alterPartitions(catName, dbName, tblName, partValsList, newParts);
    catName = normalizeIdentifier(catName);
    dbName = normalizeIdentifier(dbName);
    tblName = normalizeIdentifier(tblName);
    if (!shouldCacheTable(catName, dbName, tblName)) {
      return;
    }
    SharedCache sharedCache = sharedCacheWrapper.get();
    if (sharedCache == null) {
      return;
    }
    // Update partition cache
    try {
      // Wait if background cache update is happening
      partitionCacheLock.readLock().lock();
      isPartitionCacheDirty.set(true);
      for (int i = 0; i < partValsList.size(); i++) {
        List<String> partVals = partValsList.get(i);
        Partition newPart = newParts.get(i);
        sharedCache.alterPartitionInCache(catName, dbName, tblName, partVals, newPart);
      }
    } finally {
      partitionCacheLock.readLock().unlock();
    }
    // Update partition column stats cache
    try {
      // Wait if background cache update is happening
      partitionColStatsCacheLock.readLock().lock();
      isPartitionColStatsCacheDirty.set(true);
      for (int i = 0; i < partValsList.size(); i++) {
        List<String> partVals = partValsList.get(i);
        Partition newPart = newParts.get(i);
        sharedCache.alterPartitionInColStatsCache(catName, dbName, tblName, partVals, newPart);
      }
    } finally {
      partitionColStatsCacheLock.readLock().unlock();
    }
    // Remove aggregate partition col stats for this table
    try {
      // Wait if background cache update is happening
      partitionAggrColStatsCacheLock.readLock().lock();
      isPartitionAggrColStatsCacheDirty.set(true);
      sharedCache.removeAggrPartitionColStatsFromCache(catName, dbName, tblName);
    } finally {
      partitionAggrColStatsCacheLock.readLock().unlock();
    }
  }

  private boolean getPartitionNamesPrunedByExprNoTxn(Table table, byte[] expr,
      String defaultPartName, short maxParts, List<String> result, SharedCache sharedCache)
          throws MetaException, NoSuchObjectException {
    String catName = table.isSetCatName() ? normalizeIdentifier(table.getCatName()) :
        DEFAULT_CATALOG_NAME;
    List<Partition> parts = sharedCache.listCachedPartitions(catName,
        normalizeIdentifier(table.getDbName()),
        normalizeIdentifier(table.getTableName()), maxParts);
    for (Partition part : parts) {
      result.add(Warehouse.makePartName(table.getPartitionKeys(), part.getValues()));
    }
    if (defaultPartName == null || defaultPartName.isEmpty()) {
      defaultPartName = MetastoreConf.getVar(getConf(), ConfVars.DEFAULTPARTITIONNAME);
    }
    return expressionProxy.filterPartitionsByExpr(
        table.getPartitionKeys(), expr, defaultPartName, result);
  }

  @Override
  public List<Partition> getPartitionsByFilter(String catName, String dbName, String tblName,
      String filter, short maxParts)
      throws MetaException, NoSuchObjectException {
    return rawStore.getPartitionsByFilter(catName, dbName, tblName, filter, maxParts);
  }

  @Override
  public boolean getPartitionsByExpr(String catName, String dbName, String tblName, byte[] expr,
      String defaultPartitionName, short maxParts, List<Partition> result) throws TException {
    catName = normalizeIdentifier(catName);
    dbName = normalizeIdentifier(dbName);
    tblName = normalizeIdentifier(tblName);
    if (!sharedCacheWrapper.isInitialized() || !shouldCacheTable(catName, dbName, tblName)) {
      return rawStore.getPartitionsByExpr(catName, dbName, tblName, expr, defaultPartitionName,
          maxParts, result);
    }
    SharedCache sharedCache = sharedCacheWrapper.get();
    List<String> partNames = new LinkedList<>();
    Table table = sharedCache.getTableFromCache(catName, dbName, tblName);
    boolean hasUnknownPartitions = getPartitionNamesPrunedByExprNoTxn(table, expr,
        defaultPartitionName, maxParts, partNames, sharedCache);
    return hasUnknownPartitions;
  }

  @Override
  public int getNumPartitionsByFilter(String catName, String dbName, String tblName, String filter)
      throws MetaException, NoSuchObjectException {
    return rawStore.getNumPartitionsByFilter(catName, dbName, tblName, filter);
  }

  @Override
  public int getNumPartitionsByExpr(String catName, String dbName, String tblName, byte[] expr)
      throws MetaException, NoSuchObjectException {
    catName = normalizeIdentifier(catName);
    dbName = normalizeIdentifier(dbName);
    tblName = normalizeIdentifier(tblName);
    if (!sharedCacheWrapper.isInitialized() || !shouldCacheTable(catName, dbName, tblName)) {
      return rawStore.getNumPartitionsByExpr(catName, dbName, tblName, expr);
    }
    SharedCache sharedCache = sharedCacheWrapper.get();
    String defaultPartName = MetastoreConf.getVar(getConf(), ConfVars.DEFAULTPARTITIONNAME);
    List<String> partNames = new LinkedList<>();
    Table table = sharedCache.getTableFromCache(catName, dbName, tblName);
    getPartitionNamesPrunedByExprNoTxn(table, expr, defaultPartName, Short.MAX_VALUE, partNames,
        sharedCache);
    return partNames.size();
  }

  private static List<String> partNameToVals(String name) {
    if (name == null) {
      return null;
    }
    List<String> vals = new ArrayList<>();
    String[] kvp = name.split("/");
    for (String kv : kvp) {
      vals.add(FileUtils.unescapePathName(kv.substring(kv.indexOf('=') + 1)));
    }
    return vals;
  }

  @Override
  public List<Partition> getPartitionsByNames(String catName, String dbName, String tblName,
      List<String> partNames) throws MetaException, NoSuchObjectException {
    dbName = normalizeIdentifier(dbName);
    tblName = normalizeIdentifier(tblName);
    if (!sharedCacheWrapper.isInitialized() || !shouldCacheTable(catName, dbName, tblName)) {
      return rawStore.getPartitionsByNames(catName, dbName, tblName, partNames);
    }
    SharedCache sharedCache = sharedCacheWrapper.get();
    List<Partition> partitions = new ArrayList<>();
    for (String partName : partNames) {
      Partition part = sharedCache.getPartitionFromCache(catName, dbName, tblName, partNameToVals(partName));
      if (part!=null) {
        partitions.add(part);
      }
    }
    return partitions;
  }

  @Override
  public Table markPartitionForEvent(String catName, String dbName, String tblName,
      Map<String, String> partVals, PartitionEventType evtType)
      throws MetaException, UnknownTableException, InvalidPartitionException,
      UnknownPartitionException {
    return rawStore.markPartitionForEvent(catName, dbName, tblName, partVals, evtType);
  }

  @Override
  public boolean isPartitionMarkedForEvent(String catName, String dbName, String tblName,
      Map<String, String> partName, PartitionEventType evtType)
      throws MetaException, UnknownTableException, InvalidPartitionException,
      UnknownPartitionException {
    return rawStore.isPartitionMarkedForEvent(catName, dbName, tblName, partName, evtType);
  }

  @Override
  public boolean addRole(String rowName, String ownerName)
      throws InvalidObjectException, MetaException, NoSuchObjectException {
    return rawStore.addRole(rowName, ownerName);
  }

  @Override
  public boolean removeRole(String roleName)
      throws MetaException, NoSuchObjectException {
    return rawStore.removeRole(roleName);
  }

  @Override
  public boolean grantRole(Role role, String userName,
      PrincipalType principalType, String grantor, PrincipalType grantorType,
      boolean grantOption)
      throws MetaException, NoSuchObjectException, InvalidObjectException {
    return rawStore.grantRole(role, userName, principalType, grantor, grantorType, grantOption);
  }

  @Override
  public boolean revokeRole(Role role, String userName,
      PrincipalType principalType, boolean grantOption)
      throws MetaException, NoSuchObjectException {
    return rawStore.revokeRole(role, userName, principalType, grantOption);
  }

  @Override
  public PrincipalPrivilegeSet getUserPrivilegeSet(String userName,
      List<String> groupNames) throws InvalidObjectException, MetaException {
    return rawStore.getUserPrivilegeSet(userName, groupNames);
  }

  @Override
  public PrincipalPrivilegeSet getDBPrivilegeSet(String catName, String dbName, String userName,
      List<String> groupNames) throws InvalidObjectException, MetaException {
    return rawStore.getDBPrivilegeSet(catName, dbName, userName, groupNames);
  }

  @Override
  public PrincipalPrivilegeSet getTablePrivilegeSet(String catName, String dbName,
      String tableName, String userName, List<String> groupNames)
      throws InvalidObjectException, MetaException {
    return rawStore.getTablePrivilegeSet(catName, dbName, tableName, userName, groupNames);
  }

  @Override
  public PrincipalPrivilegeSet getPartitionPrivilegeSet(String catName, String dbName,
      String tableName, String partition, String userName,
      List<String> groupNames) throws InvalidObjectException, MetaException {
    return rawStore.getPartitionPrivilegeSet(catName, dbName, tableName, partition, userName, groupNames);
  }

  @Override
  public PrincipalPrivilegeSet getColumnPrivilegeSet(String catName, String dbName,
      String tableName, String partitionName, String columnName,
      String userName, List<String> groupNames)
      throws InvalidObjectException, MetaException {
    return rawStore.getColumnPrivilegeSet(catName, dbName, tableName, partitionName, columnName, userName, groupNames);
  }

  @Override
  public List<HiveObjectPrivilege> listPrincipalGlobalGrants(
      String principalName, PrincipalType principalType) {
    return rawStore.listPrincipalGlobalGrants(principalName, principalType);
  }

  @Override
  public List<HiveObjectPrivilege> listPrincipalDBGrants(String principalName,
      PrincipalType principalType, String catName, String dbName) {
    return rawStore.listPrincipalDBGrants(principalName, principalType, catName, dbName);
  }

  @Override
  public List<HiveObjectPrivilege> listAllTableGrants(String principalName,
      PrincipalType principalType, String catName, String dbName, String tableName) {
    return rawStore.listAllTableGrants(principalName, principalType, catName, dbName, tableName);
  }

  @Override
  public List<HiveObjectPrivilege> listPrincipalPartitionGrants(
      String principalName, PrincipalType principalType, String catName, String dbName,
      String tableName, List<String> partValues, String partName) {
    return rawStore.listPrincipalPartitionGrants(principalName, principalType, catName, dbName, tableName, partValues, partName);
  }

  @Override
  public List<HiveObjectPrivilege> listPrincipalTableColumnGrants(
      String principalName, PrincipalType principalType, String catName, String dbName,
      String tableName, String columnName) {
    return rawStore.listPrincipalTableColumnGrants(principalName, principalType, catName, dbName, tableName, columnName);
  }

  @Override
  public List<HiveObjectPrivilege> listPrincipalPartitionColumnGrants(
      String principalName, PrincipalType principalType, String catName, String dbName,
      String tableName, List<String> partValues, String partName,
      String columnName) {
    return rawStore.listPrincipalPartitionColumnGrants(principalName, principalType, catName, dbName, tableName, partValues, partName, columnName);
  }

  @Override
  public boolean grantPrivileges(PrivilegeBag privileges)
      throws InvalidObjectException, MetaException, NoSuchObjectException {
    return rawStore.grantPrivileges(privileges);
  }

  @Override
  public boolean revokePrivileges(PrivilegeBag privileges, boolean grantOption)
      throws InvalidObjectException, MetaException, NoSuchObjectException {
    return rawStore.revokePrivileges(privileges, grantOption);
  }

  @Override
  public Role getRole(String roleName) throws NoSuchObjectException {
    return rawStore.getRole(roleName);
  }

  @Override
  public List<String> listRoleNames() {
    return rawStore.listRoleNames();
  }

  @Override
  public List<Role> listRoles(String principalName,
      PrincipalType principalType) {
    return rawStore.listRoles(principalName, principalType);
  }

  @Override
  public List<RolePrincipalGrant> listRolesWithGrants(String principalName,
      PrincipalType principalType) {
    return rawStore.listRolesWithGrants(principalName, principalType);
  }

  @Override
  public List<RolePrincipalGrant> listRoleMembers(String roleName) {
    return rawStore.listRoleMembers(roleName);
  }

  @Override
  public Partition getPartitionWithAuth(String catName, String dbName, String tblName,
      List<String> partVals, String userName, List<String> groupNames)
      throws MetaException, NoSuchObjectException, InvalidObjectException {
    dbName = normalizeIdentifier(dbName);
    tblName = normalizeIdentifier(tblName);
    if (!sharedCacheWrapper.isInitialized() || !shouldCacheTable(catName, dbName, tblName)) {
      return rawStore.getPartitionWithAuth(catName, dbName, tblName, partVals, userName, groupNames);
    }
    SharedCache sharedCache = sharedCacheWrapper.get();
    Partition p = sharedCache.getPartitionFromCache(catName, dbName, tblName, partVals);
    if (p!=null) {
      Table t = sharedCache.getTableFromCache(catName, dbName, tblName);
      String partName = Warehouse.makePartName(t.getPartitionKeys(), partVals);
      PrincipalPrivilegeSet privs = getPartitionPrivilegeSet(catName, dbName, tblName, partName,
          userName, groupNames);
      p.setPrivileges(privs);
    }
    return p;
  }

  @Override
  public List<Partition> getPartitionsWithAuth(String catName, String dbName, String tblName,
      short maxParts, String userName, List<String> groupNames)
      throws MetaException, NoSuchObjectException, InvalidObjectException {
    dbName = normalizeIdentifier(dbName);
    tblName = normalizeIdentifier(tblName);
    if (!sharedCacheWrapper.isInitialized() || !shouldCacheTable(catName, dbName, tblName)) {
      return rawStore.getPartitionsWithAuth(catName, dbName, tblName, maxParts, userName, groupNames);
    }
    SharedCache sharedCache = sharedCacheWrapper.get();
    Table t = sharedCache.getTableFromCache(catName, dbName, tblName);
    List<Partition> partitions = new ArrayList<>();
    int count = 0;
    for (Partition part : sharedCache.listCachedPartitions(catName, dbName, tblName, maxParts)) {
      if (maxParts == -1 || count < maxParts) {
        String partName = Warehouse.makePartName(t.getPartitionKeys(), part.getValues());
        PrincipalPrivilegeSet privs = getPartitionPrivilegeSet(catName, dbName, tblName, partName,
            userName, groupNames);
        part.setPrivileges(privs);
        partitions.add(part);
        count++;
      }
    }
    return partitions;
  }

  @Override
  public List<String> listPartitionNamesPs(String catName, String dbName, String tblName,
      List<String> partVals, short maxParts)
      throws MetaException, NoSuchObjectException {
    catName = normalizeIdentifier(catName);
    dbName = normalizeIdentifier(dbName);
    tblName = normalizeIdentifier(tblName);
    if (!sharedCacheWrapper.isInitialized() || !shouldCacheTable(catName, dbName, tblName)) {
      return rawStore.listPartitionNamesPs(catName, dbName, tblName, partVals, maxParts);
    }
    SharedCache sharedCache = sharedCacheWrapper.get();
    List<String> partNames = new ArrayList<>();
    int count = 0;
    Table t = sharedCache.getTableFromCache(catName, dbName, tblName);
    for (Partition part : sharedCache.listCachedPartitions(catName, dbName, tblName, maxParts)) {
      boolean psMatch = true;
      for (int i=0;i<partVals.size();i++) {
        String psVal = partVals.get(i);
        String partVal = part.getValues().get(i);
        if (psVal!=null && !psVal.isEmpty() && !psVal.equals(partVal)) {
          psMatch = false;
          break;
        }
      }
      if (!psMatch) {
        continue;
      }
      if (maxParts == -1 || count < maxParts) {
        partNames.add(Warehouse.makePartName(t.getPartitionKeys(), part.getValues()));
        count++;
      }
    }
    return partNames;
  }

  @Override
  public List<Partition> listPartitionsPsWithAuth(String catName, String dbName, String tblName,
      List<String> partVals, short maxParts, String userName, List<String> groupNames)
      throws MetaException, InvalidObjectException, NoSuchObjectException {
    catName = normalizeIdentifier(catName);
    dbName = normalizeIdentifier(dbName);
    tblName = normalizeIdentifier(tblName);
    if (!sharedCacheWrapper.isInitialized() || !shouldCacheTable(catName, dbName, tblName)) {
      return rawStore.listPartitionsPsWithAuth(catName, dbName, tblName, partVals, maxParts, userName,
          groupNames);
    }
    SharedCache sharedCache = sharedCacheWrapper.get();
    List<Partition> partitions = new ArrayList<>();
    Table t = sharedCache.getTableFromCache(catName, dbName, tblName);
    int count = 0;
    for (Partition part : sharedCache.listCachedPartitions(catName, dbName, tblName, maxParts)) {
      boolean psMatch = true;
      for (int i = 0; i < partVals.size(); i++) {
        String psVal = partVals.get(i);
        String partVal = part.getValues().get(i);
        if (psVal != null && !psVal.isEmpty() && !psVal.equals(partVal)) {
          psMatch = false;
          break;
        }
      }
      if (!psMatch) {
        continue;
      }
      if (maxParts == -1 || count < maxParts) {
        String partName = Warehouse.makePartName(t.getPartitionKeys(), part.getValues());
        PrincipalPrivilegeSet privs =
            getPartitionPrivilegeSet(catName, dbName, tblName, partName, userName, groupNames);
        part.setPrivileges(privs);
        partitions.add(part);
      }
    }
    return partitions;
  }

  @Override
  public boolean updateTableColumnStatistics(ColumnStatistics colStats)
      throws NoSuchObjectException, MetaException, InvalidObjectException, InvalidInputException {
    boolean succ = rawStore.updateTableColumnStatistics(colStats);
    if (succ) {
      String catName = colStats.getStatsDesc().isSetCatName() ?
          normalizeIdentifier(colStats.getStatsDesc().getCatName()) :
          DEFAULT_CATALOG_NAME;
      String dbName = normalizeIdentifier(colStats.getStatsDesc().getDbName());
      String tblName = normalizeIdentifier(colStats.getStatsDesc().getTableName());
      if (!shouldCacheTable(catName, dbName, tblName)) {
        return succ;
      }
      SharedCache sharedCache = sharedCacheWrapper.get();
      if (sharedCache == null) {
        return succ;
      }
      List<ColumnStatisticsObj> statsObjs = colStats.getStatsObj();
      Table tbl = getTable(catName, dbName, tblName);
      List<String> colNames = new ArrayList<>();
      for (ColumnStatisticsObj statsObj : statsObjs) {
        colNames.add(statsObj.getColName());
      }
      StatsSetupConst.setColumnStatsState(tbl.getParameters(), colNames);
      // Update table
      try {
        // Wait if background cache update is happening
        tableCacheLock.readLock().lock();
        isTableCacheDirty.set(true);
        sharedCache.alterTableInCache(catName, dbName, tblName, tbl);
      } finally {
        tableCacheLock.readLock().unlock();
      }
      // Update table col stats
      try {
        // Wait if background cache update is happening
        tableColStatsCacheLock.readLock().lock();
        isTableColStatsCacheDirty.set(true);
        sharedCache.updateTableColStatsInCache(catName, dbName, tblName, statsObjs);
      } finally {
        tableColStatsCacheLock.readLock().unlock();
      }
    }
    return succ;
  }

  @Override
  public ColumnStatistics getTableColumnStatistics(String catName, String dbName, String tblName,
      List<String> colNames) throws MetaException, NoSuchObjectException {
    dbName = normalizeIdentifier(dbName);
    tblName = normalizeIdentifier(tblName);
    if (!sharedCacheWrapper.isInitialized() || !shouldCacheTable(catName, dbName, tblName)) {
      return rawStore.getTableColumnStatistics(catName, dbName, tblName, colNames);
    }
    SharedCache sharedCache = sharedCacheWrapper.get();
    ColumnStatisticsDesc csd = new ColumnStatisticsDesc(true, dbName, tblName);
    List<ColumnStatisticsObj> colStatObjs = new ArrayList<>();
    for (String colName : colNames) {
      String colStatsCacheKey = CacheUtils.buildTableColKey(catName, dbName, tblName, colName);
      ColumnStatisticsObj colStat = sharedCache.getCachedTableColStats(colStatsCacheKey);
      if (colStat != null) {
        colStatObjs.add(colStat);
      }
    }
    if (colStatObjs.isEmpty()) {
      return null;
    } else {
      return new ColumnStatistics(csd, colStatObjs);
    }
  }

  @Override
  public boolean deleteTableColumnStatistics(String catName, String dbName, String tblName,
                                             String colName)
      throws NoSuchObjectException, MetaException, InvalidObjectException, InvalidInputException {
    boolean succ = rawStore.deleteTableColumnStatistics(catName, dbName, tblName, colName);
    if (succ) {
      catName = normalizeIdentifier(catName);
      dbName = normalizeIdentifier(dbName);
      tblName = normalizeIdentifier(tblName);
      if (!shouldCacheTable(catName, dbName, tblName)) {
        return succ;
      }
      SharedCache sharedCache = sharedCacheWrapper.get();
      if (sharedCache == null) {
        return succ;
      }
      try {
        // Wait if background cache update is happening
        tableColStatsCacheLock.readLock().lock();
        isTableColStatsCacheDirty.set(true);
        sharedCache.removeTableColStatsFromCache(catName, dbName, tblName, colName);
      } finally {
        tableColStatsCacheLock.readLock().unlock();
      }
    }
    return succ;
  }

  @Override
  public boolean updatePartitionColumnStatistics(ColumnStatistics colStats, List<String> partVals)
      throws NoSuchObjectException, MetaException, InvalidObjectException, InvalidInputException {
    boolean succ = rawStore.updatePartitionColumnStatistics(colStats, partVals);
    if (succ) {
      String catName = colStats.getStatsDesc().isSetCatName() ?
          normalizeIdentifier(colStats.getStatsDesc().getCatName()) : DEFAULT_CATALOG_NAME;
      String dbName = normalizeIdentifier(colStats.getStatsDesc().getDbName());
      String tblName = normalizeIdentifier(colStats.getStatsDesc().getTableName());
      if (!shouldCacheTable(catName, dbName, tblName)) {
        return succ;
      }
      SharedCache sharedCache = sharedCacheWrapper.get();
      if (sharedCache == null) {
        return succ;
      }
      List<ColumnStatisticsObj> statsObjs = colStats.getStatsObj();
      Partition part = getPartition(catName, dbName, tblName, partVals);
      List<String> colNames = new ArrayList<>();
      for (ColumnStatisticsObj statsObj : statsObjs) {
        colNames.add(statsObj.getColName());
      }
      StatsSetupConst.setColumnStatsState(part.getParameters(), colNames);
      // Update partition
      try {
        // Wait if background cache update is happening
        partitionCacheLock.readLock().lock();
        isPartitionCacheDirty.set(true);
        sharedCache.alterPartitionInCache(catName, dbName, tblName, partVals, part);
      } finally {
        partitionCacheLock.readLock().unlock();
      }
      // Update partition column stats
      try {
        // Wait if background cache update is happening
        partitionColStatsCacheLock.readLock().lock();
        isPartitionColStatsCacheDirty.set(true);
        sharedCache.updatePartitionColStatsInCache(catName, dbName, tblName, partVals,
            colStats.getStatsObj());
      } finally {
        partitionColStatsCacheLock.readLock().unlock();
      }
      // Remove aggregate partition col stats for this table
      try {
        // Wait if background cache update is happening
        partitionAggrColStatsCacheLock.readLock().lock();
        isPartitionAggrColStatsCacheDirty.set(true);
        sharedCache.removeAggrPartitionColStatsFromCache(catName, dbName, tblName);
      } finally {
        partitionAggrColStatsCacheLock.readLock().unlock();
      }
    }
    return succ;
  }

  @Override
  // TODO: calculate from cached values.
  public List<ColumnStatistics> getPartitionColumnStatistics(String catName, String dbName, String tblName,
      List<String> partNames, List<String> colNames) throws MetaException, NoSuchObjectException {
    return rawStore.getPartitionColumnStatistics(catName, dbName, tblName, partNames, colNames);
  }

  @Override
  public boolean deletePartitionColumnStatistics(String catName, String dbName, String tblName, String partName,
      List<String> partVals, String colName)
      throws NoSuchObjectException, MetaException, InvalidObjectException, InvalidInputException {
    boolean succ =
        rawStore.deletePartitionColumnStatistics(catName, dbName, tblName, partName, partVals, colName);
    if (succ) {
      catName = normalizeIdentifier(catName);
      dbName = normalizeIdentifier(dbName);
      tblName = normalizeIdentifier(tblName);
      if (!shouldCacheTable(catName, dbName, tblName)) {
        return succ;
      }
      SharedCache sharedCache = sharedCacheWrapper.get();
      if (sharedCache == null) {
        return succ;
      }
      try {
        // Wait if background cache update is happening
        partitionColStatsCacheLock.readLock().lock();
        isPartitionColStatsCacheDirty.set(true);
        sharedCache.removePartitionColStatsFromCache(catName, dbName, tblName, partVals, colName);
      } finally {
        partitionColStatsCacheLock.readLock().unlock();
      }
      // Remove aggregate partition col stats for this table
      try {
        // Wait if background cache update is happening
        partitionAggrColStatsCacheLock.readLock().lock();
        isPartitionAggrColStatsCacheDirty.set(true);
        sharedCache.removeAggrPartitionColStatsFromCache(catName, dbName, tblName);
      } finally {
        partitionAggrColStatsCacheLock.readLock().unlock();
      }
    }
    return succ;
  }

  @Override
  public AggrStats get_aggr_stats_for(String catName, String dbName, String tblName, List<String> partNames,
      List<String> colNames) throws MetaException, NoSuchObjectException {
    List<ColumnStatisticsObj> colStats;
    catName = normalizeIdentifier(catName);
    dbName = normalizeIdentifier(dbName);
    tblName = normalizeIdentifier(tblName);
    if (!sharedCacheWrapper.isInitialized() || !shouldCacheTable(catName, dbName, tblName)) {
      rawStore.get_aggr_stats_for(catName, dbName, tblName, partNames, colNames);
    }
    SharedCache sharedCache = sharedCacheWrapper.get();
    List<String> allPartNames = rawStore.listPartitionNames(catName, dbName, tblName, (short) -1);
    if (partNames.size() == allPartNames.size()) {
      colStats = sharedCache.getAggrStatsFromCache(catName, dbName, tblName, colNames, StatsType.ALL);
      if (colStats != null) {
        return new AggrStats(colStats, partNames.size());
      }
    } else if (partNames.size() == (allPartNames.size() - 1)) {
      String defaultPartitionName = MetastoreConf.getVar(getConf(), ConfVars.DEFAULTPARTITIONNAME);
      if (!partNames.contains(defaultPartitionName)) {
        colStats =
            sharedCache.getAggrStatsFromCache(catName, dbName, tblName, colNames, StatsType.ALLBUTDEFAULT);
        if (colStats != null) {
          return new AggrStats(colStats, partNames.size());
        }
      }
    }
    LOG.debug("Didn't find aggr stats in cache. Merging them. tblName= {}, parts= {}, cols= {}",
        tblName, partNames, colNames);
    MergedColumnStatsForPartitions mergedColStats =
        mergeColStatsForPartitions(catName, dbName, tblName, partNames, colNames, sharedCache);
    return new AggrStats(mergedColStats.getColStats(), mergedColStats.getPartsFound());
  }

  private MergedColumnStatsForPartitions mergeColStatsForPartitions(
      String catName, String dbName, String tblName, List<String> partNames, List<String> colNames,
      SharedCache sharedCache) throws MetaException {
    final boolean useDensityFunctionForNDVEstimation =
        MetastoreConf.getBoolVar(getConf(), ConfVars.STATS_NDV_DENSITY_FUNCTION);
    final double ndvTuner = MetastoreConf.getDoubleVar(getConf(), ConfVars.STATS_NDV_TUNER);
    Map<ColumnStatsAggregator, List<ColStatsObjWithSourceInfo>> colStatsMap = new HashMap<>();
    boolean areAllPartsFound = true;
    long partsFound = 0;
    for (String colName : colNames) {
      long partsFoundForColumn = 0;
      ColumnStatsAggregator colStatsAggregator = null;
      List<ColStatsObjWithSourceInfo> colStatsWithPartInfoList = new ArrayList<>();
      for (String partName : partNames) {
        String colStatsCacheKey =
            CacheUtils.buildPartColKey(catName, dbName, tblName, partNameToVals(partName), colName);
        ColumnStatisticsObj colStatsForPart =
            sharedCache.getCachedPartitionColStats(colStatsCacheKey);
        if (colStatsForPart != null) {
          ColStatsObjWithSourceInfo colStatsWithPartInfo =
              new ColStatsObjWithSourceInfo(colStatsForPart, catName, dbName, tblName, partName);
          colStatsWithPartInfoList.add(colStatsWithPartInfo);
          if (colStatsAggregator == null) {
            colStatsAggregator = ColumnStatsAggregatorFactory.getColumnStatsAggregator(
                colStatsForPart.getStatsData().getSetField(), useDensityFunctionForNDVEstimation,
                ndvTuner);
          }
          partsFoundForColumn++;
        } else {
          LOG.debug(
              "Stats not found in CachedStore for: dbName={} tblName={} partName={} colName={}",
              dbName, tblName, partName, colName);
        }
      }
      if (colStatsWithPartInfoList.size() > 0) {
        colStatsMap.put(colStatsAggregator, colStatsWithPartInfoList);
      }
      if (partsFoundForColumn == partNames.size()) {
        partsFound++;
      }
      if (colStatsMap.size() < 1) {
        LOG.debug("No stats data found for: dbName={} tblName= {} partNames= {} colNames= ", dbName,
            tblName, partNames, colNames);
        return new MergedColumnStatsForPartitions(new ArrayList<ColumnStatisticsObj>(), 0);
      }
    }
    // Note that enableBitVector does not apply here because ColumnStatisticsObj
    // itself will tell whether bitvector is null or not and aggr logic can automatically apply.
    return new MergedColumnStatsForPartitions(MetaStoreUtils.aggrPartitionStats(colStatsMap,
        partNames, areAllPartsFound, useDensityFunctionForNDVEstimation, ndvTuner), partsFound);
  }

  class MergedColumnStatsForPartitions {
    List<ColumnStatisticsObj> colStats = new ArrayList<ColumnStatisticsObj>();
    long partsFound;

    MergedColumnStatsForPartitions(List<ColumnStatisticsObj> colStats, long partsFound) {
      this.colStats = colStats;
      this.partsFound = partsFound;
    }

    List<ColumnStatisticsObj> getColStats() {
      return colStats;
    }

    long getPartsFound() {
      return partsFound;
    }
  }

  @Override
  public long cleanupEvents() {
    return rawStore.cleanupEvents();
  }

  @Override
  public boolean addToken(String tokenIdentifier, String delegationToken) {
    return rawStore.addToken(tokenIdentifier, delegationToken);
  }

  @Override
  public boolean removeToken(String tokenIdentifier) {
    return rawStore.removeToken(tokenIdentifier);
  }

  @Override
  public String getToken(String tokenIdentifier) {
    return rawStore.getToken(tokenIdentifier);
  }

  @Override
  public List<String> getAllTokenIdentifiers() {
    return rawStore.getAllTokenIdentifiers();
  }

  @Override
  public int addMasterKey(String key) throws MetaException {
    return rawStore.addMasterKey(key);
  }

  @Override
  public void updateMasterKey(Integer seqNo, String key)
      throws NoSuchObjectException, MetaException {
    rawStore.updateMasterKey(seqNo, key);
  }

  @Override
  public boolean removeMasterKey(Integer keySeq) {
    return rawStore.removeMasterKey(keySeq);
  }

  @Override
  public String[] getMasterKeys() {
    return rawStore.getMasterKeys();
  }

  @Override
  public void verifySchema() throws MetaException {
    rawStore.verifySchema();
  }

  @Override
  public String getMetaStoreSchemaVersion() throws MetaException {
    return rawStore.getMetaStoreSchemaVersion();
  }

  @Override
  public void setMetaStoreSchemaVersion(String version, String comment)
      throws MetaException {
    rawStore.setMetaStoreSchemaVersion(version, comment);
  }

  @Override
  public void dropPartitions(String catName, String dbName, String tblName, List<String> partNames)
      throws MetaException, NoSuchObjectException {
    rawStore.dropPartitions(catName, dbName, tblName, partNames);
    dbName = normalizeIdentifier(dbName);
    tblName = normalizeIdentifier(tblName);
    if (!shouldCacheTable(catName, dbName, tblName)) {
      return;
    }
    SharedCache sharedCache = sharedCacheWrapper.get();
    if (sharedCache == null) {
      return;
    }
    // Remove partitions
    try {
      // Wait if background cache update is happening
      partitionCacheLock.readLock().lock();
      isPartitionCacheDirty.set(true);
      for (String partName : partNames) {
        List<String> vals = partNameToVals(partName);
        sharedCache.removePartitionFromCache(catName, dbName, tblName, vals);
      }
    } finally {
      partitionCacheLock.readLock().unlock();
    }
    // Remove partition col stats
    try {
      // Wait if background cache update is happening
      partitionColStatsCacheLock.readLock().lock();
      isPartitionColStatsCacheDirty.set(true);
      for (String partName : partNames) {
        List<String> part_vals = partNameToVals(partName);
        sharedCache.removePartitionColStatsFromCache(catName, dbName, tblName, part_vals);
      }
    } finally {
      partitionColStatsCacheLock.readLock().unlock();
    }
    // Remove aggregate partition col stats for this table
    try {
      // Wait if background cache update is happening
      partitionAggrColStatsCacheLock.readLock().lock();
      isPartitionAggrColStatsCacheDirty.set(true);
      sharedCache.removeAggrPartitionColStatsFromCache(catName, dbName, tblName);
    } finally {
      partitionAggrColStatsCacheLock.readLock().unlock();
    }
  }

  @Override
  public List<HiveObjectPrivilege> listPrincipalDBGrantsAll(
      String principalName, PrincipalType principalType) {
    return rawStore.listPrincipalDBGrantsAll(principalName, principalType);
  }

  @Override
  public List<HiveObjectPrivilege> listPrincipalTableGrantsAll(
      String principalName, PrincipalType principalType) {
    return rawStore.listPrincipalTableGrantsAll(principalName, principalType);
  }

  @Override
  public List<HiveObjectPrivilege> listPrincipalPartitionGrantsAll(
      String principalName, PrincipalType principalType) {
    return rawStore.listPrincipalPartitionGrantsAll(principalName, principalType);
  }

  @Override
  public List<HiveObjectPrivilege> listPrincipalTableColumnGrantsAll(
      String principalName, PrincipalType principalType) {
    return rawStore.listPrincipalTableColumnGrantsAll(principalName, principalType);
  }

  @Override
  public List<HiveObjectPrivilege> listPrincipalPartitionColumnGrantsAll(
      String principalName, PrincipalType principalType) {
    return rawStore.listPrincipalPartitionColumnGrantsAll(principalName, principalType);
  }

  @Override
  public List<HiveObjectPrivilege> listGlobalGrantsAll() {
    return rawStore.listGlobalGrantsAll();
  }

  @Override
  public List<HiveObjectPrivilege> listDBGrantsAll(String catName, String dbName) {
    return rawStore.listDBGrantsAll(catName, dbName);
  }

  @Override
  public List<HiveObjectPrivilege> listPartitionColumnGrantsAll(String catName, String dbName,
      String tableName, String partitionName, String columnName) {
    return rawStore.listPartitionColumnGrantsAll(catName, dbName, tableName, partitionName, columnName);
  }

  @Override
  public List<HiveObjectPrivilege> listTableGrantsAll(String catName, String dbName,
      String tableName) {
    return rawStore.listTableGrantsAll(catName, dbName, tableName);
  }

  @Override
  public List<HiveObjectPrivilege> listPartitionGrantsAll(String catName, String dbName,
      String tableName, String partitionName) {
    return rawStore.listPartitionGrantsAll(catName, dbName, tableName, partitionName);
  }

  @Override
  public List<HiveObjectPrivilege> listTableColumnGrantsAll(String catName, String dbName,
      String tableName, String columnName) {
    return rawStore.listTableColumnGrantsAll(catName, dbName, tableName, columnName);
  }

  @Override
  public void createFunction(Function func)
      throws InvalidObjectException, MetaException {
    // TODO fucntionCache
    rawStore.createFunction(func);
  }

  @Override
  public void alterFunction(String catName, String dbName, String funcName,
      Function newFunction) throws InvalidObjectException, MetaException {
    // TODO fucntionCache
    rawStore.alterFunction(catName, dbName, funcName, newFunction);
  }

  @Override
  public void dropFunction(String catName, String dbName, String funcName) throws MetaException,
      NoSuchObjectException, InvalidObjectException, InvalidInputException {
    // TODO fucntionCache
    rawStore.dropFunction(catName, dbName, funcName);
  }

  @Override
  public Function getFunction(String catName, String dbName, String funcName)
      throws MetaException {
    // TODO fucntionCache
    return rawStore.getFunction(catName, dbName, funcName);
  }

  @Override
  public List<Function> getAllFunctions(String catName) throws MetaException {
    // TODO fucntionCache
    return rawStore.getAllFunctions(catName);
  }

  @Override
  public List<String> getFunctions(String catName, String dbName, String pattern)
      throws MetaException {
    // TODO fucntionCache
    return rawStore.getFunctions(catName, dbName, pattern);
  }

  @Override
  public NotificationEventResponse getNextNotification(
      NotificationEventRequest rqst) {
    return rawStore.getNextNotification(rqst);
  }

  @Override
  public void addNotificationEvent(NotificationEvent event) {
    rawStore.addNotificationEvent(event);
  }

  @Override
  public void cleanNotificationEvents(int olderThan) {
    rawStore.cleanNotificationEvents(olderThan);
  }

  @Override
  public CurrentNotificationEventId getCurrentNotificationEventId() {
    return rawStore.getCurrentNotificationEventId();
  }

  @Override
  public NotificationEventsCountResponse getNotificationEventsCount(NotificationEventsCountRequest rqst) {
    return rawStore.getNotificationEventsCount(rqst);
  }

  @Override
  public void flushCache() {
    rawStore.flushCache();
  }

  @Override
  public ByteBuffer[] getFileMetadata(List<Long> fileIds) throws MetaException {
    return rawStore.getFileMetadata(fileIds);
  }

  @Override
  public void putFileMetadata(List<Long> fileIds, List<ByteBuffer> metadata,
      FileMetadataExprType type) throws MetaException {
    rawStore.putFileMetadata(fileIds, metadata, type);
  }

  @Override
  public boolean isFileMetadataSupported() {
    return rawStore.isFileMetadataSupported();
  }

  @Override
  public void getFileMetadataByExpr(List<Long> fileIds,
      FileMetadataExprType type, byte[] expr, ByteBuffer[] metadatas,
      ByteBuffer[] exprResults, boolean[] eliminated) throws MetaException {
    rawStore.getFileMetadataByExpr(fileIds, type, expr, metadatas, exprResults, eliminated);
  }

  @Override
  public FileMetadataHandler getFileMetadataHandler(FileMetadataExprType type) {
    return rawStore.getFileMetadataHandler(type);
  }

  @Override
  public int getTableCount() throws MetaException {
    return rawStore.getTableCount();
  }

  @Override
  public int getPartitionCount() throws MetaException {
    return rawStore.getPartitionCount();
  }

  @Override
  public int getDatabaseCount() throws MetaException {
    return rawStore.getDatabaseCount();
  }

  @Override
  public List<SQLPrimaryKey> getPrimaryKeys(String catName, String db_name, String tbl_name)
      throws MetaException {
    // TODO constraintCache
    return rawStore.getPrimaryKeys(catName, db_name, tbl_name);
  }

  @Override
  public List<SQLForeignKey> getForeignKeys(String catName, String parent_db_name,
      String parent_tbl_name, String foreign_db_name, String foreign_tbl_name)
      throws MetaException {
    // TODO constraintCache
    return rawStore.getForeignKeys(catName, parent_db_name, parent_tbl_name, foreign_db_name, foreign_tbl_name);
  }

  @Override
  public List<SQLUniqueConstraint> getUniqueConstraints(String catName, String db_name, String tbl_name)
      throws MetaException {
    // TODO constraintCache
    return rawStore.getUniqueConstraints(catName, db_name, tbl_name);
  }

  @Override
  public List<SQLNotNullConstraint> getNotNullConstraints(String catName, String db_name, String tbl_name)
      throws MetaException {
    // TODO constraintCache
    return rawStore.getNotNullConstraints(catName, db_name, tbl_name);
  }

  @Override
  public List<SQLDefaultConstraint> getDefaultConstraints(String catName, String db_name, String tbl_name)
      throws MetaException {
    // TODO constraintCache
    return rawStore.getDefaultConstraints(catName, db_name, tbl_name);
  }

  @Override
  public List<String> createTableWithConstraints(Table tbl, List<SQLPrimaryKey> primaryKeys,
      List<SQLForeignKey> foreignKeys, List<SQLUniqueConstraint> uniqueConstraints,
      List<SQLNotNullConstraint> notNullConstraints,
      List<SQLDefaultConstraint> defaultConstraints) throws InvalidObjectException, MetaException {
    // TODO constraintCache
    List<String> constraintNames = rawStore.createTableWithConstraints(tbl, primaryKeys,
        foreignKeys, uniqueConstraints, notNullConstraints, defaultConstraints);
    String dbName = normalizeIdentifier(tbl.getDbName());
    String tblName = normalizeIdentifier(tbl.getTableName());
    String catName = tbl.isSetCatName() ? normalizeIdentifier(tbl.getCatName()) :
        DEFAULT_CATALOG_NAME;
    if (!shouldCacheTable(catName, dbName, tblName)) {
      return constraintNames;
    }
    SharedCache sharedCache = sharedCacheWrapper.get();
    if (sharedCache == null) return constraintNames;
    sharedCache.addTableToCache(catName, normalizeIdentifier(tbl.getDbName()),
        normalizeIdentifier(tbl.getTableName()), tbl);
    return constraintNames;
  }

  @Override
  public void dropConstraint(String catName, String dbName, String tableName,
      String constraintName, boolean missingOk) throws NoSuchObjectException {
    // TODO constraintCache
    rawStore.dropConstraint(catName, dbName, tableName, constraintName, missingOk);
  }

  @Override
  public List<String> addPrimaryKeys(List<SQLPrimaryKey> pks)
      throws InvalidObjectException, MetaException {
    // TODO constraintCache
    return rawStore.addPrimaryKeys(pks);
  }

  @Override
  public List<String> addForeignKeys(List<SQLForeignKey> fks)
      throws InvalidObjectException, MetaException {
    // TODO constraintCache
    return rawStore.addForeignKeys(fks);
  }

  @Override
  public List<String> addUniqueConstraints(List<SQLUniqueConstraint> uks)
      throws InvalidObjectException, MetaException {
    // TODO constraintCache
    return rawStore.addUniqueConstraints(uks);
  }

  @Override
  public List<String> addNotNullConstraints(List<SQLNotNullConstraint> nns)
      throws InvalidObjectException, MetaException {
    // TODO constraintCache
    return rawStore.addNotNullConstraints(nns);
  }

  @Override
  public List<String> addDefaultConstraints(List<SQLDefaultConstraint> nns)
      throws InvalidObjectException, MetaException {
    // TODO constraintCache
    return rawStore.addDefaultConstraints(nns);
  }

  @Override
  public List<ColStatsObjWithSourceInfo> getPartitionColStatsForDatabase(String catName, String dbName)
      throws MetaException, NoSuchObjectException {
    return rawStore.getPartitionColStatsForDatabase(catName, dbName);
  }

  // TODO - not clear if we should cache these or not.  For now, don't bother
  @Override
  public void createISchema(ISchema schema)
      throws AlreadyExistsException, NoSuchObjectException, MetaException {
    rawStore.createISchema(schema);
  }

  @Override
  public void alterISchema(ISchemaName schemaName, ISchema newSchema)
      throws NoSuchObjectException, MetaException {
    rawStore.alterISchema(schemaName, newSchema);
  }

  @Override
  public ISchema getISchema(ISchemaName schemaName) throws MetaException {
    return rawStore.getISchema(schemaName);
  }

  @Override
  public void dropISchema(ISchemaName schemaName) throws NoSuchObjectException, MetaException {
    rawStore.dropISchema(schemaName);
  }

  @Override
  public void addSchemaVersion(SchemaVersion schemaVersion) throws
      AlreadyExistsException, InvalidObjectException, NoSuchObjectException, MetaException {
    rawStore.addSchemaVersion(schemaVersion);
  }

  @Override
  public void alterSchemaVersion(SchemaVersionDescriptor version, SchemaVersion newVersion) throws
      NoSuchObjectException, MetaException {
    rawStore.alterSchemaVersion(version, newVersion);
  }

  @Override
  public SchemaVersion getSchemaVersion(SchemaVersionDescriptor version) throws MetaException {
    return rawStore.getSchemaVersion(version);
  }

  @Override
  public SchemaVersion getLatestSchemaVersion(ISchemaName schemaName) throws MetaException {
    return rawStore.getLatestSchemaVersion(schemaName);
  }

  @Override
  public List<SchemaVersion> getAllSchemaVersion(ISchemaName schemaName) throws MetaException {
    return rawStore.getAllSchemaVersion(schemaName);
  }

  @Override
  public List<SchemaVersion> getSchemaVersionsByColumns(String colName, String colNamespace,
                                                        String type) throws MetaException {
    return rawStore.getSchemaVersionsByColumns(colName, colNamespace, type);
  }

  @Override
  public void dropSchemaVersion(SchemaVersionDescriptor version) throws NoSuchObjectException,
      MetaException {
    rawStore.dropSchemaVersion(version);
  }

  @Override
  public SerDeInfo getSerDeInfo(String serDeName) throws NoSuchObjectException, MetaException {
    return rawStore.getSerDeInfo(serDeName);
  }

  @Override
  public void addSerde(SerDeInfo serde) throws AlreadyExistsException, MetaException {
    rawStore.addSerde(serde);
  }

  public RawStore getRawStore() {
    return rawStore;
  }

  @VisibleForTesting
  public void setRawStore(RawStore rawStore) {
    this.rawStore = rawStore;
  }

  @Override
  public String getMetastoreDbUuid() throws MetaException {
    return rawStore.getMetastoreDbUuid();
  }

  // TODO: this is only used to hide SharedCache instance from direct use; ideally, the stuff in
  //       CachedStore that's specific to SharedCache (e.g. update threads) should be refactored to
  //       be part of this, then this could be moved out of this file (or merged with SharedCache).
  private static final class SharedCacheWrapper {
    private enum InitState {
      NOT_ENABLED, INITIALIZING, INITIALIZED, FAILED_FATAL
    }

    private final SharedCache instance = new SharedCache();
    private final Object initLock = new Object();
    private volatile InitState initState = InitState.NOT_ENABLED;
    // We preserve the old setConf init behavior, where a failed prewarm would fail the query
    // and give a chance to another query to try prewarming again. Basically, we'd increment the
    // count and all the queries waiting for prewarm would fail; however, we will retry the prewarm
    // again infinitely, so some queries might succeed later.
    private int initFailureCount;
    private Throwable lastError;

    /**
     * A callback to updates the initialization state.
     * @param error Error, if any. Null means the initialization has succeeded.
     * @param isFatal Whether the error (if present) is fatal, or whether init will be retried.
     */
    void updateInitState(Throwable error, boolean isFatal) {
      boolean isSuccessful = error == null;
      synchronized (initLock) {
        if (isSuccessful) {
          initState = InitState.INITIALIZED;
        } else if (isFatal) {
          initState = InitState.FAILED_FATAL;
          lastError = error;
        } else {
          ++initFailureCount;
          lastError = error;
        }
        initLock.notifyAll();
      }
    }

    void startInit(Configuration conf) {
      LOG.info("Initializing shared cache");
      synchronized (initLock) {
        assert initState == InitState.NOT_ENABLED;
        initState = InitState.INITIALIZING;
      }
      // The first iteration of the update thread will prewarm the cache.
      startCacheUpdateService(conf);
    }

    /**
     * Gets the SharedCache, waiting for initialization to happen if necessary.
     * Fails on any initialization error, even if the init will be retried later.
     */
    public SharedCache get() throws MetaException {
      if (!waitForInit()) {
        return null;
      }
      return instance;
    }

    /** Gets the shared cache unsafely (may not be ready to use); used by init methods. */
    SharedCache getUnsafe() {
      return instance;
    }

    private boolean waitForInit() throws MetaException {
      synchronized (initLock) {
        int localFailureCount = initFailureCount;
        while (true) {
          switch (initState) {
          case INITIALIZED: return true;
          case NOT_ENABLED: return false;
          case FAILED_FATAL: {
            throw new RuntimeException("CachedStore prewarm had a fatal error", lastError);
          }
          case INITIALIZING: {
            try {
              initLock.wait(100);
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
              throw new MetaException("Interrupted");
            }
            break;
          }
          default: throw new AssertionError(initState);
          }
          // Fail if any errors occured; mimicks the old behavior where a setConf prewarm failure
          // would fail the current task, but cause the next setConf to try prewarm again forever.
          if (initFailureCount != localFailureCount) {
            throw new RuntimeException("CachedStore prewarm failed", lastError);
          }
        }
      }
    }

    /**
     * Notify all threads blocked on initialization, to continue work. (We allow read while prewarm
     * is running; all write calls are blocked using waitForInitAndBlock).
     */
    void notifyAllBlocked() {
      synchronized (initLock) {
        initLock.notifyAll();
      }
    }

    boolean isInitialized() {
      return initState.equals(InitState.INITIALIZED);
    }
  }

  @VisibleForTesting
  void setInitializedForTest() {
    sharedCacheWrapper.updateInitState(null, false);
  }

  @Override
  public void createResourcePlan(WMResourcePlan resourcePlan, String copyFrom, int defaultPoolSize)
      throws AlreadyExistsException, InvalidObjectException, MetaException, NoSuchObjectException {
    rawStore.createResourcePlan(resourcePlan, copyFrom, defaultPoolSize);
  }

  @Override
  public WMFullResourcePlan getResourcePlan(String name)
      throws NoSuchObjectException, MetaException {
    return rawStore.getResourcePlan(name);
  }

  @Override
  public List<WMResourcePlan> getAllResourcePlans() throws MetaException {
    return rawStore.getAllResourcePlans();
  }

  @Override
  public WMFullResourcePlan alterResourcePlan(String name, WMNullableResourcePlan resourcePlan,
    boolean canActivateDisabled, boolean canDeactivate, boolean isReplace)
      throws AlreadyExistsException, NoSuchObjectException, InvalidOperationException,
          MetaException {
    return rawStore.alterResourcePlan(
      name, resourcePlan, canActivateDisabled, canDeactivate, isReplace);
  }

  @Override
  public WMFullResourcePlan getActiveResourcePlan() throws MetaException {
    return rawStore.getActiveResourcePlan();
  }

  @Override
  public WMValidateResourcePlanResponse validateResourcePlan(String name)
      throws NoSuchObjectException, InvalidObjectException, MetaException {
    return rawStore.validateResourcePlan(name);
  }

  @Override
  public void dropResourcePlan(String name) throws NoSuchObjectException, MetaException {
    rawStore.dropResourcePlan(name);
  }

  @Override
  public void createWMTrigger(WMTrigger trigger)
      throws AlreadyExistsException, MetaException, NoSuchObjectException,
          InvalidOperationException {
    rawStore.createWMTrigger(trigger);
  }

  @Override
  public void alterWMTrigger(WMTrigger trigger)
      throws NoSuchObjectException, InvalidOperationException, MetaException {
    rawStore.alterWMTrigger(trigger);
  }

  @Override
  public void dropWMTrigger(String resourcePlanName, String triggerName)
      throws NoSuchObjectException, InvalidOperationException, MetaException {
    rawStore.dropWMTrigger(resourcePlanName, triggerName);
  }

  @Override
  public List<WMTrigger> getTriggersForResourcePlan(String resourcePlanName)
      throws NoSuchObjectException, MetaException {
    return rawStore.getTriggersForResourcePlan(resourcePlanName);
  }

  @Override
  public void createPool(WMPool pool) throws AlreadyExistsException, NoSuchObjectException,
      InvalidOperationException, MetaException {
    rawStore.createPool(pool);
  }

  @Override
  public void alterPool(WMNullablePool pool, String poolPath) throws AlreadyExistsException,
      NoSuchObjectException, InvalidOperationException, MetaException {
    rawStore.alterPool(pool, poolPath);
  }

  @Override
  public void dropWMPool(String resourcePlanName, String poolPath)
      throws NoSuchObjectException, InvalidOperationException, MetaException {
    rawStore.dropWMPool(resourcePlanName, poolPath);
  }

  @Override
  public void createOrUpdateWMMapping(WMMapping mapping, boolean update)
      throws AlreadyExistsException, NoSuchObjectException, InvalidOperationException,
      MetaException {
    rawStore.createOrUpdateWMMapping(mapping, update);
  }

  @Override
  public void dropWMMapping(WMMapping mapping)
      throws NoSuchObjectException, InvalidOperationException, MetaException {
    rawStore.dropWMMapping(mapping);
  }

  @Override
  public void createWMTriggerToPoolMapping(String resourcePlanName, String triggerName,
      String poolPath) throws AlreadyExistsException, NoSuchObjectException,
      InvalidOperationException, MetaException {
    rawStore.createWMTriggerToPoolMapping(resourcePlanName, triggerName, poolPath);
  }

  @Override
  public void dropWMTriggerToPoolMapping(String resourcePlanName, String triggerName,
      String poolPath) throws NoSuchObjectException, InvalidOperationException, MetaException {
    rawStore.dropWMTriggerToPoolMapping(resourcePlanName, triggerName, poolPath);
  }

  private static boolean isNotInBlackList(String catName, String dbName, String tblName) {
    String str = catName + "." + dbName + "." + tblName;
    for (Pattern pattern : blacklistPatterns) {
      LOG.debug("Trying to match: {} against blacklist pattern: {}", str, pattern);
      Matcher matcher = pattern.matcher(str);
      if (matcher.matches()) {
        LOG.debug("Found matcher group: {} at start index: {} and end index: {}", matcher.group(),
            matcher.start(), matcher.end());
        return false;
      }
    }
    return true;
  }

  private static boolean isInWhitelist(String catName, String dbName, String tblName) {
    String str = Warehouse.getCatalogQualifiedTableName(catName, dbName, tblName);
    for (Pattern pattern : whitelistPatterns) {
      LOG.debug("Trying to match: {} against whitelist pattern: {}", str, pattern);
      Matcher matcher = pattern.matcher(str);
      if (matcher.matches()) {
        LOG.debug("Found matcher group: {} at start index: {} and end index: {}", matcher.group(),
            matcher.start(), matcher.end());
        return true;
      }
    }
    return false;
  }

  // For testing
  static void setWhitelistPattern(List<Pattern> patterns) {
    whitelistPatterns = patterns;
  }

  // For testing
  static void setBlacklistPattern(List<Pattern> patterns) {
    blacklistPatterns = patterns;
  }

  // Determines if we should cache a table (& its partitions, stats etc),
  // based on whitelist/blacklist
  static boolean shouldCacheTable(String catName, String dbName, String tblName) {
    if (!isNotInBlackList(catName, dbName, tblName)) {
      LOG.debug("{}.{} is in blacklist, skipping", dbName, tblName);
      return false;
    }
    if (!isInWhitelist(catName, dbName, tblName)) {
      LOG.debug("{}.{} is not in whitelist, skipping", dbName, tblName);
      return false;
    }
    return true;
  }

  static List<Pattern> createPatterns(String configStr) {
    List<String> patternStrs = Arrays.asList(configStr.split(","));
    List<Pattern> patterns = new ArrayList<Pattern>();
    for (String str : patternStrs) {
      patterns.add(Pattern.compile(str));
    }
    return patterns;
  }

  static boolean isBlacklistWhitelistEmpty(Configuration conf) {
    return MetastoreConf.getAsString(conf, MetastoreConf.ConfVars.CACHED_RAW_STORE_CACHED_OBJECTS_WHITELIST)
        .equals(".*")
        && MetastoreConf.getAsString(conf, MetastoreConf.ConfVars.CACHED_RAW_STORE_CACHED_OBJECTS_BLACKLIST).isEmpty();
  }

  private static class CloseableLock implements Closeable {
    private final Lock lock;

    public CloseableLock(Lock lock) {
      this.lock = lock;
    }

    @Override
    public void close() {
      lock.unlock();
    }
  }
}
