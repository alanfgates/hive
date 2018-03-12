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

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.hadoop.hive.metastore.StatObjectConverter;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.AggrStats;
import org.apache.hadoop.hive.metastore.api.Catalog;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.TableMeta;
import org.apache.hadoop.hive.metastore.cache.CachedStore.PartitionWrapper;
import org.apache.hadoop.hive.metastore.cache.CachedStore.StorageDescriptorWrapper;
import org.apache.hadoop.hive.metastore.cache.CachedStore.TableWrapper;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils.ColStatsObjWithSourceInfo;
import org.apache.hadoop.hive.metastore.utils.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

import static org.apache.hadoop.hive.metastore.utils.StringUtils.normalizeIdentifier;

public class SharedCache {
  private Map<String, Catalog> catalogCache = new TreeMap<>();
  private Map<String, Database> databaseCache = new TreeMap<>();
  private Map<String, TableWrapper> tableCache = new TreeMap<>();
  private Map<String, PartitionWrapper> partitionCache = new TreeMap<>();
  private Map<String, ColumnStatisticsObj> partitionColStatsCache = new TreeMap<>();
  private Map<String, ColumnStatisticsObj> tableColStatsCache = new TreeMap<>();
  private Map<ByteArrayWrapper, StorageDescriptorWrapper> sdCache = new HashMap<>();
  private Map<String, List<ColumnStatisticsObj>> aggrColStatsCache = new HashMap<>();
  private static MessageDigest md;

  enum StatsType {
    ALL(0), ALLBUTDEFAULT(1);

    private final int position;

    StatsType(int position) {
      this.position = position;
    }

    public int getPosition() {
      return position;
    }
  }

  private static final Logger LOG = LoggerFactory.getLogger(SharedCache.class);

  static {
    try {
      md = MessageDigest.getInstance("MD5");
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException("should not happen", e);
    }
  }

  public synchronized Catalog getCatalogFromCache(String name) {
    Catalog cat = catalogCache.get(name);
    return cat == null ? null : cat.deepCopy();
  }

  public synchronized void addCatalogToCache(Catalog cat) {
    Catalog catCopy = cat.deepCopy();
    String name = normalizeIdentifier(cat.getName());
    catCopy.setName(name);
    catalogCache.put(name, catCopy);
  }

  public synchronized void alterCatalogInCache(String catName, Catalog newCat) {
    removeCatalogFromCache(normalizeIdentifier(catName));
    addCatalogToCache(newCat.deepCopy());
  }


  public synchronized void removeCatalogFromCache(String name) {
    name = normalizeIdentifier(name);
    catalogCache.remove(name);
  }

  public synchronized List<String> listCachedCatalogs() {
    return new ArrayList<>(catalogCache.keySet());
  }

  public synchronized Database getDatabaseFromCache(String catName, String name) {
    Database db = databaseCache.get(CacheUtils.buildDbKey(catName, name));
    return db != null ? db.deepCopy() : null;
  }

  public synchronized void addDatabaseToCache(String dbName, Database db) {
    Database dbCopy = db.deepCopy();
    String catName = normalizeIdentifier(db.getCatalogName());
    dbName = normalizeIdentifier(dbName);
    dbCopy.setCatalogName(catName);
    dbCopy.setName(dbName);
    databaseCache.put(CacheUtils.buildDbKey(catName, dbName), dbCopy);
  }

  public synchronized void removeDatabaseFromCache(String catName, String dbName) {
    catName = normalizeIdentifier(catName);
    dbName = normalizeIdentifier(dbName);
    databaseCache.remove(CacheUtils.buildDbKey(catName, dbName));
  }

  public synchronized List<String> listCachedDatabases(String catName) {
    List<String> names = new ArrayList<>();
    for (String pair : databaseCache.keySet()) {
      LOG.info("XXX Looking at " + pair);
      String[] n = CacheUtils.splitDbName(pair);
      if (catName.equals(n[0])) names.add(n[1]);
    }
    LOG.info("XXX Survived");
    return names;
  }

  public synchronized void alterDatabaseInCache(String catName, String dbName, Database newDb) {
    catName = normalizeIdentifier(catName);
    removeDatabaseFromCache(catName, normalizeIdentifier(dbName));
    addDatabaseToCache(normalizeIdentifier(newDb.getName()), newDb.deepCopy());
  }

  public synchronized int getCachedDatabaseCount() {
    return databaseCache.size();
  }

  public synchronized Table getTableFromCache(String catName, String dbName, String tableName) {
    TableWrapper tblWrapper = tableCache.get(CacheUtils.buildTableKey(catName, dbName, tableName));
    if (tblWrapper == null) {
      return null;
    }
    return CacheUtils.assemble(tblWrapper, this);
  }

  public synchronized void addTableToCache(String catName, String dbName, String tblName,
                                           Table tbl) {
    Table tblCopy = tbl.deepCopy();
    tblCopy.setCatName(normalizeIdentifier(catName));
    tblCopy.setDbName(normalizeIdentifier(dbName));
    tblCopy.setTableName(normalizeIdentifier(tblName));
    if (tblCopy.getPartitionKeys() != null) {
      for (FieldSchema fs : tblCopy.getPartitionKeys()) {
        fs.setName(normalizeIdentifier(fs.getName()));
      }
    }
    TableWrapper wrapper;
    if (tbl.getSd() != null) {
      byte[] sdHash = MetaStoreUtils.hashStorageDescriptor(tbl.getSd(), md);
      StorageDescriptor sd = tbl.getSd();
      increSd(sd, sdHash);
      tblCopy.setSd(null);
      wrapper = new TableWrapper(tblCopy, sdHash, sd.getLocation(), sd.getParameters());
    } else {
      wrapper = new TableWrapper(tblCopy, null, null, null);
    }
    tableCache.put(CacheUtils.buildTableKey(catName, dbName, tblName), wrapper);
  }

  public synchronized void removeTableFromCache(String catName, String dbName, String tblName) {
    TableWrapper tblWrapper = tableCache.remove(CacheUtils.buildTableKey(catName, dbName, tblName));
    byte[] sdHash = tblWrapper.getSdHash();
    if (sdHash!=null) {
      decrSd(sdHash);
    }
  }

  public synchronized ColumnStatisticsObj getCachedTableColStats(String colStatsCacheKey) {
    return tableColStatsCache.get(colStatsCacheKey)!=null?tableColStatsCache.get(colStatsCacheKey).deepCopy():null;
  }

  public synchronized void removeTableColStatsFromCache(String catName, String dbName, String tblName) {
    String partialKey = CacheUtils.buildTableKeyWithDelimit(catName, dbName, tblName);
    Iterator<Entry<String, ColumnStatisticsObj>> iterator =
        tableColStatsCache.entrySet().iterator();
    while (iterator.hasNext()) {
      Entry<String, ColumnStatisticsObj> entry = iterator.next();
      String key = entry.getKey();
      if (key.toLowerCase().startsWith(partialKey.toLowerCase())) {
        iterator.remove();
      }
    }
  }

  public synchronized void removeTableColStatsFromCache(String catName, String dbName,
                                                        String tableName, String colName) {
    if (colName == null) {
      removeTableColStatsFromCache(catName, dbName, tableName);
    } else {
      tableColStatsCache.remove(CacheUtils.buildTableColKey(catName, dbName, tableName, colName));
    }
  }

  public synchronized void updateTableColStatsInCache(String catName, String dbName, String tableName,
      List<ColumnStatisticsObj> colStatsForTable) {
    for (ColumnStatisticsObj colStatObj : colStatsForTable) {
      // Get old stats object if present
      String key = CacheUtils.buildTableColKey(catName, dbName, tableName, colStatObj.getColName());
      ColumnStatisticsObj oldStatsObj = tableColStatsCache.get(key);
      if (oldStatsObj != null) {
        LOG.debug("CachedStore: updating table column stats for column: " + colStatObj.getColName()
            + ", of table: " + tableName + " and database: " + dbName);
        // Update existing stat object's field
        StatObjectConverter.setFieldsIntoOldStats(oldStatsObj, colStatObj);
      } else {
        // No stats exist for this key; add a new object to the cache
        tableColStatsCache.put(key, colStatObj);
      }
    }
  }

  public synchronized void alterTableInCache(String catName, String dbName, String tblName,
                                             Table newTable) {
    removeTableFromCache(catName, dbName, tblName);
    // TODO Pretty sure this is broken.  Everyone needs to add the table to the cache with
    // normalized names, so we should be removing it with normalized names.
    addTableToCache(normalizeIdentifier(catName),
        normalizeIdentifier(newTable.getDbName()),
        normalizeIdentifier(newTable.getTableName()), newTable);
  }

  public synchronized void alterTableInPartitionCache(String catName, String dbName, String tblName,
      Table newTable) {
    if (!dbName.equals(newTable.getDbName()) || !tblName.equals(newTable.getTableName())) {
      List<Partition> partitions = listCachedPartitions(catName, dbName, tblName, -1);
      for (Partition part : partitions) {
        removePartitionFromCache(catName, part.getDbName(), part.getTableName(), part.getValues());
        part.setDbName(normalizeIdentifier(newTable.getDbName()));
        part.setTableName(normalizeIdentifier(newTable.getTableName()));
        addPartitionToCache(normalizeIdentifier(newTable.getCatName()),
            normalizeIdentifier(newTable.getDbName()),
            normalizeIdentifier(newTable.getTableName()), part);
      }
    }
  }

  public synchronized void alterTableInTableColStatsCache(String catName, String dbName, String tblName,
      Table newTable) {
    if (!dbName.equals(newTable.getDbName()) || !tblName.equals(newTable.getTableName())) {
      String oldPartialTableStatsKey = CacheUtils.buildTableKeyWithDelimit(catName, dbName, tblName);
      Iterator<Entry<String, ColumnStatisticsObj>> iterator =
          tableColStatsCache.entrySet().iterator();
      Map<String, ColumnStatisticsObj> newTableColStats =
          new HashMap<>();
      while (iterator.hasNext()) {
        Entry<String, ColumnStatisticsObj> entry = iterator.next();
        String key = entry.getKey();
        ColumnStatisticsObj colStatObj = entry.getValue();
        if (key.toLowerCase().startsWith(oldPartialTableStatsKey.toLowerCase())) {
          String[] decomposedKey = CacheUtils.splitTableColStats(key);
          String newKey = CacheUtils.buildTableColKey(catName, decomposedKey[0], decomposedKey[1], decomposedKey[2]);
          newTableColStats.put(newKey, colStatObj);
          iterator.remove();
        }
      }
      tableColStatsCache.putAll(newTableColStats);
    }
  }

  public synchronized void alterTableInPartitionColStatsCache(String catName, String dbName, String tblName,
      Table newTable) {
    if (!dbName.equals(newTable.getDbName()) || !tblName.equals(newTable.getTableName())) {
      List<Partition> partitions = listCachedPartitions(catName, dbName, tblName, -1);
      Map<String, ColumnStatisticsObj> newPartitionColStats = new HashMap<>();
      for (Partition part : partitions) {
        String oldPartialPartitionKey =
            CacheUtils.buildPartKeyWithDelimit(catName, dbName, tblName, part.getValues());
        Iterator<Entry<String, ColumnStatisticsObj>> iterator =
            partitionColStatsCache.entrySet().iterator();
        while (iterator.hasNext()) {
          Entry<String, ColumnStatisticsObj> entry = iterator.next();
          String key = entry.getKey();
          ColumnStatisticsObj colStatObj = entry.getValue();
          if (key.toLowerCase().startsWith(oldPartialPartitionKey.toLowerCase())) {
            Object[] decomposedKey = CacheUtils.splitPartitionColStats(key);
            // New key has the new table name
            String newKey = CacheUtils.buildPartColKey(catName, (String)decomposedKey[0],
                newTable.getTableName(),
                (List<String>) decomposedKey[2], (String) decomposedKey[3]);
            newPartitionColStats.put(newKey, colStatObj);
            iterator.remove();
          }
        }
      }
      partitionColStatsCache.putAll(newPartitionColStats);
    }
  }

  public synchronized void alterTableInAggrPartitionColStatsCache(String catName, String dbName,
                                                                  String tblName, Table newTable) {
    if (!dbName.equals(newTable.getDbName()) || !tblName.equals(newTable.getTableName())) {
      Map<String, List<ColumnStatisticsObj>> newAggrColStatsCache =
          new HashMap<String, List<ColumnStatisticsObj>>();
      String oldPartialKey = CacheUtils.buildTableKeyWithDelimit(catName, dbName, tblName);
      Iterator<Entry<String, List<ColumnStatisticsObj>>> iterator =
          aggrColStatsCache.entrySet().iterator();
      while (iterator.hasNext()) {
        Entry<String, List<ColumnStatisticsObj>> entry = iterator.next();
        String key = entry.getKey();
        List<ColumnStatisticsObj> value = entry.getValue();
        if (key.toLowerCase().startsWith(oldPartialKey.toLowerCase())) {
          Object[] decomposedKey = CacheUtils.splitAggrColStats(key);
          // New key has the new table name
          String newKey = CacheUtils.buildTableColKey((String)decomposedKey[0],
              (String) decomposedKey[1], newTable.getTableName(), (String) decomposedKey[3]);
          newAggrColStatsCache.put(newKey, value);
          iterator.remove();
        }
      }
      aggrColStatsCache.putAll(newAggrColStatsCache);
    }
  }

  public synchronized int getCachedTableCount() {
    return tableCache.size();
  }

  public synchronized List<Table> listCachedTables(String catName, String dbName) {
    List<Table> tables = new ArrayList<>();
    for (TableWrapper wrapper : tableCache.values()) {
      if (wrapper.getTable().getDbName().equals(dbName) && wrapper.getTable().getCatName().equals(catName)) {
        tables.add(CacheUtils.assemble(wrapper, this));
      }
    }
    return tables;
  }

  public synchronized List<TableMeta> getTableMeta(String catName, String dbNames,
                                                   String tableNames, List<String> tableTypes) {
    List<TableMeta> tableMetas = new ArrayList<>();
    for (String dbName : listCachedDatabases(catName)) {
      if (CacheUtils.matches(dbName, dbNames)) {
        for (Table table : listCachedTables(catName, dbName)) {
          if (CacheUtils.matches(table.getTableName(), tableNames)) {
            if (tableTypes==null || tableTypes.contains(table.getTableType())) {
              TableMeta metaData = new TableMeta(dbName, table.getTableName(), table.getTableType());
                metaData.setComments(table.getParameters().get("comment"));
                tableMetas.add(metaData);
            }
          }
        }
      }
    }
    return tableMetas;
  }

  public synchronized void addPartitionToCache(String catName, String dbName, String tblName,
                                               Partition part) {
    Partition partCopy = part.deepCopy();
    PartitionWrapper wrapper;
    if (part.getSd()!=null) {
      byte[] sdHash = MetaStoreUtils.hashStorageDescriptor(part.getSd(), md);
      StorageDescriptor sd = part.getSd();
      increSd(sd, sdHash);
      partCopy.setSd(null);
      wrapper = new PartitionWrapper(partCopy, sdHash, sd.getLocation(), sd.getParameters());
    } else {
      wrapper = new PartitionWrapper(partCopy, null, null, null);
    }
    partitionCache.put(CacheUtils.buildPartKey(catName, dbName, tblName, part.getValues()), wrapper);
  }

  public synchronized Partition getPartitionFromCache(String key) {
    PartitionWrapper wrapper = partitionCache.get(key);
    if (wrapper == null) {
      return null;
    }
    Partition p = CacheUtils.assemble(wrapper, this);
    return p;
  }

  public synchronized Partition getPartitionFromCache(String catName, String dbName,
                                                      String tblName, List<String> part_vals) {
    return getPartitionFromCache(CacheUtils.buildPartKey(catName, dbName, tblName, part_vals));
  }

  public synchronized boolean existPartitionFromCache(String catName, String dbName,
                                                      String tblName, List<String> part_vals) {
    return partitionCache.containsKey(CacheUtils.buildPartKey(catName, dbName, tblName, part_vals));
  }

  public synchronized Partition removePartitionFromCache(String catName, String dbName,
                                                         String tblName, List<String> part_vals) {
    PartitionWrapper wrapper =
        partitionCache.remove(CacheUtils.buildPartKey(catName, dbName, tblName, part_vals));
    if (wrapper.getSdHash() != null) {
      decrSd(wrapper.getSdHash());
    }
    return wrapper.getPartition();
  }

  /**
   * Given a db + table, remove all partitions for this table from the cache
   * @param dbName
   * @param tblName
   * @return
   */
  public synchronized void removePartitionsFromCache(String catName, String dbName, String tblName) {
    String partialKey = CacheUtils.buildTableKeyWithDelimit(catName, dbName, tblName);
    Iterator<Entry<String, PartitionWrapper>> iterator = partitionCache.entrySet().iterator();
    while (iterator.hasNext()) {
      Entry<String, PartitionWrapper> entry = iterator.next();
      String key = entry.getKey();
      PartitionWrapper wrapper = entry.getValue();
      if (key.toLowerCase().startsWith(partialKey.toLowerCase())) {
        iterator.remove();
        if (wrapper.getSdHash() != null) {
          decrSd(wrapper.getSdHash());
        }
      }
    }
  }

  // Remove cached column stats for all partitions of all tables in a db
  public synchronized void removePartitionColStatsFromCache(String catName, String dbName) {
    String partialKey = CacheUtils.buildDbKeyWithDelimit(catName, dbName);
    Iterator<Entry<String, ColumnStatisticsObj>> iterator =
        partitionColStatsCache.entrySet().iterator();
    while (iterator.hasNext()) {
      Entry<String, ColumnStatisticsObj> entry = iterator.next();
      String key = entry.getKey();
      if (key.toLowerCase().startsWith(partialKey.toLowerCase())) {
        iterator.remove();
      }
    }
  }

  // Remove cached column stats for all partitions of a table
  public synchronized void removePartitionColStatsFromCache(String catName, String dbName, String tblName) {
    String partialKey = CacheUtils.buildTableKeyWithDelimit(catName, dbName, tblName);
    Iterator<Entry<String, ColumnStatisticsObj>> iterator =
        partitionColStatsCache.entrySet().iterator();
    while (iterator.hasNext()) {
      Entry<String, ColumnStatisticsObj> entry = iterator.next();
      String key = entry.getKey();
      if (key.toLowerCase().startsWith(partialKey.toLowerCase())) {
        iterator.remove();
      }
    }
  }

  // Remove cached column stats for a particular partition of a table
  public synchronized void removePartitionColStatsFromCache(String catName, String dbName, String tblName,
      List<String> partVals) {
    String partialKey = CacheUtils.buildPartKeyWithDelimit(catName, dbName, tblName, partVals);
    Iterator<Entry<String, ColumnStatisticsObj>> iterator =
        partitionColStatsCache.entrySet().iterator();
    while (iterator.hasNext()) {
      Entry<String, ColumnStatisticsObj> entry = iterator.next();
      String key = entry.getKey();
      if (key.toLowerCase().startsWith(partialKey.toLowerCase())) {
        iterator.remove();
      }
    }
  }

  // Remove cached column stats for a particular partition and a particular column of a table
  public synchronized void removePartitionColStatsFromCache(String catName, String dbName,
                                                            String tblName, List<String> partVals,
                                                            String colName) {
    partitionColStatsCache.remove(CacheUtils.buildPartColKey(catName, dbName, tblName, partVals,
        colName));
  }

  public synchronized List<Partition> listCachedPartitions(String catName, String dbName,
                                                           String tblName, int max) {
    List<Partition> partitions = new ArrayList<>();
    int count = 0;
    for (PartitionWrapper wrapper : partitionCache.values()) {
      String pCatName = wrapper.getPartition().isSetCatName() ?
          wrapper.getPartition().getCatName() : Warehouse.DEFAULT_CATALOG_NAME;
      if (pCatName.equals(catName) && wrapper.getPartition().getDbName().equals(dbName)
          && wrapper.getPartition().getTableName().equals(tblName)
          && (max == -1 || count < max)) {
        partitions.add(CacheUtils.assemble(wrapper, this));
        count++;
      }
    }
    return partitions;
  }

  public synchronized void alterPartitionInCache(String catName, String dbName, String tblName,
      List<String> partVals, Partition newPart) {
    removePartitionFromCache(catName, dbName, tblName, partVals);
    addPartitionToCache(normalizeIdentifier(catName),
        normalizeIdentifier(newPart.getDbName()),
        normalizeIdentifier(newPart.getTableName()), newPart);
  }

  public synchronized void alterPartitionInColStatsCache(String catName, String dbName, String tblName,
      List<String> partVals, Partition newPart) {
    String oldPartialPartitionKey =
        CacheUtils.buildPartKeyWithDelimit(catName, dbName, tblName, partVals);
    Map<String, ColumnStatisticsObj> newPartitionColStats = new HashMap<>();
    Iterator<Entry<String, ColumnStatisticsObj>> iterator =
        partitionColStatsCache.entrySet().iterator();
    while (iterator.hasNext()) {
      Entry<String, ColumnStatisticsObj> entry = iterator.next();
      String key = entry.getKey();
      ColumnStatisticsObj colStatObj = entry.getValue();
      if (key.toLowerCase().startsWith(oldPartialPartitionKey.toLowerCase())) {
        Object[] decomposedKey = CacheUtils.splitPartitionColStats(key);
        String newKey =
            CacheUtils.buildPartColKey(normalizeIdentifier(newPart.getCatName()),
                normalizeIdentifier(newPart.getDbName()),
                normalizeIdentifier(newPart.getTableName()), newPart.getValues(),
                (String) decomposedKey[3]);
        newPartitionColStats.put(newKey, colStatObj);
        iterator.remove();
      }
    }
    partitionColStatsCache.putAll(newPartitionColStats);
  }

  public synchronized void updatePartitionColStatsInCache(String catName, String dbName, String tableName,
      List<String> partVals, List<ColumnStatisticsObj> colStatsObjs) {
    for (ColumnStatisticsObj colStatObj : colStatsObjs) {
      // Get old stats object if present
      String key = CacheUtils.buildPartColKey(catName, dbName, tableName, partVals, colStatObj.getColName());
      ColumnStatisticsObj oldStatsObj = partitionColStatsCache.get(key);
      if (oldStatsObj != null) {
        // Update existing stat object's field
        LOG.debug("CachedStore: updating partition column stats for column: "
            + colStatObj.getColName() + ", of table: " + tableName + " and database: " + dbName);
        StatObjectConverter.setFieldsIntoOldStats(oldStatsObj, colStatObj);
      } else {
        // No stats exist for this key; add a new object to the cache
        partitionColStatsCache.put(key, colStatObj);
      }
    }
  }

  public synchronized int getCachedPartitionCount() {
    return partitionCache.size();
  }

  public synchronized ColumnStatisticsObj getCachedPartitionColStats(String key) {
    return partitionColStatsCache.get(key)!=null?partitionColStatsCache.get(key).deepCopy():null;
  }

  public synchronized void addPartitionColStatsToCache(
      List<ColStatsObjWithSourceInfo> colStatsForDB) {
    for (ColStatsObjWithSourceInfo colStatWithSourceInfo : colStatsForDB) {
      List<String> partVals;
      try {
        partVals = Warehouse.getPartValuesFromPartName(colStatWithSourceInfo.getPartName());
        ColumnStatisticsObj colStatObj = colStatWithSourceInfo.getColStatsObj();
        // TODO CAT
        String key = CacheUtils.buildPartColKey(Warehouse.DEFAULT_CATALOG_NAME,
            colStatWithSourceInfo.getDbName(),
            colStatWithSourceInfo.getTblName(), partVals, colStatObj.getColName());
        partitionColStatsCache.put(key, colStatObj);
      } catch (MetaException e) {
        LOG.info("Unable to add partition stats for: {} to SharedCache",
            colStatWithSourceInfo.getPartName(), e);
      }
    }

  }

  public synchronized void refreshPartitionColStats(String catName, String dbName,
                                                    List<ColStatsObjWithSourceInfo> colStatsForDB) {
    LOG.debug("CachedStore: updating cached partition column stats objects for database: {}",
        dbName);
    removePartitionColStatsFromCache(catName, dbName);
    addPartitionColStatsToCache(colStatsForDB);
  }

  public synchronized void addAggregateStatsToCache(String catName, String dbName, String tblName,
      AggrStats aggrStatsAllPartitions, AggrStats aggrStatsAllButDefaultPartition) {
    if (aggrStatsAllPartitions != null) {
      for (ColumnStatisticsObj colStatObj : aggrStatsAllPartitions.getColStats()) {
        String key = CacheUtils.buildTableColKey(catName, dbName, tblName, colStatObj.getColName());
        List<ColumnStatisticsObj> value = new ArrayList<ColumnStatisticsObj>();
        value.add(StatsType.ALL.getPosition(), colStatObj);
        aggrColStatsCache.put(key, value);
      }
    }
    if (aggrStatsAllButDefaultPartition != null) {
      for (ColumnStatisticsObj colStatObj : aggrStatsAllButDefaultPartition.getColStats()) {
        String key = CacheUtils.buildTableColKey(catName, dbName, tblName, colStatObj.getColName());
        List<ColumnStatisticsObj> value = aggrColStatsCache.get(key);
        if ((value != null) && (value.size() > 0)) {
          value.add(StatsType.ALLBUTDEFAULT.getPosition(), colStatObj);
        }
      }
    }
  }

  public List<ColumnStatisticsObj> getAggrStatsFromCache(String catName, String dbName, String tblName,
      List<String> colNames, StatsType statsType) {
    List<ColumnStatisticsObj> colStats = new ArrayList<ColumnStatisticsObj>();
    for (String colName : colNames) {
      String key = CacheUtils.buildTableColKey(catName, dbName, tblName, colName);
      List<ColumnStatisticsObj> colStatList = aggrColStatsCache.get(key);
      // If unable to find stats for a column, return null so we can build stats
      if (colStatList == null) {
        return null;
      }
      ColumnStatisticsObj colStatObj = colStatList.get(statsType.getPosition());
      // If unable to find stats for this StatsType, return null so we can build
      // stats
      if (colStatObj == null) {
        return null;
      }
      colStats.add(colStatObj);
    }
    return colStats;
  }

  public synchronized void removeAggrPartitionColStatsFromCache(String catName, String dbName,
                                                                String tblName) {
    String partialKey = CacheUtils.buildTableKeyWithDelimit(catName, dbName, tblName);
    Iterator<Entry<String, List<ColumnStatisticsObj>>> iterator =
        aggrColStatsCache.entrySet().iterator();
    while (iterator.hasNext()) {
      Entry<String, List<ColumnStatisticsObj>> entry = iterator.next();
      String key = entry.getKey();
      if (key.toLowerCase().startsWith(partialKey.toLowerCase())) {
        iterator.remove();
      }
    }
  }

  public synchronized void refreshAggregateStatsCache(String catName, String dbName, String tblName,
      AggrStats aggrStatsAllPartitions, AggrStats aggrStatsAllButDefaultPartition) {
    LOG.debug("CachedStore: updating aggregate stats cache for : {}",
        Warehouse.getCatalogQualifiedTableName(catName, dbName, tblName));
    removeAggrPartitionColStatsFromCache(catName, dbName, tblName);
    addAggregateStatsToCache(catName, dbName, tblName, aggrStatsAllPartitions,
        aggrStatsAllButDefaultPartition);
  }

  public synchronized void addTableColStatsToCache(String catName, String dbName, String tableName,
      List<ColumnStatisticsObj> colStatsForTable) {
    for (ColumnStatisticsObj colStatObj : colStatsForTable) {
      String key = CacheUtils.buildTableColKey(catName, dbName, tableName, colStatObj.getColName());
      tableColStatsCache.put(key, colStatObj);
    }
  }

  public synchronized void refreshTableColStats(String catName, String dbName, String tableName,
      List<ColumnStatisticsObj> colStatsForTable) {
    LOG.debug("CachedStore: updating cached table column stats objects for table: " +
        Warehouse.getCatalogQualifiedTableName(catName, dbName, tableName));
    // Remove all old cache entries for this table
    removeTableColStatsFromCache(catName, dbName, tableName);
    // Add new entries to cache
    addTableColStatsToCache(catName, dbName, tableName, colStatsForTable);
  }

  private void increSd(StorageDescriptor sd, byte[] sdHash) {
    ByteArrayWrapper byteArray = new ByteArrayWrapper(sdHash);
    if (sdCache.containsKey(byteArray)) {
      sdCache.get(byteArray).refCount++;
    } else {
      StorageDescriptor sdToCache = sd.deepCopy();
      sdToCache.setLocation(null);
      sdToCache.setParameters(null);
      sdCache.put(byteArray, new StorageDescriptorWrapper(sdToCache, 1));
    }
  }

  private void decrSd(byte[] sdHash) {
    ByteArrayWrapper byteArray = new ByteArrayWrapper(sdHash);
    StorageDescriptorWrapper sdWrapper = sdCache.get(byteArray);
    sdWrapper.refCount--;
    if (sdWrapper.getRefCount() == 0) {
      sdCache.remove(byteArray);
    }
  }

  public StorageDescriptor getSdFromCache(byte[] sdHash) {
    StorageDescriptorWrapper sdWrapper = sdCache.get(new ByteArrayWrapper(sdHash));
    return sdWrapper.getSd();
  }

  // Replace databases in databaseCache with the new list
  public synchronized void refreshDatabases(List<Database> databases) {
    LOG.debug("CachedStore: updating cached database objects");
    databaseCache.clear();
    for (Database db : databases) {
      addDatabaseToCache(db.getName(), db);
    }
  }

  // Replace tables in tableCache with the new list
  public synchronized void refreshTables(String catName, String dbName, List<Table> tables) {
    LOG.debug("CachedStore: updating cached table objects for database: " + dbName);
    for (Table tbl : listCachedTables(catName, dbName)) {
      removeTableFromCache(catName, dbName, tbl.getTableName());
    }
    for (Table tbl : tables) {
      addTableToCache(catName, dbName, tbl.getTableName(), tbl);
    }
  }

  public synchronized void refreshPartitions(String catName, String dbName, String tblName,
                                             List<Partition> partitions) {
    LOG.debug("CachedStore: updating cached partition objects for database: " + dbName
        + " and table: " + tblName);
    Iterator<Entry<String, PartitionWrapper>> iterator = partitionCache.entrySet().iterator();
    while (iterator.hasNext()) {
      PartitionWrapper partitionWrapper = iterator.next().getValue();
      if (partitionWrapper.getPartition().getDbName().equals(dbName)
          && partitionWrapper.getPartition().getTableName().equals(tblName)) {
        iterator.remove();
      }
    }
    for (Partition part : partitions) {
      addPartitionToCache(catName, dbName, tblName, part);
    }
  }

  @VisibleForTesting
  Map<String, Catalog> getCatalogCache() {
    return catalogCache;
  }

  @VisibleForTesting
  Map<String, Database> getDatabaseCache() {
    return databaseCache;
  }

  @VisibleForTesting
  Map<String, TableWrapper> getTableCache() {
    return tableCache;
  }

  @VisibleForTesting
  Map<String, PartitionWrapper> getPartitionCache() {
    return partitionCache;
  }

  @VisibleForTesting
  Map<ByteArrayWrapper, StorageDescriptorWrapper> getSdCache() {
    return sdCache;
  }

  @VisibleForTesting
  Map<String, ColumnStatisticsObj> getPartitionColStatsCache() {
    return partitionColStatsCache;
  }
}
