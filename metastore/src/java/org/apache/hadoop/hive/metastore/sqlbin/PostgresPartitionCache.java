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

import com.google.common.base.Stopwatch;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.Cache;
import com.google.common.cache.Weigher;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.HiveStatsUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.AggrStats;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.hbase.stats.ColumnStatsAggregator;
import org.apache.hadoop.hive.metastore.hbase.stats.ColumnStatsAggregatorFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

/**
 * A cache shared across all threads that caches partitions.  Table entries come with a hash so
 * if partitions are added or dropped the wrong partition set will not be fetched.  This stores
 * both the partitions and the associated stats, but not aggregated stats.
 *
 * References to the caller (PostgresKeyValue) have to be passed in each call because we need to
 * make sure all fetches from here live in the same postgres transaction as the calls they are
 * servicing.
 *
 * This cache also contains any stats that have been aggregated over these sets of partitions.
 * It is convenient to keep them here because they can be dropped along with cached partition
 * values anytime the table changes.
 */
class PostgresPartitionCache {
  private static final Logger LOG = LoggerFactory.getLogger(PostgresPartitionCache.class.getName());

  private final Cache<CacheKey, CacheValue> cache;
  private final MessageDigest md;
  private final Configuration conf;

  PostgresPartitionCache(Configuration conf) {
    cache = CacheBuilder.<CacheKey, CacheValue>newBuilder()
        .weigher(new Weigher<CacheKey, CacheValue>() {
          @Override
          public int weigh(CacheKey key, CacheValue cacheValue) {
            return cacheValue.partMap.size() + cacheValue.aggregatedStats.size();
          }
        })
        .maximumWeight(1000000) // TODO make configurable
        .build();
    try {
      md = MessageDigest.getInstance("MD5");
    } catch (NoSuchAlgorithmException e) {
      LOG.error("Unable to instantiate MD5 hash", e);
      throw new RuntimeException(e);
    }
    this.conf = conf;
  }

  /**
   * Return all partition names.
   * @param dbName database table is in
   * @param tableName table to find partitions for
   * @return all partition names for this table
   */
  List<String> getPartitionNames(PostgresKeyValue postgres, String dbName, String tableName)
      throws SQLException {
    CacheValue cached = getLatest(postgres, dbName, tableName);
    return new ArrayList<>(cached.partMap.keySet());
  }

  List<Partition> fetchPartitions(PostgresKeyValue postgres, String dbName, String tableName,
                                  List<String> partNames)
      throws SQLException {
    CacheValue cached = getLatest(postgres, dbName, tableName);
    List<Partition> parts = new ArrayList<>(partNames.size());
    for (String partName : partNames) {
      CacheValueElement cve = cached.partMap.get(partName);
      if (cve != null) parts.add(cve.part.get());
    }
    return parts;
  }

  List<Partition> getAllpartitions(PostgresKeyValue postgres, String dbName, String tableName,
                                   int maxParts) throws SQLException {
    CacheValue cached  = getLatest(postgres, dbName, tableName);
    List<Partition> parts = new ArrayList<>(Math.max(cached.partMap.size(), maxParts));
    if (maxParts < 1) maxParts = Integer.MAX_VALUE;
    int num = 0;
    for (CacheValueElement cve : cached.partMap.values()) {
      parts.add(cve.part.get());
      if (num++ > maxParts) return parts;
    }
    return parts;
  }

  SeparableColumnStatistics getSinglePartitionStats(String dbName, String tableName,
                                                    String partName) throws SQLException {
    // Don't load the entire set of partitions for one partition.  Just grab it if it's already
    // there.
    CacheValue cached  = cache.getIfPresent(new CacheKey(dbName, tableName));
    if (cached == null) return null;
    CacheValueElement cve = cached.partMap.get(partName);
    return cve == null ? null : cve.stats;
  }

  AggrStats getAggregatedStats(PostgresKeyValue postgres, String dbName, String tableName,
                               List<String> partNames, List<String> colNames) throws SQLException {
    LOG.debug("Fetching aggregate stats for " + partNames.size() + " partitions");
    CacheValue cached = getLatest(postgres, dbName, tableName);
    byte[] cacheKey = getAggrStatsKey(partNames, colNames);
    AggrStats aggrStats = cached.aggregatedStats.get(cacheKey);
    if (aggrStats == null) {
      LOG.debug("Aggregated stats not found in the cache, aggregating stats");
      List<SeparableColumnStatistics> partStats = new ArrayList<>(partNames.size());
      for (String partName : partNames) {
        CacheValueElement cve = cached.partMap.get(partName);
        if (cve != null) partStats.add(cve.stats);
      }
      if (partStats.size() > 0) {
        try {
          aggrStats = new AggrStats();
          int numBitVectors = HiveStatsUtils.getNumBitVectorsForNDVEstimation(conf);
          boolean useDensityFunctionForNDVEstimation =
              HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVE_METASTORE_STATS_NDV_DENSITY_FUNCTION);

          for (String colName : colNames) {
            List<ColumnStatistics> colStatsForCol = new ArrayList<>(partStats.size());
            List<String> partNamesList = new ArrayList<>(partStats.size());
            for (SeparableColumnStatistics separable : partStats) {
              ColumnStatisticsObj cso = separable.getStatsForCol(colName);
              if (cso != null) {
                ColumnStatistics cs = new ColumnStatistics();
                partNamesList.add(separable.getStatsDesc().getPartName());
                cs.setStatsDesc(separable.getStatsDesc());
                cs.addToStatsObj(cso);
                colStatsForCol.add(cs);
              }
            }
            ColumnStatsAggregator aggregator = ColumnStatsAggregatorFactory.getColumnStatsAggregator(
                colStatsForCol.get(0).getStatsObj().get(0).getStatsData().getSetField(),
                numBitVectors,
                useDensityFunctionForNDVEstimation);
            ColumnStatisticsObj cso = aggregator.aggregate(colName, partNamesList, colStatsForCol);
            aggrStats.setPartsFound(Math.max(aggrStats.getPartsFound(), colStatsForCol.size()));
            aggrStats.addToColStats(cso);
          }
          cached.aggregatedStats.put(cacheKey, aggrStats);
        } catch (Exception e) {
          throw new SQLException(e);
        }
      }
    }
    return aggrStats;
  }

  private CacheValue getLatest(final PostgresKeyValue postgres, final String dbName,
                               final String tableName) throws SQLException {
    // Make sure we're on the right UUID
    try {
      LOG.debug("Checking to see if we have the latest version of " + dbName + "." + tableName);
      final UUID latestUUID = postgres.getCurrentUUID(dbName, tableName);
      assert latestUUID != null;
      CacheKey key = new CacheKey(dbName, tableName);
      CacheValue cached = cache.get(key, new Callable<CacheValue>() {
        @Override
        public CacheValue call() throws Exception {
          LOG.debug(dbName + "." + tableName + " not found in cache, fetching from DB");
          Map<String, CacheValueElement> parts = postgres.cachePartitions(dbName, tableName);
          return new CacheValue(parts, latestUUID);
        }
      });
      if (!cached.uuid.equals(latestUUID)) {
        LOG.debug("Found a new version of " + dbName + "." + tableName + " in the DB, fetching " +
            "partitions over again");
        Map<String, CacheValueElement> parts = postgres.cachePartitions(dbName, tableName);
        cached = new CacheValue(parts, latestUUID);
        cache.put(key, cached);
      }
      return cached;
    } catch (ExecutionException e) {
      LOG.error("Got an exception while loading the cache ", e);
      throw new SQLException(e);
    }
  }

  private byte[] getAggrStatsKey(List<String> partNames, List<String> colNames) {
    md.reset();
    SortedSet<String> sorted = new TreeSet<>(partNames);
    for (String partName : sorted) md.update(partName.getBytes());
    sorted = new TreeSet<>(colNames);
    for (String partName : sorted) md.update(partName.getBytes());
    return md.digest();
  }

  private static class CacheKey {
    final String dbName;
    final String tableName;

    public CacheKey(String dbName, String tableName) {
      this.dbName = dbName;
      this.tableName = tableName;
    }

    @Override
    public int hashCode() {
      return dbName.hashCode() * 31 + tableName.hashCode();
    }

    @Override
    public boolean equals(Object other) {
      if (other == null || !(other instanceof CacheKey)) return false;
      return tableName.equals(((CacheKey)other).tableName) && dbName.equals(((CacheKey)other).dbName);
    }
  }

  private static class CacheValue {
    final Map<String, CacheValueElement> partMap; // map of partName to part
    final UUID uuid;
    final Map<byte[], AggrStats> aggregatedStats;

    public CacheValue(Map<String, CacheValueElement> partMap, UUID uuid) {
      this.partMap = partMap;
      this.uuid = uuid;
      aggregatedStats = new HashMap<>();
    }
  }

  static class CacheValueElement {
    final MaybeSerialized<Partition> part;
    final SeparableColumnStatistics stats;

    public CacheValueElement(MaybeSerialized<Partition> part, SeparableColumnStatistics stats) {
      this.part = part;
      this.stats = stats;
    }
  }
}
