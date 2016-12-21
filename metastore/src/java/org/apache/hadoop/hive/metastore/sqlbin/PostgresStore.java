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

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.metastore.*;
import org.apache.hadoop.hive.metastore.api.*;
import org.apache.hadoop.hive.metastore.hbase.HBaseStore;
import org.apache.hadoop.hive.metastore.parser.ExpressionTree;
import org.apache.hadoop.hive.metastore.partition.spec.PartitionSpecProxy;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class PostgresStore implements RawStore {
  static final private Logger LOG = LoggerFactory.getLogger(PostgresStore.class.getName());

  private Configuration conf;
  private int txnDepth;
  private PostgresKeyValue pgres; // Do not use this directly, call getPostgres()
  private PartitionExpressionProxy expressionProxy; // Do not use directly, call getExpressionProxy()

  @Override
  public void shutdown() {
    throw new UnsupportedOperationException();

  }

  @Override
  public boolean openTransaction() {
    try {
      if (txnDepth++ == 0) getPostgres().begin();
      return true;
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public boolean commitTransaction() {
    try {
      if (--txnDepth < 1) getPostgres().commit();
      return true;
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void rollbackTransaction() {
    try {
      getPostgres().rollback();
      txnDepth = 0;
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  // Begin for read.  This way we don't have to open a transaction and later close it.
  private void beginRead() {
    // Don't increment the transaction counter, as we don't expect a commit/rollback if a read fails.
    try {
      getPostgres().begin();
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void createDatabase(Database db) throws InvalidObjectException, MetaException {
    boolean commit = false;
    openTransaction();
    try {
      // HiveMetaStore already checks for existence of the database, don't recheck
      getPostgres().putDb(db);
      commit = true;
    } catch (SQLException e) {
      LOG.error("Unable to create database ", e);
      throw new MetaException("Unable to read from or write to postgres " + e.getMessage());
    } finally {
      commitOrRoleBack(commit);
    }

  }

  @Override
  public Database getDatabase(String name) throws NoSuchObjectException {
    try {
      beginRead();
      Database db = getPostgres().getDb(name);
      if (db == null) {
        throw new NoSuchObjectException("Unable to find db " + name);
      }
      return db;
    } catch (SQLException e) {
      LOG.error("Unable to get db", e);
      throw new NoSuchObjectException("Error reading db " + e.getMessage());
    }
  }

  @Override
  public boolean dropDatabase(String dbname) throws NoSuchObjectException, MetaException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean alterDatabase(String dbname, Database db) throws NoSuchObjectException,
      MetaException {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<String> getDatabases(String pattern) throws MetaException {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<String> getAllDatabases() throws MetaException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean createType(Type type) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Type getType(String typeName) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean dropType(String typeName) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void createTable(Table tbl) throws InvalidObjectException, MetaException {
    boolean commit = false;
    openTransaction();
    // HiveMetaStore above us checks if the table already exists, so we can blindly store it here.
    try {
      getPostgres().putTable(tbl);
      commit = true;
    } catch (SQLException e) {
      LOG.error("Unable to create table ", e);
      throw new MetaException("Unable to read from or write to postgres " + e.getMessage());
    } finally {
      commitOrRoleBack(commit);
    }
  }

  @Override
  public boolean dropTable(String dbName, String tableName) throws MetaException,
      NoSuchObjectException, InvalidObjectException, InvalidInputException {
    throw new UnsupportedOperationException();
  }

  @Override
  public Table getTable(String dbName, String tableName) throws MetaException {
    try {
      beginRead();
      Table table = getPostgres().getTable(dbName, tableName);
      if (table == null) {
        LOG.debug("Unable to find table " + tableNameForErrorMsg(dbName, tableName));
      }
      return table;
    } catch (SQLException e) {
      LOG.error("Unable to get table", e);
      throw new MetaException("Error reading table " + e.getMessage());
    }
  }

  @Override
  public boolean addPartition(Partition part) throws InvalidObjectException, MetaException {
    boolean commit = false;
    openTransaction();
    try {
      getPostgres().putPartition(part);
      commit = true;
      return true;
    } catch (SQLException e) {
      LOG.error("Unable to add partition", e);
      throw new MetaException("Unable to read from or write to hbase " + e.getMessage());
    } finally {
      commitOrRoleBack(commit);
    }
  }

  @Override
  public boolean addPartitions(String dbName, String tblName, List<Partition> parts) throws
      InvalidObjectException, MetaException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean addPartitions(String dbName, String tblName, PartitionSpecProxy partitionSpec,
                               boolean ifNotExists) throws InvalidObjectException, MetaException {
    throw new UnsupportedOperationException();
  }

  @Override
  public Partition getPartition(String dbName, String tableName, List<String> part_vals) throws
      MetaException, NoSuchObjectException {
    try {
      beginRead();
      Partition part = getPostgres().getPartition(dbName, tableName, part_vals);
      if (part == null) {
        throw new NoSuchObjectException("Unable to find partition " +
            HBaseStore.partNameForErrorMsg(dbName, tableName, part_vals));
      }
      return part;
    } catch (SQLException e) {
      LOG.error("Unable to get partition", e);
      throw new MetaException("Error reading partition " + e.getMessage());
    }
  }

  @Override
  public boolean doesPartitionExist(String dbName, String tableName, List<String> part_vals) throws
      MetaException, NoSuchObjectException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean dropPartition(String dbName, String tableName, List<String> part_vals) throws
      MetaException, NoSuchObjectException, InvalidObjectException, InvalidInputException {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<Partition> getPartitions(String dbName, String tableName, int max) throws
      MetaException, NoSuchObjectException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void alterTable(String dbname, String name, Table newTable) throws InvalidObjectException,
      MetaException {
    throw new UnsupportedOperationException();

  }

  @Override
  public List<String> getTables(String dbName, String pattern) throws MetaException {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<String> getTables(String dbName, String pattern, TableType tableType) throws
      MetaException {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<TableMeta> getTableMeta(String dbNames, String tableNames,
                                      List<String> tableTypes) throws MetaException {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<Table> getTableObjectsByName(String dbname, List<String> tableNames) throws
      MetaException, UnknownDBException {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<String> getAllTables(String dbName) throws MetaException {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<String> listTableNamesByFilter(String dbName, String filter, short max_tables) throws
      MetaException, UnknownDBException {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<String> listPartitionNames(String db_name, String tbl_name, short max_parts) throws
      MetaException {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<String> listPartitionNamesByFilter(String db_name, String tbl_name, String filter,
                                                 short max_parts) throws MetaException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void alterPartition(String db_name, String tbl_name, List<String> part_vals,
                             Partition new_part) throws InvalidObjectException, MetaException {
    throw new UnsupportedOperationException();

  }

  @Override
  public void alterPartitions(String db_name, String tbl_name, List<List<String>> part_vals_list,
                              List<Partition> new_parts) throws InvalidObjectException,
      MetaException {
    throw new UnsupportedOperationException();

  }

  @Override
  public boolean addIndex(Index index) throws InvalidObjectException, MetaException {
    throw new UnsupportedOperationException();
  }

  @Override
  public Index getIndex(String dbName, String origTableName, String indexName) throws
      MetaException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean dropIndex(String dbName, String origTableName, String indexName) throws
      MetaException {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<Index> getIndexes(String dbName, String origTableName, int max) throws MetaException {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<String> listIndexNames(String dbName, String origTableName, short max) throws
      MetaException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void alterIndex(String dbname, String baseTblName, String name, Index newIndex) throws
      InvalidObjectException, MetaException {
    throw new UnsupportedOperationException();

  }

  @Override
  public List<Partition> getPartitionsByFilter(String dbName, String tblName, String filter,
                                               short maxParts) throws MetaException,
      NoSuchObjectException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean getPartitionsByExpr(String dbName, String tblName, byte[] expr,
                                     String defaultPartitionName, short maxParts,
                                     List<Partition> result) throws TException {
    try {
      beginRead();
      ExpressionTree exprTree = PartFilterExprUtil.makeExpressionTree(getExpressionProxy(), expr);
      if (exprTree == null) {
        LOG.warn("Failed to unparse expression tree, falling back to client side partition selection");

        result.addAll(getPostgres().scanPartitionsInTable(dbName, tblName, maxParts));
        return true;
      }
      return getPostgres().scanPartitionsByExpr(dbName, tblName, exprTree, maxParts, result);
    } catch (SQLException e) {
      String msg = "Failed to get expressions by expression: " + e.getMessage();
      LOG.error(msg);
      throw new MetaException(msg);
    }
  }

  @Override
  public int getNumPartitionsByFilter(String dbName, String tblName, String filter) throws
      MetaException, NoSuchObjectException {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getNumPartitionsByExpr(String dbName, String tblName, byte[] expr) throws
      MetaException, NoSuchObjectException {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<Partition> getPartitionsByNames(String dbName, String tblName,
                                              List<String> partNames) throws MetaException,
      NoSuchObjectException {
    throw new UnsupportedOperationException();
  }

  @Override
  public Table markPartitionForEvent(String dbName, String tblName, Map<String, String> partVals,
                                     PartitionEventType evtType) throws MetaException,
      UnknownTableException, InvalidPartitionException, UnknownPartitionException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isPartitionMarkedForEvent(String dbName, String tblName,
                                           Map<String, String> partName,
                                           PartitionEventType evtType) throws MetaException,
      UnknownTableException, InvalidPartitionException, UnknownPartitionException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean addRole(String rowName, String ownerName) throws InvalidObjectException,
      MetaException, NoSuchObjectException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean removeRole(String roleName) throws MetaException, NoSuchObjectException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean grantRole(Role role, String userName, PrincipalType principalType, String grantor,
                           PrincipalType grantorType, boolean grantOption) throws MetaException,
      NoSuchObjectException, InvalidObjectException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean revokeRole(Role role, String userName, PrincipalType principalType,
                            boolean grantOption) throws MetaException, NoSuchObjectException {
    throw new UnsupportedOperationException();
  }

  @Override
  public PrincipalPrivilegeSet getUserPrivilegeSet(String userName, List<String> groupNames) throws
      InvalidObjectException, MetaException {
    throw new UnsupportedOperationException();
  }

  @Override
  public PrincipalPrivilegeSet getDBPrivilegeSet(String dbName, String userName,
                                                 List<String> groupNames) throws
      InvalidObjectException, MetaException {
    throw new UnsupportedOperationException();
  }

  @Override
  public PrincipalPrivilegeSet getTablePrivilegeSet(String dbName, String tableName,
                                                    String userName, List<String> groupNames) throws
      InvalidObjectException, MetaException {
    throw new UnsupportedOperationException();
  }

  @Override
  public PrincipalPrivilegeSet getPartitionPrivilegeSet(String dbName, String tableName,
                                                        String partition, String userName,
                                                        List<String> groupNames) throws
      InvalidObjectException, MetaException {
    throw new UnsupportedOperationException();
  }

  @Override
  public PrincipalPrivilegeSet getColumnPrivilegeSet(String dbName, String tableName,
                                                     String partitionName, String columnName,
                                                     String userName,
                                                     List<String> groupNames) throws
      InvalidObjectException, MetaException {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<HiveObjectPrivilege> listPrincipalGlobalGrants(String principalName,
                                                             PrincipalType principalType) {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<HiveObjectPrivilege> listPrincipalDBGrants(String principalName,
                                                         PrincipalType principalType,
                                                         String dbName) {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<HiveObjectPrivilege> listAllTableGrants(String principalName,
                                                      PrincipalType principalType, String dbName,
                                                      String tableName) {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<HiveObjectPrivilege> listPrincipalPartitionGrants(String principalName,
                                                                PrincipalType principalType,
                                                                String dbName, String tableName,
                                                                List<String> partValues,
                                                                String partName) {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<HiveObjectPrivilege> listPrincipalTableColumnGrants(String principalName,
                                                                  PrincipalType principalType,
                                                                  String dbName, String tableName,
                                                                  String columnName) {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<HiveObjectPrivilege> listPrincipalPartitionColumnGrants(String principalName,
                                                                      PrincipalType principalType,
                                                                      String dbName,
                                                                      String tableName,
                                                                      List<String> partValues,
                                                                      String partName,
                                                                      String columnName) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean grantPrivileges(PrivilegeBag privileges) throws InvalidObjectException,
      MetaException, NoSuchObjectException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean revokePrivileges(PrivilegeBag privileges, boolean grantOption) throws
      InvalidObjectException, MetaException, NoSuchObjectException {
    throw new UnsupportedOperationException();
  }

  @Override
  public Role getRole(String roleName) throws NoSuchObjectException {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<String> listRoleNames() {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<Role> listRoles(String principalName, PrincipalType principalType) {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<RolePrincipalGrant> listRolesWithGrants(String principalName,
                                                      PrincipalType principalType) {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<RolePrincipalGrant> listRoleMembers(String roleName) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Partition getPartitionWithAuth(String dbName, String tblName, List<String> partVals,
                                        String user_name, List<String> group_names) throws
      MetaException, NoSuchObjectException, InvalidObjectException {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<Partition> getPartitionsWithAuth(String dbName, String tblName, short maxParts,
                                               String userName, List<String> groupNames) throws
      MetaException, NoSuchObjectException, InvalidObjectException {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<String> listPartitionNamesPs(String db_name, String tbl_name, List<String> part_vals,
                                           short max_parts) throws MetaException,
      NoSuchObjectException {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<Partition> listPartitionsPsWithAuth(String db_name, String tbl_name,
                                                  List<String> part_vals, short max_parts,
                                                  String userName, List<String> groupNames) throws
      MetaException, InvalidObjectException, NoSuchObjectException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean updateTableColumnStatistics(ColumnStatistics colStats) throws
      NoSuchObjectException, MetaException, InvalidObjectException, InvalidInputException {
    boolean commit = false;
    openTransaction();
    try {
      //update table properties
      List<ColumnStatisticsObj> statsObjs = colStats.getStatsObj();
      List<String> colNames = new ArrayList<>();
      for (ColumnStatisticsObj statsObj:statsObjs) {
        colNames.add(statsObj.getColName());
      }
      String dbName = colStats.getStatsDesc().getDbName();
      String tableName = colStats.getStatsDesc().getTableName();
      Table newTable = getTable(dbName, tableName);
      Table newTableCopy = newTable.deepCopy();
      StatsSetupConst.setColumnStatsState(newTableCopy.getParameters(), colNames);
      getPostgres().putTable(newTableCopy);

      getPostgres().updateTableStatistics(colStats.getStatsDesc().getDbName(),
          colStats.getStatsDesc().getTableName(), colStats);

      commit = true;
      return true;
    } catch (SQLException e) {
      LOG.error("Unable to update column statistics", e);
      throw new MetaException("Failed to update column statistics, " + e.getMessage());
    } finally {
      commitOrRoleBack(commit);
    }
  }

  @Override
  public boolean updatePartitionColumnStatistics(ColumnStatistics colStats,
                                                 List<String> partVals) throws
      NoSuchObjectException, MetaException, InvalidObjectException, InvalidInputException {
    boolean commit = false;
    openTransaction();
    try {
      // update partition properties
      String db_name = colStats.getStatsDesc().getDbName();
      String tbl_name = colStats.getStatsDesc().getTableName();
      Partition oldPart = getPostgres().getPartition(db_name, tbl_name, partVals);
      Partition new_partCopy = oldPart.deepCopy();
      List<String> colNames = new ArrayList<>();
      List<ColumnStatisticsObj> statsObjs = colStats.getStatsObj();
      for (ColumnStatisticsObj statsObj : statsObjs) {
        colNames.add(statsObj.getColName());
      }
      StatsSetupConst.setColumnStatsState(new_partCopy.getParameters(), colNames);
      getPostgres().putPartition(new_partCopy);

      getPostgres().updatePartitionStatistics(colStats.getStatsDesc().getDbName(),
          colStats.getStatsDesc().getTableName(), partVals, colStats);
      // We need to invalidate aggregates that include this partition
      // TODO we should be able to do this from inside updatePartitionStatistics
      /*
      getPostgres().getStatsCache().invalidate(colStats.getStatsDesc().getDbName(),
          colStats.getStatsDesc().getTableName(), colStats.getStatsDesc().getPartName());
          */

      commit = true;
      return true;
    } catch (SQLException e) {
      LOG.error("Unable to update column statistics", e);
      throw new MetaException("Failed to update column statistics, " + e.getMessage());
    } finally {
      commitOrRoleBack(commit);
    }
  }

  @Override
  public ColumnStatistics getTableColumnStatistics(String dbName, String tableName,
                                                   List<String> colName)
      throws MetaException, NoSuchObjectException {
    beginRead();
    try {
      return getPostgres().getTableStatistics(dbName, tableName);
    } catch (SQLException e) {
      LOG.error("Unable to fetch column statistics", e);
      throw new MetaException("Failed to fetch column statistics, " + e.getMessage());
    }
  }

  @Override
  public List<ColumnStatistics> getPartitionColumnStatistics(String dbName, String tblName,
                                                             List<String> partNames,
                                                             List<String> colNames)
      throws MetaException, NoSuchObjectException {
    beginRead();
    List<List<String>> partVals = new ArrayList<>(partNames.size());
    for (String partName : partNames) {
      partVals.add(HBaseStore.partNameToVals(partName));
    }
    try {
      return getPostgres().getPartitionStatistics(dbName, tblName, partVals);
    } catch (SQLException e) {
      LOG.error("Unable to fetch column statistics", e);
      throw new MetaException("Failed fetching column statistics, " + e.getMessage());
    }
  }

  @Override
  public boolean deletePartitionColumnStatistics(String dbName, String tableName, String partName,
                                                 List<String> partVals, String colName) throws
      NoSuchObjectException, MetaException, InvalidObjectException, InvalidInputException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean deleteTableColumnStatistics(String dbName, String tableName, String colName) throws
      NoSuchObjectException, MetaException, InvalidObjectException, InvalidInputException {
    throw new UnsupportedOperationException();
  }

  @Override
  public long cleanupEvents() {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean addToken(String tokenIdentifier, String delegationToken) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean removeToken(String tokenIdentifier) {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getToken(String tokenIdentifier) {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<String> getAllTokenIdentifiers() {
    throw new UnsupportedOperationException();
  }

  @Override
  public int addMasterKey(String key) throws MetaException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateMasterKey(Integer seqNo, String key) throws NoSuchObjectException,
      MetaException {
    throw new UnsupportedOperationException();

  }

  @Override
  public boolean removeMasterKey(Integer keySeq) {
    throw new UnsupportedOperationException();
  }

  @Override
  public String[] getMasterKeys() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void verifySchema() throws MetaException {
    throw new UnsupportedOperationException();

  }

  @Override
  public String getMetaStoreSchemaVersion() throws MetaException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setMetaStoreSchemaVersion(String version, String comment) throws MetaException {
    throw new UnsupportedOperationException();

  }

  @Override
  public void dropPartitions(String dbName, String tblName, List<String> partNames) throws
      MetaException, NoSuchObjectException {
    throw new UnsupportedOperationException();

  }

  @Override
  public List<HiveObjectPrivilege> listPrincipalDBGrantsAll(String principalName,
                                                            PrincipalType principalType) {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<HiveObjectPrivilege> listPrincipalTableGrantsAll(String principalName,
                                                               PrincipalType principalType) {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<HiveObjectPrivilege> listPrincipalPartitionGrantsAll(String principalName,
                                                                   PrincipalType principalType) {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<HiveObjectPrivilege> listPrincipalTableColumnGrantsAll(String principalName,
                                                                     PrincipalType principalType) {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<HiveObjectPrivilege> listPrincipalPartitionColumnGrantsAll(String principalName,
                                                                         PrincipalType principalType) {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<HiveObjectPrivilege> listGlobalGrantsAll() {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<HiveObjectPrivilege> listDBGrantsAll(String dbName) {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<HiveObjectPrivilege> listPartitionColumnGrantsAll(String dbName, String tableName,
                                                                String partitionName,
                                                                String columnName) {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<HiveObjectPrivilege> listTableGrantsAll(String dbName, String tableName) {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<HiveObjectPrivilege> listPartitionGrantsAll(String dbName, String tableName,
                                                          String partitionName) {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<HiveObjectPrivilege> listTableColumnGrantsAll(String dbName, String tableName,
                                                            String columnName) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void createFunction(Function func) throws InvalidObjectException, MetaException {
    boolean commit = false;
    openTransaction();
    try {
      getPostgres().putFunction(func);
      commit = true;
    } catch (SQLException e) {
      LOG.error("Unable to create function", e);
      throw new MetaException("Unable to read from or write to postgres " + e.getMessage());
    } finally {
      commitOrRoleBack(commit);
    }

  }

  @Override
  public void alterFunction(String dbName, String funcName, Function newFunction) throws
      InvalidObjectException, MetaException {
    throw new UnsupportedOperationException();

  }

  @Override
  public void dropFunction(String dbName, String funcName) throws MetaException,
      NoSuchObjectException, InvalidObjectException, InvalidInputException {
    throw new UnsupportedOperationException();

  }

  @Override
  public Function getFunction(String dbName, String funcName) throws MetaException {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<Function> getAllFunctions() throws MetaException {
    boolean commit = false;
    openTransaction();
    try {
      List<Function> funcs = getPostgres().scanFunctions(null, null);
      commit = true;
      return funcs;
    } catch (SQLException e) {
      LOG.error("Unable to get functions" + e);
      throw new MetaException("Unable to read from or write to postgres " + e.getMessage());
    } finally {
      commitOrRoleBack(commit);
    }
  }

  @Override
  public List<String> getFunctions(String dbName, String pattern) throws MetaException {
    boolean commit = false;
    openTransaction();
    try {
      List<Function> funcs = getPostgres().scanFunctions(dbName, HBaseStore.likeToRegex(pattern));
      List<String> funcNames = new ArrayList<>(funcs.size());
      for (Function func : funcs) funcNames.add(func.getFunctionName());
      commit = true;
      return funcNames;
    } catch (SQLException e) {
      LOG.error("Unable to get functions" + e);
      throw new MetaException("Unable to read from or write to postgres " + e.getMessage());
    } finally {
      commitOrRoleBack(commit);
    }
  }

  @Override
  public AggrStats get_aggr_stats_for(String dbName, String tblName, List<String> partNames,
                                      List<String> colNames) throws MetaException,
      NoSuchObjectException {
    throw new UnsupportedOperationException();
  }

  @Override
  public NotificationEventResponse getNextNotification(NotificationEventRequest rqst) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void addNotificationEvent(NotificationEvent event) {
    throw new UnsupportedOperationException();

  }

  @Override
  public void cleanNotificationEvents(int olderThan) {
    throw new UnsupportedOperationException();

  }

  @Override
  public CurrentNotificationEventId getCurrentNotificationEventId() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void flushCache() {
    throw new UnsupportedOperationException();

  }

  @Override
  public ByteBuffer[] getFileMetadata(List<Long> fileIds) throws MetaException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void putFileMetadata(List<Long> fileIds, List<ByteBuffer> metadata,
                              FileMetadataExprType type) throws MetaException {
    throw new UnsupportedOperationException();

  }

  @Override
  public boolean isFileMetadataSupported() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void getFileMetadataByExpr(List<Long> fileIds, FileMetadataExprType type, byte[] expr,
                                    ByteBuffer[] metadatas, ByteBuffer[] exprResults,
                                    boolean[] eliminated) throws MetaException {
    throw new UnsupportedOperationException();

  }

  @Override
  public FileMetadataHandler getFileMetadataHandler(FileMetadataExprType type) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getTableCount() throws MetaException {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getPartitionCount() throws MetaException {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getDatabaseCount() throws MetaException {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<SQLPrimaryKey> getPrimaryKeys(String db_name, String tbl_name) throws MetaException {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<SQLForeignKey> getForeignKeys(String parent_db_name, String parent_tbl_name,
                                            String foreign_db_name, String foreign_tbl_name) throws
      MetaException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void createTableWithConstraints(Table tbl, List<SQLPrimaryKey> primaryKeys,
                                         List<SQLForeignKey> foreignKeys) throws
      InvalidObjectException, MetaException {
    throw new UnsupportedOperationException();

  }

  @Override
  public void dropConstraint(String dbName, String tableName, String constraintName) throws
      NoSuchObjectException {
    throw new UnsupportedOperationException();

  }

  @Override
  public void addPrimaryKeys(List<SQLPrimaryKey> pks) throws InvalidObjectException, MetaException {
    throw new UnsupportedOperationException();

  }

  @Override
  public void addForeignKeys(List<SQLForeignKey> fks) throws InvalidObjectException, MetaException {
    throw new UnsupportedOperationException();

  }

  @Override
  public void setConf(Configuration configuration) {
    conf = configuration;

  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  private PostgresKeyValue getPostgres() {
    if (pgres == null) {
      pgres = new PostgresKeyValue();
      pgres.setConf(conf);
      try {
        pgres.begin();
      } catch (SQLException e) {
        throw new RuntimeException(e);
      }
    }
    return pgres;
  }

  private PartitionExpressionProxy getExpressionProxy() {
    if (expressionProxy == null) {
      expressionProxy = ObjectStore.createExpressionProxy(conf);
    }
    return expressionProxy;
  }

  private void commitOrRoleBack(boolean commit) {
    if (commit) {
      LOG.debug("Committing transaction");
      commitTransaction();
    } else {
      LOG.debug("Rolling back transaction");
      rollbackTransaction();
    }
  }

  private String tableNameForErrorMsg(String dbName, String tableName) {
    return dbName + "." + tableName;
  }

  @VisibleForTesting
  PostgresKeyValue connectionForTest() {
    return getPostgres();
  }
}
