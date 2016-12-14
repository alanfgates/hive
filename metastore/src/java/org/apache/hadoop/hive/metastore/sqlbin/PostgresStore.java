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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.FileMetadataHandler;
import org.apache.hadoop.hive.metastore.RawStore;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.AggrStats;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.CurrentNotificationEventId;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FileMetadataExprType;
import org.apache.hadoop.hive.metastore.api.Function;
import org.apache.hadoop.hive.metastore.api.HiveObjectPrivilege;
import org.apache.hadoop.hive.metastore.api.Index;
import org.apache.hadoop.hive.metastore.api.InvalidInputException;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.InvalidPartitionException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.NotificationEvent;
import org.apache.hadoop.hive.metastore.api.NotificationEventRequest;
import org.apache.hadoop.hive.metastore.api.NotificationEventResponse;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PartitionEventType;
import org.apache.hadoop.hive.metastore.api.PrincipalPrivilegeSet;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.PrivilegeBag;
import org.apache.hadoop.hive.metastore.api.Role;
import org.apache.hadoop.hive.metastore.api.RolePrincipalGrant;
import org.apache.hadoop.hive.metastore.api.SQLForeignKey;
import org.apache.hadoop.hive.metastore.api.SQLPrimaryKey;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.TableMeta;
import org.apache.hadoop.hive.metastore.api.Type;
import org.apache.hadoop.hive.metastore.api.UnknownDBException;
import org.apache.hadoop.hive.metastore.api.UnknownPartitionException;
import org.apache.hadoop.hive.metastore.api.UnknownTableException;
import org.apache.hadoop.hive.metastore.partition.spec.PartitionSpecProxy;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

public class PostgresStore implements RawStore {
  static final private Logger LOG = LoggerFactory.getLogger(PostgresStore.class.getName());

  private Configuration conf;
  private int txnDepth;
  private PostgresKeyValue pgres; // Do not use this directly, call getPostgres()

  @Override
  public void shutdown() {
    throw new UnsupportedOperationException();

  }

  @Override
  public boolean openTransaction() {
    if (txnDepth++ == 0) getPostgres().begin();
    return true;
  }

  @Override
  public boolean commitTransaction() {
    if (--txnDepth < 1) getPostgres().commit();
    return true;
  }

  @Override
  public void rollbackTransaction() {
    getPostgres().rollback();
    txnDepth = 0;
  }

  @Override
  public void createDatabase(Database db) throws InvalidObjectException, MetaException {
    throw new UnsupportedOperationException();

  }

  @Override
  public Database getDatabase(String name) throws NoSuchObjectException {
    throw new UnsupportedOperationException();
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
    throw new UnsupportedOperationException();

  }

  @Override
  public boolean dropTable(String dbName, String tableName) throws MetaException,
      NoSuchObjectException, InvalidObjectException, InvalidInputException {
    throw new UnsupportedOperationException();
  }

  @Override
  public Table getTable(String dbName, String tableName) throws MetaException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean addPartition(Partition part) throws InvalidObjectException, MetaException {
    throw new UnsupportedOperationException();
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
    throw new UnsupportedOperationException();
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
    throw new UnsupportedOperationException();
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
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean updatePartitionColumnStatistics(ColumnStatistics statsObj,
                                                 List<String> partVals) throws
      NoSuchObjectException, MetaException, InvalidObjectException, InvalidInputException {
    throw new UnsupportedOperationException();
  }

  @Override
  public ColumnStatistics getTableColumnStatistics(String dbName, String tableName,
                                                   List<String> colName) throws MetaException,
      NoSuchObjectException {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<ColumnStatistics> getPartitionColumnStatistics(String dbName, String tblName,
                                                             List<String> partNames,
                                                             List<String> colNames) throws
      MetaException, NoSuchObjectException {
    throw new UnsupportedOperationException();
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
    throw new UnsupportedOperationException();

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
    throw new UnsupportedOperationException();
  }

  @Override
  public List<String> getFunctions(String dbName, String pattern) throws MetaException {
    throw new UnsupportedOperationException();
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
    }
    return pgres;
  }
}
