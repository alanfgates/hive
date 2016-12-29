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
import org.apache.hadoop.hive.metastore.FileMetadataHandler;
import org.apache.hadoop.hive.metastore.HiveMetaStore;
import org.apache.hadoop.hive.metastore.ObjectStore;
import org.apache.hadoop.hive.metastore.PartFilterExprUtil;
import org.apache.hadoop.hive.metastore.PartitionExpressionProxy;
import org.apache.hadoop.hive.metastore.RawStore;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.AggrStats;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
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
import org.apache.hadoop.hive.metastore.hbase.HBaseStore;
import org.apache.hadoop.hive.metastore.hbase.PrivilegeHelper;
import org.apache.hadoop.hive.metastore.hbase.RoleHelper;
import org.apache.hadoop.hive.metastore.parser.ExpressionTree;
import org.apache.hadoop.hive.metastore.partition.spec.PartitionSpecProxy;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
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
  private PrivilegeHelper privilegeHelper;
  private RoleHelper roleHelper;

  @Override
  public void shutdown() {
    if (txnDepth > 0) rollbackTransaction();
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
    boolean commit = false;
    openTransaction();
    try {
      Database db = getPostgres().getDb(name);
      if (db == null) {
        throw new NoSuchObjectException("Unable to find db " + name);
      }
      commit = true;
      return db;
    } catch (SQLException e) {
      LOG.error("Unable to get db", e);
      throw new NoSuchObjectException("Error reading db " + e.getMessage());
    } finally {
      commitOrRoleBack(commit);
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
    boolean commit = false;
    openTransaction();
    try {
      Table table = getPostgres().getTable(dbName, tableName);
      if (table == null) {
        LOG.debug("Unable to find table " + tableNameForErrorMsg(dbName, tableName));
      }
      commit = true;
      return table;
    } catch (SQLException e) {
      LOG.error("Unable to get table", e);
      throw new MetaException("Error reading table " + e.getMessage());
    } finally {
      commitOrRoleBack(commit);
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
      throw new MetaException("Unable to read from or write to Postgres " + e.getMessage());
    } finally {
      commitOrRoleBack(commit);
    }
  }

  @Override
  public boolean addPartitions(String dbName, String tblName, List<Partition> parts) throws
      InvalidObjectException, MetaException {
    boolean commit = false;
    openTransaction();
    try {
      for (Partition part : parts) getPostgres().putPartition(part);
      commit = true;
      return true;
    } catch (SQLException e) {
      LOG.error("Unable to add partition", e);
      throw new MetaException("Unable to read from or write to Postgres " + e.getMessage());
    } finally {
      commitOrRoleBack(commit);
    }
  }

  @Override
  public boolean addPartitions(String dbName, String tblName, PartitionSpecProxy partitionSpec,
                               boolean ifNotExists) throws InvalidObjectException, MetaException {
    throw new UnsupportedOperationException();
  }

  @Override
  public Partition getPartition(String dbName, String tableName, List<String> part_vals) throws
      MetaException, NoSuchObjectException {
    Partition part = getPartitionInternal(dbName, tableName, part_vals);
    if (part == null) {
      throw new NoSuchObjectException("Unable to find partition " +
          HBaseStore.partNameForErrorMsg(dbName, tableName, part_vals));
    }
    return part;
  }

  @Override
  public boolean doesPartitionExist(String dbName, String tableName, List<String> part_vals) throws
      MetaException, NoSuchObjectException {
    // Use the regular getPartition for this.  It will be slightly less efficient, but I'm
    // guessing that 9 times out of 10 if it exists they'll want to fetch it, and this call
    // will pull it into the cache.
    return getPartitionInternal(dbName, tableName, part_vals) != null;
  }

  private Partition getPartitionInternal(String dbName, String tableName, List<String> partVals)
      throws MetaException {
    boolean commit = false;
    openTransaction();
    try {
      Partition part = getPostgres().getPartition(dbName, tableName, partVals);
      commit = true;
      return part;
    } catch (SQLException e) {
      LOG.error("Unable to get partition", e);
      throw new MetaException("Error reading partition " + e.getMessage());
    } finally {
      commitOrRoleBack(commit);
    }
  }

  @Override
  public boolean dropPartition(String dbName, String tableName, List<String> part_vals) throws
      MetaException, NoSuchObjectException, InvalidObjectException, InvalidInputException {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<Partition> getPartitions(String dbName, String tableName, int max) throws
      MetaException, NoSuchObjectException {
    boolean commit = false;
    openTransaction();
    try {
      List<Partition> parts =
          getPostgres().scanPartitionsInTable(dbName, tableName, max, false);
      commit = true;
      return parts;
    } catch (SQLException e) {
      LOG.error("Unable to get partitions", e);
      throw new MetaException("Error scanning partitions");
    } finally {
      commitOrRoleBack(commit);
    }
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
    openTransaction();
    boolean commit = false;
    try {
      ExpressionTree exprTree = PartFilterExprUtil.makeExpressionTree(getExpressionProxy(), expr);
      if (exprTree == null) {
        LOG.warn("Failed to unparse expression tree, falling back to client side partition selection");

        result.addAll(getPostgres().scanPartitionsInTable(dbName, tblName, maxParts, true));
        return true;
      }
      boolean rc = getPostgres().scanPartitionsByExpr(dbName, tblName, exprTree, maxParts, result);
      commit = true;
      return rc;
    } catch (SQLException e) {
      String msg = "Failed to get expressions by expression: " + e.getMessage();
      LOG.error(msg);
      throw new MetaException(msg);
    } finally {
      commitOrRoleBack(commit);
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
  public boolean addRole(String roleName, String ownerName) throws InvalidObjectException,
      MetaException, NoSuchObjectException {
    boolean commit = false;
    openTransaction();
    try {
      getRoleHelper().addRole(roleName, ownerName);
      commit = true;
      return true;
    } catch (IOException e) {
      LOG.error("Unable to create role ", e);
      throw new MetaException("Unable to read from or write to Postgres " + e.getMessage());
    } finally {
      commitOrRoleBack(commit);
    }
  }

  @Override
  public boolean removeRole(String roleName) throws MetaException, NoSuchObjectException {
    boolean commit = false;
    openTransaction();
    try {
      getRoleHelper().removeRole(roleName);
      commit = true;
      return true;
    } catch (IOException e) {
      LOG.error("Unable to delete role" + e);
      throw new MetaException("Unable to drop role " + roleName);
    } finally {
      commitOrRoleBack(commit);
    }
  }

  @Override
  public boolean grantRole(Role role, String userName, PrincipalType principalType, String grantor,
                           PrincipalType grantorType, boolean grantOption) throws MetaException,
      NoSuchObjectException, InvalidObjectException {
    boolean commit = false;
    openTransaction();
    try {
      getRoleHelper().grantRole(role, userName, principalType, grantor, grantorType, grantOption);
      commit = true;
      return true;
    } catch (IOException e) {
      LOG.error("Unable to grant role", e);
      throw new MetaException("Unable to grant role " + e.getMessage());
    } finally {
      commitOrRoleBack(commit);
    }
  }

  @Override
  public boolean revokeRole(Role role, String userName, PrincipalType principalType,
                            boolean grantOption) throws MetaException, NoSuchObjectException {
    boolean commit = false;
    openTransaction();
    // This can have a couple of different meanings.  If grantOption is true, then this is only
    // revoking the grant option, the role itself doesn't need to be removed.  If it is false
    // then we need to remove the userName from the role altogether.
    try {
      getRoleHelper().revokeRole(role, userName, principalType, grantOption);
      commit = true;
      return true;
    } catch (IOException e) {
      LOG.error("Unable to revoke role " + role.getRoleName() + " from " + userName, e);
      throw new MetaException("Unable to revoke role " + e.getMessage());
    } finally {
      commitOrRoleBack(commit);
    }
  }

  private RoleHelper getRoleHelper() {
    if (roleHelper == null) {
      roleHelper = new RoleHelper(this, getPostgres());
    }
    return roleHelper;
  }

  @Override
  public PrincipalPrivilegeSet getUserPrivilegeSet(String userName, List<String> groupNames) throws
      InvalidObjectException, MetaException {
    boolean commit = false;
    openTransaction();
    try {
      PrincipalPrivilegeSet pps = getPrivilegeHelper().getUserPrivilegeSet(userName);
      commit = true;
      return pps;
    } catch (IOException e) {
      LOG.error("Unable to get db privileges for user", e);
      throw new MetaException("Unable to get db privileges for user, " + e.getMessage());
    } finally {
      commitOrRoleBack(commit);
    }
  }

  @Override
  public PrincipalPrivilegeSet getDBPrivilegeSet(String dbName, String userName,
                                                 List<String> groupNames) throws
      InvalidObjectException, MetaException {
    boolean commit = false;
    openTransaction();
    try {
      PrincipalPrivilegeSet pps = getPrivilegeHelper().getDBPrivilegeSet(dbName, userName);
      commit = true;
      return pps;
    } catch (IOException|NoSuchObjectException e) {
      LOG.error("Unable to get db privileges for user", e);
      throw new MetaException("Unable to get db privileges for user, " + e.getMessage());
    } finally {
      commitOrRoleBack(commit);
    }
  }

  @Override
  public PrincipalPrivilegeSet getTablePrivilegeSet(String dbName, String tableName,
                                                    String userName, List<String> groupNames) throws
      InvalidObjectException, MetaException {
    boolean commit = false;
    openTransaction();
    try {
      PrincipalPrivilegeSet pps =
          getPrivilegeHelper().getTablePrivilegeSet(dbName, tableName, userName);
      commit = true;
      return pps;
    } catch (IOException e) {
      LOG.error("Unable to get db privileges for user", e);
      throw new MetaException("Unable to get db privileges for user, " + e.getMessage());
    } finally {
      commitOrRoleBack(commit);
    }
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
    boolean commit = false;
    openTransaction();
    try {
      getPrivilegeHelper().grantPrivileges(privileges);
      commit = true;
      return true;
    } finally {
      commitOrRoleBack(commit);
    }
  }

  @Override
  public boolean revokePrivileges(PrivilegeBag privileges, boolean grantOption) throws
      InvalidObjectException, MetaException, NoSuchObjectException {
    boolean commit = false;
    openTransaction();
    try {
      getPrivilegeHelper().revokePrivileges(privileges, grantOption);
      commit = true;
      return true;
    } finally {
      commitOrRoleBack(commit);
    }
  }

  private PrivilegeHelper getPrivilegeHelper() {
    if (privilegeHelper == null) {
      privilegeHelper = new PrivilegeHelper(this, getPostgres());
    }
    return privilegeHelper;
  }

  @Override
  public Role getRole(String roleName) throws NoSuchObjectException {
    openTransaction();
    boolean commit = false;
    try {
      Role role = getPostgres().getRole(roleName);
      if (role == null) {
        throw new NoSuchObjectException("Unable to find role " + roleName);
      }
      commit = true;
      return role;
    } catch (IOException e) {
      LOG.error("Unable to get role", e);
      throw new NoSuchObjectException("Error reading table " + e.getMessage());
    } finally {
      commitOrRoleBack(commit);
    }
  }

  @Override
  public List<String> listRoleNames() {
    boolean commit = false;
    openTransaction();
    try {
      List<Role> roles = getPostgres().scanRoles();
      List<String> roleNames = new ArrayList<>(roles.size());
      for (Role role : roles) roleNames.add(role.getRoleName());
      commit = true;
      return roleNames;
    } catch (SQLException e) {
      throw new RuntimeException(e);
    } finally {
      commitOrRoleBack(commit);
    }
  }

  @Override
  public List<Role> listRoles(String principalName, PrincipalType principalType) {
    boolean commit = false;
    openTransaction();
    try {
      List<Role> roles = getRoleHelper().getPrincipalDirectRoles(principalName, principalType);
      // Add the public role if this is a user
      if (principalType == PrincipalType.USER) {
        roles.add(new Role(HiveMetaStore.PUBLIC, 0, null));
      }
      commit = true;
      return roles;
    } catch (IOException e) {
      throw new RuntimeException(e);
    } finally {
      commitOrRoleBack(commit);
    }
  }

  @Override
  public List<RolePrincipalGrant> listRolesWithGrants(String principalName,
                                                      PrincipalType principalType) {
    boolean commit = false;
    openTransaction();
    try {
      List<RolePrincipalGrant> rpgs =
          getRoleHelper().listRolesWithGrants(principalName, principalType);
      commit = true;
      return rpgs;
    } catch (IOException e) {
      throw new RuntimeException(e);
    } finally {
      commitOrRoleBack(commit);
    }
  }

  @Override
  public List<RolePrincipalGrant> listRoleMembers(String roleName) {
    boolean commit = false;
    openTransaction();
    try {
      List<RolePrincipalGrant> roleMaps = getRoleHelper().listRoleMembers(roleName);
      commit = true;
      return roleMaps;
    } catch (IOException e) {
      throw new RuntimeException(e);
    } finally {
      commitOrRoleBack(commit);
    }
  }

  @Override
  public Partition getPartitionWithAuth(String dbName, String tblName, List<String> partVals,
                                        String user_name, List<String> group_names) throws
      MetaException, NoSuchObjectException, InvalidObjectException {
    return getPartition(dbName, tblName, partVals);
  }

  @Override
  public List<Partition> getPartitionsWithAuth(String dbName, String tblName, short maxParts,
                                               String userName, List<String> groupNames) throws
      MetaException, NoSuchObjectException, InvalidObjectException {
    return getPartitions(dbName, tblName, maxParts);
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
    boolean commit = false;
    openTransaction();
    try {
      ColumnStatistics cs = getPostgres().getTableStatistics(dbName, tableName, colName);
      commit = true;
      return cs;
    } catch (SQLException e) {
      LOG.error("Unable to fetch column statistics", e);
      throw new MetaException("Failed to fetch column statistics, " + e.getMessage());
    } finally {
      commitOrRoleBack(commit);
    }
  }

  @Override
  public List<ColumnStatistics> getPartitionColumnStatistics(String dbName, String tblName,
                                                             List<String> partNames,
                                                             List<String> colNames)
      throws MetaException, NoSuchObjectException {
    List<List<String>> partVals = new ArrayList<>(partNames.size());
    for (String partName : partNames) {
      partVals.add(HBaseStore.partNameToVals(partName));
    }
    boolean commit = false;
    openTransaction();
    try {
      List<ColumnStatistics> css =
          getPostgres().getPartitionStatistics(dbName, tblName, partVals, colNames);
      commit = true;
      return css;
    } catch (SQLException e) {
      LOG.error("Unable to fetch column statistics", e);
      throw new MetaException("Failed fetching column statistics, " + e.getMessage());
    } finally {
      commitOrRoleBack(commit);
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
      List<Function> fss = getPostgres().scanFunctions(null, null);
      commit = true;
      return fss;
    } catch (SQLException e) {
      LOG.error("Unable to get functions", e);
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
      LOG.error("Unable to get functions", e);
      throw new MetaException("Unable to read from or write to postgres " + e.getMessage());
    } finally {
      commitOrRoleBack(commit);
    }
  }

  @Override
  public AggrStats get_aggr_stats_for(String dbName, String tblName, List<String> partNames,
                                      List<String> colNames) throws MetaException,
      NoSuchObjectException {
    List<List<String>> partVals = HBaseStore.partNameListToValsList(partNames);
    boolean commit = false;
    openTransaction();
    try {
      AggrStats aggrStats = getPostgres().getAggregatedStats(dbName, tblName, partNames, partVals, colNames);
      commit = true;
      return aggrStats;
    } catch (SQLException e) {
      LOG.error("Unable to get aggregated stats", e);
      throw new MetaException("Unable to read from or write to postgres " + e.getMessage());
    } finally {
      commitOrRoleBack(commit);
    }

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
    getPostgres().flushCatalogCache();
    getRoleHelper().clearRoleCache();
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
