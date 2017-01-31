/**
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

package org.apache.hadoop.hive.metastore.hbase;

import org.apache.hadoop.hive.common.ObjectPair;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.PrincipalPrivilegeSet;
import org.apache.hadoop.hive.metastore.api.Role;
import org.apache.hadoop.hive.metastore.api.Table;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

public interface MetadataStore {
  /**
   * @param fileIds file ID list.
   * @param result The ref parameter, used to return the serialized file metadata.
   */
  void getFileMetadata(List<Long> fileIds, ByteBuffer[] result) throws IOException;

  /**
   * @param fileIds file ID list.
   * @param metadataBuffers Serialized file metadata, one per file ID.
   * @param addedCols The column names for additional columns created by file-format-specific
   *                  metadata handler, to be stored in the cache.
   * @param addedVals The values for addedCols; one value per file ID per added column.
   */
  void storeFileMetadata(List<Long> fileIds, List<ByteBuffer> metadataBuffers,
      ByteBuffer[] addedCols, ByteBuffer[][] addedVals) throws IOException, InterruptedException;

  /**
   * @param fileId The file ID.
   * @param metadataBuffers Serialized file metadata.
   * @param addedCols The column names for additional columns created by file-format-specific
   *                  metadata handler, to be stored in the cache.
   * @param addedVals The values for addedCols; one value per added column.
   */
  void storeFileMetadata(long fileId, ByteBuffer metadata, ByteBuffer[] addedCols,
      ByteBuffer[] addedVals) throws IOException, InterruptedException;


  /**
   * Scan all databases.
   * @param regex regular expression to match against the database name.  If null, all databases
   *              will be returned.
   * @return list of databases.
   * @throws IOException
   */
  List<Database> scanDatabases(String regex) throws IOException;

  /**
   * Write back a set of changes to existing databases.  This should not be used to store new
   * databases.
   * @param dbs databases to store
   * @throws IOException
   */
  void writeBackChangedDatabases(List<Database> dbs) throws IOException;

  /**
   * Get the global privileges from the store
   * @return the global privileges
   * @throws IOException
   */
  PrincipalPrivilegeSet getGlobalPrivs() throws IOException;

  /**
   * Write the global privileges back to the store
   * @param privs the global privileges
   * @throws IOException
   */
  void putGlobalPrivs(PrincipalPrivilegeSet privs) throws IOException;

  /**
   * Get the definition of a role.
   * @param roleName name of the role to fetch.
   * @return the role, or null if there is no such role.
   * @throws IOException
   */
  Role getRole(String roleName) throws IOException;

  /**
   * Get a set of roles.
   * @param roleNames list of names of roles to fetch.
   * @return list of fetched roles.
   * @throws IOException
   */
  List<Role> getRoles(Collection<String> roleNames) throws IOException;

  /**
   * Store a role.
   * @param role Role to store.
   * @throws IOException
   */
  void putRole(Role role) throws IOException;

  /**
   * Given a role, find all users who are either directly or indirectly participate in this role.
   * This is expensive, it should be used sparingly.  It scan the entire userToRole table and
   * does a linear search on each entry.
   * @param roleName name of the role
   * @return set of all users in the role
   * @throws IOException
   */
  Set<String> findAllUsersInRole(String roleName) throws IOException;

  /**
   * Delete a role.
   * @param roleName role to delete
   * @return true if a role was deleted, false if not
   * @throws IOException
   */
  boolean deleteRole(String roleName) throws IOException;

  /**
   * Write back a set of changed roles.
   * @param changedRoles Map of changed roles.
   * @throws IOException
   */
  void writeBackChangedRoles(Map<String, HbaseMetastoreProto.RoleGrantInfoList> changedRoles)
    throws IOException;

  /**
   * Store the mapping from a single user to all the roles the user participates in.
   * @param userName user name
   * @param roles list of user names the user participates in.
   * @throws IOException
   */
  void storeUserToRollMapping(String userName, Set<String> roles) throws IOException;

  /**
   * Build a mapping of all role names to all principals that participate in that role.
   * @param cache mapping of role name to principals
   * @throws IOException
   */
  void populateRoleCache(Map<String, HbaseMetastoreProto.RoleGrantInfoList> cache) throws IOException;

  /**
   * Get all of the principals currently associated with a role.
   * @param roleName name of the role.
   * @return list of role principals
   * @throws IOException
   */
  HbaseMetastoreProto.RoleGrantInfoList getRolePrincipals(String roleName) throws IOException;

  /**
   * Get all tables in a database, possibly filtering by a regular expression.
   * @param dbName database to look in.  Cannot be null.
   * @param regex regular expression to filter names with.  If null, all tables in the database
   *              will be returned.
   * @return List of matching tables.
   * @throws IOException
   */
  List<Table> scanTables(String dbName, String regex) throws IOException;

  /**
   * Write a set of changes to tables.  This should not be used to create new tables.
   * @param tables tables to write
   * @throws IOException
   */
  void writeBackChangedTables(List<Table> tables) throws IOException;

  /**
   * Get a list of all roles the user participates in.
   * @param userName user
   * @return list of roles the user is a part of
   * @throws IOException
   */
  List<String> getUserRoles(String userName) throws IOException;

}