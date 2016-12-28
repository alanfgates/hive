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
package org.apache.hadoop.hive.metastore.hbase;

import org.apache.hadoop.hive.metastore.RawStore;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.PrincipalPrivilegeSet;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.Role;
import org.apache.hadoop.hive.metastore.api.RolePrincipalGrant;
import org.apache.hadoop.hive.metastore.api.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A class to help key value stores deal with roles.
 */
public class RoleHelper {
  static final private Logger LOG = LoggerFactory.getLogger(RoleHelper.class);

  private final RawStore store;
  private final MetadataStore kvs;

  private final Map<String, HbaseMetastoreProto.RoleGrantInfoList> roleCache;

  public RoleHelper(RawStore store, MetadataStore kvs) {
    this.store = store;
    this.kvs = kvs;
    roleCache = new HashMap<>();
  }

  public void addRole(String roleName, String ownerName) throws InvalidObjectException, IOException {
    int now = (int)(System.currentTimeMillis()/1000);
    Role role = new Role(roleName, now, ownerName);
    if (kvs.getRole(roleName) != null) {
      throw new InvalidObjectException("Role " + roleName + " already exists");
    }
    kvs.putRole(role);
  }

  public void removeRole(String roleName) throws IOException, NoSuchObjectException {
    Set<String> usersInRole = kvs.findAllUsersInRole(roleName);
    kvs.deleteRole(roleName);
    removeRoleGrants(roleName);
    for (String user : usersInRole) {
      buildRoleMapForUser(user);
    }
  }

  public void grantRole(Role role, String userName, PrincipalType principalType, String grantor,
                        PrincipalType grantorType, boolean grantOption) throws IOException, NoSuchObjectException {
    Set<String> usersToRemap = findUsersToRemapRolesFor(role, userName, principalType);
    HbaseMetastoreProto.RoleGrantInfo.Builder builder =
        HbaseMetastoreProto.RoleGrantInfo.newBuilder();
    if (userName != null) builder.setPrincipalName(userName);
    if (principalType != null) {
      builder.setPrincipalType(HBaseUtils.convertPrincipalTypes(principalType));
    }
    builder.setAddTime((int)(System.currentTimeMillis() / 1000));
    if (grantor != null) builder.setGrantor(grantor);
    if (grantorType != null) {
      builder.setGrantorType(HBaseUtils.convertPrincipalTypes(grantorType));
    }
    builder.setGrantOption(grantOption);

    addPrincipalToRole(role.getRoleName(), builder.build());
    for (String user : usersToRemap) {
      buildRoleMapForUser(user);
    }

  }

  public void revokeRole(Role role, String userName, PrincipalType principalType,
                         boolean grantOption) throws IOException, NoSuchObjectException {
    if (grantOption) {
      // If this is a grant only change, we don't need to rebuild the user mappings.
      dropPrincipalFromRole(role.getRoleName(), userName, principalType, grantOption);
    } else {
      Set<String> usersToRemap = findUsersToRemapRolesFor(role, userName, principalType);
      dropPrincipalFromRole(role.getRoleName(), userName, principalType, grantOption);
      for (String user : usersToRemap) {
        buildRoleMapForUser(user);
      }
    }
  }

  /**
   * Find all roles directly participated in by a given principal.  This builds the role cache
   * because it assumes that subsequent calls may be made to find roles participated in indirectly.
   * @param name username or role name
   * @param type user or role
   * @return map of role name to grant info for all roles directly participated in.
   */
  public List<Role> getPrincipalDirectRoles(String name, PrincipalType type)
      throws IOException {
    buildRoleCache();

    Set<String> rolesFound = new HashSet<>();
    for (Map.Entry<String, HbaseMetastoreProto.RoleGrantInfoList> e : roleCache.entrySet()) {
      for (HbaseMetastoreProto.RoleGrantInfo giw : e.getValue().getGrantInfoList()) {
        if (HBaseUtils.convertPrincipalTypes(giw.getPrincipalType()) == type &&
            giw.getPrincipalName().equals(name)) {
          rolesFound.add(e.getKey());
          break;
        }
      }
    }
    return kvs.getRoles(rolesFound);
  }

  public List<RolePrincipalGrant> listRolesWithGrants(String principalName,
                                                      PrincipalType principalType)
      throws IOException {
    List<Role> roles = store.listRoles(principalName, principalType);
    List<RolePrincipalGrant> rpgs = new ArrayList<>(roles.size());
    for (Role role : roles) {
      HbaseMetastoreProto.RoleGrantInfoList grants = kvs.getRolePrincipals(role.getRoleName());
      if (grants != null) {
        for (HbaseMetastoreProto.RoleGrantInfo grant : grants.getGrantInfoList()) {
          if (grant.getPrincipalType() == HBaseUtils.convertPrincipalTypes(principalType) &&
              grant.getPrincipalName().equals(principalName)) {
            rpgs.add(new RolePrincipalGrant(role.getRoleName(), principalName, principalType,
                grant.getGrantOption(), (int) grant.getAddTime(), grant.getGrantor(),
                HBaseUtils.convertPrincipalTypes(grant.getGrantorType())));
          }
        }
      }
    }
    return rpgs;
  }

  public List<RolePrincipalGrant> listRoleMembers(String roleName) throws IOException {
    HbaseMetastoreProto.RoleGrantInfoList gil = kvs.getRolePrincipals(roleName);
    List<RolePrincipalGrant> roleMaps = new ArrayList<>(gil.getGrantInfoList().size());
    for (HbaseMetastoreProto.RoleGrantInfo giw : gil.getGrantInfoList()) {
      roleMaps.add(new RolePrincipalGrant(roleName, giw.getPrincipalName(),
          HBaseUtils.convertPrincipalTypes(giw.getPrincipalType()),
          giw.getGrantOption(), (int)giw.getAddTime(), giw.getGrantor(),
          HBaseUtils.convertPrincipalTypes(giw.getGrantorType())));
    }
    return roleMaps;
  }


  public void clearRoleCache() {
    roleCache.clear();
  }

  /**
   * Remove all of the grants for a role.  This is not cheap.
   * @param roleName Role to remove from all other roles and grants
   * @throws IOException
   */
  private void removeRoleGrants(String roleName) throws IOException {
    buildRoleCache();

    Map<String, HbaseMetastoreProto.RoleGrantInfoList> changedRoles  = new HashMap<>();
    // First, walk the role table and remove any references to this role
    for (Map.Entry<String, HbaseMetastoreProto.RoleGrantInfoList> e : roleCache.entrySet()) {
      boolean madeAChange = false;
      List<HbaseMetastoreProto.RoleGrantInfo> rgil = new ArrayList<>();
      rgil.addAll(e.getValue().getGrantInfoList());
      for (int i = 0; i < rgil.size(); i++) {
        if (rgil.get(i).getPrincipalType() == HbaseMetastoreProto.PrincipalType.ROLE &&
            rgil.get(i).getPrincipalName().equals(roleName)) {
          rgil.remove(i);
          madeAChange = true;
          break;
        }
      }
      if (madeAChange) {
        HbaseMetastoreProto.RoleGrantInfoList proto =
            HbaseMetastoreProto.RoleGrantInfoList.newBuilder()
                .addAllGrantInfo(rgil)
                .build();
        roleCache.put(e.getKey(), proto);
        changedRoles.put(e.getKey(), proto);
      }
    }

    if (changedRoles.size() > 0) {
      kvs.writeBackChangedRoles(changedRoles);
    }

    // Remove any global privileges held by this role
    PrincipalPrivilegeSet global = kvs.getGlobalPrivs();
    if (global != null &&
        global.getRolePrivileges() != null &&
        global.getRolePrivileges().remove(roleName) != null) {
      kvs.putGlobalPrivs(global);
    }

    // Now, walk the db table
    List<Database> dbs = kvs.scanDatabases(null);
    List<Database> changedDbs = new ArrayList<>();
    if (dbs == null) dbs = new ArrayList<>(); // rare, but can happen
    for (Database db : dbs) {
      if (db.getPrivileges() != null &&
          db.getPrivileges().getRolePrivileges() != null &&
          db.getPrivileges().getRolePrivileges().remove(roleName) != null) {
        changedDbs.add(db);
      }
    }

    kvs.writeBackChangedDatabases(changedDbs);

    // Finally, walk the table table
    List<Table> changedTables = new ArrayList<>();
    for (Database db : dbs) {
      List<Table> tables = kvs.scanTables(db.getName(), null);
      if (tables != null) {
        for (Table table : tables) {
          if (table.getPrivileges() != null &&
              table.getPrivileges().getRolePrivileges() != null &&
              table.getPrivileges().getRolePrivileges().remove(roleName) != null) {
            changedTables.add(table);
          }
        }
      }
    }

    kvs.writeBackChangedTables(changedTables);
  }

  /**
   * Rebuild the row for a given user in the USER_TO_ROLE table.  This is expensive.  It
   * should be called as infrequently as possible.  This does NOT modify the roleCache and
   * assumes the role cache is up to date with any changes before it's called.
   * @param userName name of the user
   * @throws IOException
   */
  private void buildRoleMapForUser(String userName) throws IOException, NoSuchObjectException {
    // This is mega ugly.  Hopefully we don't have to do this too often.
    // First, scan the role table and put it all in memory
    buildRoleCache();
    LOG.debug("Building role map for " + userName);

    // Second, find every role the user participates in directly.
    Set<String> rolesToAdd = new HashSet<>();
    Set<String> rolesToCheckNext = new HashSet<>();
    for (Map.Entry<String, HbaseMetastoreProto.RoleGrantInfoList> e : roleCache.entrySet()) {
      for (HbaseMetastoreProto.RoleGrantInfo grantInfo : e.getValue().getGrantInfoList()) {
        if (grantInfo.getPrincipalType() == HbaseMetastoreProto.PrincipalType.USER &&
            userName .equals(grantInfo.getPrincipalName())) {
          rolesToAdd.add(e.getKey());
          rolesToCheckNext.add(e.getKey());
          LOG.debug("Adding " + e.getKey() + " to list of roles user is in directly");
          break;
        }
      }
    }

    // Third, find every role the user participates in indirectly (that is, they have been
    // granted into role X and role Y has been granted into role X).
    while (rolesToCheckNext.size() > 0) {
      Set<String> tmpRolesToCheckNext = new HashSet<>();
      for (String roleName : rolesToCheckNext) {
        HbaseMetastoreProto.RoleGrantInfoList grantInfos = roleCache.get(roleName);
        if (grantInfos == null) continue;  // happens when a role contains no grants
        for (HbaseMetastoreProto.RoleGrantInfo grantInfo : grantInfos.getGrantInfoList()) {
          if (grantInfo.getPrincipalType() == HbaseMetastoreProto.PrincipalType.ROLE &&
              rolesToAdd.add(grantInfo.getPrincipalName())) {
            tmpRolesToCheckNext.add(grantInfo.getPrincipalName());
            LOG.debug("Adding " + grantInfo.getPrincipalName() +
                " to list of roles user is in indirectly");
          }
        }
      }
      rolesToCheckNext = tmpRolesToCheckNext;
    }
    kvs.storeUserToRollMapping(userName, rolesToAdd);
  }

  private void buildRoleCache() throws IOException {
    if (roleCache.size() == 0) {
      kvs.populateRoleCache(roleCache);
    }
  }

  private Set<String> findUsersToRemapRolesFor(Role role, String principalName, PrincipalType type)
      throws IOException, NoSuchObjectException {
    Set<String> usersToRemap;
    switch (type) {
      case USER:
        // In this case it's just the user being added to the role that we need to remap for.
        usersToRemap = new HashSet<String>();
        usersToRemap.add(principalName);
        break;

      case ROLE:
        // In this case we need to remap for all users in the containing role (not the role being
        // granted into the containing role).
        usersToRemap = kvs.findAllUsersInRole(role.getRoleName());
        break;

      default:
        throw new RuntimeException("Unknown principal type " + type);

    }
    return usersToRemap;
  }

  private void addPrincipalToRole(String roleName, HbaseMetastoreProto.RoleGrantInfo grantInfo )
      throws IOException, NoSuchObjectException {
    HbaseMetastoreProto.RoleGrantInfoList proto = roleCache.get(roleName);
    if (proto == null) proto = kvs.getRolePrincipals(roleName);
    List<HbaseMetastoreProto.RoleGrantInfo> rolePrincipals = new ArrayList<>();
    if (proto != null) {
      rolePrincipals.addAll(proto.getGrantInfoList());
    }

    rolePrincipals.add(grantInfo);
    proto = HbaseMetastoreProto.RoleGrantInfoList.newBuilder()
        .addAllGrantInfo(rolePrincipals)
        .build();
    kvs.writeBackChangedRoles(Collections.singletonMap(roleName, proto));
    roleCache.put(roleName, proto);
  }

  private void dropPrincipalFromRole(String roleName, String principalName, PrincipalType type,
                                     boolean grantOnly)
      throws NoSuchObjectException, IOException {
    HbaseMetastoreProto.RoleGrantInfoList proto = roleCache.get(roleName);
    if (proto == null) proto = kvs.getRolePrincipals(roleName);
    if (proto == null) return;
    List<HbaseMetastoreProto.RoleGrantInfo> rolePrincipals = new ArrayList<>();
    rolePrincipals.addAll(proto.getGrantInfoList());

    for (int i = 0; i < rolePrincipals.size(); i++) {
      if (HBaseUtils.convertPrincipalTypes(rolePrincipals.get(i).getPrincipalType()) == type &&
          rolePrincipals.get(i).getPrincipalName().equals(principalName)) {
        if (grantOnly) {
          rolePrincipals.set(i,
              HbaseMetastoreProto.RoleGrantInfo.newBuilder(rolePrincipals.get(i))
                  .setGrantOption(false)
                  .build());
        } else {
          rolePrincipals.remove(i);
        }
        break;
      }
    }
    proto = HbaseMetastoreProto.RoleGrantInfoList.newBuilder()
        .addAllGrantInfo(rolePrincipals)
        .build();
    kvs.writeBackChangedRoles(Collections.singletonMap(roleName, proto));
    roleCache.put(roleName, proto);
  }

}
