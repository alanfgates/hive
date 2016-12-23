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
import org.apache.hadoop.hive.metastore.api.HiveObjectPrivilege;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.PrincipalPrivilegeSet;
import org.apache.hadoop.hive.metastore.api.PrivilegeBag;
import org.apache.hadoop.hive.metastore.api.PrivilegeGrantInfo;
import org.apache.hadoop.hive.metastore.api.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PrivilegeHelper {
  static final private Logger LOG = LoggerFactory.getLogger(PrivilegeHelper.class.getName());

  private final RawStore store;
  private final MetadataStore kvs;

  public PrivilegeHelper(RawStore store, MetadataStore kvs) {
    this.store = store;
    this.kvs = kvs;
  }

  public void grantPrivileges(PrivilegeBag privileges)
      throws InvalidObjectException, MetaException, NoSuchObjectException {
    for (HiveObjectPrivilege priv : privileges.getPrivileges()) {
      // Locate the right object to deal with
      PrivilegeInfo privilegeInfo = findPrivilegeToGrantOrRevoke(priv);

      // Now, let's see if we've already got this privilege
      for (PrivilegeGrantInfo info : privilegeInfo.grants) {
        if (info.getPrivilege().equals(priv.getGrantInfo().getPrivilege())) {
          throw new InvalidObjectException(priv.getPrincipalName() + " already has " +
              priv.getGrantInfo().getPrivilege() + " on " + privilegeInfo.typeErrMsg);
        }
      }
      privilegeInfo.grants.add(priv.getGrantInfo());

      writeBackGrantOrRevoke(priv, privilegeInfo);
    }

  }

  public void revokePrivileges(PrivilegeBag privileges, boolean grantOption)
      throws NoSuchObjectException, InvalidObjectException, MetaException {
    for (HiveObjectPrivilege priv : privileges.getPrivileges()) {
      PrivilegeInfo privilegeInfo = findPrivilegeToGrantOrRevoke(priv);

      for (int i = 0; i < privilegeInfo.grants.size(); i++) {
        if (privilegeInfo.grants.get(i).getPrivilege().equals(
            priv.getGrantInfo().getPrivilege())) {
          if (grantOption) privilegeInfo.grants.get(i).setGrantOption(false);
          else privilegeInfo.grants.remove(i);
          break;
        }
      }
      writeBackGrantOrRevoke(priv, privilegeInfo);
    }
  }

  private PrivilegeInfo findPrivilegeToGrantOrRevoke(HiveObjectPrivilege privilege)
      throws MetaException, NoSuchObjectException, InvalidObjectException {
    PrivilegeInfo result = new PrivilegeInfo();
    switch (privilege.getHiveObject().getObjectType()) {
      case GLOBAL:
        try {
          result.privSet = createOnNull(kvs.getGlobalPrivs());
        } catch (IOException e) {
          LOG.error("Unable to fetch global privileges", e);
          throw new MetaException("Unable to fetch global privileges, " + e.getMessage());
        }
        result.typeErrMsg = "global";
        break;

      case DATABASE:
        result.db = store.getDatabase(privilege.getHiveObject().getDbName());
        result.typeErrMsg = "database " + result.db.getName();
        result.privSet = createOnNull(result.db.getPrivileges());
        break;

      case TABLE:
        result.table = store.getTable(privilege.getHiveObject().getDbName(),
            privilege.getHiveObject().getObjectName());
        result.typeErrMsg = "table " + result.table.getTableName();
        result.privSet = createOnNull(result.table.getPrivileges());
        break;

      case PARTITION:
      case COLUMN:
        throw new RuntimeException("HBase metastore does not support partition or column " +
            "permissions");

      default:
        throw new RuntimeException("Woah bad, unknown object type " +
            privilege.getHiveObject().getObjectType());
    }

    // Locate the right PrivilegeGrantInfo
    Map<String, List<PrivilegeGrantInfo>> grantInfos;
    switch (privilege.getPrincipalType()) {
      case USER:
        grantInfos = result.privSet.getUserPrivileges();
        result.typeErrMsg = "user";
        break;

      case GROUP:
        throw new RuntimeException("HBase metastore does not support group permissions");

      case ROLE:
        grantInfos = result.privSet.getRolePrivileges();
        result.typeErrMsg = "role";
        break;

      default:
        throw new RuntimeException("Woah bad, unknown principal type " +
            privilege.getPrincipalType());
    }

    // Find the requested name in the grantInfo
    result.grants = grantInfos.get(privilege.getPrincipalName());
    if (result.grants == null) {
      // Means we don't have any grants for this user yet.
      result.grants = new ArrayList<PrivilegeGrantInfo>();
      grantInfos.put(privilege.getPrincipalName(), result.grants);
    }
    return result;
  }

  private PrincipalPrivilegeSet createOnNull(PrincipalPrivilegeSet pps) {
    // If this is the first time a user has been granted a privilege set will be null.
    if (pps == null) {
      pps = new PrincipalPrivilegeSet();
    }
    if (pps.getUserPrivileges() == null) {
      pps.setUserPrivileges(new HashMap<String, List<PrivilegeGrantInfo>>());
    }
    if (pps.getRolePrivileges() == null) {
      pps.setRolePrivileges(new HashMap<String, List<PrivilegeGrantInfo>>());
    }
    return pps;
  }

  private void writeBackGrantOrRevoke(HiveObjectPrivilege priv, PrivilegeInfo pi)
      throws MetaException, NoSuchObjectException, InvalidObjectException {
    // Now write it back
    switch (priv.getHiveObject().getObjectType()) {
      case GLOBAL:
        try {
          kvs.putGlobalPrivs(pi.privSet);
        } catch (IOException e) {
          LOG.error("Unable to write global privileges", e);
          throw new MetaException("Unable to write global privileges, " + e.getMessage());
        }
        break;

      case DATABASE:
        pi.db.setPrivileges(pi.privSet);
        store.alterDatabase(pi.db.getName(), pi.db);
        break;

      case TABLE:
        pi.table.setPrivileges(pi.privSet);
        store.alterTable(pi.table.getDbName(), pi.table.getTableName(), pi.table);
        break;

      default:
        throw new RuntimeException("Dude, you missed the second switch!");
    }
  }

  private static class PrivilegeInfo {
    Database db;
    Table table;
    List<PrivilegeGrantInfo> grants;
    String typeErrMsg;
    PrincipalPrivilegeSet privSet;
  }

}
