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
package org.apache.hadoop.hive.metastore.txn.inmem;

import org.apache.hadoop.hive.metastore.api.LockComponent;

class EntityKey {
  final String db;
  final String table;
  final String part;

  EntityKey(String db, String table, String part) {
    this.db = db;
    this.table = table;
    this.part = part;
  }

  EntityKey(LockComponent lc) {
    db = lc.getDbname();
    table = lc.isSetTablename() ? lc.getTablename() : null;
    part = lc.isSetPartitionname() ? lc.getPartitionname() : null;
  }

  @Override
  public int hashCode() {
    // db should never be null
    int hashCode = db.hashCode();
    if (table != null) hashCode = hashCode * 31 + table.hashCode();
    if (part != null) hashCode = hashCode * 31 + part.hashCode();
    return hashCode;
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof EntityKey)) return false;
    EntityKey other = (EntityKey)obj;
    // db should never be null
    if (db.equals(other.db)) {
      if (table == null && other.table == null ||
          table != null && table.equals(other.table)) {
        if (part == null && other.part == null ||
            part != null && part.equals(other.part)) {
          return true;
        }
      }
    }
    return false;
  }

  @Override
  public String toString() {
    StringBuilder bldr = new StringBuilder(db);
    if (table != null) {
      bldr.append('.')
          .append(table);
      if (part != null) {
        bldr.append('.')
            .append(part);
      }
    }
    return bldr.toString();
  }
}
