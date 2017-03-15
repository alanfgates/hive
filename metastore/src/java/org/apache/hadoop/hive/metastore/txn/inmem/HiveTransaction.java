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

import org.apache.hadoop.hive.metastore.api.LockType;
import org.apache.hadoop.hive.metastore.api.TxnState;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

abstract class HiveTransaction {
  protected final long txnId;
  /**
   * Aborted and Committed transactions keep track of writes they did
   */
  protected Map<EntityKey, Set<WriteSetRecordIdentifier>> writeSets;
  /**
   * Use an array rathen than a list in order to explicitly control growth.  ArrayList is memory
   * efficient (only 4 more bytes than an array) and you can control the initial
   * capacity, but when it grows you loose control of how.
   */
  protected HiveLock[] hiveLocks;

  protected HiveTransaction(long txnId) {
    this.txnId = txnId;
  }

  abstract TxnState getState();

  long getTxnId() {
    return txnId;
  }

  Map<EntityKey, Set<WriteSetRecordIdentifier>> getWriteSets() {
    return writeSets;
  }

  /**
   * Only for use during recovery.  This will add entities back into the writeSets.
   * @param entityKey entity to add.
   */
  void addWriteSet(EntityKey entityKey) {
    if (writeSets == null) writeSets = new HashMap<>();
    writeSets.put(entityKey, null);
  }

  HiveLock[] getHiveLocks() {
    return hiveLocks;
  }

  @Override
  public boolean equals(Object o) {
    if (o == null || !(o instanceof HiveTransaction)) return false;
    HiveTransaction other = (HiveTransaction)o;
    return txnId == other.txnId;
  }

  @Override
  public int hashCode() {
    return (int)txnId;
  }

  protected void buildWriteSets(HiveTransaction openTxn) {
    if (openTxn.hiveLocks != null) {
      for (HiveLock lock : openTxn.hiveLocks) {
        if (lock.getType() == LockType.SHARED_WRITE) {
          // Don't create the writeSets until we see at least one shared write
          if (writeSets == null) {
            writeSets = new HashMap<>(openTxn.hiveLocks.length);
          }
          writeSets.put(lock.getEntityLocked(), null);
        }
      }
    }
  }
}
