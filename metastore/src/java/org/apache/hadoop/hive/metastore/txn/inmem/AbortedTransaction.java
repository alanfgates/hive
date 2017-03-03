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

class AbortedTransaction extends HiveTransaction {

  private Map<EntityKey, HiveLock> compactableLocks;

  /**
   * Constructor for normal operations where an open transaction is being aborted.
   * @param openTxn transaction to abort.
   */
  AbortedTransaction(OpenTransaction openTxn) {
    super(openTxn.getTxnId());
    compactLockArray(openTxn.getHiveLocks());
  }

  AbortedTransaction(long txnId) {
    super(txnId);
  }

  @Override
  TxnState getState() {
    return TxnState.ABORTED;
  }

  /**
   * This should only ever be called when recovering an aborted transaction.  In general you
   * can't add new locks to an aborted transaction.
   * @param locks locks to put in the transaction.
   */
  @Override
  void addLocks(HiveLock[] locks) {
    compactLockArray(locks);
  }

  /**
   * Note that a dtp a lock is associated with has been compacted, so we can forget about the lock
   * @param key dtp lock is associated with
   */
  HiveLock compactLock(EntityKey key) {
    return compactableLocks.remove(key);
  }

  /**
   * Determine whether all dtps written to by an aborted transaction have been compacted.
   * @return true if all have been compacted, false otherwise.
   */
  boolean fullyCompacted() {
    return compactableLocks.size() == 0;
  }

  Map<EntityKey, HiveLock> getCompactableLocks() {
    return compactableLocks;
  }

  private void compactLockArray(HiveLock[] locks) {
    compactableLocks = new HashMap<>();
    if (locks != null) {
      for (HiveLock lock : locks) {
        if (lock.getType() == LockType.SHARED_WRITE) {
          compactableLocks.put(lock.getEntityLocked(), lock);
        }
      }
    }
  }
}
