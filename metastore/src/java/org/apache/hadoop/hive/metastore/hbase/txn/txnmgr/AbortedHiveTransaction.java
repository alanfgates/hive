/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.metastore.hbase.txn.txnmgr;

import org.apache.hadoop.hive.metastore.hbase.HbaseMetastoreProto;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class AbortedHiveTransaction extends HiveTransaction {

  private Map<TransactionManager.EntityKey, HiveLock> compactableLocks;

  /**
   * For use when creating a new aborted transaction.
   * @param openTxn open transaction moving into aborted state.
   */
  AbortedHiveTransaction(OpenHiveTransaction openTxn) {
    super(openTxn.getId());
    compactableLocks = new HashMap<>();
    for (HiveLock lock : openTxn.getHiveLocks()) {
      if (lock.getType() == HbaseMetastoreProto.LockType.SHARED_WRITE) {
        compactableLocks.put(lock.getEntityLocked(), lock);
      }
    }
  }

  /**
   * For use when recovering transactions from HBase.
   * @param hbaseTxn transaction record from HBase.
   * @param txnMgr ptr to the transaction manager
   * @throws IOException
   */
  AbortedHiveTransaction(HbaseMetastoreProto.Transaction hbaseTxn, TransactionManager txnMgr)
      throws IOException {
    super(hbaseTxn.getId());
    compactableLocks = new HashMap<>();
    for (HbaseMetastoreProto.Transaction.Lock hbaseLock : hbaseTxn.getLocksList()) {
      HiveLock hiveLock = new HiveLock(id, hbaseLock, txnMgr);
      compactableLocks.put(hiveLock.getEntityLocked(), hiveLock);
    }
  }

  @Override
  HbaseMetastoreProto.TxnState getState() {
    return HbaseMetastoreProto.TxnState.ABORTED;
  }

  /**
   * Note that a dtp a lock is associated with has been compacted, so we can forget about the lock
   * @param key dtp lock is associated with
   */
  HiveLock compactLock(TransactionManager.EntityKey key) {
    return compactableLocks.remove(key);
  }

  /**
   * Determine whether all dtps written to by an aborted transaction have been compacted.
   * @return true if all have been compacted, false otherwise.
   */
  boolean fullyCompacted() {
    return compactableLocks.size() == 0;
  }



}
