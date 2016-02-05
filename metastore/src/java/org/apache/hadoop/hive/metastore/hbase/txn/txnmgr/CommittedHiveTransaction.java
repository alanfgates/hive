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
import java.util.List;

public class CommittedHiveTransaction extends HiveTransaction {
  // I chose an array over a list so that I could explicitly control growth.  ArrayList is memory
  // efficient (only 4 more bytes than an array I believe) and you can control the initial
  // capacity, but when it grows you loose control of how.
  private HiveLock[] hiveLocks;

  /**
   * For use when creating a new transaction.  This creates the transaction in an open state.
   * @param openTxn open transaction that is moving to a committed state.
   */
  CommittedHiveTransaction(HiveTransaction openTxn) {
    super(openTxn.getId());
    if (openTxn.getState() == HbaseMetastoreProto.Transaction.TxnState.OPEN) {
      throw new RuntimeException("Logic error, trying to move transaction of type " +
          openTxn.getState() + " to committed");
    }
    hiveLocks = openTxn.getHiveLocks();
    for (HiveLock lock : hiveLocks) {
      lock.setState(HbaseMetastoreProto.Transaction.Lock.LockState.RELEASED);
    }
  }

  /**
   * For use when recovering transactions from HBase.
   * @param hbaseTxn transaction record from HBase.
   * @param txnMgr transaction manager.
   * @throws IOException
   */
  CommittedHiveTransaction(HbaseMetastoreProto.Transaction hbaseTxn, TransactionManager txnMgr)
      throws IOException {
    super(hbaseTxn.getId());
    List<HbaseMetastoreProto.Transaction.Lock> hbaseLocks = hbaseTxn.getLocksList();
    hiveLocks = new HiveLock[hbaseLocks.size()];
    for (int i = 0; i < hbaseLocks.size(); i++) {
      hiveLocks[i] = new HiveLock(id, hbaseLocks.get(i), txnMgr);
      // Don't add these to the dtps, as they're released
    }
  }

  HbaseMetastoreProto.Transaction.TxnState getState() {
    return HbaseMetastoreProto.Transaction.TxnState.COMMITTED;
  }

  @Override
  long getLastHeartbeat() {
    throw new UnsupportedOperationException("Logic error, no heartbeats for committed transactions");
  }

  @Override
  void setLastHeartbeat(long lastHeartbeat) {
    throw new UnsupportedOperationException("Logic error, no heartbeats for committed transactions");
  }

  @Override
  HiveLock[] getHiveLocks() {
    return hiveLocks;
  }

  @Override
  void addLocks(HiveLock[] newLocks) {
    throw new UnsupportedOperationException("Logic error, can't add locks to committed transactions");
  }
}
