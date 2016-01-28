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
import java.util.Arrays;
import java.util.List;

class HiveTransaction {
  private final long id;
  private HbaseMetastoreProto.Transaction.TxnState state;
  private long lastHeartbeat;

  // I chose an array over a list so that I could explicitly control growth.  ArrayList is memory
  // efficient (only 4 more bytes than an array I believe) and you can control the initial
  // capacity, but when it grows you loose control of how.
  private HiveLock[] hiveLocks;

  /**
   * For use when creating a new transaction.  This creates the transaction in an open state.
   * @param id txn id
   */
  HiveTransaction(long id) {
    this.id = id;
    state = HbaseMetastoreProto.Transaction.TxnState.OPEN;
  }

  /**
   * For use when recovering transactions from HBase.
   * @param hbaseTxn transaction record from HBase.
   * @param txnMgr transaction manager.
   * @throws IOException
   */
  HiveTransaction(HbaseMetastoreProto.Transaction hbaseTxn, TransactionManager txnMgr)
      throws IOException {
    id = hbaseTxn.getId();
    state = hbaseTxn.getTxnState();
    lastHeartbeat = hbaseTxn.getLastHeartbeat();
    List<HbaseMetastoreProto.Transaction.Lock> hbaseLocks = hbaseTxn.getLocksList();
    hiveLocks = new HiveLock[hbaseLocks.size()];
    for (int i = 0; i < hbaseLocks.size(); i++) {
      hiveLocks[i] = new HiveLock(id, hbaseLocks.get(i), txnMgr);
    }
  }

  long getId() {
    return id;
  }

  HbaseMetastoreProto.Transaction.TxnState getState() {
    return state;
  }

  void setState(HbaseMetastoreProto.Transaction.TxnState state) {
    this.state = state;
  }

  long getLastHeartbeat() {
    return lastHeartbeat;
  }

  void setLastHeartbeat(long lastHeartbeat) {
    this.lastHeartbeat = lastHeartbeat;
  }

  HiveLock[] getHiveLocks() {
    return hiveLocks;
  }

  /**
   * Add locks to the transaction.
   * @param newLocks array of locks to add.  This method assumes it can take ownership of this
   *                 array, so don't plan to do anything else with it.  All your locks are belong to
   *                 us.
   */
  void addLocks(HiveLock[] newLocks) {
    if (hiveLocks == null) {
      hiveLocks = newLocks;
    } else {
      int origSize = hiveLocks.length;
      hiveLocks = Arrays.copyOf(hiveLocks, origSize + newLocks.length);
      System.arraycopy(newLocks, 0, hiveLocks, origSize, newLocks.length);
    }
  }
}
