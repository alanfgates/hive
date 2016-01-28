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

class HiveLock {

  private final long id;
  // Transaction we are part of, needed so that we can backtrack to the txns when tracing via
  // db/table/part
  private final long txnId;
  // Lock list this lock is part of
  private final TransactionManager.DTPLockQueue dtpQueue;
  private final HbaseMetastoreProto.Transaction.Lock.LockType type;
  private HbaseMetastoreProto.Transaction.Lock.LockState state;

  /**
   * For use when creating a new lock.  This puts the lock in waiting state.
   * @param id id for this lock
   * @param txnId txn id this lock is part of
   * @param type lock type
   * @param dtpQueue DTP list this will go in.
   */
  HiveLock(long id, long txnId, HbaseMetastoreProto.Transaction.Lock.LockType type,
           TransactionManager.DTPLockQueue dtpQueue) {
    this.id = id;
    this.txnId = txnId;
    this.type = type;
    this.dtpQueue = dtpQueue;
    state = HbaseMetastoreProto.Transaction.Lock.LockState.WAITING;
  }

  /**
   * For use when recovering locks from HBase
   * @param txnId txn id this lock is a part of
   * @param hbaseLock lock record from HBase
   * @param txnMgr transaction manager
   * @throws IOException
   */
  HiveLock(long txnId, HbaseMetastoreProto.Transaction.Lock hbaseLock, TransactionManager txnMgr)
      throws IOException {
    id = hbaseLock.getId();
    this.txnId = txnId;
    dtpQueue =
        txnMgr.findDTPQueue(hbaseLock.getDb(), hbaseLock.getTable(), hbaseLock.getPartition());
    type = hbaseLock.getType();
    state = hbaseLock.getState();
  }

  long getId() {
    return id;
  }

  long getTxnId() {
    return txnId;
  }

  HbaseMetastoreProto.Transaction.Lock.LockType getType() {
    return type;
  }

  HbaseMetastoreProto.Transaction.Lock.LockState getState() {
    return state;
  }

  void setState(HbaseMetastoreProto.Transaction.Lock.LockState state) {
    this.state = state;
  }

  public TransactionManager.DTPLockQueue getDtpQueue() {
    return dtpQueue;
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof HiveLock)) return false;
    HiveLock other = (HiveLock)o;
    return txnId == other.txnId && id == other.id;
  }
}
