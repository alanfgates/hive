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

abstract class HiveTransaction {
  protected final long id;

  protected HiveTransaction(long id) {
    this.id = id;
  }

  final long getId() {
    return id;
  }

  abstract HbaseMetastoreProto.TxnState getState();

  /**
   * Get the last heartbeat timestamp.  It is only valid to call this when the transaction is in
   * open state.
   * @return timestamp in milliseconds since the epoch of the last heartbeat.
   */
  abstract long getLastHeartbeat();

  /**
   * Set the heartbeat time.  It is only valid to call this when the transaction is in the open
   * state.
   * @param lastHeartbeat timestamp in milliseconds since the epoch.
   */
  abstract void setLastHeartbeat(long lastHeartbeat);

  /**
   * Get a list of locks associated with this transaction.  It is valid to call this when the
   * transaction is open or committed, but not when it is aborted.
   * @return list of locks.  In open state, all locks will be returned, in committed state only
   * shared_write locks will be returned.
   */
  abstract HiveLock[] getHiveLocks();

  /**
   * Add locks to the transaction.  It is only valid to call this when the transaction is in open
   * state.
   * @param newLocks array of locks to add.  This method assumes it can take ownership of this
   *                 array, so don't plan to do anything else with it.  All your locks are belong to
   *                 us.
   */
  abstract void addLocks(HiveLock[] newLocks);

  /**
   * Determine if this transaction held any write locks.
   * @return true if any of the locks were shared_write
   */
  abstract boolean hasWriteLocks();

  /**
   * Get the transaction id at which this was committed.  This only makes sense to call once the
   * state of the transaction is committed (duh!).
   * @return commit id.
   */
  abstract long getCommitId();
}
