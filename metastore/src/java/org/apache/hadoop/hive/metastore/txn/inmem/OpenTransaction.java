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

import org.apache.hadoop.hive.metastore.api.TxnState;

import java.util.Arrays;

class OpenTransaction extends HiveTransaction {

  private long lastHeartbeat;

  // I chose an array over a list so that I could explicitly control growth.  ArrayList is memory
  // efficient (only 4 more bytes than an array I believe) and you can control the initial
  // capacity, but when it grows you loose control of how.
  private HiveLock[] hiveLocks;

  OpenTransaction(long txnId) {
    super(txnId);
    lastHeartbeat = System.currentTimeMillis();
  }

  // TODO constructor for recovering from db


  @Override
  TxnState getState() {
    return TxnState.OPEN;
  }

  public long getLastHeartbeat() {
    return lastHeartbeat;
  }

  public void setLastHeartbeat(long lastHeartbeat) {
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
  @Override
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
