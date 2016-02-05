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

public class AbortedHiveTransaction extends HiveTransaction {

  /**
   * For use when creating a new aborted transaction.
   * @param openTxn open transaction moving into aborted state.
   */
  AbortedHiveTransaction(HiveTransaction openTxn) {
    super(openTxn.getId());
    if (openTxn.getState() != HbaseMetastoreProto.Transaction.TxnState.OPEN) {
      throw new RuntimeException("Logic error, attempt to abort transaction in state " +
          openTxn.getState());
    }
  }

  /**
   * For use when recovering transactions from HBase.
   * @param hbaseTxn transaction record from HBase.
   * @throws IOException
   */
  AbortedHiveTransaction(HbaseMetastoreProto.Transaction hbaseTxn)
      throws IOException {
    super(hbaseTxn.getId());
  }

  @Override
  HbaseMetastoreProto.Transaction.TxnState getState() {
    return HbaseMetastoreProto.Transaction.TxnState.ABORTED;
  }

  @Override
  long getLastHeartbeat() {
    throw new UnsupportedOperationException("Logic error, no heartbeats for aborted transactions");
  }

  @Override
  void setLastHeartbeat(long lastHeartbeat) {
    throw new UnsupportedOperationException("Logic error, no heartbeats for aborted transactions");
  }

  @Override
  HiveLock[] getHiveLocks() {
    throw new UnsupportedOperationException("Logic error, no locks for aborted transactions");
  }

  @Override
  void addLocks(HiveLock[] newLocks) {
    throw new UnsupportedOperationException("Logic error, no locks for aborted transactions");
  }
}
