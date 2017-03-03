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

class CommittedTransaction extends HiveTransaction {

  private final long commitId;

  /**
   * For use when an open transaction is committed.
   * @param openTxn open transaction being committed
   * @param commitId commit id for this transaction
   */
  CommittedTransaction(OpenTransaction openTxn, long commitId) {
    super(openTxn.getTxnId());
    this.commitId = commitId;
  }

  /**
   * For use when recovering a committed transaction.
   * @param txnId transaction id
   * @param commitId commit id
   */
  CommittedTransaction(long txnId, long commitId) {
    super(txnId);
    this.commitId = commitId;
  }


  @Override
  TxnState getState() {
    return TxnState.COMMITTED;
  }

  long getCommitId() {
    return commitId;
  }

  @Override
  void addLocks(HiveLock[] locks) {
    throw new UnsupportedOperationException("You can't add locks to a committed transaction!");
  }
}
