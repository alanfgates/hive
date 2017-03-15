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

import org.apache.hadoop.hive.metastore.api.LockState;
import org.apache.hadoop.hive.metastore.api.TxnState;

class AbortedTransaction extends HiveTransaction {

  /**
   * Constructor for normal operations where an open transaction is being aborted.
   * @param openTxn transaction to abort.
   */
  AbortedTransaction(OpenTransaction openTxn) {
    super(openTxn.getTxnId());
    buildWriteSets(openTxn);
  }

  AbortedTransaction(long txnId) {
    super(txnId);
  }

  @Override
  TxnState getState() {
    return TxnState.ABORTED;
  }

  /**
   * Determine whether all dtps written to by an aborted transaction have been compacted.
   * @return true if all have been compacted, false otherwise.
   */
  boolean fullyCompacted() {
    return writeSets.size() == 0;
  }

  /**
   * Remove an entry from the WriteSets.  This is done once we've compacted the entity and we no
   * longer need to remember it.
   * @param entityKey entity to remove from the write set
   */
  void clearWriteSet(EntityKey entityKey) {
    writeSets.remove(entityKey);
  }

}
