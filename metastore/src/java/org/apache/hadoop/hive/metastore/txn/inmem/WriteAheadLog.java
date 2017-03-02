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

import org.apache.hadoop.hive.metastore.api.LockRequest;
import org.apache.hadoop.hive.metastore.api.OpenTxnRequest;
import org.apache.hadoop.hive.metastore.api.OpenTxnsResponse;

import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.Future;

interface WriteAheadLog {

  enum EntryType {
    // DON'T EVER REARRANGE THESE!  WE'RE STORING THE ORDINAL VALUES IN THE DATABASE.
    OPEN_TXN, ABORT_TXN, COMMIT_TXN, REQUEST_LOCKS, ACQUIRE_LOCKS;

    private static EntryType[] vals = values();

    public static EntryType fromInteger(int val) {
      return vals[val];
    }
  }

  /**
   * Queue a WAL write for opening a transaction.
   * @param rqst Request we received from client
   * @param txnId transaction id that was assigned to this transaction
   * @return future with a reference to this write ahead log
   */
  Future<WriteAheadLog> queueOpenTxn(long txnId ,OpenTxnRequest rqst);

  Future<WriteAheadLog> queueAbortTxn(OpenTransaction openTxn);

  Future<WriteAheadLog> queueCommitTxn(OpenTransaction openTxn);

  Future<WriteAheadLog> queueLockRequest(LockRequest rqst, List<HiveLock> newLocks);

  Future<WriteAheadLog> queueLockAcquisition(List<HiveLock> acquiredLocks);
}
