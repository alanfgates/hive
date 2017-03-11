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

import org.apache.hadoop.hive.common.ObjectPair;
import org.apache.hadoop.hive.metastore.api.LockComponent;
import org.apache.hadoop.hive.metastore.api.LockRequest;
import org.apache.hadoop.hive.metastore.api.OpenTxnRequest;
import org.apache.thrift.TException;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

interface WriteAheadLog {

  enum EntryType {
    // DON'T EVER REARRANGE THESE!  WE'RE STORING THE ORDINAL VALUES IN THE DATABASE.
    OPEN_TXN, ABORT_TXN, COMMIT_TXN, REQUEST_LOCKS, ACQUIRE_LOCKS, FORGET_TXN;

    private static EntryType[] vals = values();

    public static EntryType fromInteger(int val) {
      return vals[val];
    }
  }

  /**
   * Queue a WAL write recording opening a transaction.
   * @param rqst Request we received from client.
   * @param txnId transaction id that was assigned to this transaction.
   * @return future with a count of the number of inserted records (should be 1).  Write is not
   * guaranteed to be in * the WAL until the future.get() returns.
   */
  Future<Integer> queueOpenTxn(long txnId ,OpenTxnRequest rqst);

  /**
   * Queue a WAL write recording aborting a transaction.
   * @param openTxn the open transaction that is being aborted.
   * @return future with a count of the number of inserted records.  Write is not guaranteed to
   * be in the WAL until the future.get() returns.
   */
  Future<Integer> queueAbortTxn(OpenTransaction openTxn);

  /**
   * Queue a WAL write recording committing a transaction.
   * @param commitedTxn committed txn`
   * @return  future with a count of inserted record.  Write is not guaranteed to be in
   * * the WAL until the future.get() returns.
   */
  Future<Integer> queueCommitTxn(CommittedTransaction commitedTxn);

  /**
   * Queue a WAL write recording requesting locks.
   * @param rqst Request we received from client.
   * @param newLocks New locks that were created by the TransactionManager.
   * @return future with a count of inserted record.  Write is not guaranteed to be in
   * the WAL until the future.get() returns.
   */
  Future<Integer> queueLockRequest(LockRequest rqst, List<HiveLock> newLocks);

  /**
   * Queue a WAL write recording acquisition of locks.

   * @return future with a count of inserted record.  Write is not guaranteed to be in
   * the WAL until the future.get() returns.
   */
  Future<Integer> queueLockAcquisition(List<HiveLock> acquiredLocks);

  /**
   * Delete a transaction from the database.
   * @param txns transactions we are done with that can be removed.
   * @return future with a count of inserted record.  Write is not guaranteed to be in
   * the WAL until the future.get() returns.
   */
  Future<Integer> queueForgetTransactions(List<? extends HiveTransaction> txns);

  /**
   * This will return when all records in the WAL have been written to the database tables.  It
   * is useful for holding on things like showLocks that need all the information in the database.
   * Note, it does NOT force a checkpoint, it merely waits until the state in the database
   * reflects the state in the WAL as of the point in time that this method was called.
   * @param maxWait maximum units to wait
   * @param unit Time unit to measure wait in
   * @throws InterruptedException if Thread.sleep is interrupted
   * @throws TimeoutException if it takes more than maxWait milliseconds for the records to move.
   */
  void waitForCheckpoint(long maxWait, TimeUnit unit) throws InterruptedException, TimeoutException;

  /**
   * Start up the WAL writer.  This will first move all records from the WAL to the database (to
   * enable recovery).  It will then start up internal threads needed by the WAL.
   * @throws SQLException if something goes wrong.
   */
  void start() throws SQLException;
}
