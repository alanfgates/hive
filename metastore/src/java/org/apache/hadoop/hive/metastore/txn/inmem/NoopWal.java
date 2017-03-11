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

import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * A WAL that ignores everything you pass it.  Think of it as a tape backup system.
 */
public class NoopWal implements WriteAheadLog {
  @Override
  public Future<Integer> queueOpenTxn(long txnId, OpenTxnRequest rqst) {
    return null;
  }

  @Override
  public Future<Integer> queueAbortTxn(OpenTransaction openTxn) {
    return null;
  }

  @Override
  public Future<Integer> queueCommitTxn(CommittedTransaction commitedTxn) {
    return null;
  }

  @Override
  public Future<Integer> queueLockRequest(LockRequest rqst, List<HiveLock> newLocks) {
    return null;
  }

  @Override
  public Future<Integer> queueLockAcquisition(List<HiveLock> acquiredLocks) {
    return null;
  }

  @Override
  public Future<Integer> queueForgetTransactions(List<? extends HiveTransaction> txns) {
    return null;
  }

  @Override
  public void waitForCheckpoint(long maxWait, TimeUnit unit) throws InterruptedException, TimeoutException {

  }

  @Override
  public void start() throws SQLException {

  }
}
