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

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.hbase.HBaseStore;
import org.apache.hadoop.hive.metastore.hbase.HbaseMetastoreProto;
import org.apache.hadoop.hive.metastore.hbase.MockUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.MockitoAnnotations;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class TestTransactionManagerErrors extends MockUtils {
  HBaseStore store;
  TransactionManager txnMgr;
  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Before
  public void init() throws IOException {
    MockitoAnnotations.initMocks(this);
    HiveConf conf = new HiveConf();

    // Set the wait on the background threads to max long so that they don't run and clean things
    // up on us, since we're trying to check state.
    conf.set(TransactionManager.CONF_NO_AUTO_BACKGROUND_THREADS, Boolean.toString(Boolean.TRUE));
    // Set this value lower so we can fill up the txn mgr without creating an insane number of
    // objects.
    HiveConf.setIntVar(conf, HiveConf.ConfVars.METASTORE_HBASE_TXN_MGR_MAX_OBJECTS, 100);
    // Set poll timeout low so we don't wait forever for our locks to come back and tell us to wait.
    HiveConf.setTimeVar(conf, HiveConf.ConfVars.METASTORE_HBASE_TXN_MGR_LOCK_POLL_TIMEOUT, 500,
        TimeUnit.MILLISECONDS);
    // Set timeout low for timeout testing.  This should not affect other tests because in
    // general the background threads aren't running.
    HiveConf.setTimeVar(conf, HiveConf.ConfVars.HIVE_TXN_TIMEOUT, 100, TimeUnit.MILLISECONDS);
    store = mockInit(conf);
    txnMgr = txnCoProc.backdoor();
  }

  @After
  public void cleanup() throws IOException {
    txnMgr.shutdown();
  }

  @Test
  public void rejectTxnsWhenFull() throws Exception {
    // Test that we properly reject new transactions when we're full.  Note that this test does
    // not cover all conditions that can create fullness, just that once we are full the right
    // thing happens.
    HbaseMetastoreProto.OpenTxnsRequest rqst = HbaseMetastoreProto.OpenTxnsRequest.newBuilder()
        .setNumTxns(100)
        .setUser("me")
        .setHostname("localhost")
        .build();
    HbaseMetastoreProto.OpenTxnsResponse rsp = txnMgr.openTxns(rqst);
    Assert.assertEquals(100, rsp.getTxnIdsCount());

    long txnBase = rsp.getTxnIds(0);

    // One more should still work because the full checker hasn't run yet.
    assertNotFull();

    txnMgr.forceFullChecker();
    assertFull();

    // Close just a couple to make sure we have to go below 90% before it opens back up.
    closeTxns(txnBase, txnBase + 5);
    txnMgr.forceFullChecker();
    assertFull();

    // Close enough to get us under the limit
    closeTxns(txnBase + 5, txnBase + 12);
    txnMgr.forceFullChecker();
    assertNotFull();

    closeTxns(txnBase + 12, txnBase + 101);
    txnMgr.forceFullChecker();
    assertNotFull();
  }

  private void assertFull() throws SeverusPleaseException {
    boolean sawException = false;
    try {
      HbaseMetastoreProto.OpenTxnsRequest rqst = HbaseMetastoreProto.OpenTxnsRequest.newBuilder()
          .setNumTxns(1)
          .setUser("me")
          .setHostname("localhost")
          .build();
      txnMgr.openTxns(rqst);
    } catch (IOException e) {
      sawException = true;
      Assert.assertEquals("Full, no new transactions being accepted", e.getMessage());
    }
    Assert.assertTrue(sawException);
  }

  private void assertNotFull() throws Exception {
     HbaseMetastoreProto.OpenTxnsRequest rqst = HbaseMetastoreProto.OpenTxnsRequest.newBuilder()
        .setNumTxns(1)
        .setUser("me")
        .setHostname("localhost")
        .build();
    HbaseMetastoreProto.OpenTxnsResponse rsp = txnMgr.openTxns(rqst);
    Assert.assertEquals(1, rsp.getTxnIdsCount());
  }

  private void closeTxns(long min, long max) throws IOException, SeverusPleaseException {
    for (long i = min; i < max; i++) {
      txnMgr.commitTxn(HbaseMetastoreProto.TransactionId.newBuilder()
          .setId(i)
          .build());
    }
  }

  @Test
  public void abortNoSuchTxn() throws Exception {
    HbaseMetastoreProto.TransactionResult result = txnMgr.abortTxn(HbaseMetastoreProto.TransactionId
        .newBuilder()
        .setId(10000)
        .build());
    Assert.assertEquals(HbaseMetastoreProto.TxnStateChangeResult.NO_SUCH_TXN, result.getState());
  }

  @Test
  public void abortAbortedTxn() throws Exception {
    HbaseMetastoreProto.OpenTxnsRequest rqst = HbaseMetastoreProto.OpenTxnsRequest.newBuilder()
        .setNumTxns(1)
        .setUser("me")
        .setHostname("localhost")
        .build();
    HbaseMetastoreProto.OpenTxnsResponse rsp = txnMgr.openTxns(rqst);
    long txnId = rsp.getTxnIds(0);

    // Abort it once, this should work
    HbaseMetastoreProto.TransactionResult result = txnMgr.abortTxn(HbaseMetastoreProto.TransactionId
        .newBuilder()
        .setId(txnId)
        .build());
    Assert.assertEquals(HbaseMetastoreProto.TxnStateChangeResult.SUCCESS, result.getState());

    // Abort it again, this should bork
    result = txnMgr.abortTxn(HbaseMetastoreProto.TransactionId
        .newBuilder()
        .setId(txnId)
        .build());
    Assert.assertEquals(HbaseMetastoreProto.TxnStateChangeResult.NO_SUCH_TXN, result.getState());
  }

  @Test
  public void abortCommittedTxn() throws Exception {
    HbaseMetastoreProto.OpenTxnsRequest rqst = HbaseMetastoreProto.OpenTxnsRequest.newBuilder()
        .setNumTxns(1)
        .setUser("me")
        .setHostname("localhost")
        .build();
    HbaseMetastoreProto.OpenTxnsResponse rsp = txnMgr.openTxns(rqst);
    long txnId = rsp.getTxnIds(0);

    // Commit it, this should work
    HbaseMetastoreProto.TransactionResult result = txnMgr.commitTxn(HbaseMetastoreProto.TransactionId
        .newBuilder()
        .setId(txnId)
        .build());
    Assert.assertEquals(HbaseMetastoreProto.TxnStateChangeResult.SUCCESS, result.getState());

    // Abort it, this should bork
    result = txnMgr.abortTxn(HbaseMetastoreProto.TransactionId
        .newBuilder()
        .setId(txnId)
        .build());
    Assert.assertEquals(HbaseMetastoreProto.TxnStateChangeResult.NO_SUCH_TXN, result.getState());
  }

  @Test
  public void commitNoSuchTxn() throws Exception {
    HbaseMetastoreProto.TransactionResult result = txnMgr.commitTxn(
        HbaseMetastoreProto.TransactionId
            .newBuilder()
            .setId(10000)
            .build());
    Assert.assertEquals(HbaseMetastoreProto.TxnStateChangeResult.NO_SUCH_TXN, result.getState());
  }

  @Test
  public void commitAbortedTxn() throws Exception {
    HbaseMetastoreProto.OpenTxnsRequest rqst = HbaseMetastoreProto.OpenTxnsRequest.newBuilder()
        .setNumTxns(1)
        .setUser("me")
        .setHostname("localhost")
        .build();
    HbaseMetastoreProto.OpenTxnsResponse rsp = txnMgr.openTxns(rqst);
    long txnId = rsp.getTxnIds(0);

    // Abort it, this should work
    HbaseMetastoreProto.TransactionResult result = txnMgr.abortTxn(HbaseMetastoreProto.TransactionId
        .newBuilder()
        .setId(txnId)
        .build());
    Assert.assertEquals(HbaseMetastoreProto.TxnStateChangeResult.SUCCESS, result.getState());

    // Commit it, this should bork
    result = txnMgr.commitTxn(HbaseMetastoreProto.TransactionId
        .newBuilder()
        .setId(txnId)
        .build());
    Assert.assertEquals(HbaseMetastoreProto.TxnStateChangeResult.NO_SUCH_TXN, result.getState());
  }

  @Test
  public void commitCommittedTxn() throws Exception {
    HbaseMetastoreProto.OpenTxnsRequest rqst = HbaseMetastoreProto.OpenTxnsRequest.newBuilder()
        .setNumTxns(1)
        .setUser("me")
        .setHostname("localhost")
        .build();
    HbaseMetastoreProto.OpenTxnsResponse rsp = txnMgr.openTxns(rqst);
    long txnId = rsp.getTxnIds(0);

    // Commit it, this should work
    HbaseMetastoreProto.TransactionResult result = txnMgr.commitTxn(HbaseMetastoreProto.TransactionId
        .newBuilder()
        .setId(txnId)
        .build());
    Assert.assertEquals(HbaseMetastoreProto.TxnStateChangeResult.SUCCESS, result.getState());

    // Commit it again, this should bork
    result = txnMgr.commitTxn(HbaseMetastoreProto.TransactionId
        .newBuilder()
        .setId(txnId)
        .build());
    Assert.assertEquals(HbaseMetastoreProto.TxnStateChangeResult.NO_SUCH_TXN, result.getState());
  }

  @Test
  public void lockNoSuchTxn() throws Exception {
    HbaseMetastoreProto.LockResponse result = txnMgr.lock(HbaseMetastoreProto.LockRequest
        .newBuilder()
        .setTxnId(10000)
        .addComponents(HbaseMetastoreProto.LockComponent.newBuilder()
            .setDb("x")
            .setType(HbaseMetastoreProto.LockType.EXCLUSIVE))
        .build());
    Assert.assertEquals(HbaseMetastoreProto.LockState.TXN_ABORTED, result.getState());
  }

  @Test
  public void lockAbortedTxn() throws Exception {
    HbaseMetastoreProto.OpenTxnsRequest rqst = HbaseMetastoreProto.OpenTxnsRequest.newBuilder()
        .setNumTxns(1)
        .setUser("me")
        .setHostname("localhost")
        .build();
    HbaseMetastoreProto.OpenTxnsResponse rsp = txnMgr.openTxns(rqst);
    long txnId = rsp.getTxnIds(0);

    // Abort it, this should work
    HbaseMetastoreProto.TransactionResult result = txnMgr.abortTxn(HbaseMetastoreProto.TransactionId
        .newBuilder()
        .setId(txnId)
        .build());
    Assert.assertEquals(HbaseMetastoreProto.TxnStateChangeResult.SUCCESS, result.getState());

    // lock it, this should bork
    HbaseMetastoreProto.LockResponse lock = txnMgr.lock(HbaseMetastoreProto.LockRequest
        .newBuilder()
        .setTxnId(txnId)
        .addComponents(HbaseMetastoreProto.LockComponent.newBuilder()
            .setDb("x")
            .setType(HbaseMetastoreProto.LockType.EXCLUSIVE))
        .build());
    Assert.assertEquals(HbaseMetastoreProto.LockState.TXN_ABORTED, lock.getState());
  }

  @Test
  public void lockCommittedTxn() throws Exception {
    HbaseMetastoreProto.OpenTxnsRequest rqst = HbaseMetastoreProto.OpenTxnsRequest.newBuilder()
        .setNumTxns(1)
        .setUser("me")
        .setHostname("localhost")
        .build();
    HbaseMetastoreProto.OpenTxnsResponse rsp = txnMgr.openTxns(rqst);
    long txnId = rsp.getTxnIds(0);

    // Commit it, this should work
    HbaseMetastoreProto.TransactionResult result = txnMgr.commitTxn(
        HbaseMetastoreProto.TransactionId
            .newBuilder()
            .setId(txnId)
            .build());
    Assert.assertEquals(HbaseMetastoreProto.TxnStateChangeResult.SUCCESS, result.getState());

    // lock it, this should bork
    HbaseMetastoreProto.LockResponse lock = txnMgr.lock(HbaseMetastoreProto.LockRequest
        .newBuilder()
        .setTxnId(txnId)
        .addComponents(HbaseMetastoreProto.LockComponent.newBuilder()
            .setDb("x")
            .setType(HbaseMetastoreProto.LockType.EXCLUSIVE))
        .build());
    Assert.assertEquals(HbaseMetastoreProto.LockState.TXN_ABORTED, lock.getState());
  }

  @Test
  public void checkLocksNoSuchTxn() throws Exception {
    HbaseMetastoreProto.LockResponse result = txnMgr.checkLocks(HbaseMetastoreProto.TransactionId
        .newBuilder()
        .setId(10000)
        .build());
    Assert.assertEquals(HbaseMetastoreProto.LockState.TXN_ABORTED, result.getState());
  }

  @Test
  public void checkLocksAbortedTxn() throws Exception {
    HbaseMetastoreProto.OpenTxnsRequest rqst = HbaseMetastoreProto.OpenTxnsRequest.newBuilder()
        .setNumTxns(1)
        .setUser("me")
        .setHostname("localhost")
        .build();
    HbaseMetastoreProto.OpenTxnsResponse rsp = txnMgr.openTxns(rqst);
    long txnId = rsp.getTxnIds(0);

    // Abort it, this should work
    HbaseMetastoreProto.TransactionResult result = txnMgr.abortTxn(HbaseMetastoreProto.TransactionId
        .newBuilder()
        .setId(txnId)
        .build());
    Assert.assertEquals(HbaseMetastoreProto.TxnStateChangeResult.SUCCESS, result.getState());

    // checkLocks it, this should bork
    HbaseMetastoreProto.LockResponse lock = txnMgr.checkLocks(HbaseMetastoreProto.TransactionId
        .newBuilder()
        .setId(txnId)
        .build());
    Assert.assertEquals(HbaseMetastoreProto.LockState.TXN_ABORTED, lock.getState());
  }

  @Test
  public void checkLocksCommittedTxn() throws Exception {
    HbaseMetastoreProto.OpenTxnsRequest rqst = HbaseMetastoreProto.OpenTxnsRequest.newBuilder()
        .setNumTxns(1)
        .setUser("me")
        .setHostname("localhost")
        .build();
    HbaseMetastoreProto.OpenTxnsResponse rsp = txnMgr.openTxns(rqst);
    long txnId = rsp.getTxnIds(0);

    // Commit it, this should work
    HbaseMetastoreProto.TransactionResult result = txnMgr.commitTxn(
        HbaseMetastoreProto.TransactionId
            .newBuilder()
            .setId(txnId)
            .build());
    Assert.assertEquals(HbaseMetastoreProto.TxnStateChangeResult.SUCCESS, result.getState());

    // lock it, this should bork
    HbaseMetastoreProto.LockResponse lock = txnMgr.checkLocks(HbaseMetastoreProto.TransactionId
        .newBuilder()
        .setId(txnId)
        .build());
    Assert.assertEquals(HbaseMetastoreProto.LockState.TXN_ABORTED, lock.getState());
  }

  @Test
  public void dynamicPartitionsNoSuchTxn() throws Exception {
    HbaseMetastoreProto.TransactionResult result =
        txnMgr.addDynamicPartitions(HbaseMetastoreProto.AddDynamicPartitionsRequest
            .newBuilder()
            .setTxnId(10000)
            .setDb("a")
            .setTable("b")
            .addAllPartitions(Arrays.asList("p1", "p2"))
            .build());
    Assert.assertEquals(HbaseMetastoreProto.TxnStateChangeResult.NO_SUCH_TXN, result.getState());
  }

  @Test
  public void dynamicPartitionsAbortedTxn() throws Exception {
    HbaseMetastoreProto.OpenTxnsRequest rqst = HbaseMetastoreProto.OpenTxnsRequest.newBuilder()
        .setNumTxns(1)
        .setUser("me")
        .setHostname("localhost")
        .build();
    HbaseMetastoreProto.OpenTxnsResponse rsp = txnMgr.openTxns(rqst);
    long txnId = rsp.getTxnIds(0);

    // Abort it, this should work
    HbaseMetastoreProto.TransactionResult result = txnMgr.abortTxn(HbaseMetastoreProto.TransactionId
        .newBuilder()
        .setId(txnId)
        .build());
    Assert.assertEquals(HbaseMetastoreProto.TxnStateChangeResult.SUCCESS, result.getState());

    // add dynamic partitions, this should bork
    result = txnMgr.addDynamicPartitions(HbaseMetastoreProto.AddDynamicPartitionsRequest
            .newBuilder()
            .setTxnId(txnId)
            .setDb("a")
            .setTable("b")
            .addAllPartitions(Arrays.asList("p1", "p2"))
            .build());
    Assert.assertEquals(HbaseMetastoreProto.TxnStateChangeResult.NO_SUCH_TXN, result.getState());
  }

  @Test
  public void addDynamicPartitionsCommittedTxn() throws Exception {
    HbaseMetastoreProto.OpenTxnsRequest rqst = HbaseMetastoreProto.OpenTxnsRequest.newBuilder()
        .setNumTxns(1)
        .setUser("me")
        .setHostname("localhost")
        .build();
    HbaseMetastoreProto.OpenTxnsResponse rsp = txnMgr.openTxns(rqst);
    long txnId = rsp.getTxnIds(0);

    // Commit it, this should work
    HbaseMetastoreProto.TransactionResult result = txnMgr.commitTxn(
        HbaseMetastoreProto.TransactionId
            .newBuilder()
            .setId(txnId)
            .build());
    Assert.assertEquals(HbaseMetastoreProto.TxnStateChangeResult.SUCCESS, result.getState());

    // add dynamic partitions, this should bork
    result = txnMgr.addDynamicPartitions(HbaseMetastoreProto.AddDynamicPartitionsRequest
        .newBuilder()
        .setTxnId(txnId)
        .setDb("a")
        .setTable("b")
        .addAllPartitions(Arrays.asList("p1", "p2"))
        .build());
    Assert.assertEquals(HbaseMetastoreProto.TxnStateChangeResult.NO_SUCH_TXN, result.getState());
  }

  @Test
  public void runDeadlockDetectorNoDeadlocks() throws Exception {
    String db = "rddnd_db";

    HbaseMetastoreProto.OpenTxnsRequest rqst = HbaseMetastoreProto.OpenTxnsRequest.newBuilder()
        .setNumTxns(2)
        .setUser("me")
        .setHostname("localhost")
        .build();
    HbaseMetastoreProto.OpenTxnsResponse rsp = txnMgr.openTxns(rqst);
    long firstTxn = rsp.getTxnIds(0);
    long secondTxn = rsp.getTxnIds(1);

    HbaseMetastoreProto.LockResponse lock = txnMgr.lock(HbaseMetastoreProto.LockRequest.newBuilder()
        .setTxnId(firstTxn)
        .addComponents(HbaseMetastoreProto.LockComponent.newBuilder()
            .setDb(db)
            .setType(HbaseMetastoreProto.LockType.EXCLUSIVE))
        .build());
    Assert.assertEquals(HbaseMetastoreProto.LockState.ACQUIRED, lock.getState());

    lock = txnMgr.lock(HbaseMetastoreProto.LockRequest.newBuilder()
        .setTxnId(secondTxn)
        .addComponents(HbaseMetastoreProto.LockComponent.newBuilder()
            .setDb(db)
            .setType(HbaseMetastoreProto.LockType.EXCLUSIVE))
        .build());
    Assert.assertEquals(HbaseMetastoreProto.LockState.WAITING, lock.getState());

    // This should not change anything.
    txnMgr.forceDeadlockDetection();

    HbaseMetastoreProto.LockResponse firstLock =
        txnMgr.checkLocks(HbaseMetastoreProto.TransactionId.newBuilder()
            .setId(firstTxn)
            .build());
    HbaseMetastoreProto.LockResponse secondLock =
        txnMgr.checkLocks(HbaseMetastoreProto.TransactionId.newBuilder()
            .setId(secondTxn)
            .build());

    Assert.assertEquals(HbaseMetastoreProto.LockState.ACQUIRED, firstLock.getState());
    Assert.assertEquals(HbaseMetastoreProto.LockState.WAITING, secondLock.getState());
  }

  @Test
  public void simpleDeadlock() throws Exception {
    String db1 = "sdl_db1";
    String db2 = "sdl_db2";

    HbaseMetastoreProto.OpenTxnsRequest rqst = HbaseMetastoreProto.OpenTxnsRequest.newBuilder()
        .setNumTxns(2)
        .setUser("me")
        .setHostname("localhost")
        .build();
    HbaseMetastoreProto.OpenTxnsResponse rsp = txnMgr.openTxns(rqst);
    long firstTxn = rsp.getTxnIds(0);
    long secondTxn = rsp.getTxnIds(1);

    HbaseMetastoreProto.LockResponse lock = txnMgr.lock(HbaseMetastoreProto.LockRequest.newBuilder()
        .setTxnId(firstTxn)
        .addComponents(HbaseMetastoreProto.LockComponent.newBuilder()
            .setDb(db1)
            .setType(HbaseMetastoreProto.LockType.EXCLUSIVE))
        .build());
    Assert.assertEquals(HbaseMetastoreProto.LockState.ACQUIRED, lock.getState());

    lock = txnMgr.lock(HbaseMetastoreProto.LockRequest.newBuilder()
        .setTxnId(secondTxn)
        .addComponents(HbaseMetastoreProto.LockComponent.newBuilder()
            .setDb(db1)
            .setType(HbaseMetastoreProto.LockType.EXCLUSIVE))
        .addComponents(HbaseMetastoreProto.LockComponent.newBuilder()
            .setDb(db2)
            .setType(HbaseMetastoreProto.LockType.EXCLUSIVE))
        .build());
    Assert.assertEquals(HbaseMetastoreProto.LockState.WAITING, lock.getState());

    lock = txnMgr.lock(HbaseMetastoreProto.LockRequest.newBuilder()
        .setTxnId(firstTxn)
        .addComponents(HbaseMetastoreProto.LockComponent.newBuilder()
            .setDb(db2)
            .setType(HbaseMetastoreProto.LockType.EXCLUSIVE))
        .build());
    Assert.assertEquals(HbaseMetastoreProto.LockState.WAITING, lock.getState());

    lock = txnMgr.checkLocks(HbaseMetastoreProto.TransactionId.newBuilder()
        .setId(firstTxn)
        .build());
    Assert.assertEquals(HbaseMetastoreProto.LockState.WAITING, lock.getState());

    lock = txnMgr.checkLocks(HbaseMetastoreProto.TransactionId.newBuilder()
        .setId(secondTxn)
        .build());
    Assert.assertEquals(HbaseMetastoreProto.LockState.WAITING, lock.getState());

    txnMgr.forceDeadlockDetection();

    HbaseMetastoreProto.LockResponse firstLock =
        txnMgr.checkLocks(HbaseMetastoreProto.TransactionId.newBuilder()
        .setId(firstTxn)
        .build());
    HbaseMetastoreProto.LockResponse secondLock =
        txnMgr.checkLocks(HbaseMetastoreProto.TransactionId.newBuilder()
        .setId(secondTxn)
        .build());
    if (firstLock.getState() == HbaseMetastoreProto.LockState.ACQUIRED) {
      Assert.assertEquals(HbaseMetastoreProto.LockState.TXN_ABORTED, secondLock.getState());
    } else if (firstLock.getState() == HbaseMetastoreProto.LockState.TXN_ABORTED) {
      Assert.assertEquals(HbaseMetastoreProto.LockState.ACQUIRED, secondLock.getState());
    } else {
      Assert.fail("Completely unexpected state " + firstLock.getState());
    }

    // One of them should be dead, and the other acquired, but which is which doesn't matter.
  }

  @Test
  public void timeout() throws Exception {
    HbaseMetastoreProto.OpenTxnsRequest rqst = HbaseMetastoreProto.OpenTxnsRequest.newBuilder()
        .setNumTxns(2)
        .setUser("me")
        .setHostname("localhost")
        .build();
    HbaseMetastoreProto.OpenTxnsResponse rsp = txnMgr.openTxns(rqst);
    long firstTxn = rsp.getTxnIds(0);
    long secondTxn = rsp.getTxnIds(1);

    Thread.sleep(100);
    HbaseMetastoreProto.HeartbeatTxnRangeResponse heartbeats =
        txnMgr.heartbeat(HbaseMetastoreProto.HeartbeatTxnRangeRequest.newBuilder()
            .setMinTxn(firstTxn)
            .setMaxTxn(secondTxn)
            .build());
    txnMgr.forceTimedOutCleaner();

    Map<Long, OpenHiveTransaction> openTxns = txnMgr.copyOpenTransactions();
    Assert.assertNotNull(openTxns.get(firstTxn));
    Assert.assertNotNull(openTxns.get(secondTxn));

    Thread.sleep(100);
    heartbeats = txnMgr.heartbeat(HbaseMetastoreProto.HeartbeatTxnRangeRequest.newBuilder()
            .setMinTxn(secondTxn)
            .setMaxTxn(secondTxn)
            .build());
    txnMgr.forceTimedOutCleaner();

    openTxns = txnMgr.copyOpenTransactions();
    Assert.assertNull(openTxns.get(firstTxn));
    Assert.assertNotNull(openTxns.get(secondTxn));

    Thread.sleep(100);
    txnMgr.forceTimedOutCleaner();
    openTxns = txnMgr.copyOpenTransactions();
    Assert.assertNull(openTxns.get(secondTxn));
  }

  // TODO test deadlock with another txn in the middle

  // TODO test all conditions that can lead to fullness
}
