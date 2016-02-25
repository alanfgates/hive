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

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.hbase.HBaseReadWrite;
import org.apache.hadoop.hive.metastore.hbase.HBaseStore;
import org.apache.hadoop.hive.metastore.hbase.HbaseMetastoreProto;
import org.apache.hadoop.hive.metastore.hbase.MockUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

public class TestTransactionManager {

  @Mock
  HTableInterface htable;
  SortedMap<String, Cell> rows = new TreeMap<>();
  HBaseStore store;
  TransactionManager txnMgr;
  HBaseReadWrite hrw;

  @Before
  public void init() throws IOException {
    MockitoAnnotations.initMocks(this);
    HiveConf conf = new HiveConf();

    // Set the wait on the background threads to max long so that they don't run and clean things
    // up on us, since we're trying to check state.
    conf.set(TransactionManager.CONF_INITIAL_DELAY, Long.toString(Long.MAX_VALUE));

    store = MockUtils.init(conf, htable, rows);
    txnMgr = new TransactionManager(conf);
    hrw = HBaseReadWrite.getInstance();
  }

  @After
  public void cleanup() throws IOException {
    txnMgr.shutdown();
  }

  @Test
  public void openAndAbort() throws Exception {

    HbaseMetastoreProto.GetOpenTxnsResponse before =
        txnMgr.getOpenTxns(HbaseMetastoreProto.Void.getDefaultInstance());
    Map<Long, OpenHiveTransaction> memoryBefore = txnMgr.copyOpenTransactions();

    // Open a single transaction
    HbaseMetastoreProto.OpenTxnsRequest rqst = HbaseMetastoreProto.OpenTxnsRequest.newBuilder()
        .setNumTxns(1)
        .setUser("me")
        .setHostname("localhost")
        .build();
    HbaseMetastoreProto.OpenTxnsResponse rsp = txnMgr.openTxns(rqst);
    Assert.assertEquals(1, rsp.getTxnIdsCount());
    long txnId = rsp.getTxnIds(0);

    HbaseMetastoreProto.GetOpenTxnsResponse after =
        txnMgr.getOpenTxns(HbaseMetastoreProto.Void.getDefaultInstance());
    Map<Long, OpenHiveTransaction> memoryAfter = txnMgr.copyOpenTransactions();
    Map<Long, AbortedHiveTransaction> abortedMemoryAfter = txnMgr.copyAbortedTransactions();

    // Check that our transaction id is the same as the previous high water mark
    Assert.assertEquals(before.getHighWaterMark(), txnId);

    // Check that the highwater mark went up by one
    Assert.assertEquals(before.getHighWaterMark() + 1, after.getHighWaterMark());

    // Check that we have one more open transaction and the same number of aborted transactions
    Assert.assertEquals(before.getOpenTransactionsCount() + 1, after.getOpenTransactionsCount());
    Assert.assertEquals(before.getAbortedTransactionsCount(), after.getAbortedTransactionsCount());

    // Make sure only one value got added to the in memory structure
    Assert.assertEquals(memoryBefore.size() + 1, memoryAfter.size());

    // Make sure our entry is in memory
    OpenHiveTransaction newTxn = memoryAfter.get(txnId);
    Assert.assertNotNull(newTxn);

    // Make sure our entry is in the open state
    Assert.assertEquals(HbaseMetastoreProto.TxnState.OPEN, newTxn.getState());

    // Make sure the heartbeat is set
    Assert.assertTrue(newTxn.getLastHeartbeat() > 0);

    // Make sure it doesn't have any locks
    Assert.assertNull(newTxn.getHiveLocks());

    // Check the transaction in HBase
    HbaseMetastoreProto.Transaction hbaseTxn = hrw.getTransaction(txnId);
    Assert.assertNotNull(hbaseTxn);
    Assert.assertEquals(txnId, hbaseTxn.getId());
    Assert.assertEquals(0, hbaseTxn.getLocksCount());
    Assert.assertEquals("me", hbaseTxn.getUser());
    Assert.assertEquals("localhost", hbaseTxn.getHostname());
    Assert.assertEquals(HbaseMetastoreProto.TxnState.OPEN, hbaseTxn.getTxnState());

    // Abort this transaction.  It should promptly be forgotten as it has no locks
    HbaseMetastoreProto.TransactionResult abort =
        txnMgr.abortTxn(HbaseMetastoreProto.TransactionId.newBuilder()
            .setId(txnId)
            .build());
    Assert.assertEquals(HbaseMetastoreProto.TxnStateChangeResult.SUCCESS, abort.getState());

    HbaseMetastoreProto.GetOpenTxnsResponse afterAbort =
        txnMgr.getOpenTxns(HbaseMetastoreProto.Void.getDefaultInstance());
    Map<Long, OpenHiveTransaction> memoryAfterAbort = txnMgr.copyOpenTransactions();
    Map<Long, AbortedHiveTransaction> abortedMemoryAfterAbort = txnMgr.copyAbortedTransactions();

    // We should have the same number of aborts as before in the open txns response, since this
    // one should have been immediately forgotten
    Assert.assertEquals(before.getAbortedTransactionsCount(), afterAbort.getAbortedTransactionsCount());
    Assert.assertEquals(memoryBefore.size(), memoryAfterAbort.size());

    // Nothing should have been added to the aborted list because we didn't have any write locks
    Assert.assertEquals(abortedMemoryAfter.size(), abortedMemoryAfterAbort.size());
    Assert.assertNull(abortedMemoryAfterAbort.get(txnId));

    // Check that the transaction was forgotten in HBase as well
    hbaseTxn = hrw.getTransaction(txnId);
    Assert.assertNull(hbaseTxn);
  }

  @Test
  public void openAndCommit() throws Exception {

    HbaseMetastoreProto.GetOpenTxnsResponse before =
        txnMgr.getOpenTxns(HbaseMetastoreProto.Void.getDefaultInstance());
    Map<Long, OpenHiveTransaction> memoryBefore = txnMgr.copyOpenTransactions();

    // Open a single transaction
    HbaseMetastoreProto.OpenTxnsRequest rqst = HbaseMetastoreProto.OpenTxnsRequest.newBuilder()
        .setNumTxns(1)
        .setUser("me")
        .setHostname("localhost")
        .build();
    HbaseMetastoreProto.OpenTxnsResponse rsp = txnMgr.openTxns(rqst);
    Assert.assertEquals(1, rsp.getTxnIdsCount());
    long txnId = rsp.getTxnIds(0);

    Set<CommittedHiveTransaction> committedMemoryAfter = txnMgr.copyCommittedTransactions();

    // Commit this transaction.  It should promptly be forgotten as it has no locks
    HbaseMetastoreProto.TransactionResult commit =
        txnMgr.commitTxn(HbaseMetastoreProto.TransactionId.newBuilder()
            .setId(txnId)
            .build());
    Assert.assertEquals(HbaseMetastoreProto.TxnStateChangeResult.SUCCESS, commit.getState());

    Map<Long, OpenHiveTransaction> memoryAfterCommit = txnMgr.copyOpenTransactions();
    Set<CommittedHiveTransaction> committedMemoryAfterCommit = txnMgr.copyCommittedTransactions();

    // We should have the same number of aborts as before in the open txns response, since this
    // one should have been immediately forgotten
    Assert.assertEquals(memoryBefore.size(), memoryAfterCommit.size());

    // Nothing should have been added to the committed list because we didn't have any write locks
    Assert.assertEquals(committedMemoryAfter.size(), committedMemoryAfterCommit.size());
    for (CommittedHiveTransaction committed : committedMemoryAfterCommit) {
      Assert.assertNotEquals(txnId, committed.getId());
    }

    // Check that the transaction was forgotten in HBase as well
    HbaseMetastoreProto.Transaction hbaseTxn = hrw.getTransaction(txnId);
    Assert.assertNull(hbaseTxn);
  }

  @Test
  public void openLockAbort() throws Exception {

    String db1 = "ola_db1";
    String db2 = "ola_db2";
    String t2 = "ola_t2";
    String db3 = "ola_db3";
    String t3 = "ola_t3";
    String p3 = "ola_p3";

    HbaseMetastoreProto.GetOpenTxnsResponse before =
        txnMgr.getOpenTxns(HbaseMetastoreProto.Void.getDefaultInstance());
    Map<Long, OpenHiveTransaction> memoryBefore = txnMgr.copyOpenTransactions();

    HbaseMetastoreProto.OpenTxnsRequest rqst = HbaseMetastoreProto.OpenTxnsRequest.newBuilder()
        .setNumTxns(2)
        .setUser("me")
        .setHostname("localhost")
        .build();
    HbaseMetastoreProto.OpenTxnsResponse rsp = txnMgr.openTxns(rqst);
    Assert.assertEquals(2, rsp.getTxnIdsCount());
    long firstTxnId = rsp.getTxnIds(0);
    long secondsTxnId = rsp.getTxnIds(1);

    HbaseMetastoreProto.GetOpenTxnsResponse after =
        txnMgr.getOpenTxns(HbaseMetastoreProto.Void.getDefaultInstance());
    Map<Long, OpenHiveTransaction> memoryAfter = txnMgr.copyOpenTransactions();
    Map<Long, AbortedHiveTransaction> abortedMemoryAfter = txnMgr.copyAbortedTransactions();

    // Check that the highwater mark went up by two
    Assert.assertEquals(before.getHighWaterMark() + 2, after.getHighWaterMark());

    // Check that we have two more open transaction and the same number of aborted transactions
    Assert.assertEquals(before.getOpenTransactionsCount() + 2, after.getOpenTransactionsCount());
    Assert.assertEquals(before.getAbortedTransactionsCount(), after.getAbortedTransactionsCount());

    // Make sure two values got added to the in memory structure
    Assert.assertEquals(memoryBefore.size() + 2, memoryAfter.size());

    // Make both entries are in memory
    for (long i = firstTxnId; i <= secondsTxnId; i++) {
      OpenHiveTransaction newTxn = memoryAfter.get(i);
      Assert.assertNotNull(newTxn);

      // Make sure our entry is in the open state
      Assert.assertEquals(HbaseMetastoreProto.TxnState.OPEN, newTxn.getState());

      // Make sure the heartbeat is set
      Assert.assertTrue(newTxn.getLastHeartbeat() > 0);

      // Make sure it doesn't have any locks
      Assert.assertNull(newTxn.getHiveLocks());

      // Check the transaction in HBase
      HbaseMetastoreProto.Transaction hbaseTxn = hrw.getTransaction(i);
      Assert.assertNotNull(hbaseTxn);
      Assert.assertEquals(i, hbaseTxn.getId());
      Assert.assertEquals(0, hbaseTxn.getLocksCount());
      Assert.assertEquals("me", hbaseTxn.getUser());
      Assert.assertEquals("localhost", hbaseTxn.getHostname());
      Assert.assertEquals(HbaseMetastoreProto.TxnState.OPEN, hbaseTxn.getTxnState());
    }

    // Get some locks.  We'll do X, SW, and I on one and SR and I on the other
    // I use unique entity names to guarantee I'm creating new queues in the lockQueues
    Map<TransactionManager.EntityKey, TransactionManager.LockQueue> locksBefore =
        txnMgr.copyLockQueues();

    HbaseMetastoreProto.LockResponse firstLock = txnMgr.lock(HbaseMetastoreProto.LockRequest.newBuilder()
            .setTxnId(firstTxnId)
            .addComponents(HbaseMetastoreProto.LockComponent.newBuilder()
                .setDb(db1)
                .setType(HbaseMetastoreProto.LockType.EXCLUSIVE))
            .addComponents(HbaseMetastoreProto.LockComponent.newBuilder()
                .setDb(db2)
                .setType(HbaseMetastoreProto.LockType.INTENTION))
            .addComponents(HbaseMetastoreProto.LockComponent.newBuilder()
                .setDb(db2)
                .setTable(t2)
                .setType(HbaseMetastoreProto.LockType.SHARED_WRITE))
            .build());
    Assert.assertEquals(HbaseMetastoreProto.LockState.ACQUIRED, firstLock.getState());

    HbaseMetastoreProto.LockResponse secondLock = txnMgr.lock(HbaseMetastoreProto.LockRequest.newBuilder()
        .setTxnId(secondsTxnId)
        .addComponents(HbaseMetastoreProto.LockComponent.newBuilder()
            .setDb(db3)
            .setType(HbaseMetastoreProto.LockType.INTENTION))
        .addComponents(HbaseMetastoreProto.LockComponent.newBuilder()
            .setDb(db3)
            .setTable(t3)
            .setType(HbaseMetastoreProto.LockType.INTENTION))
        .addComponents(HbaseMetastoreProto.LockComponent.newBuilder()
            .setDb(db3)
            .setTable(t3)
            .setPartition(p3)
            .setType(HbaseMetastoreProto.LockType.SHARED_READ))
        .build());
    Assert.assertEquals(HbaseMetastoreProto.LockState.ACQUIRED, secondLock.getState());

    // Make sure the in memory transactions have the right locks.  We don't have to get a new
    // copy of the map because only the map is copied, not the contents.
    HiveLock[] locks = memoryAfter.get(firstTxnId).getHiveLocks();
    Assert.assertEquals(3, locks.length);
    locks = memoryAfter.get(secondsTxnId).getHiveLocks();
    Assert.assertEquals(3, locks.length);

    // We'll check the actual contents of the locks when checking the lock queues

    Map<TransactionManager.EntityKey, TransactionManager.LockQueue> locksAfter =
        txnMgr.copyLockQueues();
    Assert.assertEquals(locksBefore.size() + 6, locksAfter.size());

    TransactionManager.EntityKey key = new TransactionManager.EntityKey(db1, null, null);
    TransactionManager.LockQueue queue = locksAfter.get(key);
    Assert.assertNotNull(queue);
    Assert.assertEquals(1, queue.queue.size());
    Assert.assertEquals(0L, queue.maxCommitId);
    HiveLock lock = queue.queue.values().iterator().next();
    Assert.assertEquals(firstTxnId, lock.getTxnId());
    Assert.assertEquals(HbaseMetastoreProto.LockType.EXCLUSIVE, lock.getType());
    Assert.assertEquals(HbaseMetastoreProto.LockState.ACQUIRED, lock.getState());
    Assert.assertEquals(key, lock.getEntityLocked());

    key = new TransactionManager.EntityKey(db2, null, null);
    queue = locksAfter.get(key);
    Assert.assertNotNull(queue);
    Assert.assertEquals(1, queue.queue.size());
    Assert.assertEquals(0L, queue.maxCommitId);
    lock = queue.queue.values().iterator().next();
    Assert.assertEquals(firstTxnId, lock.getTxnId());
    Assert.assertEquals(HbaseMetastoreProto.LockType.INTENTION, lock.getType());
    Assert.assertEquals(HbaseMetastoreProto.LockState.ACQUIRED, lock.getState());
    Assert.assertEquals(key, lock.getEntityLocked());

    key = new TransactionManager.EntityKey(db2, t2, null);
    queue = locksAfter.get(key);
    Assert.assertNotNull(queue);
    Assert.assertEquals(1, queue.queue.size());
    Assert.assertEquals(0L, queue.maxCommitId);
    lock = queue.queue.values().iterator().next();
    Assert.assertEquals(firstTxnId, lock.getTxnId());
    Assert.assertEquals(HbaseMetastoreProto.LockType.SHARED_WRITE, lock.getType());
    Assert.assertEquals(HbaseMetastoreProto.LockState.ACQUIRED, lock.getState());
    Assert.assertEquals(key, lock.getEntityLocked());

    key = new TransactionManager.EntityKey(db3, null, null);
    queue = locksAfter.get(key);
    Assert.assertNotNull(queue);
    Assert.assertEquals(1, queue.queue.size());
    Assert.assertEquals(0L, queue.maxCommitId);
    lock = queue.queue.values().iterator().next();
    Assert.assertEquals(secondsTxnId, lock.getTxnId());
    Assert.assertEquals(HbaseMetastoreProto.LockType.INTENTION, lock.getType());
    Assert.assertEquals(HbaseMetastoreProto.LockState.ACQUIRED, lock.getState());
    Assert.assertEquals(key, lock.getEntityLocked());

    key = new TransactionManager.EntityKey(db3, t3, null);
    queue = locksAfter.get(key);
    Assert.assertNotNull(queue);
    Assert.assertEquals(1, queue.queue.size());
    Assert.assertEquals(0L, queue.maxCommitId);
    lock = queue.queue.values().iterator().next();
    Assert.assertEquals(secondsTxnId, lock.getTxnId());
    Assert.assertEquals(HbaseMetastoreProto.LockType.INTENTION, lock.getType());
    Assert.assertEquals(HbaseMetastoreProto.LockState.ACQUIRED, lock.getState());
    Assert.assertEquals(key, lock.getEntityLocked());

    key = new TransactionManager.EntityKey(db3, t3, p3);
    queue = locksAfter.get(key);
    Assert.assertNotNull(queue);
    Assert.assertEquals(1, queue.queue.size());
    Assert.assertEquals(0L, queue.maxCommitId);
    lock = queue.queue.values().iterator().next();
    Assert.assertEquals(secondsTxnId, lock.getTxnId());
    Assert.assertEquals(HbaseMetastoreProto.LockType.SHARED_READ, lock.getType());
    Assert.assertEquals(HbaseMetastoreProto.LockState.ACQUIRED, lock.getState());
    Assert.assertEquals(key, lock.getEntityLocked());

    // Check the records in HBase
    HbaseMetastoreProto.Transaction hbaseTxn = hrw.getTransaction(firstTxnId);
    Assert.assertNotNull(hbaseTxn);
    Assert.assertEquals(3, hbaseTxn.getLocksCount());
    Assert.assertEquals(HbaseMetastoreProto.TxnState.OPEN, hbaseTxn.getTxnState());

    boolean sawOne, sawTwo, sawThree;
    sawOne = sawTwo = sawThree = false;
    for (HbaseMetastoreProto.Transaction.Lock hbaseLock : hbaseTxn.getLocksList()) {
      if (hbaseLock.getDb().equals(db1)) {
        sawOne = true;
        Assert.assertEquals(HbaseMetastoreProto.LockState.ACQUIRED, hbaseLock.getState());
        Assert.assertEquals(HbaseMetastoreProto.LockType.EXCLUSIVE, hbaseLock.getType());
        Assert.assertFalse(hbaseLock.hasTable());
        Assert.assertFalse(hbaseLock.hasPartition());
        Assert.assertTrue(hbaseLock.getAcquiredAt() > 0);
      } else if (hbaseLock.getDb().equals(db2) && !hbaseLock.hasTable()) {
        sawTwo = true;
        Assert.assertEquals(HbaseMetastoreProto.LockState.ACQUIRED, hbaseLock.getState());
        Assert.assertEquals(HbaseMetastoreProto.LockType.INTENTION, hbaseLock.getType());
        Assert.assertFalse(hbaseLock.hasTable());
        Assert.assertFalse(hbaseLock.hasPartition());
        Assert.assertTrue(hbaseLock.getAcquiredAt() > 0);
      } else if (hbaseLock.getDb().equals(db2) && hbaseLock.hasTable()) {
        sawThree = true;
        Assert.assertEquals(HbaseMetastoreProto.LockState.ACQUIRED, hbaseLock.getState());
        Assert.assertEquals(HbaseMetastoreProto.LockType.SHARED_WRITE, hbaseLock.getType());
        Assert.assertEquals(t2, hbaseLock.getTable());
        Assert.assertFalse(hbaseLock.hasPartition());
        Assert.assertTrue(hbaseLock.getAcquiredAt() > 0);
      } else {
        Assert.fail();
      }
    }

    Assert.assertTrue(sawOne);
    Assert.assertTrue(sawTwo);
    Assert.assertTrue(sawThree);

    hbaseTxn = hrw.getTransaction(secondsTxnId);
    Assert.assertNotNull(hbaseTxn);
    Assert.assertEquals(3, hbaseTxn.getLocksCount());
    Assert.assertEquals(HbaseMetastoreProto.TxnState.OPEN, hbaseTxn.getTxnState());

    sawOne = sawTwo = sawThree = false;
    for (HbaseMetastoreProto.Transaction.Lock hbaseLock : hbaseTxn.getLocksList()) {
      if (hbaseLock.getDb().equals(db3) && !hbaseLock.hasTable() && !hbaseLock.hasPartition()) {
        sawOne = true;
        Assert.assertEquals(HbaseMetastoreProto.LockState.ACQUIRED, hbaseLock.getState());
        Assert.assertEquals(HbaseMetastoreProto.LockType.INTENTION, hbaseLock.getType());
        Assert.assertTrue(hbaseLock.getAcquiredAt() > 0);
      } else if (hbaseLock.getDb().equals(db3) && hbaseLock.hasTable() && !hbaseLock.hasPartition()) {
        sawTwo = true;
        Assert.assertEquals(HbaseMetastoreProto.LockState.ACQUIRED, hbaseLock.getState());
        Assert.assertEquals(HbaseMetastoreProto.LockType.INTENTION, hbaseLock.getType());
        Assert.assertEquals(t3, hbaseLock.getTable());
        Assert.assertTrue(hbaseLock.getAcquiredAt() > 0);
      } else if (hbaseLock.getDb().equals(db3) && hbaseLock.hasTable() && hbaseLock.hasPartition()) {
        sawThree = true;
        Assert.assertEquals(HbaseMetastoreProto.LockState.ACQUIRED, hbaseLock.getState());
        Assert.assertEquals(HbaseMetastoreProto.LockType.SHARED_READ, hbaseLock.getType());
        Assert.assertEquals(t3, hbaseLock.getTable());
        Assert.assertEquals(p3, hbaseLock.getPartition());
        Assert.assertTrue(hbaseLock.getAcquiredAt() > 0);
      } else {
        Assert.fail();
      }
    }

    Assert.assertTrue(sawOne);
    Assert.assertTrue(sawTwo);
    Assert.assertTrue(sawThree);

    // abort both transactions
    HbaseMetastoreProto.GetOpenTxnsResponse beforeAbort =
        txnMgr.getOpenTxns(HbaseMetastoreProto.Void.getDefaultInstance());
    Map<Long, OpenHiveTransaction> memoryBeforeAbort = txnMgr.copyOpenTransactions();
    Map<Long, AbortedHiveTransaction> abortedMemoryBeforeAbort = txnMgr.copyAbortedTransactions();

    HbaseMetastoreProto.TransactionResult abort =
        txnMgr.abortTxn(HbaseMetastoreProto.TransactionId.newBuilder()
            .setId(firstTxnId)
            .build());
    Assert.assertEquals(HbaseMetastoreProto.TxnStateChangeResult.SUCCESS, abort.getState());
    abort = txnMgr.abortTxn(HbaseMetastoreProto.TransactionId.newBuilder()
            .setId(secondsTxnId)
            .build());
    Assert.assertEquals(HbaseMetastoreProto.TxnStateChangeResult.SUCCESS, abort.getState());

    HbaseMetastoreProto.GetOpenTxnsResponse afterAbort =
        txnMgr.getOpenTxns(HbaseMetastoreProto.Void.getDefaultInstance());
    Map<Long, OpenHiveTransaction> memoryAfterAbort = txnMgr.copyOpenTransactions();
    Map<Long, AbortedHiveTransaction> abortedMemoryAfterAbort = txnMgr.copyAbortedTransactions();
    Map<TransactionManager.EntityKey, TransactionManager.LockQueue> locksAfterAbort =
        txnMgr.copyLockQueues();

    // We should have remembered one of the aborts and not the otehr
    // one should have been immediately forgotten
    Assert.assertEquals(beforeAbort.getAbortedTransactionsCount() + 1, afterAbort.getAbortedTransactionsCount());
    Assert.assertEquals(memoryBeforeAbort.size() - 2, memoryAfterAbort.size());

    // The first entry should have been added to the aborted list, but not the second
    Assert.assertEquals(abortedMemoryBeforeAbort.size() + 1, abortedMemoryAfterAbort.size());
    Assert.assertNull(abortedMemoryAfterAbort.get(secondsTxnId));
    AbortedHiveTransaction firstAbortedTxn = abortedMemoryAfterAbort.get(firstTxnId);
    Assert.assertNotNull(firstAbortedTxn);
    Assert.assertEquals(1, firstAbortedTxn.getCompactableLocks().size());

    key = new TransactionManager.EntityKey(db2, t2, null);
    queue = locksAfterAbort.get(key);
    Assert.assertEquals(0, queue.queue.size());

    lock = firstAbortedTxn.getCompactableLocks().get(key);
    Assert.assertNotNull(lock);
    Assert.assertEquals(firstTxnId, lock.getTxnId());
    Assert.assertEquals(HbaseMetastoreProto.LockType.SHARED_WRITE, lock.getType());
    Assert.assertEquals(HbaseMetastoreProto.LockState.TXN_ABORTED, lock.getState());
    Assert.assertEquals(key, lock.getEntityLocked());

    // The second transaction should have been forgotten altogether
    hbaseTxn = hrw.getTransaction(secondsTxnId);
    Assert.assertNull(hbaseTxn);

    // The first transaction should still be there
    hbaseTxn = hrw.getTransaction(firstTxnId);
    Assert.assertNotNull(hbaseTxn);
    Assert.assertEquals(HbaseMetastoreProto.TxnState.ABORTED, hbaseTxn.getTxnState());

    Assert.assertEquals(1, hbaseTxn.getLocksCount());

    HbaseMetastoreProto.Transaction.Lock hbaseLock = hbaseTxn.getLocks(0);
    Assert.assertEquals(db2, hbaseLock.getDb());
    Assert.assertEquals(HbaseMetastoreProto.LockState.TXN_ABORTED, hbaseLock.getState());
    Assert.assertEquals(HbaseMetastoreProto.LockType.SHARED_WRITE, hbaseLock.getType());
    Assert.assertEquals(t2, hbaseLock.getTable());
    Assert.assertFalse(hbaseLock.hasPartition());

    // We should have a potential compaction for the second one but not for any of the others
    HbaseMetastoreProto.PotentialCompaction pc =
        hrw.getPotentialCompaction(db2, t2, null);
    Assert.assertNotNull(pc);
    Assert.assertEquals(1, pc.getTxnIdsCount());
    Assert.assertEquals(firstTxnId, pc.getTxnIds(0));
    Assert.assertEquals(db2, pc.getDb());
    Assert.assertEquals(t2, pc.getTable());
    Assert.assertFalse(pc.hasPartition());

    pc = hrw.getPotentialCompaction(db3, t3, p3);
    Assert.assertNull(pc);
  }

  @Test
  public void openLockCommit() throws Exception {

    String db1 = "olc_db1";
    String db2 = "olc_db2";
    String t2 = "olc_t2";
    String db3 = "olc_db3";
    String t3 = "olc_t3";
    String p3 = "olc_p3";

    HbaseMetastoreProto.OpenTxnsRequest rqst = HbaseMetastoreProto.OpenTxnsRequest.newBuilder()
        .setNumTxns(2)
        .setUser("me")
        .setHostname("localhost")
        .build();
    HbaseMetastoreProto.OpenTxnsResponse rsp = txnMgr.openTxns(rqst);
    Assert.assertEquals(2, rsp.getTxnIdsCount());
    long firstTxnId = rsp.getTxnIds(0);
    long secondsTxnId = rsp.getTxnIds(1);

    HbaseMetastoreProto.GetOpenTxnsResponse after =
        txnMgr.getOpenTxns(HbaseMetastoreProto.Void.getDefaultInstance());

    // Get some locks.  We'll do X, SW, and I on one and SR and I on the other
    // I use unique entity names to guarantee I'm creating new queues in the lockQueues
    Map<TransactionManager.EntityKey, TransactionManager.LockQueue> locksBefore =
        txnMgr.copyLockQueues();

    HbaseMetastoreProto.LockResponse firstLock = txnMgr.lock(HbaseMetastoreProto.LockRequest.newBuilder()
        .setTxnId(firstTxnId)
        .addComponents(HbaseMetastoreProto.LockComponent.newBuilder()
            .setDb(db1)
            .setType(HbaseMetastoreProto.LockType.EXCLUSIVE))
        .addComponents(HbaseMetastoreProto.LockComponent.newBuilder()
            .setDb(db2)
            .setType(HbaseMetastoreProto.LockType.INTENTION))
        .addComponents(HbaseMetastoreProto.LockComponent.newBuilder()
            .setDb(db2)
            .setTable(t2)
            .setType(HbaseMetastoreProto.LockType.SHARED_WRITE))
        .build());
    Assert.assertEquals(HbaseMetastoreProto.LockState.ACQUIRED, firstLock.getState());

    HbaseMetastoreProto.LockResponse secondLock = txnMgr.lock(HbaseMetastoreProto.LockRequest.newBuilder()
        .setTxnId(secondsTxnId)
        .addComponents(HbaseMetastoreProto.LockComponent.newBuilder()
            .setDb(db3)
            .setType(HbaseMetastoreProto.LockType.INTENTION))
        .addComponents(HbaseMetastoreProto.LockComponent.newBuilder()
            .setDb(db3)
            .setTable(t3)
            .setType(HbaseMetastoreProto.LockType.INTENTION))
        .addComponents(HbaseMetastoreProto.LockComponent.newBuilder()
            .setDb(db3)
            .setTable(t3)
            .setPartition(p3)
            .setType(HbaseMetastoreProto.LockType.SHARED_READ))
        .build());
    Assert.assertEquals(HbaseMetastoreProto.LockState.ACQUIRED, secondLock.getState());

    // commit both transactions
    HbaseMetastoreProto.GetOpenTxnsResponse beforeCommit =
        txnMgr.getOpenTxns(HbaseMetastoreProto.Void.getDefaultInstance());
    Map<Long, OpenHiveTransaction> memoryBeforeCommit = txnMgr.copyOpenTransactions();
    Set<CommittedHiveTransaction> committedMemoryBeforeCommit = txnMgr.copyCommittedTransactions();

    HbaseMetastoreProto.TransactionResult commit =
        txnMgr.commitTxn(HbaseMetastoreProto.TransactionId.newBuilder()
            .setId(firstTxnId)
            .build());
    Assert.assertEquals(HbaseMetastoreProto.TxnStateChangeResult.SUCCESS, commit.getState());
    commit = txnMgr.commitTxn(HbaseMetastoreProto.TransactionId.newBuilder()
        .setId(secondsTxnId)
        .build());
    Assert.assertEquals(HbaseMetastoreProto.TxnStateChangeResult.SUCCESS, commit.getState());

    HbaseMetastoreProto.GetOpenTxnsResponse afterCommit =
        txnMgr.getOpenTxns(HbaseMetastoreProto.Void.getDefaultInstance());
    Map<Long, OpenHiveTransaction> memoryAfterCommit = txnMgr.copyOpenTransactions();
    Set<CommittedHiveTransaction> committedMemoryAfterCommit = txnMgr.copyCommittedTransactions();
    Map<TransactionManager.EntityKey, TransactionManager.LockQueue> locksAfterCommit =
        txnMgr.copyLockQueues();

    Assert.assertEquals(memoryBeforeCommit.size() - 2, memoryAfterCommit.size());

    // The first entry should have been added to the commit list, but not the second
    Assert.assertEquals(committedMemoryBeforeCommit.size() + 1, committedMemoryAfterCommit.size());

    CommittedHiveTransaction firstCommittedTxn = null;
    for (CommittedHiveTransaction ct : committedMemoryAfterCommit) {
      if (ct.getId() == firstTxnId) {
        firstCommittedTxn = ct;
        break;
      }
    }
    Assert.assertNotNull(firstCommittedTxn);
    Assert.assertEquals(after.getHighWaterMark(), firstCommittedTxn.getCommitId());

    TransactionManager.EntityKey key = new TransactionManager.EntityKey(db2, t2, null);
    TransactionManager.LockQueue queue = locksAfterCommit.get(key);
    Assert.assertEquals(0, queue.queue.size());
    Assert.assertEquals(firstCommittedTxn.getCommitId(), queue.maxCommitId);

    // The second transaction should have been forgotten altogether
    HbaseMetastoreProto.Transaction hbaseTxn = hrw.getTransaction(secondsTxnId);
    Assert.assertNull(hbaseTxn);

    // The first transaction should still be there
    hbaseTxn = hrw.getTransaction(firstTxnId);
    Assert.assertNotNull(hbaseTxn);
    Assert.assertEquals(HbaseMetastoreProto.TxnState.COMMITTED, hbaseTxn.getTxnState());
    Assert.assertEquals(firstCommittedTxn.getCommitId(), hbaseTxn.getCommitId());

    Assert.assertEquals(1, hbaseTxn.getLocksCount());

    HbaseMetastoreProto.Transaction.Lock hbaseLock = hbaseTxn.getLocks(0);
    Assert.assertEquals(db2, hbaseLock.getDb());
    Assert.assertEquals(HbaseMetastoreProto.LockState.RELEASED, hbaseLock.getState());
    Assert.assertEquals(HbaseMetastoreProto.LockType.SHARED_WRITE, hbaseLock.getType());
    Assert.assertEquals(t2, hbaseLock.getTable());
    Assert.assertFalse(hbaseLock.hasPartition());

    // We should have a potential compaction for the second one but not for any of the others
    HbaseMetastoreProto.PotentialCompaction pc =
        hrw.getPotentialCompaction(db2, t2, null);
    Assert.assertNotNull(pc);
    Assert.assertEquals(1, pc.getTxnIdsCount());
    Assert.assertEquals(firstTxnId, pc.getTxnIds(0));
    Assert.assertEquals(db2, pc.getDb());
    Assert.assertEquals(t2, pc.getTable());
    Assert.assertFalse(pc.hasPartition());

    pc = hrw.getPotentialCompaction(db3, t3, p3);
    Assert.assertNull(pc);
  }

  @Test
  public void heartbeat() throws Exception {
    HbaseMetastoreProto.OpenTxnsRequest rqst = HbaseMetastoreProto.OpenTxnsRequest.newBuilder()
        .setNumTxns(1)
        .setUser("me")
        .setHostname("localhost")
        .build();
    HbaseMetastoreProto.OpenTxnsResponse rsp = txnMgr.openTxns(rqst);
    long txnId = rsp.getTxnIds(0);

    // Figure out what the last heartbeat was
    Map<Long, OpenHiveTransaction> openTxns = txnMgr.copyOpenTransactions();
    OpenHiveTransaction openTxn = openTxns.get(txnId);
    long lastHeartbeat = openTxn.getLastHeartbeat();
    long now = System.currentTimeMillis();
    Assert.assertTrue(now >= lastHeartbeat);

    Thread.sleep(10);

    HbaseMetastoreProto.HeartbeatTxnRangeResponse heartbeats =
        txnMgr.heartbeat(HbaseMetastoreProto.HeartbeatTxnRangeRequest.newBuilder()
            .setMinTxn(txnId)
            .setMaxTxn(txnId)
            .build());
    Assert.assertEquals(0, heartbeats.getAbortedCount());
    Assert.assertEquals(0, heartbeats.getNoSuchCount());

    // No need to re-fetch the map as we still have the pointer to the actual transaction
    long latestHeartbeat = openTxn.getLastHeartbeat();
    Assert.assertTrue(latestHeartbeat > lastHeartbeat);
  }

  @Test
  public void heartbeatAbortedAndNoSuch() throws Exception {
    HbaseMetastoreProto.OpenTxnsRequest rqst = HbaseMetastoreProto.OpenTxnsRequest.newBuilder()
        .setNumTxns(1)
        .setUser("me")
        .setHostname("localhost")
        .build();
    HbaseMetastoreProto.OpenTxnsResponse rsp = txnMgr.openTxns(rqst);
    long txnId = rsp.getTxnIds(0);

    // Have to get a write lock so that the aborted transaction is remembered
    txnMgr.lock(HbaseMetastoreProto.LockRequest.newBuilder()
        .setTxnId(txnId)
        .addComponents(HbaseMetastoreProto.LockComponent.newBuilder()
            .setDb("d")
            .setTable("t4")
            .setType(HbaseMetastoreProto.LockType.SHARED_WRITE)
            .build())
        .build());
    txnMgr.abortTxn(HbaseMetastoreProto.TransactionId.newBuilder().setId(txnId).build());

    HbaseMetastoreProto.HeartbeatTxnRangeResponse heartbeats =
        txnMgr.heartbeat(HbaseMetastoreProto.HeartbeatTxnRangeRequest.newBuilder()
            .setMinTxn(txnId)
            .setMaxTxn(txnId + 1)
            .build());
    Assert.assertEquals(1, heartbeats.getAbortedCount());
    Assert.assertEquals(1, heartbeats.getNoSuchCount());
    Assert.assertEquals(txnId, heartbeats.getAborted(0));
    Assert.assertEquals(txnId + 1, heartbeats.getNoSuch(0));
  }

  @Ignore
  public void cleanupNoPotential() throws Exception {
    // Test that everything's ok when we call cleanupAfterCompaction when there was no
    // PotentialCompaction

    txnMgr.cleanupAfterCompaction(HbaseMetastoreProto.Compaction.newBuilder()
        .setId(1)
        .setDb("a")
        .setTable("b")
        .setHighestTxnId(17)
        .setState(HbaseMetastoreProto.CompactionState.READY_FOR_CLEANING)
        .setType(HbaseMetastoreProto.CompactionType.MINOR)
        .build());

    // TODO add check for no record in potential compactions
  }

  @Ignore
  public void partialCleanup() throws Exception {
    // Test that when the highestTxnId is lower than some of the transactions in the potential
    // the potential is kept but earlier txn ids are trimmed out.
    // Have to open and commit the transactions serially to avoid write/write conflict
    HbaseMetastoreProto.OpenTxnsRequest rqst = HbaseMetastoreProto.OpenTxnsRequest.newBuilder()
        .setNumTxns(1)
        .setUser("me")
        .setHostname("localhost")
        .build();
    HbaseMetastoreProto.OpenTxnsResponse rsp = txnMgr.openTxns(rqst);
    long firstTxn = rsp.getTxnIds(0);

    HbaseMetastoreProto.LockResponse lock =
        txnMgr.lock(HbaseMetastoreProto.LockRequest.newBuilder()
            .setTxnId(firstTxn)
            .addComponents(HbaseMetastoreProto.LockComponent.newBuilder()
                .setDb("c")
                .setTable("d")
                .setPartition("e")
                .setType(HbaseMetastoreProto.LockType.SHARED_WRITE))
            .addComponents(HbaseMetastoreProto.LockComponent.newBuilder()
                .setDb("c")
                .setType(HbaseMetastoreProto.LockType.INTENTION))
            .addComponents(HbaseMetastoreProto.LockComponent.newBuilder()
                .setDb("c")
                .setTable("b")
                .setType(HbaseMetastoreProto.LockType.INTENTION))
            .build());
    Assert.assertEquals(HbaseMetastoreProto.LockState.ACQUIRED, lock.getState());




  }

  // TODO full cleanup
  // TODO make sure we are cleaning up aborted txns, including ones with multiple locks
  // TODO make sure we don't clean up aborted txns that still have uncompacted locks
  /*
    // Side test, see if we properly decide when transactions can and can't be cleaned.
    HbaseMetastoreProto.CompactionList cleanable = txnMgr.verifyCompactionCanBeCleaned(
        HbaseMetastoreProto.CompactionList.newBuilder()
            .addCompactions(
                HbaseMetastoreProto.Compaction.newBuilder()
                    .setDb("x")
                    .setTable("y")
                    .setPartition("z")
                    .setHighestTxnId(2)
                    .setId(1)
                    .setType(HbaseMetastoreProto.CompactionType.MAJOR)
                    .setState(HbaseMetastoreProto.CompactionState.READY_FOR_CLEANING))
            .addCompactions(
                HbaseMetastoreProto.Compaction.newBuilder()
                    .setDb("a")
                    .setTable("b")
                    .setHighestTxnId(7)
                    .setId(2)
                    .setType(HbaseMetastoreProto.CompactionType.MAJOR)
                    .setState(HbaseMetastoreProto.CompactionState.READY_FOR_CLEANING))
            .build());
    Assert.assertEquals(cleanable.getCompactionsCount(), 1);
    Assert.assertEquals(cleanable.getCompactions(0).getDb(), "x");

    // Declare that we've compacted the aborted transaction.  This should cause us to clean up
    // the aborted txn.
    txnMgr.cleanupAfterCompaction(HbaseMetastoreProto.Compaction.newBuilder()
        .setDb("d")
        .setTable("t2")
        .setId(10)
        .setHighestTxnId(4)
        .setType(HbaseMetastoreProto.CompactionType.MINOR)
        .setState(HbaseMetastoreProto.CompactionState.READY_FOR_CLEANING)
        .build());
    assertInternalState(6L, "\\{\\}", "{}", "[{\"commitId\":6}]");

    // add dynamic partitions
    rqst = HbaseMetastoreProto.OpenTxnsRequest.newBuilder()
        .setNumTxns(1)
        .setUser("me")
        .setHostname("localhost")
        .build();
    rsp = txnMgr.openTxns(rqst);

    txnMgr.addDynamicPartitions(HbaseMetastoreProto.AddDynamicPartitionsRequest.newBuilder()
        .setTxnId(6)
        .setDb("x")
        .setTable("y")
        .addAllPartitions(Arrays.asList("p1", "p2"))
        .build());

    assertInternalState(7L, "\\{\"6\":\\{\"lastHeartbeat\":[0-9]+," +
        "\"hiveLocks\":\\[\\{\"id\":-1,\"txnId\":6,\"entityLocked\":\\{\"db\":\"x\"," +
        "\"table\":\"y\",\"part\":\"p1\"\\},\"type\":\"SHARED_WRITE\",\"state\":\"RELEASED\"\\}," +
        "\\{\"id\":-1,\"txnId\":6,\"entityLocked\":\\{\"db\":\"x\",\"table\":\"y\"," +
        "\"part\":\"p2\"\\},\"type\":\"SHARED_WRITE\",\"state\":\"RELEASED\"\\}\\]\\}\\}", "{}",
        "[{\"commitId\":6}]");
    Assert.assertEquals("{\"d.t3.p\":{\"queue\":{},\"maxCommitId\":0},\"d.t2\":{\"queue\":{}," +
        "\"maxCommitId\":0},\"d.t4\":{\"queue\":{},\"maxCommitId\":6},\"d.t.p\":{\"queue\":{}," +
        "\"maxCommitId\":0},\"x.y.p2\":{\"queue\":{},\"maxCommitId\":0},\"x.y.p1\":{\"queue\":{}," +
        "\"maxCommitId\":0}}", txnMgr.stringifyLockQueues());

    txnMgr.commitTxn(HbaseMetastoreProto.TransactionId.newBuilder()
        .setId(6)
        .build());
    Assert.assertEquals("{\"d.t3.p\":{\"queue\":{},\"maxCommitId\":0},\"d.t2\":{\"queue\":{}," +
        "\"maxCommitId\":0},\"d.t4\":{\"queue\":{},\"maxCommitId\":6},\"d.t.p\":{\"queue\":{}," +
        "\"maxCommitId\":0},\"x.y.p2\":{\"queue\":{},\"maxCommitId\":7},\"x.y.p1\":{\"queue\":{}," +
        "\"maxCommitId\":7}}", txnMgr.stringifyLockQueues());
  }

  */

}
