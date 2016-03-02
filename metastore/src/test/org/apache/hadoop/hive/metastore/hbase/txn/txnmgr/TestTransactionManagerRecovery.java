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
import org.apache.hadoop.hive.metastore.hbase.HBaseReadWrite;
import org.apache.hadoop.hive.metastore.hbase.HBaseStore;
import org.apache.hadoop.hive.metastore.hbase.HbaseMetastoreProto;
import org.apache.hadoop.hive.metastore.hbase.MockUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockitoAnnotations;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class TestTransactionManagerRecovery extends MockUtils {
  HBaseStore store;
  TransactionManager txnMgr;
  HBaseReadWrite hrw;
  HiveConf conf;

  @Before
  public void init() throws IOException {
    MockitoAnnotations.initMocks(this);
    conf = new HiveConf();

    // Set the wait on the background threads to max long so that they don't run and clean things
    // up on us, since we're trying to check state.
    conf.set(TransactionManager.CONF_NO_AUTO_BACKGROUND_THREADS, Boolean.toString(Boolean.TRUE));
    // Set poll timeout low so we don't wait forever for our locks to come back and tell us to wait.
    HiveConf.setTimeVar(conf, HiveConf.ConfVars.METASTORE_HBASE_TXN_MGR_LOCK_POLL_TIMEOUT, 500,
        TimeUnit.MILLISECONDS);

    store = mockInit(conf);
    txnMgr = new TransactionManager(conf);
    hrw = HBaseReadWrite.getInstance();
  }

  @After
  public void cleanup() throws IOException {
    txnMgr.shutdown();
  }

  @Test
  public void recover() throws Exception {
    String db[] = {"lqsactc_db1", "lqsactc_db2", "lqsactc_db3"};
    String t[] = {"lqsactc_t1", "lqsactc_t2", "lqsactc_t3"};
    String p[] = {"lqsactc_p1", "lqsactc_p2", "lqsactc_p3"};

    HbaseMetastoreProto.OpenTxnsRequest rqst = HbaseMetastoreProto.OpenTxnsRequest.newBuilder()
        .setNumTxns(5)
        .setUser("me")
        .setHostname("localhost")
        .build();
    HbaseMetastoreProto.OpenTxnsResponse rsp = txnMgr.openTxns(rqst);
    Assert.assertEquals(5, rsp.getTxnIdsCount());
    long firstTxn = rsp.getTxnIds(0);
    long secondTxn = rsp.getTxnIds(1);
    long thirdTxn = rsp.getTxnIds(2);
    long fourthTxn = rsp.getTxnIds(3);
    long fifthTxn = rsp.getTxnIds(4);

    HbaseMetastoreProto.LockResponse lock = txnMgr.lock(HbaseMetastoreProto.LockRequest.newBuilder()
        .setTxnId(firstTxn)
        .addComponents(HbaseMetastoreProto.LockComponent.newBuilder()
            .setDb(db[0])
            .setTable(t[0])
            .setPartition(p[0])
            .setType(HbaseMetastoreProto.LockType.SHARED_WRITE))
        .addComponents(HbaseMetastoreProto.LockComponent.newBuilder()
            .setDb(db[0])
            .setType(HbaseMetastoreProto.LockType.INTENTION))
        .addComponents(HbaseMetastoreProto.LockComponent.newBuilder()
            .setDb(db[0])
            .setTable(t[0])
            .setType(HbaseMetastoreProto.LockType.INTENTION))
        .build());
    Assert.assertEquals(HbaseMetastoreProto.LockState.ACQUIRED, lock.getState());

    HbaseMetastoreProto.TransactionResult abort =
        txnMgr.abortTxn(HbaseMetastoreProto.TransactionId.newBuilder()
            .setId(firstTxn)
            .build());
    Assert.assertEquals(HbaseMetastoreProto.TxnStateChangeResult.SUCCESS, abort.getState());

    lock = txnMgr.lock(HbaseMetastoreProto.LockRequest.newBuilder()
        .setTxnId(secondTxn)
        .addComponents(HbaseMetastoreProto.LockComponent.newBuilder()
            .setDb(db[1])
            .setTable(t[1])
            .setPartition(p[1])
            .setType(HbaseMetastoreProto.LockType.SHARED_READ))
        .addComponents(HbaseMetastoreProto.LockComponent.newBuilder()
            .setDb(db[1])
            .setType(HbaseMetastoreProto.LockType.INTENTION))
        .addComponents(HbaseMetastoreProto.LockComponent.newBuilder()
            .setDb(db[1])
            .setTable(t[1])
            .setType(HbaseMetastoreProto.LockType.INTENTION))
        .build());
    Assert.assertEquals(HbaseMetastoreProto.LockState.ACQUIRED, lock.getState());

    lock = txnMgr.lock(HbaseMetastoreProto.LockRequest.newBuilder()
        .setTxnId(thirdTxn)
        .addComponents(HbaseMetastoreProto.LockComponent.newBuilder()
            .setDb(db[2])
            .setTable(t[2])
            .setPartition(p[2])
            .setType(HbaseMetastoreProto.LockType.SHARED_WRITE))
        .addComponents(HbaseMetastoreProto.LockComponent.newBuilder()
            .setDb(db[2])
            .setType(HbaseMetastoreProto.LockType.INTENTION))
        .addComponents(HbaseMetastoreProto.LockComponent.newBuilder()
            .setDb(db[2])
            .setTable(t[2])
            .setType(HbaseMetastoreProto.LockType.INTENTION))
        .build());
    Assert.assertEquals(HbaseMetastoreProto.LockState.ACQUIRED, lock.getState());

    HbaseMetastoreProto.TransactionResult commit =
        txnMgr.commitTxn(HbaseMetastoreProto.TransactionId.newBuilder()
            .setId(thirdTxn)
            .build());
    Assert.assertEquals(HbaseMetastoreProto.TxnStateChangeResult.SUCCESS, commit.getState());

    lock = txnMgr.lock(HbaseMetastoreProto.LockRequest.newBuilder()
        .setTxnId(fourthTxn)
        .addComponents(HbaseMetastoreProto.LockComponent.newBuilder()
            .setDb(db[1])
            .setType(HbaseMetastoreProto.LockType.EXCLUSIVE))
        .build());
    Assert.assertEquals(HbaseMetastoreProto.LockState.WAITING, lock.getState());

    // Now, shut it down and start a new one to see if recovery works properly
    txnMgr.shutdown();
    txnMgr = new TransactionManager(conf);

    // We should have one aborted transaction with a single lock
    Map<Long, AbortedHiveTransaction> abortedTxns = txnMgr.copyAbortedTransactions();
    Assert.assertEquals(1, abortedTxns.size());
    AbortedHiveTransaction aborted = abortedTxns.get(firstTxn);
    Assert.assertNotNull(aborted);
    Assert.assertFalse(aborted.fullyCompacted());
    Assert.assertEquals(1, aborted.getCompactableLocks().size());
    HiveLock abortedLock = aborted.getCompactableLocks().values().iterator().next();
    Assert.assertEquals(new TransactionManager.EntityKey(db[0], t[0], p[0]),
        abortedLock.getEntityLocked());
    Assert.assertEquals(firstTxn, abortedLock.getTxnId());
    Assert.assertEquals(HbaseMetastoreProto.LockType.SHARED_WRITE, abortedLock.getType());
    Assert.assertEquals(HbaseMetastoreProto.LockState.TXN_ABORTED, abortedLock.getState());

    // We should have 3 open transactions.  The second overall transaction should have 3 locks,
    // and the fourth 1
    Map<Long, OpenHiveTransaction> openTxns = txnMgr.copyOpenTransactions();
    Assert.assertEquals(3, openTxns.size());
    OpenHiveTransaction open = openTxns.get(secondTxn);
    Assert.assertNotNull(open);
    HiveLock[] locks = open.getHiveLocks();
    Assert.assertEquals(3, locks.length);
    boolean sawDbLock = false, sawTableLock = false, sawPartLock = false;
    for (HiveLock openLock : locks) {
      if (openLock.getEntityLocked().part == null) {
        if (openLock.getEntityLocked().table == null) {
          Assert.assertEquals(new TransactionManager.EntityKey(db[1], null, null),
              openLock.getEntityLocked());
          Assert.assertEquals(secondTxn, openLock.getTxnId());
          Assert.assertEquals(HbaseMetastoreProto.LockType.INTENTION, openLock.getType());
          Assert.assertEquals(HbaseMetastoreProto.LockState.ACQUIRED, openLock.getState());
          sawDbLock = true;
        } else {
          Assert.assertEquals(new TransactionManager.EntityKey(db[1], t[1], null),
              openLock.getEntityLocked());
          Assert.assertEquals(secondTxn, openLock.getTxnId());
          Assert.assertEquals(HbaseMetastoreProto.LockType.INTENTION, openLock.getType());
          Assert.assertEquals(HbaseMetastoreProto.LockState.ACQUIRED, openLock.getState());
          sawTableLock = true;
        }
      } else {
        Assert.assertEquals(new TransactionManager.EntityKey(db[1], t[1], p[1]),
            openLock.getEntityLocked());
        Assert.assertEquals(secondTxn, openLock.getTxnId());
        Assert.assertEquals(HbaseMetastoreProto.LockType.SHARED_READ, openLock.getType());
        Assert.assertEquals(HbaseMetastoreProto.LockState.ACQUIRED, openLock.getState());
        sawPartLock = true;
      }
    }
    Assert.assertTrue(sawDbLock);
    Assert.assertTrue(sawTableLock);
    Assert.assertTrue(sawPartLock);

    open = openTxns.get(fourthTxn);
    Assert.assertNotNull(open);
    locks = open.getHiveLocks();
    Assert.assertEquals(1, locks.length);
    Assert.assertEquals(new TransactionManager.EntityKey(db[1], null, null),
        locks[0].getEntityLocked());
    Assert.assertEquals(fourthTxn, locks[0].getTxnId());
    Assert.assertEquals(HbaseMetastoreProto.LockType.EXCLUSIVE, locks[0].getType());
    Assert.assertEquals(HbaseMetastoreProto.LockState.WAITING, locks[0].getState());

    // We should have 1 committed txn
    Set<CommittedHiveTransaction> committedTxns = txnMgr.copyCommittedTransactions();
    Assert.assertEquals(1, committedTxns.size());
    CommittedHiveTransaction committedTxn = committedTxns.iterator().next();
    Assert.assertEquals(thirdTxn, committedTxn.getId());
    Assert.assertEquals(fifthTxn, committedTxn.getCommitId());

    // We should have 5 lock queues (3 for the open txn locks, one for the committed txn lock,
    // and one for the aborted txn.
    Map<TransactionManager.EntityKey, TransactionManager.LockQueue> lockQueues =
        txnMgr.copyLockQueues();
    Assert.assertEquals(5, lockQueues.size());

    // The aborted queue will be empty
    TransactionManager.LockQueue queue =
        lockQueues.get(new TransactionManager.EntityKey(db[0], t[0], p[0]));
    Assert.assertNotNull(queue);
    Assert.assertEquals(0, queue.getMaxCommitId());
    Assert.assertEquals(0, queue.queue.size());

    queue = lockQueues.get(new TransactionManager.EntityKey(db[1], t[1], p[1]));
    Assert.assertNotNull(queue);
    Assert.assertEquals(0, queue.getMaxCommitId());
    Assert.assertEquals(1, queue.queue.size());
    // We've already checked the contents of the lock above, just check that this is the right lock
    Assert.assertEquals(secondTxn, queue.queue.values().iterator().next().getTxnId());

    queue = lockQueues.get(new TransactionManager.EntityKey(db[1], t[1], null));
    Assert.assertNotNull(queue);
    Assert.assertEquals(0, queue.getMaxCommitId());
    Assert.assertEquals(1, queue.queue.size());
    // We've already checked the contents of the lock above, just check that this is the right lock
    Assert.assertEquals(secondTxn, queue.queue.values().iterator().next().getTxnId());

    queue = lockQueues.get(new TransactionManager.EntityKey(db[1], null, null));
    Assert.assertNotNull(queue);
    Assert.assertEquals(0, queue.getMaxCommitId());
    Assert.assertEquals(2, queue.queue.size());
    // We've already checked the contents of the lock above, just check that this is the right lock
    // Order matters on this one
    Iterator<HiveLock> iter = queue.queue.values().iterator();
    Assert.assertEquals(secondTxn, iter.next().getTxnId());
    Assert.assertEquals(fourthTxn, iter.next().getTxnId());

    // The queue will be empty for the committed one, but the commit id should be set.
    queue = lockQueues.get(new TransactionManager.EntityKey(db[2], t[2], p[2]));
    Assert.assertNotNull(queue);
    Assert.assertEquals(fifthTxn, queue.getMaxCommitId());
    Assert.assertEquals(0, queue.queue.size());
  }

}
