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

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.AbortTxnRequest;
import org.apache.hadoop.hive.metastore.api.CheckLockRequest;
import org.apache.hadoop.hive.metastore.api.CommitTxnRequest;
import org.apache.hadoop.hive.metastore.api.CompactionType;
import org.apache.hadoop.hive.metastore.api.GetOpenTxnsResponse;
import org.apache.hadoop.hive.metastore.api.HeartbeatRequest;
import org.apache.hadoop.hive.metastore.api.LockComponent;
import org.apache.hadoop.hive.metastore.api.LockLevel;
import org.apache.hadoop.hive.metastore.api.LockRequest;
import org.apache.hadoop.hive.metastore.api.LockResponse;
import org.apache.hadoop.hive.metastore.api.LockState;
import org.apache.hadoop.hive.metastore.api.LockType;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchLockException;
import org.apache.hadoop.hive.metastore.api.NoSuchTxnException;
import org.apache.hadoop.hive.metastore.api.OpenTxnRequest;
import org.apache.hadoop.hive.metastore.api.OpenTxnsResponse;
import org.apache.hadoop.hive.metastore.api.TxnAbortedException;
import org.apache.hadoop.hive.metastore.txn.CompactionInfo;
import org.apache.hadoop.hive.metastore.txn.TxnDbUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class TestTransactionManager {
  static final private Logger LOG = LoggerFactory.getLogger(TransactionManager.class.getName());

  private TransactionManager txnManager;

  @Before
  public void before() throws Exception {
    TxnDbUtil.prepDb();
    HiveConf hiveConf = new HiveConf();
    TransactionManager.setHiveConf(hiveConf);
    TransactionManager.unitTesting = true;
    txnManager = TransactionManager.get();
  }

  @After
  public void after() throws Exception {
    // Take down the transaction manager so we have a clean slate for the next test.
    txnManager.selfDestruct("just testing");
    TxnDbUtil.cleanDb();
  }

  @Test
  public void emptyTxns() throws MetaException {
    GetOpenTxnsResponse rsp = txnManager.getOpenTxns();
    Assert.assertEquals(1, rsp.getTxn_high_water_mark());
    Assert.assertEquals(0, rsp.getOpen_txnsSize());
  }

  @Test
  public void openAndAbort() throws MetaException, NoSuchTxnException, TxnAbortedException {
    OpenTxnsResponse rsp = txnManager.openTxns(new OpenTxnRequest(1, "me", "localhost"));
    Assert.assertEquals(1, rsp.getTxn_idsSize());
    Assert.assertEquals(1, (long)rsp.getTxn_ids().get(0));
    GetOpenTxnsResponse openTxns = txnManager.getOpenTxns();
    Assert.assertEquals(2, openTxns.getTxn_high_water_mark());
    Assert.assertEquals(1, openTxns.getOpen_txnsSize());
    Assert.assertEquals(1, txnManager.copyOpenTxns().size());
    Assert.assertEquals(0, txnManager.copyAbortedTxns().size());

    txnManager.abortTxn(new AbortTxnRequest(1));
    openTxns = txnManager.getOpenTxns();
    Assert.assertEquals(2, openTxns.getTxn_high_water_mark());

    Assert.assertEquals(0, openTxns.getOpen_txnsSize());
    Assert.assertEquals(0, txnManager.copyOpenTxns().size());
    // No shared write locks, so this transaction will be forgotten
    Assert.assertEquals(0, txnManager.copyAbortedTxns().size());
    Assert.assertEquals(0, txnManager.copyCommittedTxnsByTxnId().size());
    Assert.assertEquals(0, txnManager.copyCommittedTxnsByCommitId().size());
  }

  @Test
  public void openAndCommit() throws MetaException, NoSuchTxnException, TxnAbortedException {
    OpenTxnsResponse rsp = txnManager.openTxns(new OpenTxnRequest(1, "me", "localhost"));
    txnManager.commitTxn(new CommitTxnRequest(1));
    GetOpenTxnsResponse openTxns = txnManager.getOpenTxns();
    Assert.assertEquals(2, openTxns.getTxn_high_water_mark());
    Assert.assertEquals(0, openTxns.getOpen_txnsSize());
    Assert.assertEquals(0, txnManager.copyOpenTxns().size());
    Assert.assertEquals(0, txnManager.copyAbortedTxns().size());
    // There were no shared write locks, so this transaction should be forgotten
    Assert.assertEquals(0, txnManager.copyCommittedTxnsByTxnId().size());
    Assert.assertEquals(0, txnManager.copyCommittedTxnsByCommitId().size());
  }

  @Test
  public void singleLockCommit() throws MetaException, NoSuchTxnException, TxnAbortedException {
    OpenTxnsResponse rsp = txnManager.openTxns(new OpenTxnRequest(1, "me", "localhost"));
    long txnId = rsp.getTxn_ids().get(0);
    LockComponent lockComponent = new LockComponent(LockType.SHARED_READ, LockLevel.TABLE, "db");
    lockComponent.setTablename("t");
    LockRequest lockRqst = new LockRequest(Collections.singletonList(lockComponent), "me", "localhost");
    lockRqst.setTxnid(txnId);
    LockResponse lockResponse = txnManager.lock(lockRqst);
    Assert.assertEquals(1, lockResponse.getLockid());
    Assert.assertEquals(LockState.ACQUIRED, lockResponse.getState());

    Assert.assertEquals(2, txnManager.copyLockQueues().size());
    Assert.assertTrue(txnManager.copyLockQueues().containsKey(new EntityKey("db", "t", null)));

    txnManager.commitTxn(new CommitTxnRequest(txnId));

    Assert.assertEquals(0, txnManager.copyLockQueues().get(new EntityKey("db", "t", null)).size());

    // This was a shared read lock, so it should have been dropped
    Assert.assertEquals(0, txnManager.copyCommittedTxnsByCommitId().size());
    Assert.assertEquals(0, txnManager.copyCommittedTxnsByTxnId().size());

    txnManager.forceQueueShrinker();
    Assert.assertEquals(0, txnManager.copyLockQueues().size());
  }

  @Test
  public void singleLockAbort() throws MetaException, NoSuchTxnException, TxnAbortedException {
    OpenTxnsResponse rsp = txnManager.openTxns(new OpenTxnRequest(1, "me", "localhost"));
    long txnId = rsp.getTxn_ids().get(0);
    LockComponent lockComponent = new LockComponent(LockType.SHARED_READ, LockLevel.TABLE, "db");
    lockComponent.setTablename("t");
    LockRequest lockRqst = new LockRequest(Collections.singletonList(lockComponent), "me", "localhost");
    lockRqst.setTxnid(txnId);
    LockResponse lockResponse = txnManager.lock(lockRqst);

    txnManager.abortTxn(new AbortTxnRequest(txnId));

    Assert.assertEquals(0, txnManager.copyLockQueues().get(new EntityKey("db", "t", null)).size());

    Assert.assertEquals(0, txnManager.copyAbortedTxns().size());
  }

  @Test
  public void singleSharedWriteLockCommit() throws MetaException, NoSuchTxnException, TxnAbortedException {
    OpenTxnsResponse rsp = txnManager.openTxns(new OpenTxnRequest(1, "me", "localhost"));
    long txnId = rsp.getTxn_ids().get(0);
    LockComponent lockComponent = new LockComponent(LockType.SHARED_WRITE, LockLevel.TABLE, "db");
    lockComponent.setTablename("t");
    LockRequest lockRqst = new LockRequest(Collections.singletonList(lockComponent), "me", "localhost");
    lockRqst.setTxnid(txnId);
    LockResponse lockResponse = txnManager.lock(lockRqst);
    Assert.assertEquals(1, lockResponse.getLockid());
    Assert.assertEquals(LockState.ACQUIRED, lockResponse.getState());

    Assert.assertEquals(2, txnManager.copyLockQueues().size());
    Assert.assertTrue(txnManager.copyLockQueues().containsKey(new EntityKey("db", "t", null)));

    txnManager.commitTxn(new CommitTxnRequest(txnId));

    Assert.assertEquals(0, txnManager.copyLockQueues().get(new EntityKey("db", "t", null)).size());

    Assert.assertEquals(1, txnManager.copyCommittedTxnsByCommitId().size());
    Assert.assertEquals(1, txnManager.copyCommittedTxnsByTxnId().size());
  }

  @Test
  public void singleSharedWriteLockAbort() throws MetaException, NoSuchTxnException, TxnAbortedException {
    OpenTxnsResponse rsp = txnManager.openTxns(new OpenTxnRequest(1, "me", "localhost"));
    long txnId = rsp.getTxn_ids().get(0);
    LockComponent lockComponent = new LockComponent(LockType.SHARED_WRITE, LockLevel.TABLE, "db");
    lockComponent.setTablename("t");
    LockRequest lockRqst = new LockRequest(Collections.singletonList(lockComponent), "me", "localhost");
    lockRqst.setTxnid(txnId);
    LockResponse lockResponse = txnManager.lock(lockRqst);

    txnManager.abortTxn(new AbortTxnRequest(txnId));

    Assert.assertEquals(0, txnManager.copyLockQueues().get(new EntityKey("db", "t", null)).size());

    Assert.assertEquals(1, txnManager.copyAbortedTxns().size());

    Assert.assertEquals(1, txnManager.copyAbortedWrites().size());
  }

  @Test
  public void multipleReadLocks() throws MetaException, NoSuchTxnException, TxnAbortedException {
    OpenTxnsResponse rsp = txnManager.openTxns(new OpenTxnRequest(1, "me", "localhost"));
    long txnId = rsp.getTxn_ids().get(0);
    OpenTxnsResponse rsp2 = txnManager.openTxns(new OpenTxnRequest(1, "me", "localhost"));
    long txnId2 = rsp2.getTxn_ids().get(0);
    LockComponent lockComponent = new LockComponent(LockType.SHARED_READ, LockLevel.TABLE, "db");
    lockComponent.setTablename("t");
    LockRequest lockRqst =
        new LockRequest(Collections.singletonList(lockComponent), "me", "localhost");
    lockRqst.setTxnid(txnId);
    LockResponse lockResponse = txnManager.lock(lockRqst);
    Assert.assertEquals(1, lockResponse.getLockid());
    Assert.assertEquals(LockState.ACQUIRED, lockResponse.getState());

    lockRqst = new LockRequest(Collections.singletonList(lockComponent), "me", "localhost");
    lockRqst.setTxnid(txnId2);
    lockResponse = txnManager.lock(lockRqst);
    Assert.assertEquals(LockState.ACQUIRED, lockResponse.getState());
  }

  @Test
  public void multipleSharedWriteLocks() throws MetaException, NoSuchTxnException,
      TxnAbortedException, InterruptedException, ExecutionException, NoSuchLockException {
    OpenTxnsResponse rsp = txnManager.openTxns(new OpenTxnRequest(1, "me", "localhost"));
    long txnId = rsp.getTxn_ids().get(0);
    OpenTxnsResponse rsp2 = txnManager.openTxns(new OpenTxnRequest(1, "me", "localhost"));
    long txnId2 = rsp2.getTxn_ids().get(0);
    LockComponent lockComponent = new LockComponent(LockType.SHARED_WRITE, LockLevel.TABLE, "db");
    lockComponent.setTablename("t");
    LockRequest lockRqst =
        new LockRequest(Collections.singletonList(lockComponent), "me", "localhost");
    lockRqst.setTxnid(txnId);
    LockResponse lockResponse = txnManager.lock(lockRqst);
    Assert.assertEquals(1, lockResponse.getLockid());
    Assert.assertEquals(LockState.ACQUIRED, lockResponse.getState());

    lockRqst = new LockRequest(Collections.singletonList(lockComponent), "me", "localhost");
    lockRqst.setTxnid(txnId2);
    lockResponse = txnManager.lock(lockRqst);
    Assert.assertEquals(LockState.ACQUIRED, lockResponse.getState());

    txnManager.commitTxn(new CommitTxnRequest(txnId));

    // Wait for the lockChecker to finish
    txnManager.waitForLockChecker.get();

    CheckLockRequest check = new CheckLockRequest(lockResponse.getLockid());
    check.setTxnid(txnId2);
    lockResponse = txnManager.checkLock(check);
    Assert.assertEquals(LockState.ACQUIRED, lockResponse.getState());

    // Second commit should check for conflict
    txnManager.commitTxn(new CommitTxnRequest(txnId2));
  }

  @Test
  public void conflictingWrites() throws MetaException, NoSuchTxnException,
      TxnAbortedException, InterruptedException, ExecutionException, NoSuchLockException {
    String retriever = TransactionManager.getHiveConf().getVar(
        HiveConf.ConfVars.TXNMGR_INMEM_WRITE_SET_RETRIEVER_IMPL);
    TransactionManager.getHiveConf().setVar(
        HiveConf.ConfVars.TXNMGR_INMEM_WRITE_SET_RETRIEVER_IMPL,
        TransactionManager.AlwaysConflictingRetriever.class.getName());
    LOG.debug("XXX");
    try {
      OpenTxnsResponse rsp = txnManager.openTxns(new OpenTxnRequest(1, "me", "localhost"));
      long txnId = rsp.getTxn_ids().get(0);
      OpenTxnsResponse rsp2 = txnManager.openTxns(new OpenTxnRequest(1, "me", "localhost"));
      long txnId2 = rsp2.getTxn_ids().get(0);
      LockComponent lockComponent = new LockComponent(LockType.SHARED_WRITE, LockLevel.TABLE, "db");
      lockComponent.setTablename("t");
      LockRequest lockRqst =
          new LockRequest(Collections.singletonList(lockComponent), "me", "localhost");
      lockRqst.setTxnid(txnId);
      LockResponse lockResponse = txnManager.lock(lockRqst);
      Assert.assertEquals(1, lockResponse.getLockid());
      Assert.assertEquals(LockState.ACQUIRED, lockResponse.getState());

      lockRqst = new LockRequest(Collections.singletonList(lockComponent), "me", "localhost");
      lockRqst.setTxnid(txnId2);
      lockResponse = txnManager.lock(lockRqst);
      Assert.assertEquals(LockState.ACQUIRED, lockResponse.getState());

      txnManager.commitTxn(new CommitTxnRequest(txnId));

      // Wait for the lockChecker to finish
      txnManager.waitForLockChecker.get();

      CheckLockRequest check = new CheckLockRequest(lockResponse.getLockid());
      check.setTxnid(txnId2);
      lockResponse = txnManager.checkLock(check);
      Assert.assertEquals(LockState.ACQUIRED, lockResponse.getState());

      // Second commit should conflict
      txnManager.commitTxn(new CommitTxnRequest(txnId2));
      Assert.fail();
    } catch (TxnAbortedException e) {
      // All good, this is what we expected.
    } finally {
      TransactionManager.getHiveConf().setVar(
          HiveConf.ConfVars.TXNMGR_INMEM_WRITE_SET_RETRIEVER_IMPL, retriever);
    }
  }

  @Test
  public void overlappingTxnsDifferentEntities() throws MetaException, NoSuchTxnException,
      TxnAbortedException, InterruptedException, ExecutionException, NoSuchLockException {
    String retriever = TransactionManager.getHiveConf().getVar(
        HiveConf.ConfVars.TXNMGR_INMEM_WRITE_SET_RETRIEVER_IMPL);
    TransactionManager.getHiveConf().setVar(
        HiveConf.ConfVars.TXNMGR_INMEM_WRITE_SET_RETRIEVER_IMPL,
        TransactionManager.AlwaysConflictingRetriever.class.getName());
    try {
      OpenTxnsResponse rsp = txnManager.openTxns(new OpenTxnRequest(1, "me", "localhost"));
      long txnId = rsp.getTxn_ids().get(0);
      OpenTxnsResponse rsp2 = txnManager.openTxns(new OpenTxnRequest(1, "me", "localhost"));
      long txnId2 = rsp2.getTxn_ids().get(0);
      LockComponent lockComponent = new LockComponent(LockType.SHARED_WRITE, LockLevel.TABLE, "db");
      lockComponent.setTablename("t");
      LockRequest lockRqst =
          new LockRequest(Collections.singletonList(lockComponent), "me", "localhost");
      lockRqst.setTxnid(txnId);
      LockResponse lockResponse = txnManager.lock(lockRqst);
      Assert.assertEquals(1, lockResponse.getLockid());
      Assert.assertEquals(LockState.ACQUIRED, lockResponse.getState());

      LockComponent lockComponent2 = new LockComponent(LockType.SHARED_WRITE, LockLevel.TABLE, "db");
      lockComponent.setTablename("u");
      lockRqst = new LockRequest(Collections.singletonList(lockComponent2), "me", "localhost");
      lockRqst.setTxnid(txnId2);
      lockResponse = txnManager.lock(lockRqst);
      Assert.assertEquals(LockState.ACQUIRED, lockResponse.getState());

      txnManager.commitTxn(new CommitTxnRequest(txnId));

      // Wait for the lockChecker to finish
      txnManager.waitForLockChecker.get();

      CheckLockRequest check = new CheckLockRequest(lockResponse.getLockid());
      check.setTxnid(txnId2);
      lockResponse = txnManager.checkLock(check);
      Assert.assertEquals(LockState.ACQUIRED, lockResponse.getState());

      txnManager.commitTxn(new CommitTxnRequest(txnId2));
    } finally {
      TransactionManager.getHiveConf().setVar(
          HiveConf.ConfVars.TXNMGR_INMEM_WRITE_SET_RETRIEVER_IMPL, retriever);
    }
  }

  @Test
  public void conflictingWritesNonOverlappingTxns() throws MetaException, NoSuchTxnException,
      TxnAbortedException, InterruptedException, ExecutionException, NoSuchLockException {
    String retriever = TransactionManager.getHiveConf().getVar(
        HiveConf.ConfVars.TXNMGR_INMEM_WRITE_SET_RETRIEVER_IMPL);
    TransactionManager.getHiveConf().setVar(
        HiveConf.ConfVars.TXNMGR_INMEM_WRITE_SET_RETRIEVER_IMPL,
        TransactionManager.AlwaysConflictingRetriever.class.getName());
    try {
      OpenTxnsResponse rsp = txnManager.openTxns(new OpenTxnRequest(1, "me", "localhost"));
      long txnId = rsp.getTxn_ids().get(0);
      LockComponent lockComponent = new LockComponent(LockType.SHARED_WRITE, LockLevel.TABLE, "db");
      lockComponent.setTablename("t");
      LockRequest lockRqst =
          new LockRequest(Collections.singletonList(lockComponent), "me", "localhost");
      lockRqst.setTxnid(txnId);
      LockResponse lockResponse = txnManager.lock(lockRqst);
      Assert.assertEquals(1, lockResponse.getLockid());
      Assert.assertEquals(LockState.ACQUIRED, lockResponse.getState());

      txnManager.commitTxn(new CommitTxnRequest(txnId));

      OpenTxnsResponse rsp2 = txnManager.openTxns(new OpenTxnRequest(1, "me", "localhost"));
      long txnId2 = rsp2.getTxn_ids().get(0);

      lockRqst = new LockRequest(Collections.singletonList(lockComponent), "me", "localhost");
      lockRqst.setTxnid(txnId2);
      lockResponse = txnManager.lock(lockRqst);
      Assert.assertEquals(LockState.ACQUIRED, lockResponse.getState());

      CheckLockRequest check = new CheckLockRequest(lockResponse.getLockid());
      check.setTxnid(txnId2);
      lockResponse = txnManager.checkLock(check);
      Assert.assertEquals(LockState.ACQUIRED, lockResponse.getState());

      // Second commit should not conflict
      txnManager.commitTxn(new CommitTxnRequest(txnId2));
    } finally {
      TransactionManager.getHiveConf().setVar(
          HiveConf.ConfVars.TXNMGR_INMEM_WRITE_SET_RETRIEVER_IMPL, retriever);
    }
  }

  @Test
  public void multipleSharedWriteLocksAbort() throws MetaException, NoSuchTxnException,
      TxnAbortedException, InterruptedException, ExecutionException, NoSuchLockException {
    OpenTxnsResponse rsp = txnManager.openTxns(new OpenTxnRequest(1, "me", "localhost"));
    long txnId = rsp.getTxn_ids().get(0);
    OpenTxnsResponse rsp2 = txnManager.openTxns(new OpenTxnRequest(1, "me", "localhost"));
    long txnId2 = rsp2.getTxn_ids().get(0);
    LockComponent lockComponent = new LockComponent(LockType.SHARED_WRITE, LockLevel.TABLE, "db");
    lockComponent.setTablename("t");
    LockRequest lockRqst =
        new LockRequest(Collections.singletonList(lockComponent), "me", "localhost");
    lockRqst.setTxnid(txnId);
    LockResponse lockResponse = txnManager.lock(lockRqst);
    Assert.assertEquals(1, lockResponse.getLockid());
    Assert.assertEquals(LockState.ACQUIRED, lockResponse.getState());

    lockRqst = new LockRequest(Collections.singletonList(lockComponent), "me", "localhost");
    lockRqst.setTxnid(txnId2);
    lockResponse = txnManager.lock(lockRqst);
    Assert.assertEquals(LockState.ACQUIRED, lockResponse.getState());

    txnManager.abortTxn(new AbortTxnRequest(txnId));

    // Wait for the lockChecker to finish
    txnManager.waitForLockChecker.get();

    CheckLockRequest check = new CheckLockRequest(lockResponse.getLockid());
    check.setTxnid(txnId2);
    lockResponse = txnManager.checkLock(check);
    Assert.assertEquals(LockState.ACQUIRED, lockResponse.getState());
  }

  // test that acquiring one lock doesn't cause acquisition of other unrelated locks
  @Test
  public void acquisitionInProperQueue() throws MetaException, NoSuchTxnException,
      TxnAbortedException, InterruptedException, ExecutionException, NoSuchLockException {
    OpenTxnsResponse rsp = txnManager.openTxns(new OpenTxnRequest(1, "me", "localhost"));
    long txnId = rsp.getTxn_ids().get(0);
    LockComponent lockComponent = new LockComponent(LockType.EXCLUSIVE, LockLevel.TABLE, "db");
    lockComponent.setTablename("t");
    LockComponent lockComponent2 = new LockComponent(LockType.EXCLUSIVE, LockLevel.TABLE, "db");
    lockComponent2.setTablename("u");
    LockRequest lockRqst =
        new LockRequest(Collections.singletonList(lockComponent), "me", "localhost");
    lockRqst.setTxnid(txnId);
    LockResponse lockResponse = txnManager.lock(lockRqst);
    Assert.assertEquals(1, lockResponse.getLockid());
    Assert.assertEquals(LockState.ACQUIRED, lockResponse.getState());

    OpenTxnsResponse rsp2 = txnManager.openTxns(new OpenTxnRequest(1, "me", "localhost"));
    long txnId2 = rsp2.getTxn_ids().get(0);
    lockRqst = new LockRequest(Collections.singletonList(lockComponent), "me", "localhost");
    lockRqst.setTxnid(txnId2);
    lockResponse = txnManager.lock(lockRqst);
    Assert.assertEquals(LockState.WAITING, lockResponse.getState());

    OpenTxnsResponse rsp3 = txnManager.openTxns(new OpenTxnRequest(1, "me", "localhost"));
    long txnId3 = rsp3.getTxn_ids().get(0);
    lockRqst = new LockRequest(Collections.singletonList(lockComponent2), "me", "localhost");
    lockRqst.setTxnid(txnId3);
    LockResponse lockResponse3 = txnManager.lock(lockRqst);
    Assert.assertEquals(LockState.ACQUIRED, lockResponse3.getState());

    OpenTxnsResponse rsp4 = txnManager.openTxns(new OpenTxnRequest(1, "me", "localhost"));
    long txnId4 = rsp4.getTxn_ids().get(0);
    lockRqst = new LockRequest(Collections.singletonList(lockComponent2), "me", "localhost");
    lockRqst.setTxnid(txnId4);
    LockResponse lockResponse4 = txnManager.lock(lockRqst);
    Assert.assertEquals(LockState.WAITING, lockResponse4.getState());

    txnManager.commitTxn(new CommitTxnRequest(txnId));

    // Wait for the lockChecker to finish
    txnManager.waitForLockChecker.get();

    CheckLockRequest check = new CheckLockRequest(lockResponse.getLockid());
    check.setTxnid(txnId2);
    lockResponse = txnManager.checkLock(check);
    Assert.assertEquals(LockState.ACQUIRED, lockResponse.getState());

    check = new CheckLockRequest(lockResponse3.getLockid());
    check.setTxnid(txnId4);
    lockResponse = txnManager.checkLock(check);
    Assert.assertEquals(LockState.WAITING, lockResponse.getState());
  }

  // test that when some locks are acquired and some not we are still told to wait
  @Test
  public void someWaitingMeansWaiting() throws MetaException, NoSuchTxnException,
      TxnAbortedException, InterruptedException, ExecutionException, NoSuchLockException {
    OpenTxnsResponse rsp = txnManager.openTxns(new OpenTxnRequest(1, "me", "localhost"));
    long txnId = rsp.getTxn_ids().get(0);
    LockComponent lockComponent = new LockComponent(LockType.EXCLUSIVE, LockLevel.TABLE, "db");
    lockComponent.setTablename("t");
    LockRequest lockRqst =
        new LockRequest(Collections.singletonList(lockComponent), "me", "localhost");
    lockRqst.setTxnid(txnId);
    LockResponse lockResponse = txnManager.lock(lockRqst);
    Assert.assertEquals(1, lockResponse.getLockid());
    Assert.assertEquals(LockState.ACQUIRED, lockResponse.getState());

    OpenTxnsResponse rsp2 = txnManager.openTxns(new OpenTxnRequest(1, "me", "localhost"));
    long txnId2 = rsp2.getTxn_ids().get(0);
    LockComponent lockComponent2 = new LockComponent(LockType.SHARED_WRITE, LockLevel.TABLE, "db");
    lockComponent2.setTablename("u");
    lockRqst = new LockRequest(Arrays.asList(lockComponent, lockComponent2), "me", "localhost");
    lockRqst.setTxnid(txnId2);
    lockResponse = txnManager.lock(lockRqst);
    Assert.assertEquals(LockState.WAITING, lockResponse.getState());
  }

  // test that intention locks work to block others
  @Test
  public void intentionBlocks() throws MetaException, NoSuchTxnException,
      TxnAbortedException, InterruptedException, ExecutionException, NoSuchLockException {
    OpenTxnsResponse rsp = txnManager.openTxns(new OpenTxnRequest(1, "me", "localhost"));
    long txnId = rsp.getTxn_ids().get(0);
    LockComponent lockComponent = new LockComponent(LockType.SHARED_READ, LockLevel.TABLE, "db");
    lockComponent.setTablename("t");
    LockRequest lockRqst =
        new LockRequest(Collections.singletonList(lockComponent), "me", "localhost");
    lockRqst.setTxnid(txnId);
    LockResponse lockResponse = txnManager.lock(lockRqst);
    Assert.assertEquals(1, lockResponse.getLockid());
    Assert.assertEquals(LockState.ACQUIRED, lockResponse.getState());

    Assert.assertEquals(2, txnManager.copyOpenTxns().get(txnId).getHiveLocks().length);

    OpenTxnsResponse rsp3 = txnManager.openTxns(new OpenTxnRequest(1, "me", "localhost"));
    long txnId3 = rsp3.getTxn_ids().get(0);
    LockComponent lockComponent3 = new LockComponent(LockType.SHARED_READ, LockLevel.PARTITION, "db");
    lockComponent3.setTablename("t");
    lockComponent3.setPartitionname("p");
    LockRequest lockRqst3 = new LockRequest(Collections.singletonList(lockComponent3), "me",
        "localhost");
    lockRqst3.setTxnid(txnId3);
    LockResponse lockResponse3 = txnManager.lock(lockRqst3);
    Assert.assertEquals(LockState.ACQUIRED, lockResponse3.getState());
    Assert.assertEquals(3, txnManager.copyOpenTxns().get(txnId3).getHiveLocks().length);

    OpenTxnsResponse rsp2 = txnManager.openTxns(new OpenTxnRequest(1, "me", "localhost"));
    long txnId2 = rsp2.getTxn_ids().get(0);
    LockComponent lockComponent2 = new LockComponent(LockType.EXCLUSIVE, LockLevel.DB, "db");
    LockRequest lockRqst2 = new LockRequest(Collections.singletonList(lockComponent2), "me",
        "localhost");
    lockRqst2.setTxnid(txnId2);
    LockResponse lockResponse2 = txnManager.lock(lockRqst2);
    Assert.assertEquals(LockState.WAITING, lockResponse2.getState());
    Assert.assertEquals(1, txnManager.copyOpenTxns().get(txnId2).getHiveLocks().length);

    txnManager.commitTxn(new CommitTxnRequest(txnId));
    txnManager.commitTxn(new CommitTxnRequest(txnId3));
    txnManager.waitForLockChecker.get();

    CheckLockRequest check = new CheckLockRequest(lockResponse2.getLockid());
    check.setTxnid(txnId2);
    LockResponse lockResponse4 = txnManager.checkLock(check);
    Assert.assertEquals(LockState.ACQUIRED, lockResponse4.getState());
  }

  @Test
  public void multipleExclusiveLocksDifferentEntities() throws MetaException, NoSuchTxnException,
      TxnAbortedException {
    OpenTxnsResponse rsp = txnManager.openTxns(new OpenTxnRequest(1, "me", "localhost"));
    long txnId = rsp.getTxn_ids().get(0);
    OpenTxnsResponse rsp2 = txnManager.openTxns(new OpenTxnRequest(1, "me", "localhost"));
    long txnId2 = rsp2.getTxn_ids().get(0);
    LockComponent lockComponent = new LockComponent(LockType.EXCLUSIVE, LockLevel.TABLE, "db");
    lockComponent.setTablename("t");
    LockRequest lockRqst = new LockRequest(Collections.singletonList(lockComponent), "me", "localhost");
    lockRqst.setTxnid(txnId);
    LockResponse lockResponse = txnManager.lock(lockRqst);
    Assert.assertEquals(1, lockResponse.getLockid());
    Assert.assertEquals(LockState.ACQUIRED, lockResponse.getState());

    lockComponent = new LockComponent(LockType.EXCLUSIVE, LockLevel.TABLE, "db");
    lockComponent.setTablename("u");
    lockRqst = new LockRequest(Collections.singletonList(lockComponent), "me", "localhost");
    lockRqst.setTxnid(txnId2);
    lockResponse = txnManager.lock(lockRqst);
    Assert.assertEquals(LockState.ACQUIRED, lockResponse.getState());
  }

  @Test
  public void abortedTxnForgetter() throws MetaException, NoSuchTxnException, TxnAbortedException {
    OpenTxnsResponse rsp = txnManager.openTxns(new OpenTxnRequest(1, "me", "localhost"));
    long txnId = rsp.getTxn_ids().get(0);
    LockComponent lockComponent = new LockComponent(LockType.SHARED_WRITE, LockLevel.TABLE, "db");
    lockComponent.setTablename("t");
    LockRequest lockRqst = new LockRequest(Collections.singletonList(lockComponent), "me",
        "localhost");
    lockRqst.setTxnid(txnId);
    LockResponse lockResponse = txnManager.lock(lockRqst);

    txnManager.abortTxn(new AbortTxnRequest(txnId));

    Assert.assertEquals(1, txnManager.copyAbortedTxns().size());
    Assert.assertEquals(1, txnManager.copyAbortedWrites().size());

    txnManager.markCompacted(new CompactionInfo("db", "t", null, CompactionType.MAJOR));

    txnManager.forceTxnForgetter();
    txnManager.forceQueueShrinker();
    Assert.assertEquals(0, txnManager.copyAbortedTxns().size());
    Assert.assertEquals(0, txnManager.copyAbortedWrites().size());
  }

  @Test
  public void committedTxnsForgotten() throws MetaException, NoSuchTxnException,
      TxnAbortedException {
    OpenTxnsResponse rsp = txnManager.openTxns(new OpenTxnRequest(1, "me", "localhost"));
    long txnId = rsp.getTxn_ids().get(0);
    LockComponent lockComponent = new LockComponent(LockType.SHARED_WRITE, LockLevel.TABLE, "db");
    lockComponent.setTablename("t");
    LockRequest lockRqst = new LockRequest(Collections.singletonList(lockComponent), "me",
        "localhost");
    lockRqst.setTxnid(txnId);
    LockResponse lockResponse = txnManager.lock(lockRqst);

    txnManager.commitTxn(new CommitTxnRequest(txnId));

    // This one should be forgotten, as there are no possible conflicts.
    Assert.assertEquals(1, txnManager.copyCommittedTxnsByTxnId().size());
    Assert.assertEquals(1, txnManager.copyCommittedTxnsByCommitId().size());
    Assert.assertEquals(1, txnManager.copyCommittedWrites().size());
    txnManager.forceTxnForgetter();
    txnManager.forceQueueShrinker();
    Assert.assertEquals(0, txnManager.copyCommittedTxnsByTxnId().size());
    Assert.assertEquals(0, txnManager.copyCommittedTxnsByCommitId().size());
    Assert.assertEquals(0, txnManager.copyCommittedWrites().size());

    OpenTxnsResponse rsp2 = txnManager.openTxns(new OpenTxnRequest(1, "me", "localhost"));
    long txnId2 = rsp2.getTxn_ids().get(0);
    LockRequest lockRqst2 = new LockRequest(Collections.singletonList(lockComponent), "me",
        "localhost");
    lockRqst2.setTxnid(txnId2);
    LockResponse lockResponse2 = txnManager.lock(lockRqst2);

    OpenTxnsResponse rsp3 = txnManager.openTxns(new OpenTxnRequest(1, "me", "localhost"));
    long txnId3 = rsp3.getTxn_ids().get(0);
    LockRequest lockRqst3 = new LockRequest(Collections.singletonList(lockComponent), "me",
        "localhost");
    lockRqst3.setTxnid(txnId3);
    LockResponse lockResponse3 = txnManager.lock(lockRqst3);

    txnManager.commitTxn(new CommitTxnRequest(txnId2));

    // This one should be kept, as it could conflict with txn3 later
    Assert.assertEquals(1, txnManager.copyCommittedTxnsByTxnId().size());
    Assert.assertEquals(1, txnManager.copyCommittedTxnsByCommitId().size());
    Assert.assertEquals(1, txnManager.copyCommittedWrites().size());
    txnManager.forceTxnForgetter();
    txnManager.forceQueueShrinker();
    Assert.assertEquals(1, txnManager.copyCommittedTxnsByTxnId().size());
    Assert.assertEquals(1, txnManager.copyCommittedTxnsByCommitId().size());
    Assert.assertEquals(1, txnManager.copyCommittedWrites().size());
  }

  @Test
  public void heartbeatAndTimeout() throws MetaException, NoSuchTxnException, TxnAbortedException, NoSuchLockException, InterruptedException {
    OpenTxnsResponse rsp = txnManager.openTxns(new OpenTxnRequest(1, "me", "localhost"));
    Assert.assertEquals(1, rsp.getTxn_idsSize());
    Assert.assertEquals(1, (long)rsp.getTxn_ids().get(0));
    long txnId = rsp.getTxn_ids().get(0);

    HeartbeatRequest heartbeat = new HeartbeatRequest();
    heartbeat.setTxnid(txnId);
    txnManager.heartbeat(heartbeat);

    txnManager.forceTimedOutCleaner();

    // Should be fine, we shouldn't have timed out
    txnManager.heartbeat(heartbeat);

    long timeout = TransactionManager.getHiveConf().getTimeVar(
        HiveConf.ConfVars.HIVE_TXN_TIMEOUT, TimeUnit.MILLISECONDS);
    TransactionManager.getHiveConf().setTimeVar(
        HiveConf.ConfVars.HIVE_TXN_TIMEOUT, 1, TimeUnit.MILLISECONDS);
    try {
      Thread.sleep(2);

      // Now we should timeout
      txnManager.forceTimedOutCleaner();
      txnManager.heartbeat(heartbeat);
      Assert.fail();
    } catch (TxnAbortedException|NoSuchTxnException e) {
      // This is what we wanted.
    } finally {
      TransactionManager.getHiveConf().setTimeVar(
          HiveConf.ConfVars.HIVE_TXN_TIMEOUT, timeout, TimeUnit.MILLISECONDS);
    }
  }

  // Test that deadlock detection works
  @Test
  public void deadlockDetection() throws MetaException, NoSuchTxnException,
      TxnAbortedException, NoSuchLockException {
    OpenTxnsResponse rsp = txnManager.openTxns(new OpenTxnRequest(1, "me", "localhost"));
    long txnId = rsp.getTxn_ids().get(0);
    OpenTxnsResponse rsp2 = txnManager.openTxns(new OpenTxnRequest(1, "me", "localhost"));
    long txnId2 = rsp2.getTxn_ids().get(0);
    LockComponent lockComponent = new LockComponent(LockType.EXCLUSIVE, LockLevel.TABLE, "db");
    lockComponent.setTablename("t");
    LockRequest lockRqst = new LockRequest(Collections.singletonList(lockComponent), "me", "localhost");
    lockRqst.setTxnid(txnId);
    LockResponse lockResponse = txnManager.lock(lockRqst);
    Assert.assertEquals(LockState.ACQUIRED, lockResponse.getState());

    LockComponent lockComponent2 = new LockComponent(LockType.EXCLUSIVE, LockLevel.TABLE, "db");
    lockComponent2.setTablename("u");
    LockRequest lockRqst2 = new LockRequest(Collections.singletonList(lockComponent2), "me", "localhost");
    lockRqst2.setTxnid(txnId2);
    LockResponse lockResponse2 = txnManager.lock(lockRqst2);
    LOG.debug("Second lockRequest got back " + lockResponse2);
    Assert.assertEquals(LockState.ACQUIRED, lockResponse2.getState());

    LockRequest lockRqst3 = new LockRequest(Collections.singletonList(lockComponent2), "me", "localhost");
    lockRqst3.setTxnid(txnId);
    LockResponse lockResponse3 = txnManager.lock(lockRqst3);
    Assert.assertEquals(LockState.WAITING, lockResponse3.getState());

    LockRequest lockRqst4 = new LockRequest(Collections.singletonList(lockComponent), "me", "localhost");
    lockRqst4.setTxnid(txnId2);
    LockResponse lockResponse4 = txnManager.lock(lockRqst4);
    Assert.assertEquals(LockState.WAITING, lockResponse4.getState());

    // Excellent, we're deadlocked
    txnManager.forceDeadlockDetector();

    // One of the transactions should be aborted.  Which one is not determined.  But it can't be
    // both.
    boolean oneAborted = false, twoAborted = false;
    try {
      HeartbeatRequest heartbeat = new HeartbeatRequest();
      heartbeat.setTxnid(txnId);
      txnManager.heartbeat(heartbeat);
    } catch (TxnAbortedException|NoSuchTxnException e) {
      oneAborted = true;
    }
    try {
      HeartbeatRequest heartbeat = new HeartbeatRequest();
      heartbeat.setTxnid(txnId2);
      txnManager.heartbeat(heartbeat);
    } catch (TxnAbortedException|NoSuchTxnException e) {
      twoAborted = true;
    }

    Assert.assertTrue("Expected one or two to be aborted, not both, one aborted = " + oneAborted
        + " twoAborted = " + twoAborted, (oneAborted || twoAborted) && !(oneAborted && twoAborted));
  }


  // TODO test recovery

}
