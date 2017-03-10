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
import org.apache.hadoop.hive.metastore.api.CommitTxnRequest;
import org.apache.hadoop.hive.metastore.api.CompactionType;
import org.apache.hadoop.hive.metastore.api.GetOpenTxnsResponse;
import org.apache.hadoop.hive.metastore.api.LockComponent;
import org.apache.hadoop.hive.metastore.api.LockLevel;
import org.apache.hadoop.hive.metastore.api.LockRequest;
import org.apache.hadoop.hive.metastore.api.LockResponse;
import org.apache.hadoop.hive.metastore.api.LockState;
import org.apache.hadoop.hive.metastore.api.LockType;
import org.apache.hadoop.hive.metastore.api.MetaException;
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

import java.util.Arrays;
import java.util.Collections;

import static org.apache.logging.log4j.core.impl.ThrowableFormatOptions.CLASS_NAME;

public class TestTransactionManager {

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
    txnManager.selfDestruct();
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
    Assert.assertEquals(0, txnManager.copyCommittedTxns().size());
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
    Assert.assertEquals(0, txnManager.copyCommittedTxns().size());
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

    Assert.assertEquals(1, txnManager.copyLockQueues().size());
    Assert.assertTrue(txnManager.copyLockQueues().containsKey(new EntityKey("db", "t", null)));

    txnManager.commitTxn(new CommitTxnRequest(txnId));

    Assert.assertEquals(0, txnManager.copyLockQueues().get(new EntityKey("db", "t", null)).queue.size());

    // This was a shared read lock, so it should have been dropped
    Assert.assertEquals(0, txnManager.copyCommittedTxns().size());

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

    Assert.assertEquals(0, txnManager.copyLockQueues().get(new EntityKey("db", "t", null)).queue.size());

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

    Assert.assertEquals(1, txnManager.copyLockQueues().size());
    Assert.assertTrue(txnManager.copyLockQueues().containsKey(new EntityKey("db", "t", null)));

    txnManager.commitTxn(new CommitTxnRequest(txnId));

    Assert.assertEquals(0, txnManager.copyLockQueues().get(new EntityKey("db", "t", null)).queue.size());
    Assert.assertEquals(txnId, txnManager.copyLockQueues().get(new EntityKey("db", "t", null)).maxCommitId);

    Assert.assertEquals(1, txnManager.copyCommittedTxns().size());
  }

  @Test
  public void singleSharedWriteLockAbort() throws MetaException, NoSuchTxnException, TxnAbortedException {
    OpenTxnsResponse rsp = txnManager.openTxns(new OpenTxnRequest(1, "me", "localhost"));
    long txnId = rsp.getTxn_ids().get(0);
    LockComponent lockComponent = new LockComponent(LockType.SHARED_WRITE, LockLevel.TABLE, "db");
    lockComponent.setTablename("t");
    LockComponent lockComponent2 = new LockComponent(LockType.INTENTION, LockLevel.DB, "db");
    LockRequest lockRqst = new LockRequest(Arrays.asList(lockComponent, lockComponent2), "me", "localhost");
    lockRqst.setTxnid(txnId);
    LockResponse lockResponse = txnManager.lock(lockRqst);

    txnManager.abortTxn(new AbortTxnRequest(txnId));

    Assert.assertEquals(0, txnManager.copyLockQueues().get(new EntityKey("db", "t", null)).queue.size());

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
    LockRequest lockRqst = new LockRequest(Collections.singletonList(lockComponent), "me", "localhost");
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
  public void multipleSharedWriteLocks() throws MetaException, NoSuchTxnException, TxnAbortedException {
    OpenTxnsResponse rsp = txnManager.openTxns(new OpenTxnRequest(1, "me", "localhost"));
    long txnId = rsp.getTxn_ids().get(0);
    OpenTxnsResponse rsp2 = txnManager.openTxns(new OpenTxnRequest(1, "me", "localhost"));
    long txnId2 = rsp2.getTxn_ids().get(0);
    LockComponent lockComponent = new LockComponent(LockType.SHARED_WRITE, LockLevel.TABLE, "db");
    lockComponent.setTablename("t");
    LockRequest lockRqst = new LockRequest(Collections.singletonList(lockComponent), "me", "localhost");
    lockRqst.setTxnid(txnId);
    LockResponse lockResponse = txnManager.lock(lockRqst);
    Assert.assertEquals(1, lockResponse.getLockid());
    Assert.assertEquals(LockState.ACQUIRED, lockResponse.getState());

    lockRqst = new LockRequest(Collections.singletonList(lockComponent), "me", "localhost");
    lockRqst.setTxnid(txnId2);
    lockResponse = txnManager.lock(lockRqst);
    Assert.assertEquals(LockState.WAITING, lockResponse.getState());
  }

  @Test
  public void multipleSharedWriteLocksDifferentEntities() throws MetaException, NoSuchTxnException, TxnAbortedException {
    OpenTxnsResponse rsp = txnManager.openTxns(new OpenTxnRequest(1, "me", "localhost"));
    long txnId = rsp.getTxn_ids().get(0);
    OpenTxnsResponse rsp2 = txnManager.openTxns(new OpenTxnRequest(1, "me", "localhost"));
    long txnId2 = rsp2.getTxn_ids().get(0);
    LockComponent lockComponent = new LockComponent(LockType.SHARED_WRITE, LockLevel.TABLE, "db");
    lockComponent.setTablename("t");
    LockRequest lockRqst = new LockRequest(Collections.singletonList(lockComponent), "me", "localhost");
    lockRqst.setTxnid(txnId);
    LockResponse lockResponse = txnManager.lock(lockRqst);
    Assert.assertEquals(1, lockResponse.getLockid());
    Assert.assertEquals(LockState.ACQUIRED, lockResponse.getState());

    lockComponent = new LockComponent(LockType.SHARED_WRITE, LockLevel.TABLE, "db");
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
    LockComponent lockComponent2 = new LockComponent(LockType.INTENTION, LockLevel.DB, "db");
    LockRequest lockRqst = new LockRequest(Arrays.asList(lockComponent, lockComponent2), "me", "localhost");
    lockRqst.setTxnid(txnId);
    LockResponse lockResponse = txnManager.lock(lockRqst);

    txnManager.abortTxn(new AbortTxnRequest(txnId));

    Assert.assertEquals(1, txnManager.copyAbortedTxns().size());
    Assert.assertEquals(1, txnManager.copyAbortedWrites().size());

    txnManager.markCompacted(new CompactionInfo("db", "t", null, CompactionType.MAJOR));

    txnManager.forceAbortedTxnForgetter();
    txnManager.forceQueueShrinker();
    Assert.assertEquals(0, txnManager.copyAbortedTxns().size());
    Assert.assertEquals(0, txnManager.copyAbortedWrites().size());
  }
  // TODO Test that the various threads do what they are supposed to.

}
