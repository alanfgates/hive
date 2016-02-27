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
import org.apache.hadoop.hive.metastore.hbase.HBaseStore;
import org.apache.hadoop.hive.metastore.hbase.HbaseMetastoreProto;
import org.apache.hadoop.hive.metastore.hbase.MockUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.IOException;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

/**
 * Many of these tests involve timing, so they're kept in a separate test class so they can be
 * excluded from the Jenkins runs.
 */
public class TestTransactionManagerLocks {
  @Mock
  HTableInterface htable;
  SortedMap<String, Cell> rows = new TreeMap<>();
  HBaseStore store;
  TransactionManager txnMgr;

  @Before
  public void init() throws IOException {
    MockitoAnnotations.initMocks(this);
    HiveConf conf = new HiveConf();

    // Set the wait on the background threads to max long so that they don't run and clean things
    // up on us, since we're trying to check state.
    conf.set(TransactionManager.CONF_NO_AUTO_BACKGROUND_THREADS, Boolean.toString(Boolean.TRUE));

    // Set this super low so the test doesn't take forever
    HiveConf.setTimeVar(conf, HiveConf.ConfVars.METASTORE_HBASE_TXN_MGR_LOCK_POLL_TIMEOUT,
        100, TimeUnit.MILLISECONDS);

    store = MockUtils.init(conf, htable, rows);
    txnMgr = new TransactionManager(conf);
  }

  @After
  public void cleanup() throws IOException {
    txnMgr.shutdown();
  }

  @Test
  public void exclusiveExclusive() throws Exception {
    lockCombo(HbaseMetastoreProto.LockType.EXCLUSIVE, HbaseMetastoreProto.LockType.EXCLUSIVE,
        HbaseMetastoreProto.LockState.WAITING);
  }

  @Test
  public void exclusiveSharedWrite() throws Exception {
    lockCombo(HbaseMetastoreProto.LockType.EXCLUSIVE, HbaseMetastoreProto.LockType.SHARED_WRITE,
        HbaseMetastoreProto.LockState.WAITING);
  }

  @Test
  public void exclusiveSharedRead() throws Exception {
    lockCombo(HbaseMetastoreProto.LockType.EXCLUSIVE, HbaseMetastoreProto.LockType.SHARED_READ,
        HbaseMetastoreProto.LockState.WAITING);
  }

  @Test
  public void exclusiveIntention() throws Exception {
    lockCombo(HbaseMetastoreProto.LockType.EXCLUSIVE, HbaseMetastoreProto.LockType.INTENTION,
        HbaseMetastoreProto.LockState.WAITING);
  }

  @Test
  public void sharedWriteExclusive() throws Exception {
    lockCombo(HbaseMetastoreProto.LockType.SHARED_WRITE, HbaseMetastoreProto.LockType.EXCLUSIVE,
        HbaseMetastoreProto.LockState.WAITING);
  }

  @Test
  public void sharedWriteSharedWrite() throws Exception {
    lockCombo(HbaseMetastoreProto.LockType.SHARED_WRITE, HbaseMetastoreProto.LockType.SHARED_WRITE,
        HbaseMetastoreProto.LockState.WAITING);
  }

  @Test
  public void sharedWriteSharedRead() throws Exception {
    lockCombo(HbaseMetastoreProto.LockType.SHARED_WRITE, HbaseMetastoreProto.LockType.SHARED_READ,
        HbaseMetastoreProto.LockState.ACQUIRED);
  }

  @Test
  public void sharedWriteIntention() throws Exception {
    lockCombo(HbaseMetastoreProto.LockType.SHARED_WRITE, HbaseMetastoreProto.LockType.INTENTION,
        HbaseMetastoreProto.LockState.ACQUIRED);
  }

  @Test
  public void sharedReadExclusive() throws Exception {
    lockCombo(HbaseMetastoreProto.LockType.SHARED_READ, HbaseMetastoreProto.LockType.EXCLUSIVE,
        HbaseMetastoreProto.LockState.WAITING);
  }

  @Test
  public void sharedReadSharedWrite() throws Exception {
    lockCombo(HbaseMetastoreProto.LockType.SHARED_READ, HbaseMetastoreProto.LockType.SHARED_WRITE,
        HbaseMetastoreProto.LockState.ACQUIRED);
  }

  @Test
  public void sharedReadSharedRead() throws Exception {
    lockCombo(HbaseMetastoreProto.LockType.SHARED_READ, HbaseMetastoreProto.LockType.SHARED_READ,
        HbaseMetastoreProto.LockState.ACQUIRED);
  }

  @Test
  public void sharedReadIntention() throws Exception {
    lockCombo(HbaseMetastoreProto.LockType.SHARED_READ, HbaseMetastoreProto.LockType.INTENTION,
        HbaseMetastoreProto.LockState.ACQUIRED);
  }

  @Test
  public void intentionExclusive() throws Exception {
    lockCombo(HbaseMetastoreProto.LockType.INTENTION, HbaseMetastoreProto.LockType.EXCLUSIVE,
        HbaseMetastoreProto.LockState.WAITING);
  }

  @Test
  public void intentionSharedWrite() throws Exception {
    lockCombo(HbaseMetastoreProto.LockType.INTENTION, HbaseMetastoreProto.LockType.SHARED_WRITE,
        HbaseMetastoreProto.LockState.ACQUIRED);
  }

  @Test
  public void intentionSharedRead() throws Exception {
    lockCombo(HbaseMetastoreProto.LockType.INTENTION, HbaseMetastoreProto.LockType.SHARED_READ,
        HbaseMetastoreProto.LockState.ACQUIRED);
  }

  @Test
  public void intentionIntention() throws Exception {
    lockCombo(HbaseMetastoreProto.LockType.INTENTION, HbaseMetastoreProto.LockType.INTENTION,
        HbaseMetastoreProto.LockState.ACQUIRED);
  }

  private void lockCombo(HbaseMetastoreProto.LockType first, HbaseMetastoreProto.LockType second,
                         HbaseMetastoreProto.LockState expectedState)  throws Exception {
    HbaseMetastoreProto.OpenTxnsRequest rqst = HbaseMetastoreProto.OpenTxnsRequest.newBuilder()
        .setNumTxns(2)
        .setUser("me")
        .setHostname("localhost")
        .build();
    HbaseMetastoreProto.OpenTxnsResponse rsp = txnMgr.openTxns(rqst);
    long firstTxn = rsp.getTxnIds(0);
    long secondTxn = rsp.getTxnIds(1);
    try {

      HbaseMetastoreProto.LockResponse lock =
          txnMgr.lock(HbaseMetastoreProto.LockRequest.newBuilder()
              .setTxnId(firstTxn)
              .addComponents(HbaseMetastoreProto.LockComponent.newBuilder()
                  .setDb("d")
                  .setType(first)
                  .build())
              .build());
      Assert.assertEquals(HbaseMetastoreProto.LockState.ACQUIRED, lock.getState());

      lock = txnMgr.lock(HbaseMetastoreProto.LockRequest.newBuilder()
          .setTxnId(secondTxn)
          .addComponents(HbaseMetastoreProto.LockComponent.newBuilder()
              .setDb("d")
              .setType(second)
              .build())
          .build());
      Assert.assertEquals(expectedState, lock.getState());
    } finally {
      for (long i = firstTxn; i <= secondTxn; i++) {
        txnMgr.commitTxn(HbaseMetastoreProto.TransactionId
            .newBuilder()
            .setId(i)
            .build());
      }
    }
  }

  @Test
  public void acquireAfterAbort()  throws Exception {
    HbaseMetastoreProto.OpenTxnsRequest rqst = HbaseMetastoreProto.OpenTxnsRequest.newBuilder()
        .setNumTxns(2)
        .setUser("me")
        .setHostname("localhost")
        .build();
    HbaseMetastoreProto.OpenTxnsResponse rsp = txnMgr.openTxns(rqst);
    long firstTxn = rsp.getTxnIds(0);
    long secondTxn = rsp.getTxnIds(1);
    try {

      HbaseMetastoreProto.LockResponse lock =
          txnMgr.lock(HbaseMetastoreProto.LockRequest.newBuilder()
              .setTxnId(firstTxn)
              .addComponents(HbaseMetastoreProto.LockComponent.newBuilder()
                  .setDb("d")
                  .setTable("t")
                  .setType(HbaseMetastoreProto.LockType.SHARED_WRITE)
                  .build())
              .addComponents(HbaseMetastoreProto.LockComponent.newBuilder()
                  .setDb("d")
                  .setType(HbaseMetastoreProto.LockType.INTENTION)
                  .build())
              .build());
      Assert.assertEquals(HbaseMetastoreProto.LockState.ACQUIRED, lock.getState());

      lock = txnMgr.lock(HbaseMetastoreProto.LockRequest.newBuilder()
          .setTxnId(secondTxn)
          .addComponents(HbaseMetastoreProto.LockComponent.newBuilder()
              .setDb("d")
              .setTable("t")
              .setType(HbaseMetastoreProto.LockType.SHARED_WRITE)
              .build())
          .addComponents(HbaseMetastoreProto.LockComponent.newBuilder()
              .setDb("d")
              .setType(HbaseMetastoreProto.LockType.INTENTION)
              .build())
          .build());
      Assert.assertEquals(HbaseMetastoreProto.LockState.WAITING, lock.getState());

      HbaseMetastoreProto.TransactionResult abort =
          txnMgr.abortTxn(HbaseMetastoreProto.TransactionId.newBuilder()
              .setId(firstTxn)
              .build());
      Assert.assertEquals(HbaseMetastoreProto.TxnStateChangeResult.SUCCESS, abort.getState());

      // Give it a bit to run and check the locks
      Thread.sleep(100);

      lock = txnMgr.checkLocks(HbaseMetastoreProto.TransactionId.newBuilder()
              .setId(secondTxn)
              .build());
      Assert.assertEquals(HbaseMetastoreProto.LockState.ACQUIRED, lock.getState());
    } finally {
      for (long i = firstTxn; i <= secondTxn; i++) {
        txnMgr.commitTxn(HbaseMetastoreProto.TransactionId
            .newBuilder()
            .setId(i)
            .build());
      }
    }
  }

  @Test
  public void lostUpdateDetection()  throws Exception {
    // Detect that if a partition has potentially changed underneath a writer their transaction
    // is aborted.
    HbaseMetastoreProto.OpenTxnsRequest rqst = HbaseMetastoreProto.OpenTxnsRequest.newBuilder()
        .setNumTxns(2)
        .setUser("me")
        .setHostname("localhost")
        .build();
    HbaseMetastoreProto.OpenTxnsResponse rsp = txnMgr.openTxns(rqst);
    long firstTxn = rsp.getTxnIds(0);
    long secondTxn = rsp.getTxnIds(1);
    try {

      HbaseMetastoreProto.LockResponse lock =
          txnMgr.lock(HbaseMetastoreProto.LockRequest.newBuilder()
              .setTxnId(firstTxn)
              .addComponents(HbaseMetastoreProto.LockComponent.newBuilder()
                  .setDb("d")
                  .setTable("t")
                  .setType(HbaseMetastoreProto.LockType.SHARED_WRITE)
                  .build())
              .addComponents(HbaseMetastoreProto.LockComponent.newBuilder()
                  .setDb("d")
                  .setType(HbaseMetastoreProto.LockType.INTENTION)
                  .build())
              .build());
      Assert.assertEquals(HbaseMetastoreProto.LockState.ACQUIRED, lock.getState());

      lock = txnMgr.lock(HbaseMetastoreProto.LockRequest.newBuilder()
          .setTxnId(secondTxn)
          .addComponents(HbaseMetastoreProto.LockComponent.newBuilder()
              .setDb("d")
              .setTable("t")
              .setType(HbaseMetastoreProto.LockType.SHARED_WRITE)
              .build())
          .addComponents(HbaseMetastoreProto.LockComponent.newBuilder()
              .setDb("d")
              .setType(HbaseMetastoreProto.LockType.INTENTION)
              .build())
          .build());
      Assert.assertEquals(HbaseMetastoreProto.LockState.WAITING, lock.getState());

      HbaseMetastoreProto.TransactionResult commit =
          txnMgr.commitTxn(HbaseMetastoreProto.TransactionId.newBuilder()
              .setId(firstTxn)
              .build());
      Assert.assertEquals(HbaseMetastoreProto.TxnStateChangeResult.SUCCESS, commit.getState());

      // Give it a bit to run and check the locks
      Thread.sleep(100);

      lock = txnMgr.checkLocks(HbaseMetastoreProto.TransactionId.newBuilder()
          .setId(secondTxn)
          .build());
      Assert.assertEquals(HbaseMetastoreProto.LockState.TXN_ABORTED, lock.getState());
    } finally {
      for (long i = firstTxn; i <= secondTxn; i++) {
        txnMgr.commitTxn(HbaseMetastoreProto.TransactionId
            .newBuilder()
            .setId(i)
            .build());
      }
    }
  }

  @Test
  public void acquireAfterCommit()  throws Exception {
    // Detect that if a partition has potentially changed underneath a writer their transaction
    // is aborted.
    HbaseMetastoreProto.OpenTxnsRequest rqst = HbaseMetastoreProto.OpenTxnsRequest.newBuilder()
        .setNumTxns(2)
        .setUser("me")
        .setHostname("localhost")
        .build();
    HbaseMetastoreProto.OpenTxnsResponse rsp = txnMgr.openTxns(rqst);
    long firstTxn = rsp.getTxnIds(0);
    long secondTxn = rsp.getTxnIds(1);
    try {

      HbaseMetastoreProto.LockResponse lock =
          txnMgr.lock(HbaseMetastoreProto.LockRequest.newBuilder()
              .setTxnId(firstTxn)
              .addComponents(HbaseMetastoreProto.LockComponent.newBuilder()
                  .setDb("d")
                  .setTable("t")
                  .setType(HbaseMetastoreProto.LockType.EXCLUSIVE)
                  .build())
              .addComponents(HbaseMetastoreProto.LockComponent.newBuilder()
                  .setDb("d")
                  .setType(HbaseMetastoreProto.LockType.INTENTION)
                  .build())
              .build());
      Assert.assertEquals(HbaseMetastoreProto.LockState.ACQUIRED, lock.getState());

      lock = txnMgr.lock(HbaseMetastoreProto.LockRequest.newBuilder()
          .setTxnId(secondTxn)
          .addComponents(HbaseMetastoreProto.LockComponent.newBuilder()
              .setDb("d")
              .setTable("t")
              .setType(HbaseMetastoreProto.LockType.SHARED_WRITE)
              .build())
          .addComponents(HbaseMetastoreProto.LockComponent.newBuilder()
              .setDb("d")
              .setType(HbaseMetastoreProto.LockType.INTENTION)
              .build())
          .build());
      Assert.assertEquals(HbaseMetastoreProto.LockState.WAITING, lock.getState());

      HbaseMetastoreProto.TransactionResult commit =
          txnMgr.commitTxn(HbaseMetastoreProto.TransactionId.newBuilder()
              .setId(firstTxn)
              .build());
      Assert.assertEquals(HbaseMetastoreProto.TxnStateChangeResult.SUCCESS, commit.getState());

      // Give it a bit to run and check the locks
      Thread.sleep(100);

      lock = txnMgr.checkLocks(HbaseMetastoreProto.TransactionId.newBuilder()
          .setId(secondTxn)
          .build());
      Assert.assertEquals(HbaseMetastoreProto.LockState.ACQUIRED, lock.getState());
    } finally {
      for (long i = firstTxn; i <= secondTxn; i++) {
        txnMgr.commitTxn(HbaseMetastoreProto.TransactionId
            .newBuilder()
            .setId(i)
            .build());
      }
    }
  }

  @Test
  public void acquireSharedAfterAbort()  throws Exception {
    // Detect that if a partition has potentially changed underneath a writer their transaction
    // is aborted.
    HbaseMetastoreProto.OpenTxnsRequest rqst = HbaseMetastoreProto.OpenTxnsRequest.newBuilder()
        .setNumTxns(3)
        .setUser("me")
        .setHostname("localhost")
        .build();
    HbaseMetastoreProto.OpenTxnsResponse rsp = txnMgr.openTxns(rqst);
    long firstTxn = rsp.getTxnIds(0);
    long secondTxn = rsp.getTxnIds(1);
    long thirdTxn = rsp.getTxnIds(2);
    try {

      HbaseMetastoreProto.LockResponse lock =
          txnMgr.lock(HbaseMetastoreProto.LockRequest.newBuilder()
              .setTxnId(firstTxn)
              .addComponents(HbaseMetastoreProto.LockComponent.newBuilder()
                  .setDb("d")
                  .setTable("t")
                  .setType(HbaseMetastoreProto.LockType.EXCLUSIVE)
                  .build())
              .addComponents(HbaseMetastoreProto.LockComponent.newBuilder()
                  .setDb("d")
                  .setType(HbaseMetastoreProto.LockType.INTENTION)
                  .build())
              .build());
      Assert.assertEquals(HbaseMetastoreProto.LockState.ACQUIRED, lock.getState());

      lock = txnMgr.lock(HbaseMetastoreProto.LockRequest.newBuilder()
          .setTxnId(secondTxn)
          .addComponents(HbaseMetastoreProto.LockComponent.newBuilder()
              .setDb("d")
              .setTable("t")
              .setType(HbaseMetastoreProto.LockType.SHARED_READ)
              .build())
          .addComponents(HbaseMetastoreProto.LockComponent.newBuilder()
              .setDb("d")
              .setType(HbaseMetastoreProto.LockType.INTENTION)
              .build())
          .build());
      Assert.assertEquals(HbaseMetastoreProto.LockState.WAITING, lock.getState());

      lock = txnMgr.lock(HbaseMetastoreProto.LockRequest.newBuilder()
          .setTxnId(thirdTxn)
          .addComponents(HbaseMetastoreProto.LockComponent.newBuilder()
              .setDb("d")
              .setTable("t")
              .setType(HbaseMetastoreProto.LockType.SHARED_READ)
              .build())
          .addComponents(HbaseMetastoreProto.LockComponent.newBuilder()
              .setDb("d")
              .setType(HbaseMetastoreProto.LockType.INTENTION)
              .build())
          .build());
      Assert.assertEquals(HbaseMetastoreProto.LockState.WAITING, lock.getState());

      HbaseMetastoreProto.TransactionResult abort =
          txnMgr.abortTxn(HbaseMetastoreProto.TransactionId.newBuilder()
              .setId(firstTxn)
              .build());
      Assert.assertEquals(HbaseMetastoreProto.TxnStateChangeResult.SUCCESS, abort.getState());

      // Give it a bit to run and check the locks
      Thread.sleep(100);

      lock = txnMgr.checkLocks(HbaseMetastoreProto.TransactionId.newBuilder()
          .setId(secondTxn)
          .build());
      Assert.assertEquals(HbaseMetastoreProto.LockState.ACQUIRED, lock.getState());
      lock = txnMgr.checkLocks(HbaseMetastoreProto.TransactionId.newBuilder()
          .setId(thirdTxn)
          .build());
      Assert.assertEquals(HbaseMetastoreProto.LockState.ACQUIRED, lock.getState());
    } finally {
      for (long i = firstTxn; i <= thirdTxn; i++) {
        txnMgr.commitTxn(HbaseMetastoreProto.TransactionId
            .newBuilder()
            .setId(i)
            .build());
      }
    }
  }

  @Test
  public void acquireSharedAfterCommit()  throws Exception {
    // Detect that if a partition has potentially changed underneath a writer their transaction
    // is aborted.
    HbaseMetastoreProto.OpenTxnsRequest rqst = HbaseMetastoreProto.OpenTxnsRequest.newBuilder()
        .setNumTxns(3)
        .setUser("me")
        .setHostname("localhost")
        .build();
    HbaseMetastoreProto.OpenTxnsResponse rsp = txnMgr.openTxns(rqst);
    long firstTxn = rsp.getTxnIds(0);
    long secondTxn = rsp.getTxnIds(1);
    long thirdTxn = rsp.getTxnIds(2);
    try {

      HbaseMetastoreProto.LockResponse lock =
          txnMgr.lock(HbaseMetastoreProto.LockRequest.newBuilder()
              .setTxnId(firstTxn)
              .addComponents(HbaseMetastoreProto.LockComponent.newBuilder()
                  .setDb("d")
                  .setTable("t")
                  .setType(HbaseMetastoreProto.LockType.EXCLUSIVE)
                  .build())
              .addComponents(HbaseMetastoreProto.LockComponent.newBuilder()
                  .setDb("d")
                  .setType(HbaseMetastoreProto.LockType.INTENTION)
                  .build())
              .build());
      Assert.assertEquals(HbaseMetastoreProto.LockState.ACQUIRED, lock.getState());

      lock = txnMgr.lock(HbaseMetastoreProto.LockRequest.newBuilder()
          .setTxnId(secondTxn)
          .addComponents(HbaseMetastoreProto.LockComponent.newBuilder()
              .setDb("d")
              .setTable("t")
              .setType(HbaseMetastoreProto.LockType.SHARED_READ)
              .build())
          .addComponents(HbaseMetastoreProto.LockComponent.newBuilder()
              .setDb("d")
              .setType(HbaseMetastoreProto.LockType.INTENTION)
              .build())
          .build());
      Assert.assertEquals(HbaseMetastoreProto.LockState.WAITING, lock.getState());

      lock = txnMgr.lock(HbaseMetastoreProto.LockRequest.newBuilder()
          .setTxnId(thirdTxn)
          .addComponents(HbaseMetastoreProto.LockComponent.newBuilder()
              .setDb("d")
              .setTable("t")
              .setType(HbaseMetastoreProto.LockType.SHARED_READ)
              .build())
          .addComponents(HbaseMetastoreProto.LockComponent.newBuilder()
              .setDb("d")
              .setType(HbaseMetastoreProto.LockType.INTENTION)
              .build())
          .build());
      Assert.assertEquals(HbaseMetastoreProto.LockState.WAITING, lock.getState());

      HbaseMetastoreProto.TransactionResult commit =
          txnMgr.commitTxn(HbaseMetastoreProto.TransactionId.newBuilder()
              .setId(firstTxn)
              .build());
      Assert.assertEquals(HbaseMetastoreProto.TxnStateChangeResult.SUCCESS, commit.getState());

      // Give it a bit to run and check the locks
      Thread.sleep(100);

      lock = txnMgr.checkLocks(HbaseMetastoreProto.TransactionId.newBuilder()
          .setId(secondTxn)
          .build());
      Assert.assertEquals(HbaseMetastoreProto.LockState.ACQUIRED, lock.getState());
      lock = txnMgr.checkLocks(HbaseMetastoreProto.TransactionId.newBuilder()
          .setId(thirdTxn)
          .build());
      Assert.assertEquals(HbaseMetastoreProto.LockState.ACQUIRED, lock.getState());
    } finally {
      for (long i = firstTxn; i <= thirdTxn; i++) {
        txnMgr.commitTxn(HbaseMetastoreProto.TransactionId
            .newBuilder()
            .setId(i)
            .build());
      }
    }
  }
}
