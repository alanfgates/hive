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

import org.apache.hadoop.hbase.ipc.BlockingRpcCallback;
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
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

/**
 * This just tests that we can communicate through the coprocessor.  Actual functionality of the
 * transaction manager is tested in the TestTransactionManger* classes.  This should call every
 * RPC call in the transaction coprocessor.
 */
public class TestTransactionCoprocessor extends MockUtils {
  HBaseStore store;
  HBaseReadWrite hrw;

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

    store = mockInit(conf);
    hrw = HBaseReadWrite.getInstance();
  }

  @After
  public void cleanup() throws IOException {
    mockShutdown();
  }

  @Test
  public void openAndAbort() throws Exception {

    // Open a single transaction
    HbaseMetastoreProto.OpenTxnsRequest rqst = HbaseMetastoreProto.OpenTxnsRequest.newBuilder()
        .setNumTxns(1)
        .setUser("me")
        .setHostname("localhost")
        .build();

    BlockingRpcCallback<HbaseMetastoreProto.OpenTxnsResponse> openRpcRsp =
        new BlockingRpcCallback<>();
    txnCoProc.openTxns(getController(), rqst, openRpcRsp);

    HbaseMetastoreProto.OpenTxnsResponse openRsp = openRpcRsp.get();
    Assert.assertEquals(1, openRsp.getTxnIdsCount());
    long txnId = openRsp.getTxnIds(0);

    BlockingRpcCallback<HbaseMetastoreProto.GetOpenTxnsResponse> txnRpcRsp =
        new BlockingRpcCallback<>();
    txnCoProc.getOpenTxns(getController(), HbaseMetastoreProto.Void.getDefaultInstance(), txnRpcRsp);

    HbaseMetastoreProto.GetOpenTxnsResponse validTxns = txnRpcRsp.get();
    Assert.assertNotNull(validTxns);

    // Make sure there's at least one open txn
    Assert.assertTrue(validTxns.getOpenTransactionsCount() > 0);

    // Make sure our transaction is open
    boolean sawOurTxn = false;
    for (long txn : validTxns.getOpenTransactionsList()) {
      if (txn == txnId) {
        sawOurTxn = true;
        break;
      }
    }
    Assert.assertTrue(sawOurTxn);

    // Abort this transaction.  It should promptly be forgotten as it has no locks
    HbaseMetastoreProto.TransactionId toAbort = HbaseMetastoreProto.TransactionId.newBuilder()
            .setId(txnId)
            .build();

    BlockingRpcCallback<HbaseMetastoreProto.TransactionResult> abortRpcRsp =
        new BlockingRpcCallback<>();

    txnCoProc.abortTxn(getController(), toAbort, abortRpcRsp);
    HbaseMetastoreProto.TransactionResult abortRsp = abortRpcRsp.get();
    Assert.assertEquals(HbaseMetastoreProto.TxnStateChangeResult.SUCCESS, abortRsp.getState());


    // Our txn won't get put in the abort list because it didn't have any locks
    txnRpcRsp = new BlockingRpcCallback<>();
    txnCoProc.getOpenTxns(getController(), HbaseMetastoreProto.Void.getDefaultInstance(), txnRpcRsp);

    validTxns = txnRpcRsp.get();
    Assert.assertNotNull(validTxns);

  }

  public void openAndCommit() throws Exception {
    String db = "oac_db";

    HbaseMetastoreProto.OpenTxnsRequest rqst = HbaseMetastoreProto.OpenTxnsRequest.newBuilder()
        .setNumTxns(2)
        .setUser("me")
        .setHostname("localhost")
        .build();

    BlockingRpcCallback<HbaseMetastoreProto.OpenTxnsResponse> openRpcRsp =
        new BlockingRpcCallback<>();
    txnCoProc.openTxns(getController(), rqst, openRpcRsp);

    HbaseMetastoreProto.OpenTxnsResponse openRsp = openRpcRsp.get();
    Assert.assertEquals(2, openRsp.getTxnIdsCount());
    long firstTxnId = openRsp.getTxnIds(0);
    long secondTxnId = openRsp.getTxnIds(1);

    // Send a heartbeat for these transactions
    HbaseMetastoreProto.HeartbeatTxnRangeRequest heartbeat =
        HbaseMetastoreProto.HeartbeatTxnRangeRequest.newBuilder()
            .setMinTxn(firstTxnId)
            .setMaxTxn(secondTxnId)
            .build();
    BlockingRpcCallback<HbaseMetastoreProto.HeartbeatTxnRangeResponse> heartbeatRpcRsp =
        new BlockingRpcCallback<>();
    txnCoProc.heartbeat(getController(), heartbeat, heartbeatRpcRsp);
    HbaseMetastoreProto.HeartbeatTxnRangeResponse heartbeatRsp = heartbeatRpcRsp.get();
    Assert.assertEquals(0, heartbeatRsp.getAbortedCount());
    Assert.assertEquals(0, heartbeatRsp.getNoSuchCount());

    // Lock in one transaction
    HbaseMetastoreProto.LockRequest lockRequest = HbaseMetastoreProto.LockRequest.newBuilder()
        .setTxnId(firstTxnId)
        .addComponents(HbaseMetastoreProto.LockComponent.newBuilder()
            .setDb(db)
            .setType(HbaseMetastoreProto.LockType.EXCLUSIVE))
        .build();
    BlockingRpcCallback<HbaseMetastoreProto.LockResponse> lockRpcRsp = new BlockingRpcCallback<>();
    txnCoProc.lock(getController(), lockRequest, lockRpcRsp);
    HbaseMetastoreProto.LockResponse lockRsp = lockRpcRsp.get();
    Assert.assertEquals(HbaseMetastoreProto.LockState.ACQUIRED, lockRsp.getState());

    // Lock in the other transaction
    lockRequest = HbaseMetastoreProto.LockRequest.newBuilder()
        .setTxnId(secondTxnId)
        .addComponents(HbaseMetastoreProto.LockComponent.newBuilder()
            .setDb(db)
            .setType(HbaseMetastoreProto.LockType.EXCLUSIVE))
        .build();
    lockRpcRsp = new BlockingRpcCallback<>();
    txnCoProc.lock(getController(), lockRequest, lockRpcRsp);
    lockRsp = lockRpcRsp.get();
    Assert.assertEquals(HbaseMetastoreProto.LockState.WAITING, lockRsp.getState());

    // Commit the first transaction
    HbaseMetastoreProto.TransactionId toCommit = HbaseMetastoreProto.TransactionId.newBuilder()
        .setId(firstTxnId)
        .build();

    BlockingRpcCallback<HbaseMetastoreProto.TransactionResult> commitRpcRsp =
        new BlockingRpcCallback<>();

    txnCoProc.commitTxn(getController(), toCommit, commitRpcRsp);
    HbaseMetastoreProto.TransactionResult commitRsp = commitRpcRsp.get();
    Assert.assertEquals(HbaseMetastoreProto.TxnStateChangeResult.SUCCESS, commitRsp.getState());

    // Check the locks on the second txn
    HbaseMetastoreProto.TransactionId toCheck = HbaseMetastoreProto.TransactionId.newBuilder()
        .setId(secondTxnId)
        .build();
    lockRpcRsp = new BlockingRpcCallback<>();
    txnCoProc.checkLocks(getController(), toCheck, lockRpcRsp);
    lockRsp = lockRpcRsp.get();
    Assert.assertEquals(HbaseMetastoreProto.LockState.ACQUIRED, lockRsp.getState());

    // Commit the second txn just to keep things clean
    toCommit = HbaseMetastoreProto.TransactionId.newBuilder()
        .setId(secondTxnId)
        .build();

    commitRpcRsp = new BlockingRpcCallback<>();

    txnCoProc.commitTxn(getController(), toCommit, commitRpcRsp);
    commitRsp = commitRpcRsp.get();
    Assert.assertEquals(HbaseMetastoreProto.TxnStateChangeResult.SUCCESS, commitRsp.getState());
  }

  public void dynamicPartitionsAndCompaction() throws Exception {
    String db = "dpac_db";
    String table = "dpac_t";
    String partbase = "dpac_p";

    // Open a single transaction
    HbaseMetastoreProto.OpenTxnsRequest rqst = HbaseMetastoreProto.OpenTxnsRequest.newBuilder()
        .setNumTxns(1)
        .setUser("me")
        .setHostname("localhost")
        .build();

    BlockingRpcCallback<HbaseMetastoreProto.OpenTxnsResponse> openRpcRsp =
        new BlockingRpcCallback<>();
    txnCoProc.openTxns(getController(), rqst, openRpcRsp);

    HbaseMetastoreProto.OpenTxnsResponse openRsp = openRpcRsp.get();
    Assert.assertEquals(1, openRsp.getTxnIdsCount());
    long txnId = openRsp.getTxnIds(0);

    // Add dynamic partitions
    HbaseMetastoreProto.AddDynamicPartitionsRequest dParts =
        HbaseMetastoreProto.AddDynamicPartitionsRequest.newBuilder()
        .setDb(db)
        .setTable(table)
        .setTxnId(txnId)
        .addAllPartitions(Arrays.asList(partbase + "1", partbase + "2"))
        .build();
    BlockingRpcCallback<HbaseMetastoreProto.TransactionResult> dPartsRpcRsp =
        new BlockingRpcCallback<>();
    txnCoProc.addDynamicPartitions(getController(), dParts, dPartsRpcRsp);
    HbaseMetastoreProto.TransactionResult dPartsRsp = dPartsRpcRsp.get();
    Assert.assertEquals(HbaseMetastoreProto.TxnStateChangeResult.SUCCESS, dPartsRsp.getState());

    // Commit this transaction.
    HbaseMetastoreProto.TransactionId toCommit = HbaseMetastoreProto.TransactionId.newBuilder()
        .setId(txnId)
        .build();

    BlockingRpcCallback<HbaseMetastoreProto.TransactionResult> commitRpcRsp =
        new BlockingRpcCallback<>();

    txnCoProc.commitTxn(getController(), toCommit, commitRpcRsp);
    HbaseMetastoreProto.TransactionResult commitRsp = commitRpcRsp.get();
    Assert.assertEquals(HbaseMetastoreProto.TxnStateChangeResult.SUCCESS, commitRsp.getState());

    // Cleanupt after a compaction
    HbaseMetastoreProto.Compaction compaction = HbaseMetastoreProto.Compaction.newBuilder()
        .setDb(db)
        .setTable(table)
        .setPartition(partbase + "1")
        .setId(1)
        .build();
    BlockingRpcCallback<HbaseMetastoreProto.Void> compactionRpcRsp = new BlockingRpcCallback<>();
    txnCoProc.cleanupAfterCompaction(getController(), compaction, compactionRpcRsp);

    // Verify compaction can be cleaned up
    HbaseMetastoreProto.CompactionList clist = HbaseMetastoreProto.CompactionList.newBuilder()
        .addCompactions(HbaseMetastoreProto.Compaction.newBuilder()
            .setDb(db)
            .setTable(table)
            .setPartition(partbase + "1")
            .setId(1))
        .build();
    BlockingRpcCallback<HbaseMetastoreProto.CompactionList> clistRpcRsp =
        new BlockingRpcCallback<>();
    txnCoProc.verifyCompactionCanBeCleaned(getController(), clist, clistRpcRsp);
    HbaseMetastoreProto.CompactionList clistRsp = clistRpcRsp.get();
    // Just make sure the response contains something
    Assert.assertEquals(1, clistRsp.getCompactionsCount());

  }
  // TODO write wrapper around TestTxnHandler to re-route its calls to HBase metastore
  // TODO test what happens when exception is thrown, currently only happens on full
}

