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
import java.util.Arrays;
import java.util.SortedMap;
import java.util.TreeMap;

public class TestTransactionManagerBasic {

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
    conf.set(TransactionManager.CONF_INITIAL_DELAY, Long.toString(Long.MAX_VALUE));

    store = MockUtils.init(conf, htable, rows);
    txnMgr = new TransactionManager(conf);
  }

  @After
  public void cleanup() throws IOException {
    txnMgr.shutdown();
  }

  // Unfortunately we have to do this as one big test to make sure we keep the state where we
  // think it should be at all times.

  @Test
  public void oneBigHairyTest() throws Exception {
    // Open a single transaction
    HbaseMetastoreProto.OpenTxnsRequest rqst = HbaseMetastoreProto.OpenTxnsRequest.newBuilder()
        .setNumTxns(1)
        .setUser("me")
        .setHostname("localhost")
        .build();
    HbaseMetastoreProto.OpenTxnsResponse rsp = txnMgr.openTxns(rqst);
    Assert.assertEquals(1, rsp.getTxnIdsCount());
    Assert.assertEquals(0L, rsp.getTxnIds(0));

    assertInternalState(1L, "\\{\"0\":\\{\"lastHeartbeat\":[0-9]+,\"hiveLocks\":null\\}\\}",
        "{}", "[]");

    // Open many transactions
    rqst = HbaseMetastoreProto.OpenTxnsRequest.newBuilder()
        .setNumTxns(3)
        .setUser("me")
        .setHostname("localhost")
        .build();
    rsp = txnMgr.openTxns(rqst);
    Assert.assertEquals(3L, rsp.getTxnIdsCount());
    Assert.assertEquals(1L, rsp.getTxnIds(0));
    Assert.assertEquals(3L, rsp.getTxnIds(2));

    assertInternalState(4L, "\\{\"0\":\\{\"lastHeartbeat\":[0-9]+,\"hiveLocks\":null\\}," +
        "\"1\":\\{\"lastHeartbeat\":[0-9]+,\"hiveLocks\":null\\},\"2\":\\{\"lastHeartbeat\":[0-9]+," +
        "\"hiveLocks\":null\\},\"3\":\\{\"lastHeartbeat\":[0-9]+,\"hiveLocks\":null\\}\\}",
        "{}", "[]");

    // Abort a transaction
    HbaseMetastoreProto.TransactionResult result =
        txnMgr.abortTxn(HbaseMetastoreProto.TransactionId.newBuilder().setId(0).build());
    Assert.assertEquals(HbaseMetastoreProto.TxnStateChangeResult.SUCCESS, result.getState());


    // This one will immediately be forgotten since it had no write locks, so abort count goes to
    // 0 not 1.
    assertInternalState(4L, "\\{\"1\":\\{\"lastHeartbeat\":[0-9]+,\"hiveLocks\":null\\}," +
        "\"2\":\\{\"lastHeartbeat\":[0-9]+,\"hiveLocks\":null\\},\"3\":\\{\"lastHeartbeat\":[0-9]+," +
        "\"hiveLocks\":null\\}\\}", "{}", "[]");

    // Commit a transaction
    result = txnMgr.commitTxn(HbaseMetastoreProto.TransactionId.newBuilder().setId(2).build());
    Assert.assertEquals(HbaseMetastoreProto.TxnStateChangeResult.SUCCESS, result.getState());
    // No write locks, so it should be immediately forgotten
    assertInternalState(4L, "\\{\"1\":\\{\"lastHeartbeat\":[0-9]+,\"hiveLocks\":null\\}," +
        "\"3\":\\{\"lastHeartbeat\":[0-9]+,\"hiveLocks\":null\\}\\}", "{}", "[]");

    // Open read locks on a transaction
    txnMgr.lock(HbaseMetastoreProto.LockRequest.newBuilder()
        .setTxnId(1)
        .addComponents(HbaseMetastoreProto.LockComponent.newBuilder()
            .setDb("d")
            .setTable("t")
            .setPartition("p")
            .setType(HbaseMetastoreProto.LockType.SHARED_READ)
            .build())
        .build());
    assertInternalState(4L, "\\{\"1\":\\{\"lastHeartbeat\":[0-9]+," +
        "\"hiveLocks\":\\[\\{\"id\":0," +
        "\"txnId\":1,\"entityLocked\":\\{\"db\":\"d\",\"table\":\"t\",\"part\":\"p\"\\}," +
        "\"type\":\"SHARED_READ\",\"state\":\"ACQUIRED\"\\}\\]\\}," +
        "\"3\":\\{\"lastHeartbeat\":[0-9]+," +
        "\"hiveLocks\":null\\}\\}", "{}", "[]");
    Assert.assertEquals("{\"d.t.p\":{\"queue\":{\"0\":{\"id\":0,\"txnId\":1," +
        "\"entityLocked\":{\"db\":\"d\",\"table\":\"t\",\"part\":\"p\"},\"type\":\"SHARED_READ\"," +
        "\"state\":\"ACQUIRED\"}},\"maxCommitId\":0}}", txnMgr.stringifyLockQueues());

    // Abort a transaction with a read lock
    txnMgr.abortTxn(HbaseMetastoreProto.TransactionId.newBuilder().setId(1).build());
    assertInternalState(4L, "\\{\"3\":\\{\"lastHeartbeat\":[0-9]+," +
        "\"hiveLocks\":null\\}\\}", "{}", "[]");
    Assert.assertEquals("{\"d.t.p\":{\"queue\":{},\"maxCommitId\":0}}",
        txnMgr.stringifyLockQueues());

    // Commit a transaction with a read lock
    txnMgr.lock(HbaseMetastoreProto.LockRequest.newBuilder()
        .setTxnId(3)
        .addComponents(HbaseMetastoreProto.LockComponent.newBuilder()
            .setDb("d")
            .setTable("t")
            .setPartition("p")
            .setType(HbaseMetastoreProto.LockType.SHARED_READ)
            .build())
        .build());
    assertInternalState(4L, "\\{\"3\":\\{\"lastHeartbeat\":[0-9]+," +
            "\"hiveLocks\":\\[\\{\"id\":1,\"txnId\":3,\"entityLocked\":\\{\"db\":\"d\"," +
            "\"table\":\"t\",\"part\":\"p\"},\"type\":\"SHARED_READ\"," +
            "\"state\":\"ACQUIRED\"\\}\\]\\}\\}", "{}", "[]");
    Assert.assertEquals("{\"d.t.p\":{\"queue\":{\"1\":{\"id\":1,\"txnId\":3," +
        "\"entityLocked\":{\"db\":\"d\",\"table\":\"t\",\"part\":\"p\"},\"type\":\"SHARED_READ\"," +
        "\"state\":\"ACQUIRED\"}},\"maxCommitId\":0}}", txnMgr.stringifyLockQueues());
    txnMgr.commitTxn(HbaseMetastoreProto.TransactionId.newBuilder().setId(3).build());
    assertInternalState(4L, "\\{\\}", "{}", "[]");
    Assert.assertEquals("{\"d.t.p\":{\"queue\":{},\"maxCommitId\":0}}", txnMgr.stringifyLockQueues());

    // Open a couple of more transactions to test that we properly remember transactions when
    // they have write locks.  Also test that read locks mixed in are properly forgotten.  Keep a
    // transaction open when checking for the committed transaction so that the commit cleaner
    // doesn't remove it on us.
    rqst = HbaseMetastoreProto.OpenTxnsRequest.newBuilder()
        .setNumTxns(2)
        .setUser("me")
        .setHostname("localhost")
        .build();
    rsp = txnMgr.openTxns(rqst);


    txnMgr.lock(HbaseMetastoreProto.LockRequest.newBuilder()
        .setTxnId(5)
        .addComponents(HbaseMetastoreProto.LockComponent.newBuilder()
            .setDb("d")
            .setTable("t3")
            .setPartition("p")
            .setType(HbaseMetastoreProto.LockType.SHARED_READ)
            .build())
        .addComponents(HbaseMetastoreProto.LockComponent.newBuilder()
            .setDb("d")
            .setTable("t4")
            .setType(HbaseMetastoreProto.LockType.SHARED_WRITE)
            .build())
        .build());

    assertInternalState(6L, "\\{\"4\":\\{\"lastHeartbeat\":[0-9]+,\"hiveLocks\":null\\}," +
        "\"5\":\\{\"lastHeartbeat\":[0-9]+,\"hiveLocks\":\\[\\{\"id\":2,\"txnId\":5," +
        "\"entityLocked\":\\{\"db\":\"d\",\"table\":\"t3\",\"part\":\"p\"\\}," +
        "\"type\":\"SHARED_READ\",\"state\":\"ACQUIRED\"\\},\\{\"id\":3,\"txnId\":5," +
        "\"entityLocked\":\\{\"db\":\"d\",\"table\":\"t4\",\"part\":null\\}," +
        "\"type\":\"SHARED_WRITE\",\"state\":\"ACQUIRED\"\\}\\]\\}\\}", "{}", "[]");
    Assert.assertEquals("{\"d.t3.p\":{\"queue\":{\"2\":{\"id\":2,\"txnId\":5," +
        "\"entityLocked\":{\"db\":\"d\",\"table\":\"t3\",\"part\":\"p\"}," +
        "\"type\":\"SHARED_READ\",\"state\":\"ACQUIRED\"}},\"maxCommitId\":0},\"d.t4" +
        "\":{\"queue\":{\"3\":{\"id\":3,\"txnId\":5,\"entityLocked\":{\"db\":\"d\"," +
        "\"table\":\"t4\",\"part\":null},\"type\":\"SHARED_WRITE\",\"state\":\"ACQUIRED\"}}," +
        "\"maxCommitId\":0},\"d.t.p\":{\"queue\":{},\"maxCommitId\":0}}",
        txnMgr.stringifyLockQueues());

    txnMgr.commitTxn(HbaseMetastoreProto.TransactionId.newBuilder()
        .setId(5)
        .build());

    assertInternalState(6L, "\\{\"4\":\\{\"lastHeartbeat\":[0-9]+,\"hiveLocks\":null\\}\\}", "{}",
        "[{\"commitId\":6}]");
    Assert.assertEquals("{\"d.t3.p\":{\"queue\":{},\"maxCommitId\":0},\"d.t4\":{\"queue\":{}," +
            "\"maxCommitId\":6},\"d.t.p\":{\"queue\":{},\"maxCommitId\":0}}",
        txnMgr.stringifyLockQueues());

    // Get a read and write lock on one of the transactions
    txnMgr.lock(HbaseMetastoreProto.LockRequest.newBuilder()
        .setTxnId(4)
        .addComponents(HbaseMetastoreProto.LockComponent.newBuilder()
            .setDb("d")
            .setTable("t")
            .setPartition("p")
            .setType(HbaseMetastoreProto.LockType.SHARED_READ)
            .build())
        .addComponents(HbaseMetastoreProto.LockComponent.newBuilder()
            .setDb("d")
            .setTable("t2")
            .setType(HbaseMetastoreProto.LockType.SHARED_WRITE)
            .build())
        .build());

    assertInternalState(6L, "\\{\"4\":\\{\"lastHeartbeat\":[0-9]+," +
        "\"hiveLocks\":\\[\\{\"id\":4,\"txnId\":4,\"entityLocked\":\\{\"db\":\"d\"," +
        "\"table\":\"t\",\"part\":\"p\"\\},\"type\":\"SHARED_READ\",\"state\":\"ACQUIRED\"\\}," +
        "\\{\"id\":5,\"txnId\":4,\"entityLocked\":\\{\"db\":\"d\",\"table\":\"t2\"," +
        "\"part\":null\\},\"type\":\"SHARED_WRITE\",\"state\":\"ACQUIRED\"\\}\\]\\}\\}", "{}",
        "[{\"commitId\":6}]");
    Assert.assertEquals("{\"d.t3.p\":{\"queue\":{},\"maxCommitId\":0},\"d" +
        ".t2\":{\"queue\":{\"5\":{\"id\":5,\"txnId\":4,\"entityLocked\":{\"db\":\"d\"," +
        "\"table\":\"t2\",\"part\":null},\"type\":\"SHARED_WRITE\",\"state\":\"ACQUIRED\"}}," +
        "\"maxCommitId\":0},\"d.t4\":{\"queue\":{},\"maxCommitId\":6},\"d.t" +
        ".p\":{\"queue\":{\"4\":{\"id\":4,\"txnId\":4,\"entityLocked\":{\"db\":\"d\"," +
        "\"table\":\"t\",\"part\":\"p\"},\"type\":\"SHARED_READ\",\"state\":\"ACQUIRED\"}}," +
        "\"maxCommitId\":0}}", txnMgr.stringifyLockQueues());

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

    // Abort the transaction
    txnMgr.abortTxn(HbaseMetastoreProto.TransactionId.newBuilder()
        .setId(4)
        .build());

    assertInternalState(6L, "\\{\\}", "{\"4\":{\"compactableLocks\":{\"d.t2\":{\"id\":5," +
        "\"txnId\":4,\"entityLocked\":{\"db\":\"d\",\"table\":\"t2\",\"part\":null}," +
        "\"type\":\"SHARED_WRITE\",\"state\":\"TXN_ABORTED\"}}}}", "[{\"commitId\":6}]");
    Assert.assertEquals("{\"d.t3.p\":{\"queue\":{},\"maxCommitId\":0},\"d.t2\":{\"queue\":{}," +
        "\"maxCommitId\":0},\"d.t4\":{\"queue\":{},\"maxCommitId\":6},\"d.t.p\":{\"queue\":{}," +
        "\"maxCommitId\":0}}", txnMgr.stringifyLockQueues());

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

  private void assertInternalState(long expectedHighWaterMark, String openTxnRegex,
                                   String abortTxns, String commitTxns) throws Exception {
    HbaseMetastoreProto.GetOpenTxnsResponse txns =
        txnMgr.getOpenTxns(HbaseMetastoreProto.Void.getDefaultInstance());
    Assert.assertEquals(expectedHighWaterMark, txns.getHighWaterMark());
    Assert.assertTrue("Expected <" + openTxnRegex + "> got <" + txnMgr.stringifyOpenTxns() + ">",
        txnMgr.stringifyOpenTxns().matches(openTxnRegex));
    Assert.assertEquals(abortTxns, txnMgr.stringifyAbortedTxns());
    Assert.assertEquals(commitTxns, txnMgr.stringifyCommittedTxns());
  }

}
