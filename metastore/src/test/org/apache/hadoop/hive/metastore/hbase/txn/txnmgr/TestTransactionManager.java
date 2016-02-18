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
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.IOException;
import java.util.SortedMap;
import java.util.TreeMap;

public class TestTransactionManager {

  @Mock
  HTableInterface htable;
  SortedMap<String, Cell> rows = new TreeMap<>();
  HBaseStore store;
  TransactionManager txnMgr;

  @Before
  public void init() throws IOException {
    MockitoAnnotations.initMocks(this);
    HiveConf conf = new HiveConf();
    store = MockUtils.init(conf, htable, rows);
    txnMgr = new TransactionManager(conf);
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

    assertInternalState(1L, 0L, 1L, "{0=0,[]}", "{}", "[]");
    Assert.assertNotNull(store.backdoor().getTransaction(0));

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

    assertInternalState(4L, 0L, 4L, "{0=0,[], 1=1,[], 2=2,[], 3=3,[]}", "{}", "[]");

    // Abort a transaction
    txnMgr.abortTxn(HbaseMetastoreProto.TransactionId.newBuilder().setId(0).build());
    // This one will immediately be forgotten since it had no write locks, so abort count goes to
    // 0 not 1.
    assertInternalState(4L, 0L, 3L, "{1=1,[], 2=2,[], 3=3,[]}", "{}", "[]");

    // Commit a transaction
    txnMgr.commitTxn(HbaseMetastoreProto.TransactionId.newBuilder().setId(2).build());
    // No write locks, so it should be immediately forgotten
    assertInternalState(4L, 0L, 2L, "{1=1,[], 3=3,[]}", "{}", "[]");

    // Open read locks on a transaction
    txnMgr.lock(HbaseMetastoreProto.LockRequest.newBuilder()
        .setTxnId(1)
        .addComponents(HbaseMetastoreProto.LockComponent.newBuilder()
            .setDb("db")
            .setTable("table")
            .setPartition("part")
            .setType(HbaseMetastoreProto.LockType.SHARED_READ)
            .build())
        .build());
    assertInternalState(4L, 0L, 2L, "{1=1,[1:0,db.table.part,SHARED_READ,ACQUIRED], 3=3,[]}", "{}",
        "[]");
  }

  private void assertInternalState(long expectedHighWaterMark, long expectedAbortCnt,
                                   long expectedOpenCnt, String openTxnString,
                                   String abortTxnString, String commitTxnString) throws Exception {
    HbaseMetastoreProto.GetOpenTxnsResponse txns =
        txnMgr.getOpenTxns(HbaseMetastoreProto.Void.getDefaultInstance());
    Assert.assertEquals(expectedHighWaterMark, txns.getHighWaterMark());
    Assert.assertEquals(expectedAbortCnt, txns.getAbortedTransactionsCount());
    Assert.assertEquals(expectedOpenCnt, txns.getOpenTransactionsCount());
    Assert.assertEquals(openTxnString, txnMgr.stringifyOpenTxns());
    Assert.assertEquals(abortTxnString, txnMgr.stringifyAbortedTxns());
    Assert.assertEquals(commitTxnString, txnMgr.stringifyCommittedTxns());
  }

}
