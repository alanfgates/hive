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
import org.apache.hadoop.hive.metastore.api.AbortTxnRequest;
import org.apache.hadoop.hive.metastore.api.CheckLockRequest;
import org.apache.hadoop.hive.metastore.api.CommitTxnRequest;
import org.apache.hadoop.hive.metastore.api.GetOpenTxnsInfoResponse;
import org.apache.hadoop.hive.metastore.api.GetOpenTxnsResponse;
import org.apache.hadoop.hive.metastore.api.LockComponent;
import org.apache.hadoop.hive.metastore.api.LockLevel;
import org.apache.hadoop.hive.metastore.api.LockRequest;
import org.apache.hadoop.hive.metastore.api.LockResponse;
import org.apache.hadoop.hive.metastore.api.LockState;
import org.apache.hadoop.hive.metastore.api.LockType;
import org.apache.hadoop.hive.metastore.api.OpenTxnRequest;
import org.apache.hadoop.hive.metastore.api.OpenTxnsResponse;
import org.apache.hadoop.hive.metastore.api.TxnInfo;
import org.apache.hadoop.hive.metastore.hbase.HBaseReadWrite;
import org.apache.hadoop.hive.metastore.hbase.HBaseStore;
import org.apache.hadoop.hive.metastore.hbase.MockUtils;
import org.apache.hadoop.hive.metastore.hbase.txn.HBaseTxnHandler;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockitoAnnotations;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Tests for HBaseTxnHandler.  This is focussed on making sure the txn handler properly calls the
 * coprocessor.  Functionality that is handled completely by the transaction handler is also
 * tested here.
 */
public class TestHBaseTxnHandler extends MockUtils {
  HBaseStore store;
  HBaseReadWrite hrw;
  HBaseTxnHandler txnHandler;

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
    // Set this value lower so we can fill up the txn mgr without creating an insane number of
    // objects.
    HiveConf.setIntVar(conf, HiveConf.ConfVars.METASTORE_HBASE_TXN_MGR_MAX_OBJECTS, 100);

    store = mockInit(conf);
    hrw = HBaseReadWrite.getInstance();
    txnHandler = new HBaseTxnHandler();
    txnHandler.setConf(conf);
  }

  @After
  public void cleanup() throws IOException {
    mockShutdown();
  }

  @Test
  public void openGetInfoAbort() throws Exception {
    OpenTxnRequest txnRequest = new OpenTxnRequest(1, "me", "localhost");
    txnRequest.setAgentInfo("agent");
    OpenTxnsResponse txnsResponse = txnHandler.openTxns(txnRequest);
    List<Long> txnIds = txnsResponse.getTxn_ids();
    Assert.assertEquals(1, txnIds.size());
    long txnId = txnIds.get(0);

    // Check that our transaction is open
    GetOpenTxnsResponse openTxns = txnHandler.getOpenTxns();
    Set<Long> openTxnIds = openTxns.getOpen_txns();
    Assert.assertTrue(openTxnIds.contains(txnId));
    long hwm = openTxns.getTxn_high_water_mark();

    // Check that we can get info about our transaction
    GetOpenTxnsInfoResponse txnInfo = txnHandler.getOpenTxnsInfo();
    Assert.assertEquals(hwm, txnInfo.getTxn_high_water_mark());
    List<TxnInfo> infos = txnInfo.getOpen_txns();
    boolean foundOurs = false;
    for (TxnInfo info : infos) {
      if (info.getId() == txnId) {
        foundOurs = true;
        Assert.assertEquals("me", info.getUser());
        Assert.assertEquals("localhost", info.getHostname());
        Assert.assertEquals("agent", info.getAgentInfo());
      }
    }
    Assert.assertTrue(foundOurs);

    AbortTxnRequest abort = new AbortTxnRequest(txnId);
    txnHandler.abortTxn(abort);
  }

  @Test
  public void openLockHeartbeatCommit() throws Exception {
    String db = "olhc_db";
    String table = "olhc_t";

    OpenTxnRequest txnRequest = new OpenTxnRequest(2, "me", "localhost");
    txnRequest.setAgentInfo("agent");
    OpenTxnsResponse txnsResponse = txnHandler.openTxns(txnRequest);
    List<Long> txnIds = txnsResponse.getTxn_ids();
    Assert.assertEquals(2, txnIds.size());
    long firstTxn = txnIds.get(0);
    long secondTxn = txnIds.get(1);

    // Check that we can get info about our transaction
    GetOpenTxnsInfoResponse txnInfo = txnHandler.getOpenTxnsInfo();
    List<TxnInfo> infos = txnInfo.getOpen_txns();
    boolean foundFirst = false, foundSecond = false;
    for (TxnInfo info : infos) {
      if (info.getId() == firstTxn) {
        foundFirst = true;
        Assert.assertEquals("me", info.getUser());
        Assert.assertEquals("localhost", info.getHostname());
        Assert.assertEquals("agent", info.getAgentInfo());
      } else if (info.getId() == secondTxn) {
        foundSecond = true;
        Assert.assertEquals("me", info.getUser());
        Assert.assertEquals("localhost", info.getHostname());
        Assert.assertEquals("agent", info.getAgentInfo());
      }
    }
    Assert.assertTrue(foundFirst && foundSecond);

    // Lock from the first txn
    List<LockComponent> components = new ArrayList<>(2);
    LockComponent component =  new LockComponent(LockType.INTENTION, LockLevel.DB, db);
    components.add(component);
    component = new LockComponent(LockType.EXCLUSIVE, LockLevel.TABLE, db);
    component.setTablename(table);
    components.add(component);

    LockRequest lock = new LockRequest(components, "me", "localhost");
    lock.setAgentInfo("agent");
    lock.setTxnid(firstTxn);
    LockResponse lockResponse = txnHandler.lock(lock);
    Assert.assertEquals(LockState.ACQUIRED, lockResponse.getState());

    // Lock from the second txn
    components = new ArrayList<>(2);
    component =  new LockComponent(LockType.INTENTION, LockLevel.DB, db);
    components.add(component);
    component = new LockComponent(LockType.EXCLUSIVE, LockLevel.TABLE, db);
    component.setTablename(table);
    components.add(component);

    lock = new LockRequest(components, "me", "localhost");
    lock.setAgentInfo("second_agent");
    lock.setTxnid(secondTxn);
    lockResponse = txnHandler.lock(lock);
    Assert.assertEquals(LockState.WAITING, lockResponse.getState());

    // Commit the first transaction
    CommitTxnRequest commit = new CommitTxnRequest(firstTxn);
    txnHandler.commitTxn(commit);

    CheckLockRequest checkLockRequest = new CheckLockRequest(-1);
    checkLockRequest.setTxnid(secondTxn);
    lockResponse = txnHandler.checkLock(checkLockRequest);
    Assert.assertEquals(LockState.ACQUIRED, lockResponse.getState());

    commit = new CommitTxnRequest(secondTxn);
    txnHandler.commitTxn(commit);
  }

  @Test
  public void show() throws Exception {
    // Test all the various options in show
    String dbs[] = { "show_db1", "show_db2" };
    String tables[] = { "show_table1", "show_table2" };
    String parts[] = { "show_part1", "show_part2" };

    OpenTxnRequest txnRequest = new OpenTxnRequest(2, "me", "localhost");
    txnRequest.setAgentInfo("agent");
    OpenTxnsResponse txnsResponse = txnHandler.openTxns(txnRequest);
    List<Long> txnIds = txnsResponse.getTxn_ids();
    Assert.assertEquals(1, txnIds.size());
    long firstTxn = txnIds.get(0);
    long secondTxn = txnIds.get(1);

    List<LockComponent> components = new ArrayList<>();
    LockComponent component =  new LockComponent(LockType.EXCLUSIVE, LockLevel.DB, dbs[0]);
    components.add(component);

    component = new LockComponent(LockType.INTENTION, LockLevel.DB, dbs[1]);
    components.add(component);
    component = new LockComponent(LockType.SHARED_READ, LockLevel.TABLE, dbs[1]);
    component.setTablename(tables[0]);
    components.add(component);

    component = new LockComponent(LockType.INTENTION, LockLevel.DB, dbs[1]);
    components.add(component);
    component = new LockComponent(LockType.INTENTION, LockLevel.TABLE, dbs[1]);
    component.setTablename(tables[1]);
    component = new LockComponent(LockType.SHARED_WRITE, LockLevel.PARTITION, dbs[1]);
    component.setTablename(tables[1]);
    component.setPartitionname(parts[0]);
    components.add(component);

    LockRequest lock = new LockRequest(components, "me", "localhost");
    lock.setAgentInfo("agent");
    lock.setTxnid(firstTxn);
    LockResponse lockResponse = txnHandler.lock(lock);
    Assert.assertEquals(LockState.ACQUIRED, lockResponse.getState());

    components = new ArrayList<>();
    component = new LockComponent(LockType.INTENTION, LockLevel.DB, dbs[1]);
    components.add(component);
    component = new LockComponent(LockType.INTENTION, LockLevel.TABLE, dbs[1]);
    component.setTablename(tables[1]);
    component = new LockComponent(LockType.SHARED_WRITE, LockLevel.PARTITION, dbs[1]);
    component.setTablename(tables[1]);
    component.setPartitionname(parts[1]);
    components.add(component);

    lock = new LockRequest(components, "me", "localhost");
    lock.setAgentInfo("agent");
    lock.setTxnid(firstTxn);
    lockResponse = txnHandler.lock(lock);
    Assert.assertEquals(LockState.ACQUIRED, lockResponse.getState());

    //ShowLocksResponse locksResponse = txnHandler.showLocks();
  }
}
