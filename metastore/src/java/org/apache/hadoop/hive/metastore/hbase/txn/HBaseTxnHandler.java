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
package org.apache.hadoop.hive.metastore.hbase.txn;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.AbortTxnRequest;
import org.apache.hadoop.hive.metastore.api.AddDynamicPartitions;
import org.apache.hadoop.hive.metastore.api.CheckLockRequest;
import org.apache.hadoop.hive.metastore.api.CommitTxnRequest;
import org.apache.hadoop.hive.metastore.api.CompactionRequest;
import org.apache.hadoop.hive.metastore.api.GetOpenTxnsInfoResponse;
import org.apache.hadoop.hive.metastore.api.GetOpenTxnsResponse;
import org.apache.hadoop.hive.metastore.api.HeartbeatRequest;
import org.apache.hadoop.hive.metastore.api.HeartbeatTxnRangeRequest;
import org.apache.hadoop.hive.metastore.api.HeartbeatTxnRangeResponse;
import org.apache.hadoop.hive.metastore.api.LockRequest;
import org.apache.hadoop.hive.metastore.api.LockResponse;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchLockException;
import org.apache.hadoop.hive.metastore.api.NoSuchTxnException;
import org.apache.hadoop.hive.metastore.api.OpenTxnRequest;
import org.apache.hadoop.hive.metastore.api.OpenTxnsResponse;
import org.apache.hadoop.hive.metastore.api.ShowCompactRequest;
import org.apache.hadoop.hive.metastore.api.ShowCompactResponse;
import org.apache.hadoop.hive.metastore.api.ShowLocksRequest;
import org.apache.hadoop.hive.metastore.api.ShowLocksResponse;
import org.apache.hadoop.hive.metastore.api.TxnAbortedException;
import org.apache.hadoop.hive.metastore.api.TxnOpenException;
import org.apache.hadoop.hive.metastore.api.UnlockRequest;
import org.apache.hadoop.hive.metastore.hbase.HBaseReadWrite;
import org.apache.hadoop.hive.metastore.hbase.HbaseMetastoreProto;
import org.apache.hadoop.hive.metastore.txn.CompactionInfo;
import org.apache.hadoop.hive.metastore.txn.TxnStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.Set;

public class HBaseTxnHandler implements TxnStore {
  static final private Logger LOG = LoggerFactory.getLogger(HBaseTxnHandler.class.getName());

  private HBaseReadWrite hbase = null;
  private HiveConf conf;

  @Override
  public void setConf(HiveConf conf) {
    this.conf = conf;
  }

  @Override
  public GetOpenTxnsInfoResponse getOpenTxnsInfo() throws MetaException {
    // We have to go to the table to get this information because much of it isn't kept in memory.
    try {
      List<HbaseMetastoreProto.Transaction> txns = getHBase().scanTransactions();
      long hwm = getHBase().readCurrentSequence()
      return new GetOpenTxnsInfoResponse(hwm, openTxns);
    } catch (IOException e) {
      LOG.error("Failed to scan transactions", e);
      throw new MetaException(e.getMessage());
    }
  }

  @Override
  public GetOpenTxnsResponse getOpenTxns() throws MetaException {
    // TODO -actually call co-processor
    /*
    try {
      return txnMgr.getOpenTxns();
    } catch (IOException e) {
      throw new MetaException(e.getMessage());
    }
    */
    return null;
  }

  @Override
  public OpenTxnsResponse openTxns(OpenTxnRequest rqst) throws MetaException {
    // TODO -actually call co-processor
    /*
    try {
      return txnMgr.openTxns(rqst);
    } catch (IOException e) {
      throw new MetaException(e.getMessage());
    }
    */
    return null;
  }

  @Override
  public void abortTxn(AbortTxnRequest rqst) throws NoSuchTxnException, MetaException {
    // TODO -actually call co-processor
    /*
    try {
      txnMgr.abortTxn(rqst);
    } catch (IOException e) {
      throw new MetaException(e.getMessage());
    }
    */
  }

  @Override
  public void commitTxn(CommitTxnRequest rqst) throws NoSuchTxnException, TxnAbortedException,
      MetaException {
    // TODO -actually call co-processor
    /*
    try {
      txnMgr.commitTxn(rqst);
    } catch (IOException e) {
      throw new MetaException(e.getMessage());
    }
    */
  }

  @Override
  public LockResponse lock(LockRequest rqst) throws NoSuchTxnException, TxnAbortedException,
      MetaException {
    // TODO -actually call co-processor
    /*
    try {
      return txnMgr.lock(rqst);
    } catch (IOException e) {
      throw new MetaException(e.getMessage());
    }
    */
    // TODO - wait and piece together lock responses until I get all my locks
    return null;
  }

  @Override
  public LockResponse checkLock(CheckLockRequest rqst) throws NoSuchTxnException,
      NoSuchLockException, TxnAbortedException, MetaException {
    // TODO Should be a no-op in the new world, except for old clients.
    return null;
  }

  @Override
  public void unlock(UnlockRequest rqst) throws NoSuchLockException, TxnOpenException,
      MetaException {
    // TODO likely to make this an invalid call

  }

  @Override
  public ShowLocksResponse showLocks(ShowLocksRequest rqst) throws MetaException {
    // TODO get lock info from HBase table
    return null;
  }

  @Override
  public void heartbeat(HeartbeatRequest ids) throws NoSuchTxnException, NoSuchLockException,
      TxnAbortedException, MetaException {
    // TODO call heartbeatTxnRange with range of 1

  }

  @Override
  public HeartbeatTxnRangeResponse heartbeatTxnRange(HeartbeatTxnRangeRequest rqst) throws
      MetaException {
    // TODO -actually call co-processor
    /*
    try {
      return txnMgr.heartbeat(rqst);
    } catch (IOException e) {
      throw new MetaException(e.getMessage());
    }
    */
    return null;
  }

  @Override
  public long compact(CompactionRequest rqst) throws MetaException {
    // TODO - add initiated request to queue
    return 0;
  }

  @Override
  public ShowCompactResponse showCompact(ShowCompactRequest rqst) throws MetaException {
    // TODO - get all current requests from queue
    return null;
  }

  @Override
  public void addDynamicPartitions(AddDynamicPartitions rqst) throws NoSuchTxnException,
      TxnAbortedException, MetaException {
    // TODO -actually call co-processor
    /*
    try {
      txnMgr.addDynamicPartitions(rqst);
    } catch (IOException e) {
      throw new MetaException(e.getMessage());
    }
    */
  }

  @Override
  public void performTimeOuts() {
    // TODO - I think this goes away and we get rid of AcidHouseKeeperService

  }

  @Override
  public Set<CompactionInfo> findPotentialCompactions(int maxAborted) throws MetaException {
    // TODO look list potentialCompactionsTable in HBase (or use new column family in table and
    // partition tables) and find entries with high number of potentials.
    return null;
  }

  @Override
  public void setRunAs(long cq_id, String user) throws MetaException {
    // TODO modify compaction in HBase

  }

  @Override
  public CompactionInfo findNextToCompact(String workerId) throws MetaException {
    // TODO get next initiated compaction out of the queue
    return null;
  }

  @Override
  public void markCompacted(CompactionInfo info) throws MetaException {
    // TODO get list of txns from potentialCompactionsTable compacted this (modified by
    // highestCompaction listed in info), look up those compactions in aborted list and remove them.

    // TODO modify state in HBase for each txn as well, and set lock states to compacted

    /*
    HbaseMetastoreProto.Transaction txn = null;
    for (HbaseMetastoreProto.Transaction.Lock lock : txn.getLocksList()) {
      if (!lock.getCompacted()) return;
    }
    // TODO -actually call co-processor
    try {
      txnMgr.removeCompletelyCompactedAbortedTxn(txn.getId());
    } catch (IOException e) {
      throw new MetaException(e.getMessage());
    }
    */
  }

  @Override
  public List<CompactionInfo> findReadyToClean() throws MetaException {
    // TODO get list of entries that are ready to clean
    // TODO need to understand if all overlapping txns are finished before allowing cleaning
    // something real to do
    return null;
  }

  @Override
  public void markCleaned(CompactionInfo info) throws MetaException {
    // TODO update state in HBase

  }

  @Override
  public void markFailed(CompactionInfo info) throws MetaException {
    // TODO update state in HBase

  }

  @Override
  public void cleanEmptyAbortedTxns() throws MetaException {
    // TODO I think we can ignore this now, as the TransactionManager will decide

  }

  @Override
  public void revokeFromLocalWorkers(String hostname) throws MetaException {
    // TODO - change state in metastore

  }

  @Override
  public void revokeTimedoutWorkers(long timeout) throws MetaException {
    // TODO - change state in metastore

  }

  @Override
  public List<String> findColumnsWithStats(CompactionInfo ci) throws MetaException {
    // TODO - query partitions or table table in HBase to answer this
    return null;
  }

  @Override
  public void setCompactionHighestTxnId(CompactionInfo ci, long highestTxnId) throws MetaException {
    // TODO - modify info in HBase

  }

  @Override
  public void purgeCompactionHistory() throws MetaException {
    // TODO - change state in metastore

  }

  @Override
  public boolean checkFailedCompactions(CompactionInfo ci) throws MetaException {
    // TODO - get info from HBase
    return false;
  }

  @Override
  public int numLocksInLockTable() throws SQLException, MetaException {
    return 0;
  }

  @Override
  public long setTimeout(long milliseconds) {
    return 0;
  }

  private HBaseReadWrite getHBase() {
    if (hbase == null) {
      HBaseReadWrite.setConf(conf);
      hbase = HBaseReadWrite.getInstance();
    }
    return hbase;
  }

}
