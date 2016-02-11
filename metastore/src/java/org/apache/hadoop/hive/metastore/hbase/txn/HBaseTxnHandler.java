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

import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.ipc.BlockingRpcCallback;
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
import org.apache.hadoop.hive.metastore.api.ShowCompactResponseElement;
import org.apache.hadoop.hive.metastore.api.ShowLocksRequest;
import org.apache.hadoop.hive.metastore.api.ShowLocksResponse;
import org.apache.hadoop.hive.metastore.api.TxnAbortedException;
import org.apache.hadoop.hive.metastore.api.TxnInfo;
import org.apache.hadoop.hive.metastore.api.TxnOpenException;
import org.apache.hadoop.hive.metastore.api.UnlockRequest;
import org.apache.hadoop.hive.metastore.hbase.HBaseReadWrite;
import org.apache.hadoop.hive.metastore.hbase.HBaseUtils;
import org.apache.hadoop.hive.metastore.hbase.HbaseMetastoreProto;
import org.apache.hadoop.hive.metastore.hbase.txn.txnmgr.TransactionManager;
import org.apache.hadoop.hive.metastore.txn.CompactionInfo;
import org.apache.hadoop.hive.metastore.txn.TxnStore;
import org.apache.hadoop.hive.metastore.txn.TxnUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class HBaseTxnHandler implements TxnStore {
  static final private Logger LOG = LoggerFactory.getLogger(HBaseTxnHandler.class.getName());

  private HBaseReadWrite hbase = null;
  private HiveConf conf;

  @Override
  public void setConf(HiveConf conf) {
    this.conf = conf;
  }

  // TODO I'm not sure how the exceptions come back to me.  Need to figure that out.

  @Override
  public GetOpenTxnsInfoResponse getOpenTxnsInfo() throws MetaException {
    // We have to go to the table to get this information because much of it isn't kept in memory.
    try {
      List<HbaseMetastoreProto.Transaction> txns = getHBase().scanTransactions();
      long hwm = getHBase().readCurrentSequence(HBaseReadWrite.TXN_SEQUENCE);
      List<TxnInfo> openTxns = new ArrayList<>(txns.size());
      for (HbaseMetastoreProto.Transaction txn : txns) openTxns.add(HBaseUtils.pbToThrift(txn));
      return new GetOpenTxnsInfoResponse(hwm, openTxns);
    } catch (IOException e) {
      LOG.error("Failed to scan transactions", e);
      throw new MetaException(e.getMessage());
    }
  }

  @Override
  public GetOpenTxnsResponse getOpenTxns() throws MetaException {
    Batch.Call<TransactionManager, HbaseMetastoreProto.GetOpenTxnsResponse> call =
        new Batch.Call<TransactionManager, HbaseMetastoreProto.GetOpenTxnsResponse>() {
          @Override
          public HbaseMetastoreProto.GetOpenTxnsResponse call(TransactionManager txnMgr) throws IOException {
            BlockingRpcCallback<HbaseMetastoreProto.GetOpenTxnsResponse> rpcCallback =
                new BlockingRpcCallback<>();
            txnMgr.getOpenTxns(null, HbaseMetastoreProto.Void.getDefaultInstance(), rpcCallback);
            return rpcCallback.get();
          }
        };
    try {
      return HBaseUtils.pbToThrift(getHBase().callTransactionManager(call));
    } catch (Throwable e) {
      LOG.error("Failed to get open transactions", e);
      throw new MetaException(e.getMessage());
    }
  }

  @Override
  public OpenTxnsResponse openTxns(OpenTxnRequest rqst) throws MetaException {
    final HbaseMetastoreProto.OpenTxnsRequest pbRqst = HBaseUtils.thriftToPb(rqst);
    Batch.Call<TransactionManager, HbaseMetastoreProto.OpenTxnsResponse> call =
        new Batch.Call<TransactionManager, HbaseMetastoreProto.OpenTxnsResponse>() {
          @Override
          public HbaseMetastoreProto.OpenTxnsResponse call(TransactionManager txnMgr) throws
              IOException {
            BlockingRpcCallback<HbaseMetastoreProto.OpenTxnsResponse> rpcCallback =
                new BlockingRpcCallback<>();
            txnMgr.openTxns(null, pbRqst, rpcCallback);
            return rpcCallback.get();
          }
        };
    try {
      return HBaseUtils.pbToThrift(getHBase().callTransactionManager(call));
    } catch (Throwable e) {
      LOG.error("Failed to open transactions", e);
      throw new MetaException(e.getMessage());
    }
  }

  @Override
  public void abortTxn(AbortTxnRequest rqst) throws NoSuchTxnException, MetaException {
    final HbaseMetastoreProto.TransactionId pbRqst = HBaseUtils.thriftToPb(rqst);
    Batch.Call<TransactionManager, HbaseMetastoreProto.TransactionResult> call =
        new Batch.Call<TransactionManager, HbaseMetastoreProto.TransactionResult>() {
          @Override
          public HbaseMetastoreProto.TransactionResult call(TransactionManager txnMgr) throws
              IOException {
            BlockingRpcCallback<HbaseMetastoreProto.TransactionResult> rpcCallback =
                new BlockingRpcCallback<>();
            txnMgr.abortTxn(null, pbRqst, rpcCallback);
            return rpcCallback.get();
          }
        };
    try {
      // Thrift doesn't return a value for abort, even though the co-processor does.
      getHBase().callTransactionManager(call);
    } catch (Throwable e) {
      LOG.error("Failed to abort transaction", e);
      throw new MetaException(e.getMessage());
    }
  }

  @Override
  public void commitTxn(CommitTxnRequest rqst) throws NoSuchTxnException, TxnAbortedException,
      MetaException {
    final HbaseMetastoreProto.TransactionId pbRqst = HBaseUtils.thriftToPb(rqst);
    Batch.Call<TransactionManager, HbaseMetastoreProto.TransactionResult> call =
        new Batch.Call<TransactionManager, HbaseMetastoreProto.TransactionResult>() {
          @Override
          public HbaseMetastoreProto.TransactionResult call(TransactionManager txnMgr) throws
              IOException {
            BlockingRpcCallback<HbaseMetastoreProto.TransactionResult> rpcCallback =
                new BlockingRpcCallback<>();
            txnMgr.commitTxn(null, pbRqst, rpcCallback);
            return rpcCallback.get();
          }
        };
    try {
      // Thrift doesn't return a value for abort, even though the co-processor does.
      getHBase().callTransactionManager(call);
    } catch (Throwable e) {
      LOG.error("Failed to abort transaction", e);
      throw new MetaException(e.getMessage());
    }
  }

  @Override
  public LockResponse lock(LockRequest rqst) throws NoSuchTxnException, TxnAbortedException,
      MetaException {
    if (!rqst.isSetTxnid()) {
      throw new MetaException("You must now set a transaction id when requesting locks");
    }

    final HbaseMetastoreProto.LockRequest pbRqst = HBaseUtils.thriftToPb(rqst);
    Batch.Call<TransactionManager, HbaseMetastoreProto.LockResponse> call =
        new Batch.Call<TransactionManager, HbaseMetastoreProto.LockResponse>() {
          @Override
          public HbaseMetastoreProto.LockResponse call(TransactionManager txnMgr) throws
              IOException {
            BlockingRpcCallback<HbaseMetastoreProto.LockResponse> rpcCallback =
                new BlockingRpcCallback<>();
            txnMgr.lock(null, pbRqst, rpcCallback);
            return rpcCallback.get();
          }
        };
    try {
      return HBaseUtils.pbToThrift(getHBase().callTransactionManager(call));
    } catch (Throwable e) {
      LOG.error("Failed to get locks", e);
      throw new MetaException(e.getMessage());
    }
  }

  @Override
  public LockResponse checkLock(CheckLockRequest rqst) throws NoSuchTxnException,
      NoSuchLockException, TxnAbortedException, MetaException {
    if (!rqst.isSetTxnid()) {
      throw new MetaException("You must now set a transaction id when requesting locks");
    }
    final HbaseMetastoreProto.TransactionId pbRqst = HBaseUtils.thriftToPb(rqst);
    Batch.Call<TransactionManager, HbaseMetastoreProto.LockResponse> call =
        new Batch.Call<TransactionManager, HbaseMetastoreProto.LockResponse>() {
          @Override
          public HbaseMetastoreProto.LockResponse call(TransactionManager txnMgr) throws
              IOException {
            BlockingRpcCallback<HbaseMetastoreProto.LockResponse> rpcCallback =
                new BlockingRpcCallback<>();
            txnMgr.checkLocks(null, pbRqst, rpcCallback);
            return rpcCallback.get();
          }
        };
    try {
      return HBaseUtils.pbToThrift(getHBase().callTransactionManager(call));
    } catch (Throwable e) {
      LOG.error("Failed to check locks", e);
      throw new MetaException(e.getMessage());
    }
  }

  @Override
  public void unlock(UnlockRequest rqst) throws NoSuchLockException, TxnOpenException,
      MetaException {
    throw new TxnOpenException("All locks must now be part of a txn, unlocking not allowed.");

  }

  @Override
  public ShowLocksResponse showLocks(ShowLocksRequest rqst) throws MetaException {
    // TODO get lock info from HBase table.
    // TODO implement a filter to look for dbname etc. info early
    return null;
  }

  @Override
  public void heartbeat(HeartbeatRequest ids) throws NoSuchTxnException, NoSuchLockException,
      TxnAbortedException, MetaException {
    if (!ids.isSetTxnid()) {
      throw new NoSuchLockException("You must now set a transaction id when heartbeating");
    }
    heartbeatTxnRange(new HeartbeatTxnRangeRequest(ids.getTxnid(), ids.getTxnid()));
  }

  @Override
  public HeartbeatTxnRangeResponse heartbeatTxnRange(HeartbeatTxnRangeRequest rqst) throws
      MetaException {
    final HbaseMetastoreProto.HeartbeatTxnRangeRequest pbRqst = HBaseUtils.thriftToPb(rqst);
    Batch.Call<TransactionManager, HbaseMetastoreProto.HeartbeatTxnRangeResponse> call =
        new Batch.Call<TransactionManager, HbaseMetastoreProto.HeartbeatTxnRangeResponse>() {
          @Override
          public HbaseMetastoreProto.HeartbeatTxnRangeResponse call(TransactionManager txnMgr) throws
              IOException {
            BlockingRpcCallback<HbaseMetastoreProto.HeartbeatTxnRangeResponse> rpcCallback =
                new BlockingRpcCallback<>();
            txnMgr.heartbeat(null, pbRqst, rpcCallback);
            return rpcCallback.get();
          }
        };
    try {
      return HBaseUtils.pbToThrift(getHBase().callTransactionManager(call));
    } catch (Throwable e) {
      LOG.error("Failed to heartbeat", e);
      throw new MetaException(e.getMessage());
    }
  }

  @Override
  public long compact(CompactionRequest rqst) throws MetaException {
    try {
      HbaseMetastoreProto.Compaction.Builder builder = HbaseMetastoreProto.Compaction.newBuilder();
      long compactionId = getHBase().getNextSequence(HBaseReadWrite.COMPACTION_SEQUENCE);
      builder.setId(compactionId);
      builder.setDb(rqst.getDbname());
      builder.setTable(rqst.getTablename());
      if (rqst.isSetPartitionname()) builder.setPartition(rqst.getPartitionname());
      builder.setState(HbaseMetastoreProto.CompactionState.INITIATED);
      builder.setType(HBaseUtils.thriftToPb(rqst.getType()));
      getHBase().putCompaction(builder.build());
    } catch (IOException e) {
      LOG.error("Failed to request compaction", e);
      throw new MetaException(e.getMessage());
    }
    return 0;
  }

  @Override
  public ShowCompactResponse showCompact(ShowCompactRequest rqst) throws MetaException {
    try {
      List<HbaseMetastoreProto.Compaction> compactions = getHBase().scanCompactions(null);
      List<ShowCompactResponseElement> elements = new ArrayList<>(compactions.size());
      for (HbaseMetastoreProto.Compaction compaction : compactions) {
        elements.add(HBaseUtils.pbToThrift(compaction));
      }
      return new ShowCompactResponse(elements);
    } catch (IOException e) {
      LOG.error("Failed to get compactions", e);
      throw new MetaException(e.getMessage());
    }
  }

  @Override
  public void addDynamicPartitions(AddDynamicPartitions rqst) throws NoSuchTxnException,
      TxnAbortedException, MetaException {
    final HbaseMetastoreProto.AddDynamicPartitionsRequest pbRqst = HBaseUtils.thriftToPb(rqst);
    Batch.Call<TransactionManager, HbaseMetastoreProto.TransactionResult> call =
        new Batch.Call<TransactionManager, HbaseMetastoreProto.TransactionResult>() {
          @Override
          public HbaseMetastoreProto.TransactionResult call(TransactionManager txnMgr) throws
              IOException {
            BlockingRpcCallback<HbaseMetastoreProto.TransactionResult> rpcCallback =
                new BlockingRpcCallback<>();
            txnMgr.addDynamicPartitions(null, pbRqst, rpcCallback);
            return rpcCallback.get();
          }
        };
    try {
      getHBase().callTransactionManager(call);
    } catch (Throwable e) {
      LOG.error("Failed to heartbeat", e);
      throw new MetaException(e.getMessage());
    }
  }

  @Override
  public void performTimeOuts() {
    // TODO - Figure out how to shut off AcidHouseKeeperService when running the HBase metastore
    // NOP
  }

  @Override
  public Set<CompactionInfo> findPotentialCompactions(int maxAborted) throws MetaException {
    // We ignore max aborted.
    // TODO change initiator to compact based on number of txns instead of number of aborts
    try {
      Set<CompactionInfo> cis = new HashSet<>();
      Iterator<HbaseMetastoreProto.PotentialCompaction> iter =
          getHBase().scanPotentialCompactions();
      while (iter.hasNext()) cis.add(HBaseUtils.pbToCompactor(iter.next()));
      return cis;
    } catch (IOException e) {
      LOG.error("Failed to find potential compactions", e);
      throw new MetaException(e.getMessage());
    }
  }

  @Override
  public void setRunAs(long cq_id, String user) throws MetaException {
    try {
      HbaseMetastoreProto.Compaction compaction = getHBase().getCompaction(cq_id);
      HbaseMetastoreProto.Compaction newCompaction =
          HbaseMetastoreProto.Compaction.newBuilder(compaction)
          .setRunAs(user)
          .build();
      getHBase().putCompaction(newCompaction);
    } catch (IOException e) {
      LOG.error("Failed to set run as", e);
      throw new MetaException(e.getMessage());

    }
  }

  @Override
  public CompactionInfo findNextToCompact(String workerId) throws MetaException {
    try {
      List<HbaseMetastoreProto.Compaction> initiated =
          getHBase().scanCompactions(HbaseMetastoreProto.CompactionState.INITIATED);
      if (initiated.size() == 0) return null;

      // Pick the first one and set the worker id and the start time
      HbaseMetastoreProto.Compaction toWorkOn =
          HbaseMetastoreProto.Compaction.newBuilder(initiated.get(0))
              .setState(HbaseMetastoreProto.CompactionState.WORKING)
              .setWorkerId(workerId)
              .setStartedWorkingAt(System.currentTimeMillis())
              .build();
      getHBase().putCompaction(toWorkOn);
      return HBaseUtils.pbToCompactor(toWorkOn);
    } catch (IOException e) {
      LOG.error("Failed to find next compaction to work on", e);
      throw new MetaException(e.getMessage());
    }
  }

  @Override
  public void markCompacted(CompactionInfo info) throws MetaException {
    try {
      final HbaseMetastoreProto.Compaction pbRqst = getHBase().getCompaction(info.id);
      if (pbRqst == null) {
        throw new MetaException("No such compaction " + info.id);
      }
      Batch.Call<TransactionManager, HbaseMetastoreProto.Void> call =
        new Batch.Call<TransactionManager, HbaseMetastoreProto.Void>() {
          @Override
          public HbaseMetastoreProto.Void call(TransactionManager txnMgr) throws IOException {
            BlockingRpcCallback<HbaseMetastoreProto.Void> rpcCallback = new BlockingRpcCallback<>();
            txnMgr.cleanupAfterCompaction(null, pbRqst, rpcCallback);
            return rpcCallback.get();
          }
        };
      getHBase().callTransactionManager(call);

      HbaseMetastoreProto.Compaction toWorkOn =
          HbaseMetastoreProto.Compaction.newBuilder(pbRqst)
              .setState(HbaseMetastoreProto.CompactionState.READY_FOR_CLEANING)
              .build();
      getHBase().putCompaction(toWorkOn);
    } catch (Throwable e) {
      LOG.error("Failed to cleanup after compaction", e);
      throw new MetaException(e.getMessage());
    }
  }

  @Override
  public List<CompactionInfo> findReadyToClean() throws MetaException {
    try {
      List<HbaseMetastoreProto.Compaction> compacted =
          getHBase().scanCompactions(HbaseMetastoreProto.CompactionState.READY_FOR_CLEANING);
      if (compacted.size() == 0) return null;

      // For each of these we also need to assure that all overlapping transactions have already
      // completed.  Otherwise we have to not clean yet because there could still be readers
      // making use of the old files
      final HbaseMetastoreProto.CompactionList pbRqst = HbaseMetastoreProto.CompactionList
          .newBuilder()
          .addAllCompactions(compacted)
          .build();
      Batch.Call<TransactionManager, HbaseMetastoreProto.CompactionList> call =
          new Batch.Call<TransactionManager, HbaseMetastoreProto.CompactionList>() {
            @Override
            public HbaseMetastoreProto.CompactionList call(TransactionManager txnMgr) throws IOException {
              BlockingRpcCallback<HbaseMetastoreProto.CompactionList> rpcCallback = new BlockingRpcCallback<>();
              txnMgr.verifyCompactionCanBeCleaned(null, pbRqst, rpcCallback);
              return rpcCallback.get();
            }
          };
      HbaseMetastoreProto.CompactionList cleaningList = getHBase().callTransactionManager(call);

      // Put each of these in the cleaning state and return them
      List<HbaseMetastoreProto.Compaction> cleanable =
          new ArrayList<>(cleaningList.getCompactionsCount());
      for (HbaseMetastoreProto.Compaction compaction : cleaningList.getCompactionsList()) {
        cleanable.add(HbaseMetastoreProto.Compaction.newBuilder(compaction)
            .setState(HbaseMetastoreProto.CompactionState.CLEANING)
            .build());
      }

      getHBase().putCompactions(cleanable);

      List<CompactionInfo> returns = new ArrayList<>(cleanable.size());
      for (HbaseMetastoreProto.Compaction compaction : cleanable) {
        returns.add(HBaseUtils.pbToCompactor(compaction));
      }
      return returns;
    } catch (Throwable e) {
      LOG.error("Failed to find next compaction to work on", e);
      throw new MetaException(e.getMessage());
    }
  }

  @Override
  public void markCleaned(CompactionInfo info) throws MetaException {
    changeCompactionState(info, HbaseMetastoreProto.CompactionState.SUCCEEDED);
  }

  @Override
  public void markFailed(CompactionInfo info) throws MetaException {
    changeCompactionState(info, HbaseMetastoreProto.CompactionState.FAILED);
  }

  private void changeCompactionState(CompactionInfo info,
                                     HbaseMetastoreProto.CompactionState state)
      throws MetaException {
    try {
      final HbaseMetastoreProto.Compaction pbRqst = getHBase().getCompaction(info.id);
      if (pbRqst == null) {
        throw new MetaException("No such compaction " + info.id);
      }

      HbaseMetastoreProto.Compaction toMarkCleaned =
          HbaseMetastoreProto.Compaction.newBuilder(pbRqst)
              .setState(state)
              .build();
      getHBase().putCompaction(toMarkCleaned);
    } catch (IOException e) {
      LOG.error("Failed to change compaction state", e);
      throw new MetaException(e.getMessage());
    }
  }

  @Override
  public void cleanEmptyAbortedTxns() throws MetaException {
    // NOP
  }

  @Override
  public void revokeFromLocalWorkers(String hostname) throws MetaException {
    try {
      List<HbaseMetastoreProto.Compaction> working =
          getHBase().scanCompactions(HbaseMetastoreProto.CompactionState.WORKING);
      if (working.size() == 0) return;

      List<HbaseMetastoreProto.Compaction> revokable = new ArrayList<>(working.size());
      for (HbaseMetastoreProto.Compaction compaction : working) {
        if (compaction.hasWorkerId() && compaction.getWorkerId().startsWith(hostname)) {
          revokable.add(compaction);
        }
      }
      if (revokable.size() == 0) return;

      List<HbaseMetastoreProto.Compaction> newCompactions = new ArrayList<>(revokable.size());
      for (HbaseMetastoreProto.Compaction compaction : revokable) {
        newCompactions.add(HbaseMetastoreProto.Compaction.newBuilder(compaction)
            .setState(HbaseMetastoreProto.CompactionState.INITIATED)
            .clearWorkerId()
            .clearStartedWorkingAt()
            .build());
      }

      getHBase().putCompactions(newCompactions);
    } catch (Throwable e) {
      LOG.error("Failed to find next compaction to work on", e);
      throw new MetaException(e.getMessage());
    }
  }

  @Override
  public void revokeTimedoutWorkers(long timeout) throws MetaException {
    try {
      List<HbaseMetastoreProto.Compaction> working =
          getHBase().scanCompactions(HbaseMetastoreProto.CompactionState.WORKING);
      if (working.size() == 0) return;

      long mustBeStartedBefore = System.currentTimeMillis() - timeout;
      List<HbaseMetastoreProto.Compaction> revokable = new ArrayList<>(working.size());
      for (HbaseMetastoreProto.Compaction compaction : working) {
        if (compaction.hasStartedWorkingAt() &&
            compaction.getStartedWorkingAt() < mustBeStartedBefore) {
          revokable.add(compaction);
        }
      }
      if (revokable.size() == 0) return;

      List<HbaseMetastoreProto.Compaction> newCompactions = new ArrayList<>(revokable.size());
      for (HbaseMetastoreProto.Compaction compaction : revokable) {
        newCompactions.add(HbaseMetastoreProto.Compaction.newBuilder(compaction)
            .setState(HbaseMetastoreProto.CompactionState.INITIATED)
            .clearWorkerId()
            .clearStartedWorkingAt()
            .build());
      }

      getHBase().putCompactions(newCompactions);
    } catch (Throwable e) {
      LOG.error("Failed to find next compaction to work on", e);
      throw new MetaException(e.getMessage());
    }
  }

  @Override
  public List<String> findColumnsWithStats(CompactionInfo ci) throws MetaException {
    // TODO - query partitions or table table in HBase to answer this
    return null;
  }

  @Override
  public void setCompactionHighestTxnId(CompactionInfo ci, long highestTxnId) throws MetaException {
    try {
      HbaseMetastoreProto.Compaction compaction = getHBase().getCompaction(ci.id);
      HbaseMetastoreProto.Compaction newCompaction =
          HbaseMetastoreProto.Compaction.newBuilder(compaction)
          .setHighestTxnId(highestTxnId)
          .build();
      getHBase().putCompaction(newCompaction);
    } catch (IOException e) {
      LOG.error("Failed to set highest txn id", e);
      throw new MetaException(e.getMessage());
    }
  }

  @Override
  public void purgeCompactionHistory() throws MetaException {
    try {
      List<HbaseMetastoreProto.Compaction> compactions = getHBase().scanCompactions(null);
      if (compactions.size() == 0) return;

      TxnStore.RetentionCounters rc = new RetentionCounters(
          conf.getIntVar(HiveConf.ConfVars.COMPACTOR_HISTORY_RETENTION_ATTEMPTED),
          TxnUtils.getFailedCompactionRetention(conf),
          conf.getIntVar(HiveConf.ConfVars.COMPACTOR_HISTORY_RETENTION_SUCCEEDED));

      List<Long> deleteSet = new ArrayList<>();
      Map<Long, HbaseMetastoreProto.Compaction> lookups = new HashMap<>(compactions.size());
      for (HbaseMetastoreProto.Compaction compaction : compactions) {
        if (compaction.getState() == HbaseMetastoreProto.CompactionState.FAILED ||
            compaction.getState() == HbaseMetastoreProto.CompactionState.SUCCEEDED) {
          lookups.put(compaction.getId(), compaction);
          CompactionInfo ci = HBaseUtils.pbToCompactor(compaction);
          TxnUtils.checkForDeletion(deleteSet, ci, rc);
        }
      }

      if (deleteSet.size() > 0) {
        getHBase().deleteCompactions(deleteSet);
      }
    } catch (IOException e) {
      LOG.error("Failed to purge compaction history", e);
      throw new MetaException(e.getMessage());

    }

  }

  @Override
  public boolean checkFailedCompactions(CompactionInfo ci) throws MetaException {
    try {
      List<HbaseMetastoreProto.Compaction> compactions =
          getHBase().scanCompactions(HbaseMetastoreProto.CompactionState.FAILED);
      if (compactions.size() == 0) return false;

      int failedThreshold = conf.getIntVar(HiveConf.ConfVars.COMPACTOR_INITIATOR_FAILED_THRESHOLD);

      int numFails = 0;
      for (HbaseMetastoreProto.Compaction compaction : compactions) {
        if (compaction.getDb().equals(ci.dbname) &&
            compaction.getTable().equals(ci.tableName) &&
            (compaction.hasPartition() && compaction.getPartition().equals(ci.partName) ||
                !compaction.hasPartition() && ci.partName == null)) {
          numFails++;
        }
      }

      return numFails > failedThreshold;
    } catch (IOException e) {
      LOG.error("Failed to purge compaction history", e);
      throw new MetaException(e.getMessage());

    }

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
