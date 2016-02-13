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

import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.CoprocessorService;
import org.apache.hadoop.hbase.protobuf.ResponseConverter;
import org.apache.hadoop.hive.common.ObjectPair;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.hbase.HBaseReadWrite;
import org.apache.hadoop.hive.metastore.hbase.HbaseMetastoreProto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * A transaction and lock manager written to work inside an HBase co-processor.  This class keeps
 * a lot of state in memory.  It remembers all open transactions as well as committed ones that
 * might be needed to avoid lost updates and aborted ones that haven't been compacted out yet.
 *
 * Locks are a part of a transaction, so each txn has a list of locks.  All transaction oriented
 * operations are done via txnId.
 *
 * Locks are also kept in 'dtp queues' which are sorted trees keyed by db, table, partition that
 * the lock is on.  This allows the system to quickly evaluate which locks should be granted next.
 *
 * All write operations are written through to HBase.  When the class first starts it recovers
 * the current state from HBase.
 *
 * <em>All</em> reads and writes to the Transaction table in HBase must be done through this
 * class.  Access to the table and internal memory structures is controlled by a read/write lock.
 *
 * All writes to the PotentialCompactions table are done here, reads are done by
 * {@link org.apache.hadoop.hive.metastore.hbase.txn.HBaseTxnHandler}
 */
public class TransactionManager extends HbaseMetastoreProto.TxnMgr implements Coprocessor,
                                                                              CoprocessorService {

  // TODO add new object types in HBaseSchemaTool
  // TODO Write some tests so you have some clue if this works

  // Someday - handle lock promotion

  static final private Logger LOG = LoggerFactory.getLogger(TransactionManager.class.getName());

  // Track what locks types are compatible.  First array is holder, second is requester
  private static boolean[][] lockCompatibilityTable;

  // This lock needs to be acquired in write mode only when modifying structures (opening,
  // aborting, committing txns, adding locks).  To modify structures (ie heartbeat) it is
  // only needed in the read mode.  Anything looking at this structure should acquire it in the
  // read mode.
  private ReadWriteLock masterLock;

  // A list of all active transactions.  Obtain the globalLock before changing this list.  You
  // can read this list without obtaining the global lock.
  private Map<Long, OpenHiveTransaction> openTxns;

  // List of aborted transactions, kept in memory for efficient reading when readers need a valid
  // transaction list.
  private Map<Long, AbortedHiveTransaction> abortedTxns;

  // A set of all committed transactions.
  private Set<CommittedHiveTransaction> committedTxns;

  private Configuration conf;

  // Protected by synchronized code section on openTxn;
  private long nextTxnId;

  // Protected by synchronized code section on getLockId;
  private long nextLockId;

  // A structure to store the locks according to which database/table/partition they lock.
  private Map<EntityKey, LockQueue> lockQueues;

  private HBaseReadWrite hbase;

  // A queue used to signal to the lockChecker which dtp queues it should look in.
  private Queue<EntityKey> lockQueuesToCheck;

  // Keep track of all our service threads so we can stop them when we shut down.
  private List<Thread> serviceThreads;

  // Flag to tell threads to keep going
  private boolean go;

  // Configuration values cached to avoid re-reading config all the time.
  private long lockPollTimeout;
  private int maxObjects;

  // If true, then we've filled the capacity of this thing and we don't want to accept any new
  // open transactions until some have bled off.
  private boolean full;

  public TransactionManager() {

  }

  @Override
  public void start(CoprocessorEnvironment coprocessorEnvironment) throws IOException {
    LOG.info("Starting transaction manager co-processor service");
    masterLock = new ReentrantReadWriteLock();
    // Don't set the values here, as for efficiency in iteration we want the size to match as
    // closely as possible to the actual number of entries.
    openTxns = new HashMap<>();
    abortedTxns = new HashMap<>();
    committedTxns = new HashSet<>();
    lockQueues = new HashMap<>();
    lockQueuesToCheck = new ArrayDeque<>();
    conf = coprocessorEnvironment.getConfiguration();
    lockPollTimeout = HiveConf.getTimeVar(conf, HiveConf.ConfVars.METASTORE_HBASE_LOCK_POLL_TIMEOUT,
        TimeUnit.MILLISECONDS);
    maxObjects = HiveConf.getIntVar(conf, HiveConf.ConfVars.METASTORE_HBASE_TXN_MANAGER_MAX_OBJECTS);
    try {
      recover();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    go = true;
    serviceThreads = new ArrayList<>(services.length);
    for (NamedRunnable service : services) {
      Thread t = new Thread(service);
      LOG.info("Starting background thread " + t.getName() + " to run service " + service.getName());
      t.setDaemon(true);
      t.start();
      serviceThreads.add(t);
    }
    LOG.info("Finished startup");
  }

  @Override
  public void stop(CoprocessorEnvironment coprocessorEnvironment) throws IOException {
    go = false;
    LOG.info("Shutting down transaction manager co-processor service");
    for (Thread t : serviceThreads) {
      LOG.info("Waiting for thread " + t.getName() + " to stop");
      t.interrupt();
      try {
        t.wait(1000);
      } catch (InterruptedException e) {
        LOG.warn("Interrupted while waiting for thread " + t.getName());
      }
    }
    LOG.info("Finished shut down of transaction manager co-processor service");
  }

  @Override
  public Service getService() {
    return this;
  }

  @Override
  public void openTxns(RpcController controller, HbaseMetastoreProto.OpenTxnsRequest request,
                       RpcCallback<HbaseMetastoreProto.OpenTxnsResponse> done) {
    HbaseMetastoreProto.OpenTxnsResponse response = null;
    if (full) {
      LOG.error("Request for new transaction rejected because the transaction manager has used " +
          "available memory and cannot accept new transactions until some existing ones are " +
          "closed out.");
      ResponseConverter.setControllerException(controller,
          new IOException("Transaction manager full"));
      done.run(response);
    }

    if (LOG.isDebugEnabled()) LOG.debug("Opening " + request.getNumTxns() + " transactions");
    boolean shouldCommit = false;
    getHBase().begin();
    try (LockKeeper lk = new LockKeeper(masterLock.writeLock())) {
      List<HbaseMetastoreProto.Transaction> hbaseTxns = new ArrayList<>();
      HbaseMetastoreProto.OpenTxnsResponse.Builder rspBuilder =
          HbaseMetastoreProto.OpenTxnsResponse.newBuilder();
      for (int i = 0; i < request.getNumTxns(); i++) {
        OpenHiveTransaction txn = new OpenHiveTransaction(nextTxnId++);
        openTxns.put(txn.getId(), txn);

        HbaseMetastoreProto.Transaction.Builder builder = HbaseMetastoreProto.Transaction
            .newBuilder()
            .setId(txn.getId())
            .setTxnState(txn.getState())
            .setUser(request.getUser())
            .setHostname(request.getHostname());
        if (request.hasAgentInfo()) {
          builder.setAgentInfo(request.getAgentInfo());
        }
        hbaseTxns.add(builder.build());
        rspBuilder.addTxnIds(txn.getId());
      }
      long newTxnVal = getHBase().addToSequence(HBaseReadWrite.TXN_SEQUENCE, request.getNumTxns());
      assert newTxnVal == nextTxnId;
      getHBase().putTransactions(hbaseTxns);
      shouldCommit = true;
      response = rspBuilder.build();
    } catch (IOException e) {
      ResponseConverter.setControllerException(controller, e);
    } finally {
      if (shouldCommit) getHBase().commit();
      else getHBase().rollback();
    }
    done.run(response);
  }

  @Override
  public void getOpenTxns(RpcController controller, HbaseMetastoreProto.Void request,
                          RpcCallback<HbaseMetastoreProto.GetOpenTxnsResponse> done) {
    LOG.debug("Getting open transactions");
    HbaseMetastoreProto.GetOpenTxnsResponse response = null;
    try (LockKeeper lk = new LockKeeper(masterLock.readLock())) {
      response = HbaseMetastoreProto.GetOpenTxnsResponse.newBuilder()
          .setHighWaterMark(nextTxnId)
          .addAllOpenTransactions(openTxns.keySet())
          .addAllAbortedTransactions(abortedTxns.keySet())
          .build();
    } catch (IOException e) {
      ResponseConverter.setControllerException(controller, e);
    }
    done.run(response);
  }

  @Override
  public void abortTxn(RpcController controller, HbaseMetastoreProto.TransactionId request,
                       RpcCallback<HbaseMetastoreProto.TransactionResult> done) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Aborting txn " + request.getId());
    }
    HbaseMetastoreProto.TransactionResult response = null;
    try (LockKeeper lk = new LockKeeper(masterLock.writeLock())) {
      OpenHiveTransaction txn = openTxns.get(request.getId());
      if (txn == null) {
        LOG.warn("Unable to find transaction to abort " + request.getId());
        response = HbaseMetastoreProto.TransactionResult.newBuilder()
            .setState(HbaseMetastoreProto.TxnStateChangeResult.NO_SUCH_TXN)
            .build();
      } else {
        if (txn.getState() == HbaseMetastoreProto.TxnState.COMMITTED ||
            txn.getState() == HbaseMetastoreProto.TxnState.ABORTED) {
          suicide("Logic error, found a committed or aborted txn in the open list");
        }
        abortTxn(txn);
        response = HbaseMetastoreProto.TransactionResult.newBuilder()
            .setState(HbaseMetastoreProto.TxnStateChangeResult.SUCCESS)
            .build();
      }
    } catch (IOException e) {
      ResponseConverter.setControllerException(controller, e);
    }
    done.run(response);
  }

  /**
   * Abort a transaction.  You must own the master write lock before entering this method.  You
   * must NOT be in an HBase transaction.
   * @param txn transaction to abort
   * @throws IOException
   */
  private void abortTxn(OpenHiveTransaction txn)
      throws IOException {
    HiveLock[] locks = txn.getHiveLocks();
    if (locks != null) {
      for (HiveLock lock : locks) {
        lock.setState(HbaseMetastoreProto.LockState.TXN_ABORTED);
        lockQueues.get(lock.getEntityLocked()).queue.remove(lock.getId());
        // It's ok to put these in the queue now even though we're still removing things because
        // we hold the master write lock.  The lockChecker won't be able to run until we've
        // released that lock anyway.
        lockQueuesToCheck.add(lock.getEntityLocked());
      }
      // Signal on the lockQueuesToCheck so that the lockChecker knows to go see what can be
      // acquired now.
      LOG.debug("Signally on lockQueuesToCheck");
      lockQueuesToCheck.notify();
    }

    // Move the entry to the aborted txns list
    openTxns.remove(txn.getId());
    if (txn.hasWriteLocks()) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Transaction " + txn.getId() + " has write locks, creating aborted txn");
      }
      AbortedHiveTransaction abortedTxn =  new AbortedHiveTransaction(txn);
      abortedTxns.put(txn.getId(), abortedTxn);

      List<HBaseReadWrite.PotentialCompactionEntity> pces = new ArrayList<>();

      // This is where protocol buffers suck.  Since they're read only we have to make a whole
      // new copy
      HbaseMetastoreProto.Transaction hbaseTxn = getHBase().getTransaction(abortedTxn.getId());
      HbaseMetastoreProto.Transaction.Builder txnBuilder =
          HbaseMetastoreProto.Transaction.newBuilder(hbaseTxn);
      txnBuilder.clearLocks();
      txnBuilder.setTxnState(HbaseMetastoreProto.TxnState.ABORTED);
      List<HbaseMetastoreProto.Transaction.Lock> hbaseLocks = hbaseTxn.getLocksList();
      for (HbaseMetastoreProto.Transaction.Lock hbaseLock : hbaseLocks) {
        // We only need to remember the shared_write locks, all the rest can be ignored as they
        // aren't a part of compaction.
        if (hbaseLock.getType() == HbaseMetastoreProto.LockType.SHARED_WRITE) {
          txnBuilder.addLocks(hbaseLock);
          if (LOG.isDebugEnabled()) {
            LOG.debug("Found potential compaction for " + hbaseLock.getDb() + "." +
                hbaseLock.getTable() +
                (hbaseLock.hasPartition() ? "." + hbaseLock.getPartition() : ""));
          }
          pces.add(new HBaseReadWrite.PotentialCompactionEntity(hbaseLock.getDb(),
              hbaseLock.getTable(), hbaseLock.hasPartition() ? hbaseLock.getPartition() : null));
        }
      }
      boolean shouldCommit = false;
      getHBase().begin();
      try {
        getHBase().putTransaction(txnBuilder.build());
        getHBase().putPotentialCompactions(abortedTxn.getId(), pces);
        shouldCommit = true;
      } finally {
        if (shouldCommit) getHBase().commit();
        else getHBase().rollback();
      }
    } else {
      // There's no reason to remember this txn anymore, it either had no locks or was read only.
      if (LOG.isDebugEnabled()) {
        LOG.debug("Forgetting aborted txn " + txn.getId() + " as it has no write locks");
      }
      getHBase().deleteTransaction(txn.getId());
    }
  }

  @Override
  public void commitTxn(RpcController controller, HbaseMetastoreProto.TransactionId request,
                        RpcCallback<HbaseMetastoreProto.TransactionResult> done) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Committing txn " + request.getId());
    }
    HbaseMetastoreProto.TransactionResult response = null;
    boolean shouldCommit = false;
    getHBase().begin();
    try (LockKeeper lk = new LockKeeper(masterLock.writeLock())) {
      OpenHiveTransaction txn = openTxns.get(request.getId());
      if (txn == null) {
        LOG.info("Unable to find txn to commit " + request.getId());
        response = HbaseMetastoreProto.TransactionResult.newBuilder()
            .setState(HbaseMetastoreProto.TxnStateChangeResult.NO_SUCH_TXN)
            .build();
        done.run(response);
        return;
      } else {
        if (txn.getState() == HbaseMetastoreProto.TxnState.COMMITTED ||
            txn.getState() == HbaseMetastoreProto.TxnState.ABORTED) {
          suicide("Logic error, found a committed or aborted txn in the open list");
        }
      }
      HiveLock[] locks = txn.getHiveLocks();
      if (locks != null) {
        for (HiveLock lock : locks) {
          lock.setState(HbaseMetastoreProto.LockState.RELEASED);
          lockQueues.get(lock.getEntityLocked()).queue.remove(lock.getId());
          lockQueuesToCheck.add(lock.getEntityLocked());
        }
        // Signal on the lockQueuesToCheck so that the lockChecker knows to go see what can be
        // acquired now.
        LOG.debug("Signaling on lockQueuesToCheck");
        lockQueuesToCheck.notify();
      }

      openTxns.remove(txn.getId());
      // We only need to remember the transaction if it had write locks.  If it's read only or
      // DDL we can forget it.
      if (txn.hasWriteLocks()) {
        // There's no need to move the transaction counter ahead
        CommittedHiveTransaction committedTxn = new CommittedHiveTransaction(txn, nextTxnId);

        if (LOG.isDebugEnabled()) {
          LOG.debug("Created new committed transaction with txn id " + committedTxn.getId() +
              " and commit id " + committedTxn.getCommitId());
        }

        committedTxns.add(committedTxn);
        // Record all of the commit ids in the lockQueues so other transactions can quickly look
        // it up when getting locks.
        for (HiveLock lock : txn.getHiveLocks()) {
          LockQueue queue = lockQueues.get(lock.getEntityLocked());
          if (LOG.isDebugEnabled()) {
            LOG.debug(lock.getEntityLocked().toString() + " has max commit id of " +
                queue.maxCommitId);
          }
          queue.maxCommitId = Math.max(queue.maxCommitId, committedTxn.getCommitId());
          if (LOG.isDebugEnabled()) {
            LOG.debug("Set " + lock.getEntityLocked().toString() + " max commit id to " +
                queue.maxCommitId);
          }
        }
        List<HBaseReadWrite.PotentialCompactionEntity> pces = new ArrayList<>();

        HbaseMetastoreProto.Transaction hbaseTxn = getHBase().getTransaction(committedTxn.getId());
        HbaseMetastoreProto.Transaction.Builder txnBuilder =
            HbaseMetastoreProto.Transaction.newBuilder(hbaseTxn);
        txnBuilder.setTxnState(HbaseMetastoreProto.TxnState.COMMITTED);
        txnBuilder.setCommitId(committedTxn.getCommitId());
        txnBuilder.clearLocks();
        List<HbaseMetastoreProto.Transaction.Lock> hbaseLocks = hbaseTxn.getLocksList();
        for (HbaseMetastoreProto.Transaction.Lock hbaseLock : hbaseLocks) {
          // We only need to remember the shared_write locks, all the rest can be ignored as they
          // aren't of interest for building write sets
          if (hbaseLock.getType() == HbaseMetastoreProto.LockType.SHARED_WRITE) {
            HbaseMetastoreProto.Transaction.Lock.Builder lockBuilder =
                HbaseMetastoreProto.Transaction.Lock.newBuilder(hbaseLock);
            txnBuilder.addLocks(lockBuilder);
            if (LOG.isDebugEnabled()) {
              LOG.debug("Adding new entry to the potential compactions " + hbaseLock.getDb() +
                  "." + hbaseLock.getTable() +
                  (hbaseLock.hasPartition() ? "." + hbaseLock.getPartition() : ""));
            }
            pces.add(new HBaseReadWrite.PotentialCompactionEntity(hbaseLock.getDb(), hbaseLock
                .getTable(), hbaseLock.getPartition()));
          }
        }
        getHBase().putTransaction(txnBuilder.build());
        getHBase().putPotentialCompactions(committedTxn.getId(), pces);
      } else {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Forgetting transaction " + txn.getId() + " as it is committed and held no " +
              "write locks");
        }
        getHBase().deleteTransaction(txn.getId());
      }
      shouldCommit = true;
      response = HbaseMetastoreProto.TransactionResult.newBuilder()
          .setState(HbaseMetastoreProto.TxnStateChangeResult.SUCCESS)
          .build();
    } catch (IOException e) {
      ResponseConverter.setControllerException(controller, e);
    } finally {
      if (shouldCommit) getHBase().commit();
      else getHBase().rollback();
    }
    done.run(response);
  }

  @Override
  public void heartbeat(RpcController controller,
                        HbaseMetastoreProto.HeartbeatTxnRangeRequest request,
                        RpcCallback<HbaseMetastoreProto.HeartbeatTxnRangeResponse> done) {
    long now = System.currentTimeMillis();
    HbaseMetastoreProto.HeartbeatTxnRangeResponse.Builder builder =
        HbaseMetastoreProto.HeartbeatTxnRangeResponse.newBuilder();

    if (LOG.isDebugEnabled()) {
      LOG.debug("Heartbeating transactions from " + request.getMinTxn() + " to " +
          request.getMaxTxn());
    }

    try (LockKeeper lk = new LockKeeper(masterLock.readLock())) {
      for (long txnId = request.getMinTxn(); txnId <= request.getMaxTxn(); txnId++) {
        OpenHiveTransaction txn = openTxns.get(txnId);
        if (txn != null) {
          txn.setLastHeartbeat(now);
          // Don't set the value in HBase, we don't track it there for efficiency
        } else {
          AbortedHiveTransaction abortedTxn = abortedTxns.get(txnId);
          if (abortedTxn == null) builder.addNoSuch(txnId);
          else builder.addAborted(txnId);
        }
      }
    } catch (IOException e) {
      ResponseConverter.setControllerException(controller, e);
    }
    done.run(builder.build());
  }

  @Override
  public void lock(RpcController controller, HbaseMetastoreProto.LockRequest request,
                   RpcCallback<HbaseMetastoreProto.LockResponse> done) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Requesting locks for transaction " + request.getTxnId());
      for (HbaseMetastoreProto.LockComponent component : request.getComponentsList()) {
        LOG.debug("entity: " + component.getDb() +
            (component.hasTable() ? component.getTable() : "") +
            (component.hasPartition() ? component.getPartition() : ""));
      }
    }
    HbaseMetastoreProto.LockResponse response = null;
    List<HbaseMetastoreProto.LockComponent> components = request.getComponentsList();
    HiveLock[] hiveLocks = new HiveLock[components.size()];
    OpenHiveTransaction txn;
    try {
      try (LockKeeper lk = new LockKeeper(masterLock.writeLock())) {
        txn = openTxns.get(request.getTxnId());
        if (txn == null) {
          LOG.info("Asked to get locks for non-open transaction " + request.getTxnId());
          done.run(HbaseMetastoreProto.LockResponse.newBuilder()
                 .setState(HbaseMetastoreProto.LockState.TXN_ABORTED)
                 .build());
          return;
        }
        if (txn.getState() != HbaseMetastoreProto.TxnState.OPEN) {
          suicide("Non-open txn in open list");
        }

        List<HbaseMetastoreProto.Transaction.Lock> lockBuilders = new ArrayList<>();
        for (int i = 0; i < components.size(); i++) {
          HbaseMetastoreProto.LockComponent component = components.get(i);
          long lockId = nextLockId++;
          hiveLocks[i] = new HiveLock(lockId, request.getTxnId(), component.getType(),
              findOrCreateLockQueue(component.getDb(), component.getTable(),
                  component.getPartition()).getFirst());
          // Build the hbase lock so we have it later when we need to record the locks in HBase
          HbaseMetastoreProto.Transaction.Lock.Builder builder =
              HbaseMetastoreProto.Transaction.Lock
              .newBuilder()
              .setId(lockId)
              .setState(HbaseMetastoreProto.LockState.WAITING)
              .setType(component.getType())
              .setDb(component.getDb());
          if (component.hasTable()) builder.setTable(component.getTable());
          if (component.hasPartition()) builder.setPartition(component.getPartition());
          lockBuilders.add(builder.build());

          // Add to the appropriate DTP queue
          lockQueues.get(hiveLocks[i].getEntityLocked()).queue.put(hiveLocks[i].getId(), hiveLocks[i]);
          lockQueuesToCheck.add(hiveLocks[i].getEntityLocked());
        }
        lockQueuesToCheck.notify();
        txn.addLocks(hiveLocks);

        // Record changes in HBase
        long lockVal =
              getHBase().addToSequence(HBaseReadWrite.LOCK_SEQUENCE, request.getComponentsCount());
        assert lockVal == nextLockId;

        HbaseMetastoreProto.Transaction hbaseTxn = getHBase().getTransaction(request.getTxnId());
        HbaseMetastoreProto.Transaction.Builder txnBuilder =
            HbaseMetastoreProto.Transaction.newBuilder(hbaseTxn);
        txnBuilder.addAllLocks(lockBuilders);
        boolean shouldCommit = false;
        getHBase().begin();
        try {
          getHBase().putTransaction(txnBuilder.build());
          shouldCommit = true;
        } finally {
          if (shouldCommit) getHBase().commit();
          else getHBase().rollback();
        }
      }
      // Poll on the state of all the locks, return once they've all gone acquired.  But don't wait
      // more than a few seconds, as this is tying down a thread in HBase.  After that they can call
      // check locks to determine if their locks have acquired.
      response = pollForLocks(hiveLocks);
    } catch (IOException e) {
      ResponseConverter.setControllerException(controller, e);
    }
    done.run(response);
  }

  // This checks that all locks in the transaction are acquired.  It will hold for a few seconds
  // to see if it can acquire them all.
  @Override
  public void checkLocks(RpcController controller, HbaseMetastoreProto.TransactionId request,
                         RpcCallback<HbaseMetastoreProto.LockResponse> done) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Checking locks for transaction " + request.getId());
    }
    HbaseMetastoreProto.LockResponse response = null;
    try {
      OpenHiveTransaction txn;
      try (LockKeeper lk = new LockKeeper(masterLock.readLock())) {
        txn = openTxns.get(request.getId());
      }
      if (txn == null) {
        LOG.info("Attempt to check locks for non-open transaction " + request.getId());
        response = HbaseMetastoreProto.LockResponse.newBuilder()
            .setState(HbaseMetastoreProto.LockState.TXN_ABORTED)
            .build();
      } else {
        response = pollForLocks(txn.getHiveLocks());
      }
    } catch (IOException e) {
      ResponseConverter.setControllerException(controller, e);
    }
    done.run(response);
  }

  private HbaseMetastoreProto.LockResponse pollForLocks(HiveLock[] hiveLocks)
      throws IOException {
    LOG.debug("Polling for locks");
    long waitUntil = System.currentTimeMillis() + lockPollTimeout;
    try {
      long now = System.currentTimeMillis();
      while (now < waitUntil) {
        boolean sawWaiting = false;
        try (LockKeeper lk = new LockKeeper(masterLock.readLock())) {
          for (HiveLock lock : hiveLocks) {
            if (lock.getState() == HbaseMetastoreProto.LockState.WAITING) {
              sawWaiting = true;
              break;
            }
            if (lock.getState() != HbaseMetastoreProto.LockState.ACQUIRED) {
              suicide("Lock not in waiting or acquired state, not sure what to do");
            }
          }
        }
        if (!sawWaiting) {
          LOG.debug("All locks acquired, returning ACQUIRED");
          return HbaseMetastoreProto.LockResponse.newBuilder()
              .setState(HbaseMetastoreProto.LockState.ACQUIRED)
              .build();
        }
        now = System.currentTimeMillis();
        // Don't wait long.  This way if we missed a previous notify from the lock checker we'll
        // still check occasionally to see if our locks are ready.
        LOG.debug("Waiting on the lockChecker");
        if (now < waitUntil) lockChecker.wait(lockPollTimeout / 10);
        LOG.debug("Done waiting on the lockChecker");
      }

      // If we got here it means we waited too long.
      LOG.debug("Lost patience, returning WAITING");
      return HbaseMetastoreProto.LockResponse.newBuilder()
          .setState(HbaseMetastoreProto.LockState.WAITING)
          .build();

    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }

  private boolean twoLocksCompatible(HbaseMetastoreProto.LockType holder,
                                     HbaseMetastoreProto.LockType requester) {
    if (lockCompatibilityTable == null) {
      // TODO not at all sure I have intention locks correct in this table
      lockCompatibilityTable = new boolean[HbaseMetastoreProto.LockType.values().length][HbaseMetastoreProto.LockType.values().length];
      Arrays.fill(lockCompatibilityTable[HbaseMetastoreProto.LockType.EXCLUSIVE_VALUE], false);
      lockCompatibilityTable[HbaseMetastoreProto.LockType.SHARED_WRITE_VALUE][HbaseMetastoreProto.LockType.EXCLUSIVE_VALUE] = false;
      lockCompatibilityTable[HbaseMetastoreProto.LockType.SHARED_WRITE_VALUE][HbaseMetastoreProto.LockType.SHARED_WRITE_VALUE] = false;
      lockCompatibilityTable[HbaseMetastoreProto.LockType.SHARED_WRITE_VALUE][HbaseMetastoreProto.LockType.SHARED_READ_VALUE] = true;
      lockCompatibilityTable[HbaseMetastoreProto.LockType.SHARED_WRITE_VALUE][HbaseMetastoreProto.LockType.INTENTION_VALUE] = true;
      lockCompatibilityTable[HbaseMetastoreProto.LockType.SHARED_READ_VALUE][HbaseMetastoreProto.LockType.EXCLUSIVE_VALUE] = false;
      lockCompatibilityTable[HbaseMetastoreProto.LockType.SHARED_READ_VALUE][HbaseMetastoreProto.LockType.SHARED_WRITE_VALUE] = true;
      lockCompatibilityTable[HbaseMetastoreProto.LockType.SHARED_READ_VALUE][HbaseMetastoreProto.LockType.SHARED_READ_VALUE] = true;
      lockCompatibilityTable[HbaseMetastoreProto.LockType.SHARED_READ_VALUE][HbaseMetastoreProto.LockType.INTENTION_VALUE] = true;
      lockCompatibilityTable[HbaseMetastoreProto.LockType.INTENTION_VALUE][HbaseMetastoreProto.LockType.EXCLUSIVE_VALUE] = false;
      lockCompatibilityTable[HbaseMetastoreProto.LockType.INTENTION_VALUE][HbaseMetastoreProto.LockType.SHARED_WRITE_VALUE] = true;
      lockCompatibilityTable[HbaseMetastoreProto.LockType.INTENTION_VALUE][HbaseMetastoreProto.LockType.SHARED_READ_VALUE] = true;
      lockCompatibilityTable[HbaseMetastoreProto.LockType.INTENTION_VALUE][HbaseMetastoreProto.LockType.INTENTION_VALUE] = true;
    }
    return lockCompatibilityTable[holder.getNumber()][requester.getNumber()];
  }

  // add dynamic partitions
  @Override
  public void addDynamicPartitions(RpcController controller,
                                   HbaseMetastoreProto.AddDynamicPartitionsRequest request,
                                   RpcCallback<HbaseMetastoreProto.TransactionResult> done) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Adding dynamic partitions for transaction " + request.getTxnId() + " table " +
          request.getDb() + "." + request.getTable());
      for (String part : request.getPartitionsList()) {
        LOG.debug("Partition: " + part);
      }
    }
    HbaseMetastoreProto.TransactionResult response = null;
    boolean shouldCommit = false;
    getHBase().begin();
    // This needs to acquire the write lock because it might add entries to dtps, which is
    // unfortunate.
    try (LockKeeper lk = new LockKeeper(masterLock.writeLock())) {
      // Add the locks to the appropriate transaction so that we know what things to compact and
      // so we know what partitions were touched by this change.  Don't put the locks in the dtps
      // because we're actually covered by the table lock.  Do increment the counters in the dtps.
      OpenHiveTransaction txn = openTxns.get(request.getTxnId());
      if (txn == null) {
        LOG.info("Attempt to add dynamic partitions to non-open transaction " + request.getTxnId());
        response = HbaseMetastoreProto.TransactionResult.newBuilder()
            .setState(HbaseMetastoreProto.TxnStateChangeResult.NO_SUCH_TXN)
            .build();
        done.run(response);
        return;
      }
      if (txn.getState() != HbaseMetastoreProto.TxnState.OPEN) {
        suicide("Attempt to add dynamic partitions to aborted or committed txn");
      }

      HbaseMetastoreProto.Transaction hbaseTxn = getHBase().getTransaction(request.getTxnId());
      HbaseMetastoreProto.Transaction.Builder txnBuilder =
          HbaseMetastoreProto.Transaction.newBuilder(hbaseTxn);

      List<String> partitionNames = request.getPartitionsList();
      HiveLock[] partitionsWrittenTo = new HiveLock[partitionNames.size()];
      List<HBaseReadWrite.PotentialCompactionEntity> pces = new ArrayList<>();
      for (int i = 0; i < partitionNames.size(); i++) {
        partitionsWrittenTo[i] = new HiveLock(-1, request.getTxnId(),
            HbaseMetastoreProto.LockType.SHARED_WRITE,
            findOrCreateLockQueue(request.getDb(), request.getTable(), partitionNames.get(i)).getFirst());
        // Set the lock in released state so it doesn't get put in the DTP queue on recovery
        partitionsWrittenTo[i].setState(HbaseMetastoreProto.LockState.RELEASED);

        txnBuilder.addLocks(HbaseMetastoreProto.Transaction.Lock.newBuilder()
            .setId(-1)
            .setType(HbaseMetastoreProto.LockType.SHARED_WRITE)
            .setState(HbaseMetastoreProto.LockState.RELEASED)
            .setDb(request.getDb())
            .setTable(request.getTable())
            .setPartition(partitionNames.get(i)));
        if (LOG.isDebugEnabled()) {
          LOG.debug("Adding potential compaction " + request.getDb() + "." + request.getTable() +
              "." + partitionNames.get(i));
        }
        pces.add(new HBaseReadWrite.PotentialCompactionEntity(request.getDb(), request.getTable(),
            partitionNames.get(i)));
      }
      txn.addLocks(partitionsWrittenTo);


      getHBase().putTransaction(txnBuilder.build());
      getHBase().putPotentialCompactions(txn.getId(), pces);
      shouldCommit = true;
      response = HbaseMetastoreProto.TransactionResult.newBuilder()
          .setState(HbaseMetastoreProto.TxnStateChangeResult.SUCCESS)
          .build();
    } catch (IOException e) {
      ResponseConverter.setControllerException(controller, e);
    } finally {
      if (shouldCommit) getHBase().commit();
      else getHBase().rollback();
    }
    done.run(response);
  }

  @Override
  public void cleanupAfterCompaction(RpcController controller,
                                     HbaseMetastoreProto.Compaction request,
                                     RpcCallback<HbaseMetastoreProto.Void> done) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Cleaning up after compaction of " + request.getDb() + "." + request.getTable() +
          (request.hasPartition() ? "." + request.getPartition() : ""));
    }
    boolean shouldCommit = false;
    getHBase().begin();
    try {
      // First, go find the potential compaction that matches this compaction.  That has the map
      // to which transactions we now need to modify or forget.
      HbaseMetastoreProto.PotentialCompaction potential =
          getHBase().getPotentialCompaction(request.getDb(), request.getTable(),
              request.hasPartition() ? request.getPartition() : null);
      // It is possible for the potential to be null, as the user could have requested a compaction
      // when there was nothing really to do.
      if (potential != null) {
        LOG.debug("Found appropriate potential compaction");
        List<Long> compactedTxns = new ArrayList<>(); // All txns that were compacted
        // Txns in the potential that weren't compacted as part of this compaction.  We need to
        // track this so we can properly write the potential back.
        List<Long> uncompactedTxns = new ArrayList<>();
        for (long txnId : potential.getTxnIdsList()) {
          // We only need to worry about this transaction if it's writes were compacted (that
          // is, it's <= the highestTxnId)
          if (txnId <= request.getHighestTxnId()) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("Adding " + txnId + " to list of transactions that have been compacted");
            }
            compactedTxns.add(txnId);
          } else {
            if (LOG.isDebugEnabled()) {
              LOG.debug("Adding " + txnId + " to list of transactions there were not compacted");
            }
            uncompactedTxns.add(txnId);
          }
        }
        if (compactedTxns.size() > 0) {
          // Transactions that have had all the tables/partitions they touched compacted.
          List<Long> fullyCompacted = new ArrayList<>();
          // Transactions that have had locks marked as compacted but are not themselves fully
          // compacted yet.
          Map<Long, HiveLock> modifiedTxns = new HashMap<>();
          // We need the write lock because we might remove entries from abortedTxns
          try (LockKeeper lk = new LockKeeper(masterLock.writeLock())) {
            for (long txnId : compactedTxns) {
              AbortedHiveTransaction txn = abortedTxns.get(txnId);
              // The transaction could be null if this was a completed transaction and not an
              // aborted one.
              if (txn != null) {
                // Mark the lock on that partition in this aborted txn as compacted.
                HiveLock compactedLock = txn.compactLock(new EntityKey(request.getDb(),
                    request.getTable(), request.hasPartition() ? request.getPartition() : null));
                if (LOG.isDebugEnabled()) {
                  LOG.debug("Found aborted txn " + txn.getId() + " that was in the compacted list"
                          + " removing lock for " + compactedLock.getEntityLocked().toString());
                }
                if (txn.fullyCompacted()) {
                  LOG.debug("Transaction is now fully compacted");
                  fullyCompacted.add(txnId);
                } else {
                  LOG.debug("Transaction has remaining uncompacted locks");
                  modifiedTxns.put(txnId, compactedLock);
                }
              }
            }

            if (fullyCompacted.size() > 0) {
              for (long txnId : fullyCompacted) {
                if (LOG.isDebugEnabled()) {
                  LOG.debug("Forgeting fully compacted aborted transaction " + txnId);
                }
                abortedTxns.remove(txnId);
              }
              getHBase().deleteTransactions(fullyCompacted);
            }
            if (modifiedTxns.size() > 0) {
              List<HbaseMetastoreProto.Transaction> toModify =
                  getHBase().getTransactions(modifiedTxns.keySet());
              // New transaction objects we're writing back to HBase
              List<HbaseMetastoreProto.Transaction> toWrite = new ArrayList<>(modifiedTxns.size());
              for (HbaseMetastoreProto.Transaction hbaseTxn : toModify) {
                if (LOG.isDebugEnabled()) {
                  LOG.debug("Writing back modified aborted transaction " + hbaseTxn.getId());
                }
                HiveLock compactedLock =  modifiedTxns.get(hbaseTxn.getId());
                HbaseMetastoreProto.Transaction.Builder txnBldr =
                    HbaseMetastoreProto.Transaction.newBuilder(hbaseTxn);
                List<HbaseMetastoreProto.Transaction.Lock> hbaseLocks = hbaseTxn.getLocksList();
                txnBldr.clearLocks();
                for (HbaseMetastoreProto.Transaction.Lock hbaseLock : hbaseLocks) {
                  if (hbaseLock.getId() != compactedLock.getId()) {
                    txnBldr.addLocks(hbaseLock);
                  }
                }
                toWrite.add(txnBldr.build());
              }
              getHBase().putTransactions(toWrite);
            }
            // rewrite potential compaction
            if (uncompactedTxns.size() > 0) {
              LOG.debug("Rewriting potential compaction minus compacted transactions");
              // Rewrite the potential compaction to remove txns that have been compacted out
              HbaseMetastoreProto.PotentialCompaction.Builder potentialBldr =
                  HbaseMetastoreProto.PotentialCompaction.newBuilder(potential);
              potentialBldr.clearTxnIds();
              potentialBldr.addAllTxnIds(uncompactedTxns);
              getHBase().putPotentialCompaction(potentialBldr.build());
            } else {
              LOG.debug("Forgetting potential compaction as all transactions compacted");
              // This was the last transaction in this potential compaction, so remove it
              getHBase().deletePotentialCompaction(potential.getDb(), potential.getTable(),
                  potential.hasPartition() ? potential.getPartition() : null);
            }
            shouldCommit = true;
          }
        }
      } else {
        LOG.info("Unable to find requested potential compaction " + request.getDb() + "." +
            request.getTable() + (request.hasPartition() ? request.getPartition() : ""));
      }
    } catch (IOException e) {
      ResponseConverter.setControllerException(controller, e);
    } finally {
      if (shouldCommit) getHBase().commit();
      else getHBase().rollback();
    }
    done.run(HbaseMetastoreProto.Void.getDefaultInstance());
  }

  @Override
  public void verifyCompactionCanBeCleaned(RpcController controller,
                                           HbaseMetastoreProto.CompactionList request,
                                           RpcCallback<HbaseMetastoreProto.CompactionList> done) {
    // TODO - I may be double doing this.  Can I ever have a highestCompactionId higher than any
    // open transactions?
    // This takes a set of compactions that have been compacted and are ready to be cleaned.  It
    // returns only ones that have highestCompactionId > min(openTxns).  Any for which this is not
    // true, there may still be open transactions referencing the files that were compacted, thus
    // they should not yet be cleaned.
    if (LOG.isDebugEnabled()) {
      LOG.debug("Received list of compactions cleans to be verified");
      for (HbaseMetastoreProto.Compaction compaction : request.getCompactionsList()) {
        LOG.debug("Compaction id " + compaction.getId() + " for " + compaction.getDb() + "." +
            compaction.getTable() + (compaction.hasPartition() ? "." + compaction.getPartition()
            : ""));
      }
    }
    long minOpenTxn;
    HbaseMetastoreProto.CompactionList.Builder builder =
        HbaseMetastoreProto.CompactionList.newBuilder();
    // We only need the read lock to look at the open transaction list
    try (LockKeeper lk = new LockKeeper(masterLock.readLock())) {
      minOpenTxn = findMinOpenTxn();
    } catch (IOException e) {
      ResponseConverter.setControllerException(controller, e);
      done.run(builder.build());
      return;
    }

    for (HbaseMetastoreProto.Compaction compaction : request.getCompactionsList()) {
      if (compaction.getHighestTxnId() < minOpenTxn) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Approving cleaning id " + compaction.getId() + " " + compaction.getDb() + "." +
              compaction.getTable() + (compaction.hasPartition() ? "." + compaction.getPartition()
              : ""));
        }
        builder.addCompactions(compaction);
      }
    }
    done.run(builder.build());
  }

  /**
   * Find the appropriate lock queue given entity information.  If an appropriate entry is not in
   * the lockQueues it will be added.  This method does not acquire any locks, but you should *
   * assure that you are inside the write lock before calling it, or you're in recover and thus *
   * don't need any locks.
   * @param hbaseLock hbase lock
   * @return object pair, first element is EntityKey, second is LockQueue.
   * @throws IOException
   */
  ObjectPair<EntityKey, LockQueue> findOrCreateLockQueue(HbaseMetastoreProto.Transaction.Lock hbaseLock)
      throws IOException {
    return findOrCreateLockQueue(hbaseLock.getDb(),
        hbaseLock.hasTable() ? hbaseLock.getTable() : null,
        hbaseLock.hasPartition() ? hbaseLock.getPartition() : null);
  }

  /**
   * Find the appropriate lock queue given entity information.  If an appropriate entry is not in
   * the lockQueues it will be added.  This method does not acquire any locks, but you should
   * assure that you are inside the write lock before calling it, or you're in recover and thus
   * don't need any locks.
   * @param db database
   * @param table table name (may be null)
   * @param part partition name (may be null)
   * @return object pair, first element is EntityKey, second is LockQueue.
   * @throws IOException
   */
  ObjectPair<EntityKey, LockQueue> findOrCreateLockQueue(String db, String table, String part)
      throws IOException {
    EntityKey key = new EntityKey(db, table, part);
    LockQueue queue = lockQueues.get(key);
    if (queue == null) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Creating new LockQueue for " + key.toString());
      }
      queue = new LockQueue();
      lockQueues.put(key, queue);
    }
    return new ObjectPair<>(key, queue);
  }

  // This method assumes you are holding the read lock.
  private long findMinOpenTxn() {
    long minOpenTxn = Long.MAX_VALUE;
    for (Long txnId : openTxns.keySet()) {
      minOpenTxn = Math.min(txnId, minOpenTxn);
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Found minimum open transaction of " + minOpenTxn);
    }
    return minOpenTxn;
  }

  private void suicide(String logMsg) {
    LOG.error("Received suicide request: " + logMsg);
    throw new RuntimeException("Killing self due to " + logMsg);
    // TODO - figure out how to kill this instance of the co-processor without taking down the
    // region server so we force a recovery
  }

  private void checkFull() throws IOException {
    // Check to see if the transaction manager is full.  If it isn't currently set as full but
    // has too many objects this will switch the full flag on.  If the full flag is on and we've
    // fallen below 90% this will turn it off.
    int objectCnt = countObjects();
    if (full) {
      if (objectCnt < maxObjects * 0.9) {
        LOG.info("Transaction manager has drained, switching off full flag");
        full = false;
      } else {
        LOG.debug("Still full...");
      }
    } else {
      if (objectCnt > maxObjects) {
        // Try to shrink the lock queues and then check again
        tryToShrinkLockQueues();
        if (countObjects() > maxObjects) {
          LOG.error("Transation manager object count exceeds configured max object count " +
              maxObjects + ", marking full and no longer accepting new transactions some current " +
              "objects have drained off");
          full = true;
        }
      } else {
        LOG.debug("Still not full...");
      }
    }
  }

  private int countObjects() throws IOException {

    try (LockKeeper lk = new LockKeeper(masterLock.readLock())) {
      // First count all the transactions, this is relatively easy.
      int objectCnt = committedTxns.size() + abortedTxns.size() + openTxns.size() +
          lockQueues.keySet().size();
      // Early out if this alone fills us up, as the next part gets harder.
      if (objectCnt > maxObjects) return objectCnt;

      for (LockQueue queue : lockQueues.values()) {
        objectCnt += queue.queue.size();
        if (objectCnt > maxObjects) return objectCnt;
      }
      return objectCnt;
    }
  }

  private void tryToShrinkLockQueues() {
    LOG.debug("Seeing if we can shrink the lock queue");
    List<EntityKey> empties = new ArrayList<>();
    // First get the read lock and find all currently empty keys.  Then release the read
    // lock, get the write lock and remove all those keys, checking again that they are
    // truly empty.
    try (LockKeeper lk = new LockKeeper(masterLock.readLock())) {
      for (Map.Entry<EntityKey, LockQueue> entry : lockQueues.entrySet()) {
        if (entry.getValue().queue.size() == 0 && entry.getValue().maxCommitId == 0) {
          empties.add(entry.getKey());
        }
      }
    } catch (IOException e) {
      LOG.warn("Caught exception while examining lock queues", e);
    }

    if (empties.size() > 0) {
      try (LockKeeper lk = new LockKeeper(masterLock.writeLock())) {
        for (EntityKey key : empties) {
          LockQueue queue = lockQueues.get(key);
          if (queue.queue.size() == 0 && queue.maxCommitId == 0) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("Removing lockQueue " + key.toString());
            }
            lockQueues.remove(key);
          }
        }
      } catch (IOException e) {
        LOG.warn("Caught exception while removing empty lock queue", e);
      }
    } else {
      LOG.debug("No lock queues found to remove");
    }
  }

  private void recover() throws IOException {
    // No locking is required here because we're still in the constructor and we're guaranteed no
    // one else is muddying the waters.
    // Get existing transactions from HBase
    LOG.info("Beginning recovery");
    try {
      List<HbaseMetastoreProto.Transaction> hbaseTxns = getHBase().scanTransactions();
      if (hbaseTxns != null) {
        for (HbaseMetastoreProto.Transaction hbaseTxn : hbaseTxns) {
          switch (hbaseTxn.getTxnState()) {
          case ABORTED:
            if (LOG.isDebugEnabled()) {
              LOG.debug("Found aborted transaction " + hbaseTxn.getId());
            }
            AbortedHiveTransaction abortedTxn = new AbortedHiveTransaction(hbaseTxn, this);
            abortedTxns.put(abortedTxn.getId(), abortedTxn);
            break;

          case OPEN:
            if (LOG.isDebugEnabled()) {
              LOG.debug("Found open transaction " + hbaseTxn.getId());
            }
            OpenHiveTransaction openTxn = new OpenHiveTransaction(hbaseTxn, this);
            openTxns.put(openTxn.getId(), openTxn);
            break;

          case COMMITTED:
            if (LOG.isDebugEnabled()) {
              LOG.debug("Found committed transaction " + hbaseTxn.getId());
            }
            for (HbaseMetastoreProto.Transaction.Lock hbaseLock : hbaseTxn.getLocksList()) {
              LockQueue queue = findOrCreateLockQueue(hbaseLock.getDb(), hbaseLock.getTable(),
                  hbaseLock.hasPartition() ? hbaseLock.getPartition() : null).getSecond();
              queue.maxCommitId = Math.max(queue.maxCommitId, hbaseTxn.getCommitId());
            }
            committedTxns.add(new CommittedHiveTransaction(hbaseTxn));
            break;
          }
        }
      }
      nextTxnId = getHBase().readCurrentSequence(HBaseReadWrite.TXN_SEQUENCE);
      nextLockId = getHBase().readCurrentSequence(HBaseReadWrite.LOCK_SEQUENCE);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Set nextTxnId to " + nextTxnId + " and nextLockId to " + nextLockId);
      }
      checkFull();
    } finally {
      // It's a read only operation, so always commit
      getHBase().commit();
    }
    LOG.info("Completed recovery");
  }

  private HBaseReadWrite getHBase() {
    if (hbase == null) {
      HBaseReadWrite.setConf(conf);
      hbase = HBaseReadWrite.getInstance();
    }
    return hbase;
  }

  public static class LockKeeper implements Closeable {
    final Lock lock;

    public LockKeeper(Lock lock) {
      this.lock = lock;
      if (LOG.isDebugEnabled()) {
        LOG.debug("Waiting for Java lock " + lock.getClass().getName());
      }
      this.lock.lock();
      if (LOG.isDebugEnabled()) {
        LOG.debug("Acquired Java lock " + lock.getClass().getName());
      }
    }

    @Override
    public void close() throws IOException {
      lock.unlock();
      if (LOG.isDebugEnabled()) {
        LOG.debug("Released Java lock " + lock.getClass().getName());
      }
    }
  }

  static class EntityKey {
    final String db;
    final String table;
    final String part;

    private EntityKey(String db, String table, String part) {
      this.db = db;
      this.table = table;
      this.part = part;
    }

    @Override
    public int hashCode() {
      // db should never be null
      int hashCode = db.hashCode();
      if (table != null) hashCode = hashCode * 31 + table.hashCode();
      if (part != null) hashCode = hashCode * 31 + part.hashCode();
      return hashCode;
    }

    @Override
    public boolean equals(Object obj) {
      if (!(obj instanceof EntityKey)) return false;
      EntityKey other = (EntityKey)obj;
      // db should never be null
      if (db.equals(other.db)) {
        if (table == null && other.table == null ||
            table != null && table.equals(other.table)) {
          if (part == null && other.part == null ||
              part != null && part.equals(other.part)) {
            return true;
          }
        }
      }
      return false;
    }

    @Override
    public String toString() {
      StringBuilder bldr = new StringBuilder(db);

      if (table != null) {
        bldr.append('.')
            .append(table);
        if (part != null) {
          bldr.append('.')
              .append(part);
        }
      }
      return bldr.toString();
    }
  }

  static class LockQueue {
    final SortedMap<Long, HiveLock> queue;
    long maxCommitId; // commit id of highest transaction that wrote to this DTP

    private LockQueue() {
      queue = new TreeMap<>();
      maxCommitId = 0;
    }
  }

  interface NamedRunnable extends Runnable {
    String getName();
  }

  private NamedRunnable timedOutCleaner = new NamedRunnable() {
    @Override
    public String getName() {
      return "Timeout Cleaner";
    }

    @Override
    public void run() {
      LOG.info("Beginning timeout cleaner");
      // Set initial sleep time long so we have time after recovery for things to heartbeat.
      long sleepTime = HiveConf.getTimeVar(conf,
          HiveConf.ConfVars.METASTORE_HBASE_TIMEOUT_CLEANER_INITIAL_WAIT, TimeUnit.MILLISECONDS);
      long timeout =
          HiveConf.getTimeVar(conf, HiveConf.ConfVars.HIVE_TXN_TIMEOUT, TimeUnit.MILLISECONDS);
      long sleepInterval = HiveConf.getTimeVar(conf,
          HiveConf.ConfVars.METASTORE_HBASE_TIMEOUT_CLEANER_FREQUENCY, TimeUnit.MILLISECONDS);
      while (go) {
        // Sleep first, so we don't kill things right after recovery, give them a chance to
        // heartbeat if we've been out of it for a few seconds.
        try {
          Thread.sleep(sleepTime);
        } catch (InterruptedException e) {
          // Go back through the while loop, as this might mean it's time to quit
          continue;
        }
        LOG.debug("Looking for timed out transactions");
        sleepTime = sleepInterval;
        // First get the read lock and find all of the potential timeouts.
        List<OpenHiveTransaction> potentials = new ArrayList<>();
        long now = System.currentTimeMillis();
        try (LockKeeper lk = new LockKeeper(masterLock.readLock())) {
          for (OpenHiveTransaction txn : openTxns.values()) {
            if (txn.getLastHeartbeat() + timeout < now) {
              if (LOG.isDebugEnabled()) {
                LOG.debug("Adding " + txn.getId() + " to list of potential timeouts");
              }
              potentials.add(txn);
            }
          }
        } catch (IOException e) {
          LOG.warn("Caught exception in timeOutCleaner, most likely during locking, not much we " +
              "can do about it", e);
        }

        // Now go back through the potentials list, holding the write lock, and remove any that
        // still haven't heartbeat
        try (LockKeeper lk = new LockKeeper(masterLock.writeLock())) {
          for (OpenHiveTransaction txn : potentials) {
            if (txn.getLastHeartbeat() + timeout < now) {
              LOG.info("Aborting transaction " + txn.getId() + " due to heartbeat timeout");
              abortTxn(txn);
            }
          }
        } catch (IOException e) {
          LOG.warn("Caught exception aborting transaction", e);
        }
      }
      LOG.info("Received stop signal, timeout cleaner finishing");
    }
  };

  private NamedRunnable deadlockDetector = new NamedRunnable() {
    @Override
    public String getName() {
      return "Deadlock detector";
    }

    // Rather than follow the general pattern of go through all the entries and find potentials
    // and then remove all potentials this thread kills a deadlock as soon as it sees it and then
    // starts looking all over again.  Otherwise we'd likely be too aggressive and kill all
    // participants in the deadlock.
    @Override
    public void run() {
      LOG.info("Beginning deadlock detector");
      long sleepInterval = HiveConf.getTimeVar(conf,
          HiveConf.ConfVars.METASTORE_HBASE_DEADLOCK_DETECTOR_FREQUENCY, TimeUnit.MILLISECONDS);
      long sleepTime = sleepInterval;
      while (go) {
        try {
          Thread.sleep(sleepTime);
        } catch (InterruptedException e) {
          // Go back through the while loop, as this might mean it's time to quit
          continue;
        }
        sleepTime = sleepInterval;
        LOG.debug("Looking for deadlocks");
        try (LockKeeper lk = new LockKeeper(masterLock.readLock())) {
          // We're looking only for transactions that have 1+ acquired locks and 1+ waiting locks
          for (OpenHiveTransaction txn : openTxns.values()) {
            boolean sawAcquired = false, sawWaiting = false;
            for (HiveLock lock : txn.getHiveLocks()) {
              if (lock.getState() == HbaseMetastoreProto.LockState.WAITING) {
                sawWaiting = true;
              } else if (lock.getState() == HbaseMetastoreProto.LockState.ACQUIRED) {
                sawAcquired = true;
              }
            }
            // Only check if we have both acquired and waiting locks.  A transaction might be in
            // a cycle without that, but it won't be key to the cycle without it.
            if (sawAcquired && sawWaiting) {
              if (lookForDeadlock(txn.getId(), txn, true)) {
                LOG.warn("Detected deadlock, aborting transaction " + txn.getId() +
                    " to resolve it");
                // It's easiest to always kill this one rather than try to figure out where in
                // the graph we can remove something and break the cycle.  Given that which txn
                // we examine first is mostly random this should be ok (I hope).
                // Must unlock the read lock before acquiring the write lock
                masterLock.readLock().unlock();
                try (LockKeeper lk1 = new LockKeeper(masterLock.writeLock())) {
                  abortTxn(openTxns.get(txn.getId()));
                }
                // We've released the readlock and messed with the data structures we're
                // traversing, so don't keep going.  Instead set our sleep time very low so we
                // run again soon.
                sleepTime = 10;
                break;
              }
            }
          }
        } catch (IOException e) {
          LOG.warn("Received exception in deadlock detector", e);
        }
      }
      LOG.info("deadlock detector finishing");
    }

    /**
     * This looks for cycles in the lock graph.  It remembers the first transaction we started
     * looking at, and if it gets back that transaction then it returns true.
     * @param initialTxnId Starting transaction in the graph traversal.
     * @param currentTxn Current transaction in the graph traversal.
     * @param initial Whether this call is the first time this is called, so initialTxn should
     *                equals currentTxn
     * @return true if a cycle is detected.
     */
    private boolean lookForDeadlock(long initialTxnId, OpenHiveTransaction currentTxn,
                                    boolean initial) {
      if (!initial && initialTxnId == currentTxn.getId()) return true;
      for (HiveLock lock : currentTxn.getHiveLocks()) {
        if (lock.getState() == HbaseMetastoreProto.LockState.WAITING &&
            lookForDeadlock(initialTxnId, openTxns.get(lock.getTxnId()), false)) {
          return true;
        }
      }
      return false;
    }
  };

  private NamedRunnable lockQueueShrinker = new NamedRunnable() {
    @Override
    public String getName() {
      return "Lock Queue Shrinker";
    }

    @Override
    public void run() {
      LOG.info("Beginning lock queue shrinker");
      long sleepTime = HiveConf.getTimeVar(conf,
          HiveConf.ConfVars.METASTORE_HBASE_LOCK_QUEUE_SHRINKER_FREQUENCY, TimeUnit.MILLISECONDS);
      while (go) {
        try {
          Thread.sleep(sleepTime);
        } catch (InterruptedException e) {
          // Go back through the while loop, as this might mean it's time to quit
          continue;
        }
        tryToShrinkLockQueues();
      }
      LOG.info("lock queue shrinker finishing");
    }
  };

  private NamedRunnable fullChecker = new NamedRunnable() {
    @Override
    public String getName() {
      return "Full Checker";
    }

    @Override
    public void run() {
      LOG.info("Beginning full checker");
      long sleepTime = HiveConf.getTimeVar(conf,
          HiveConf.ConfVars.METASTORE_HBASE_FULL_CHECKER_FREQUENCY, TimeUnit.MILLISECONDS);
      while (go) {
        try {
          // Sleep less if we're full, as we want to see if we can open up again as soon as possible
          Thread.sleep(full ? 100 : sleepTime);
          checkFull();
        } catch (InterruptedException e) {
          // Go back through the while loop, as this might mean it's time to quit
          continue;
        } catch (IOException e) {
          LOG.warn("Caught exception while checking to see if transaction manager is full");
        }
      }
      LOG.info("full checker finishing");
    }
  };

  private NamedRunnable committedTxnCleaner = new NamedRunnable() {
    @Override
    public String getName() {
      return "Committed Transaction Cleaner";
    }

    // This looks through the list of committed transactions and figures out what can be
    // forgotten.
    @Override
    public void run() {
      while (go) {
        long sleepTime = HiveConf.getTimeVar(conf,
            HiveConf.ConfVars.METASTORE_HBASE_COMMITTED_TXN_CLEANER_FREQUENCY, TimeUnit.MILLISECONDS);
        LOG.info("Beginning committed transaction cleaner");
        try {
          Thread.sleep(sleepTime);
          LOG.debug("Cleaning committed transactions");
          Set<Long> forgetableIds = new HashSet<>();
          Set<Long> forgetableCommitIds = new HashSet<>();
          List<CommittedHiveTransaction> forgetableTxns = new ArrayList<>();
          Map<LockQueue, Long> forgetableDtps = new HashMap<>();
          long minOpenTxn;
          try (LockKeeper lk = new LockKeeper(masterLock.readLock())) {
            minOpenTxn = findMinOpenTxn();
            for (CommittedHiveTransaction txn : committedTxns) {
              // Look to see if all open transactions have a txnId greater than this txn's commitId
              if (txn.getCommitId() < minOpenTxn) {
                if (LOG.isDebugEnabled()) {
                  LOG.debug("Adding " + txn.getId() + " to list of forgetable transactions");
                }
                forgetableIds.add(txn.getId());
                forgetableTxns.add(txn);
                forgetableCommitIds.add(txn.getCommitId());
              }
            }
            // For any of these found transactions, see if we can remove them from the lock queues.
            // This is important because it enables us eventually to shrink the lock queues
            for (Map.Entry<EntityKey, LockQueue> entry : lockQueues.entrySet()) {
              LockQueue queue = entry.getValue();
              if (forgetableCommitIds.contains(queue.maxCommitId)) {
                forgetableDtps.put(queue, queue.maxCommitId);
                if (LOG.isDebugEnabled()) {
                  LOG.debug("Adding lock queue " + entry.getKey().toString() +
                      " to forgetable list");
                }
              }
            }
          } // exiting read lock
          if (forgetableIds.size() > 0) {
            try (LockKeeper lk1 = new LockKeeper(masterLock.writeLock())) {
              committedTxns.removeAll(forgetableTxns);
              boolean shouldCommit = false;
              getHBase().begin();
              try {
                getHBase().deleteTransactions(forgetableIds);
                shouldCommit = true;
              } finally {
                if (shouldCommit) getHBase().commit();
                else getHBase().rollback();
              }
              for (Map.Entry<LockQueue, Long> entry : forgetableDtps.entrySet()) {
                // Make sure no one else has changed the value in the meantime
                if (entry.getKey().maxCommitId == entry.getValue()) {
                  entry.getKey().maxCommitId = 0;
                }
              }
            }
          } else {
            LOG.debug("No forgettable committed transactions found");
          }
        } catch (InterruptedException e) {
          // Go back through the while loop, as this might mean it's time to quit
          continue;
        } catch (IOException e) {
          LOG.warn("Caught exception cleaning committed transactions", e);
        }
      }
      LOG.info("committed transaction cleaner finishing");
    }
  };

  private NamedRunnable lockChecker = new NamedRunnable() {
    @Override
    public String getName() {
      return "Lock Checker";
    }

    @Override
    public void run() {
      LOG.info("Beginning lock checker");
      long sleepTime = HiveConf.getTimeVar(conf,
          HiveConf.ConfVars.METASTORE_HBASE_LOCK_CHECKER_MAX_WAIT, TimeUnit.MILLISECONDS);
      while (go) {
        try {
          lockQueuesToCheck.wait(sleepTime);
          if (lockQueuesToCheck.peek() == null) continue;
          LOG.debug("Checking locks");
          List<EntityKey> keys = new ArrayList<>();
          Map<Long, HiveLock> toAcquire = new HashMap<>();
          Set<OpenHiveTransaction> acquiringTxns = new HashSet<>();
          Set<Long> writeConflicts = new HashSet<>();
          try (LockKeeper lk = new LockKeeper(masterLock.readLock())) {
            // Many keys may have been added to the queue, grab them all so we can do this just
            // once.
            EntityKey k;
            while ((k = lockQueuesToCheck.poll()) != null) {
              keys.add(k);
            }
            for (EntityKey key : keys) {
              LockQueue queue = lockQueues.get(key);
              HiveLock lastLock = null;
              for (HiveLock lock : queue.queue.values()) {
                if (lock.getState() == HbaseMetastoreProto.LockState.WAITING) {
                  // See if we can acquire this lock
                  if (lastLock == null || twoLocksCompatible(lastLock.getType(), lock.getType())) {
                    // Before deciding we can acquire it we have to assure that we don't have a
                    // lost update problem where another transaction not in the acquiring
                    // transaction's read set has written to the same entity.  If so, abort the
                    // acquiring transaction.  Only do this if this is also a write lock.  It's
                    // ok if we're reading old information, as this isn't serializable.
                    if (lock.getType() == HbaseMetastoreProto.LockType.SHARED_WRITE &&
                        lockQueues.get(lock.getEntityLocked()).maxCommitId > lock.getTxnId()) {
                      LOG.warn("Transaction " + lock.getTxnId() +
                          " attempted to obtain shared write lock for " +
                          lock.getEntityLocked().toString() + " but that entity more recently " +
                          "updated with transaction with commit id " +
                          lockQueues.get(lock.getEntityLocked()).maxCommitId
                          + " so later transaction will be aborted.");
                      writeConflicts.add(lock.getTxnId());
                    } else {
                      acquiringTxns.add(openTxns.get(lock.getTxnId()));
                      toAcquire.put(lock.getId(), lock);
                      if (LOG.isDebugEnabled()) {
                        LOG.debug("Adding lock " + lock.getTxnId() + "." + lock.getId() +
                            " to list of locks to acquire");
                      }
                    }
                  } else {
                    // If we can't acquire then nothing behind us can either
                    // TODO prove to yourself this is true
                    break;
                  }

                }
                lastLock = lock;
              }
            }
          } // Now outside read lock
          try (LockKeeper lk = new LockKeeper(masterLock.writeLock())) {
            // Abort the transactions that ran into write conflicts.  We logged this above, so no
            // need to log it here.
            for (long txnId : writeConflicts) abortTxn(openTxns.get(txnId));

            for (HiveLock lock : toAcquire.values()) {
              lock.setState(HbaseMetastoreProto.LockState.ACQUIRED);
            }

            List<HbaseMetastoreProto.Transaction> txnsToPut = new ArrayList<>();
            for (OpenHiveTransaction txn : acquiringTxns) {
              HbaseMetastoreProto.Transaction hbaseTxn = getHBase().getTransaction(txn.getId());
              HbaseMetastoreProto.Transaction.Builder txnBuilder =
                  HbaseMetastoreProto.Transaction.newBuilder(hbaseTxn);
              List<HbaseMetastoreProto.Transaction.Lock> hbaseLocks = hbaseTxn.getLocksList();
              txnBuilder.clearLocks();
              for (HbaseMetastoreProto.Transaction.Lock hbaseLock : hbaseLocks) {
                if (toAcquire.containsKey(hbaseLock.getId())) {
                  HbaseMetastoreProto.Transaction.Lock.Builder lockBuilder =
                      HbaseMetastoreProto.Transaction.Lock.newBuilder(hbaseLock);
                  lockBuilder.setAcquiredAt(System.currentTimeMillis());
                  lockBuilder.setState(HbaseMetastoreProto.LockState.ACQUIRED);
                  txnBuilder.addLocks(lockBuilder);
                } else {
                  txnBuilder.addLocks(hbaseLock);
                }
              }
              txnsToPut.add(txnBuilder.build());
            }
            boolean shouldCommit = false;
            getHBase().begin();
            try {
              getHBase().putTransactions(txnsToPut);
              shouldCommit = true;
            } finally {
              if (shouldCommit) getHBase().commit();
              else getHBase().rollback();
            }
          } // out of the write lock
          // Notify all waiters so they know to see if they've acquired their locks.
          LOG.debug("Notifying lock waiters");
          this.notifyAll();

        } catch (InterruptedException ie) {
          // go back through the loop and make sure it's not time to stop
          continue;

        } catch (Exception ie) {
          LOG.warn("Received exception while checking for locks", ie);
        }
      }
      LOG.info("lock checker finishing");
    }
  };

  private NamedRunnable[] services = {timedOutCleaner, deadlockDetector, lockQueueShrinker,
                                      fullChecker, committedTxnCleaner, lockChecker};

}
