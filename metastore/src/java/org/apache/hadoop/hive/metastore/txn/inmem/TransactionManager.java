/*
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

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.AbortTxnRequest;
import org.apache.hadoop.hive.metastore.api.AbortTxnsRequest;
import org.apache.hadoop.hive.metastore.api.AddDynamicPartitions;
import org.apache.hadoop.hive.metastore.api.CheckLockRequest;
import org.apache.hadoop.hive.metastore.api.CommitTxnRequest;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.GetOpenTxnsInfoResponse;
import org.apache.hadoop.hive.metastore.api.GetOpenTxnsResponse;
import org.apache.hadoop.hive.metastore.api.HeartbeatRequest;
import org.apache.hadoop.hive.metastore.api.HeartbeatTxnRangeRequest;
import org.apache.hadoop.hive.metastore.api.HeartbeatTxnRangeResponse;
import org.apache.hadoop.hive.metastore.api.HiveObjectType;
import org.apache.hadoop.hive.metastore.api.LockComponent;
import org.apache.hadoop.hive.metastore.api.LockRequest;
import org.apache.hadoop.hive.metastore.api.LockResponse;
import org.apache.hadoop.hive.metastore.api.LockState;
import org.apache.hadoop.hive.metastore.api.LockType;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchLockException;
import org.apache.hadoop.hive.metastore.api.NoSuchTxnException;
import org.apache.hadoop.hive.metastore.api.OpenTxnRequest;
import org.apache.hadoop.hive.metastore.api.OpenTxnsResponse;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.ShowLocksRequest;
import org.apache.hadoop.hive.metastore.api.ShowLocksResponse;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.TxnAbortedException;
import org.apache.hadoop.hive.metastore.api.TxnOpenException;
import org.apache.hadoop.hive.metastore.api.UnlockRequest;
import org.apache.hadoop.hive.metastore.txn.CompactionInfo;
import org.apache.hadoop.hive.metastore.txn.CompactionTxnHandler;
import org.apache.hadoop.hive.metastore.txn.TxnHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * An in memory transaction and lock manager.  It remembers all open transactions as well as
 * committed ones that might be needed to avoid lost updates and aborted ones that haven't been
 * compacted out yet.
 *
 * Locks are a part of a transaction, so each txn has a list of locks.  All transaction oriented
 * operations are done via txnId.
 *
 * Locks are also kept in queues which are sorted trees keyed by db, table, partition that
 * the lock is on.  This allows the system to quickly evaluate which locks should be granted next.
 *
 * All write operations are written through to a WAL.  A background thread handles moving
 * operations from the WAL to the DB.  When the class first starts it recovers the current state
 * from the DB + WAL.
 *
 * The database layout is unchanged from the initial transaction manager.  This allows
 * show operations and compaction operations to pass through largely unchanged, though some
 * massaging is necessary to deal with the fact that some information is only in the WAL and not
 * yet distributed to the appropriate database tables.  Also, some compactor operations modify
 * our in memory structures.
 *
 * The error handling approach is fast fail.  If an exception happens when writing to the WAL the
 * transaction manager promptly self destructs and trusts that a new instance of itself recovered
 * from the DB should be able to do better.
 */
public class TransactionManager extends CompactionTxnHandler {

  static final private Logger LOG = LoggerFactory.getLogger(TransactionManager.class.getName());

  static private TransactionManager self = null;
  static private HiveConf conf;

  /**
   * This must be called before {@link #get()}.
   * @param configuration HiveConf to use.
   */
  static public void setHiveConf(HiveConf configuration) {
    conf = configuration;
  }

  /**
   * Get the transaction manager instance.  In the case where the transaction manager runs into
   * an error and dies this will generate a new one.  So it is best to call this each time rather
   * than keeping a reference.
   * @return
   */
  static public TransactionManager get() {
    if (self == null) {
      synchronized (TransactionManager.class) {
        if (self == null) {
          self = new TransactionManager();
        }
      }
    }
    return self;
  }

  /**
   * When this is set the background threads will not be run automatically.  This must be set
   * before you call {@link #get()}.  After that it won't have any affect.
   */
  @VisibleForTesting static boolean unitTesting = false;

  // This lock needs to be acquired in write mode only when modifying the structures that store
  // locks and transactions (e.g. opening, aborting, committing txns, adding locks).  To modify
  // locks or transactions (e.g. heartbeat) it is only needed in the read mode.  Anything looking
  // at locks or transactions should acquire it in the read mode.
  private ReadWriteLock masterLock;

  // BEGIN THINGS PROTECTED BY masterLock
  // A list of all active transactions.
  private Map<Long, OpenTransaction> openTxns;

  // List of aborted transactions, kept in memory for efficient reading when readers need a valid
  // transaction list.
  private Map<Long, AbortedTransaction> abortedTxns;

  // A set of all committed transactions.
  private Map<Long, CommittedTransaction> committedTxns;

  // A structure to store the locks according to which database/table/partition they lock.
  private Map<EntityKey, LockQueue> lockQueues;

  // Lock queues that should be checked for whether a lock can be acquired.
  private List<EntityKey> lockQueuesToCheck;

  // Map of objects that have (possibly) been written to by aborted transactions.  This helps us
  // know where to look after a compaction to see which aborted transactions we can forget.
  private Map<EntityKey, List<AbortedTransaction>> abortedWrites;
  // TODO need to figure out how and when we can drop Aborted Transactions from these lists

  private long nextTxnId;

  // END THINGS PROTECTED BY masterLock

  // This is managed outside the masterLock as an atomic value because in addDynamicPartitions we
  // increment the value while only holding the read lock.  Hopefully the additional overhead of
  // using Atmoic here is not excessive in return for the trade off of not needing the write lock
  // in addDynamicPartitions
  private AtomicLong nextLockId;


  private ScheduledThreadPoolExecutor threadPool;

  // Track what locks types are compatible.  First array is holder, second is requester
  static private boolean[][] lockCompatibilityTable;

  private WriteAheadLog wal;
  private long lockPollTimeout;
  private long walWaitForCheckpointTimeout;
  private boolean hesDeadJim;
  private int maxOpenTxns;

  private TransactionManager() {
    LOG.info("Initializing the TransactionManager...");
    // Initialize TxnHandler.
    super.setConf(conf);
    hesDeadJim = false;
    masterLock = new ReentrantReadWriteLock();
    openTxns = new HashMap<>();
    abortedTxns = new HashMap<>();
    committedTxns = new HashMap<>();
    abortedWrites = new HashMap<>();
    lockQueues = new HashMap<>();
    lockQueuesToCheck = new ArrayList<>();
    nextLockId = new AtomicLong();

    maxOpenTxns = conf.getIntVar(HiveConf.ConfVars.HIVE_MAX_OPEN_TXNS);

    threadPool = new ScheduledThreadPoolExecutor(conf.getIntVar(
        HiveConf.ConfVars.TXNMGR_INMEM_THREADPOOL_CORE_THREADS));
    threadPool.setMaximumPoolSize(conf.getIntVar(
        HiveConf.ConfVars.TXNMGR_INMEM_THREADPOOL_MAX_THREADS));
    wal = new DbWal(this);
    recover();

    if (unitTesting) {
      lockPollTimeout = 1;
    } else {
      lockPollTimeout = conf.getTimeVar(HiveConf.ConfVars.TXNMGR_INMEM_LOCK_POLL_TIMEOUT,
          TimeUnit.MILLISECONDS);
    }
    walWaitForCheckpointTimeout = conf.getTimeVar(
        HiveConf.ConfVars.TXNMGR_INMEM_WAL_CHECKPOINT_TIMEOUT, TimeUnit.MILLISECONDS);

    // TODO not at all sure I have intention locks correct in this table
    lockCompatibilityTable = new boolean[LockType.values().length][LockType.values().length];
    Arrays.fill(lockCompatibilityTable[LockType.EXCLUSIVE.ordinal()], false);
    lockCompatibilityTable[LockType.SHARED_WRITE.ordinal()][LockType.EXCLUSIVE.ordinal()] = false;
    lockCompatibilityTable[LockType.SHARED_WRITE.ordinal()][LockType.SHARED_WRITE.ordinal()] = false;
    lockCompatibilityTable[LockType.SHARED_WRITE.ordinal()][LockType.SHARED_READ.ordinal()] = true;
    lockCompatibilityTable[LockType.SHARED_WRITE.ordinal()][LockType.INTENTION.ordinal()] = true;
    lockCompatibilityTable[LockType.SHARED_READ.ordinal()][LockType.EXCLUSIVE.ordinal()] = false;
    lockCompatibilityTable[LockType.SHARED_READ.ordinal()][LockType.SHARED_WRITE.ordinal()] = true;
    lockCompatibilityTable[LockType.SHARED_READ.ordinal()][LockType.SHARED_READ.ordinal()] = true;
    lockCompatibilityTable[LockType.SHARED_READ.ordinal()][LockType.INTENTION.ordinal()] = true;
    lockCompatibilityTable[LockType.INTENTION.ordinal()][LockType.EXCLUSIVE.ordinal()] = false;
    lockCompatibilityTable[LockType.INTENTION.ordinal()][LockType.SHARED_WRITE.ordinal()] = true;
    lockCompatibilityTable[LockType.INTENTION.ordinal()][LockType.SHARED_READ.ordinal()] = true;
    lockCompatibilityTable[LockType.INTENTION.ordinal()][LockType.INTENTION.ordinal()] = true;

    if (!unitTesting) {
      // Randomizes initial delay of threads so we don't accidentally get them all starting
      // together.
      Random rand = new Random();
      long period = conf.getTimeVar(HiveConf.ConfVars.TXNMGR_INMEM_TXN_FORGETTER_THREAD_PERIOD,
          TimeUnit.MILLISECONDS);
      threadPool.scheduleAtFixedRate(abortedTxnForgetter, period + rand.nextInt((int)period),
          period, TimeUnit.MILLISECONDS);

      period = conf.getTimeVar(HiveConf.ConfVars.TXNMGR_INMEM_LOCK_QUEUE_SHRINKER_THREAD_PERIOD,
          TimeUnit.MILLISECONDS);
      threadPool.scheduleAtFixedRate(queueShrinker, period + rand.nextInt((int)period), period,
          TimeUnit.MILLISECONDS);

      period = conf.getTimeVar(HiveConf.ConfVars.TXNMGR_INMEM_COMMITTED_TXN_CLEANER_THREAD_PERIOD,
          TimeUnit.MILLISECONDS);
      threadPool.scheduleAtFixedRate(committedTxnCleaner, period + rand.nextInt((int)period),
          period, TimeUnit.MILLISECONDS);

      period = conf.getTimeVar(HiveConf.ConfVars.TXNMGR_INMEM_TXN_TIMEOUT_THREAD_PERIOD,
          TimeUnit.MILLISECONDS);
      threadPool.scheduleAtFixedRate(timedOutCleaner, period + rand.nextInt((int)period),
          period, TimeUnit.MILLISECONDS);

      period = conf.getTimeVar(HiveConf.ConfVars.TXNMGR_INMEM_DEADLOCK_DETECTOR_THREAD_PERIOD,
          TimeUnit.MILLISECONDS);
      threadPool.scheduleAtFixedRate(deadlockDetector, period + rand.nextInt((int)period),
          period, TimeUnit.MILLISECONDS);
    }

    LOG.info("TransactionManager initialization compelte");
  }

  /**
   * Do a graceful shutdown.  This does not bother to flush the queues are try to get everything
   * on disk, as the WAL will already have it recorded.  It just handles shutting down resources
   * like the thread pools.
   */
  public void shutdown() {
    LOG.info("Shutting down the TransactionManager...");
    threadPool.shutdownNow(); // This will terminate the threads in the WAL as well.
    LOG.info("TransactionManager shutdown complete");
  }

  /**
   * Shutdown the transaction manager hard.  This is only intended for use in error conditions.
   */
  void selfDestruct() {
    hesDeadJim = true;
    LOG.error("Memory and db got out of sync!  Dying now so we can recover and move on.");
    threadPool.shutdownNow(); // This will terminate the threads in the WAL as well.
    synchronized (TransactionManager.class) {
      self = null;
    }
  }

  DataSource getConnectionPool() {
    return connPool;
  }

  ScheduledThreadPoolExecutor getThreadPool() {
    return threadPool;
  }

  HiveConf getConf() {
    return conf;
  }

  @Override
  public OpenTxnsResponse openTxns(OpenTxnRequest rqst) throws MetaException {
    checkAlive();
    if (openTxns.size() + rqst.getNum_txns() > maxOpenTxns) {
      LOG.warn("Maximum allowed number of open transactions (" + maxOpenTxns + ") has been " +
          "reached. Current number of open transactions: " + openTxns.size());
      throw new MetaException("Maximum allowed number of open transactions has been reached. " +
          "See hive.max.open.txns.");
    }

    // 99.9% of the time we're only opening one txn.  Special case it to avoid needing to create
    // extra arrays, lists, etc.
    if (rqst.getNum_txns() > 1) return openMultipleTxns(rqst);
    LOG.debug("Opening a transaction");

    Future<Integer> waitForWal;
    OpenTransaction txn;
    try (LockKeeper lk = new LockKeeper(masterLock.writeLock())) {
      txn = new OpenTransaction(nextTxnId++);
      openTxns.put(txn.getTxnId(), txn);

      waitForWal = wal.queueOpenTxn(txn.getTxnId(), rqst);
    } catch (IOException e) {
      throw new RuntimeException("LockKeeper.close doesn't throw, how did this happen?", e);
    }

    try {
      waitForWal.get();
    } catch (InterruptedException|ExecutionException e) {
      LOG.error("Unable to record transaction open in the WAL", e);
      selfDestruct();
    }
    return new OpenTxnsResponse(Collections.singletonList(txn.getTxnId()));
  }

  private OpenTxnsResponse openMultipleTxns(OpenTxnRequest rqst) throws MetaException {
    checkAlive();
    throw new UnsupportedOperationException();
  }

  @Override
  public GetOpenTxnsResponse getOpenTxns() throws MetaException {
    checkAlive();
    LOG.debug("Getting open transactions");
    try (LockKeeper lk = new LockKeeper(masterLock.readLock())) {
      // We need to make copies of these keySets, otherwise changes in the transaction structures
      // after we release the lock could be reflected in the results.
      Set<Long> txnIds = new HashSet<>(openTxns.keySet());
      txnIds.addAll(abortedTxns.keySet());
      return new GetOpenTxnsResponse(nextTxnId, txnIds);
    } catch (IOException e) {
      throw new RuntimeException("LockKeeper.close doesn't throw, how did this happen?", e);
    }
  }

  @Override
  public GetOpenTxnsInfoResponse getOpenTxnsInfo() throws MetaException {
    checkAlive();
    // Put a checkpoint on the WAL so that everything in there get's moved before we go look in
    // the database.  This is slower (since we're waiting for the WAL to clear) but
    // much easier than combining data from the database tables and the WAL.  Since this method
    // is used to satisfy 'show transactions' (not an operation that needs to perform in under a
    // second) this should be fine.
    try {
      wal.waitForCheckpoint(walWaitForCheckpointTimeout, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      // This likely means that we're being shutdown anyway, so just return nothing
      LOG.warn("Received interrupt exception while waiting for checkpoint");
      return new GetOpenTxnsInfoResponse();
    } catch (TimeoutException e) {
      LOG.warn("Timed out waiting for WAL checkpoint");
      throw new MetaException("Timed out waiting for the WAL checkpoint");
    }
    return super.getOpenTxnsInfo();
  }

  @Override
  public void countOpenTxns() throws MetaException {
    // This is no longer required, as we can easily check in memory when a new transaction is
    // opened.
  }

  @Override
  public void abortTxn(AbortTxnRequest rqst) throws NoSuchTxnException, MetaException,
      TxnAbortedException {
    checkAlive();
    if (LOG.isDebugEnabled()) LOG.debug("Aborting transaction " + rqst.getTxnid());
    OpenTransaction txn;
    try (LockKeeper lk = new LockKeeper(masterLock.readLock())) {
      txn = openTxns.get(rqst.getTxnid());
      if (txn == null) throwAbortedOrNonExistent(rqst.getTxnid(), "abort");
    } catch (IOException e) {
      throw new RuntimeException("LockKeeper.close doesn't throw, how did this happen?", e);
    }
    abortTxnInternal(txn);
  }

  // This call will acquire the write lock.  So you need to either have it or have no locks.
  private void abortTxnInternal(OpenTransaction openTxn) {
    HiveLock[] locks = openTxn.getHiveLocks();
    AbortedTransaction abortedTxn = null;
    Future<Integer> waitForWal;
    try (LockKeeper lk = new LockKeeper(masterLock.writeLock())) {
      waitForWal = wal.queueAbortTxn(openTxn);
      openTxns.remove(openTxn.getTxnId());
      // We only need to remember this transaction in the aborted queue if it had some shared
      // write locks.  Otherwise, it can't have done anything we care about remembering.
      if (locks != null && locks.length > 0) {
        for (HiveLock lock : locks) {
          lockQueues.get(lock.getEntityLocked()).queue.remove(lock.getLockId());
          lockQueuesToCheck.add(lock.getEntityLocked());
          // Remember that we might have written to this object
          if (lock.getType() == LockType.SHARED_WRITE) {
            if (abortedTxn == null) abortedTxn = new AbortedTransaction(openTxn);
            lock.setState(LockState.TXN_ABORTED);
            List<AbortedTransaction> aborted = abortedWrites.get(lock.getEntityLocked());
            if (aborted == null) {
              aborted = new ArrayList<>();
              abortedWrites.put(lock.getEntityLocked(), aborted);
            }
            aborted.add(abortedTxn);
          }
        }
      }
      if (abortedTxn != null) abortedTxns.put(abortedTxn.getTxnId(), abortedTxn);
    } catch (IOException e) {
      throw new RuntimeException("LockKeeper.close doesn't throw, how did this happen?", e);
    }

    if (locks != null && locks.length > 0) {
      LOG.debug("Requesting lockChecker run");
      threadPool.execute(lockChecker);
    }

    try {
      waitForWal.get();
    } catch (InterruptedException|ExecutionException e) {
      LOG.error("Unable to record transaction abort in the WAL", e);
      selfDestruct();
    }
  }

  @Override
  public void abortTxns(AbortTxnsRequest rqst) throws NoSuchTxnException, MetaException {
    checkAlive();
    // TODO
    throw new UnsupportedOperationException();
  }

  @Override
  public void commitTxn(CommitTxnRequest rqst) throws NoSuchTxnException, TxnAbortedException,
      MetaException {
    checkAlive();
    if (LOG.isDebugEnabled()) {
      LOG.debug("Committing txn " + rqst.getTxnid());
    }
    Future<Integer> waitForWal;
    try (LockKeeper lk = new LockKeeper(masterLock.writeLock())) {
      OpenTransaction txn = openTxns.get(rqst.getTxnid());
      if (txn == null) throwAbortedOrNonExistent(rqst.getTxnid(), "commit");
      HiveLock[] locks = txn.getHiveLocks();
      boolean sawWriteLock = false;
      if (locks != null) {
        for (HiveLock lock : locks) {
          lock.setState(LockState.RELEASED);
          if (lock.getType() == LockType.SHARED_WRITE) sawWriteLock = true;
          lockQueues.get(lock.getEntityLocked()).queue.remove(lock.getLockId());
          lockQueuesToCheck.add(lock.getEntityLocked());
        }
        // Request a lockChecker run since we've released locks.
        LOG.debug("Requesting lockChecker run");
        threadPool.execute(lockChecker);
      }

      openTxns.remove(txn.getTxnId());
      // We only need to remember the transaction if it had write locks.  If it's read only or
      // DDL we can forget it.
      if (sawWriteLock) {
        // There's no need to move the transaction counter ahead
        CommittedTransaction committedTxn = new CommittedTransaction(txn, nextTxnId - 1);
        waitForWal = wal.queueCommitTxn(committedTxn);

        if (LOG.isDebugEnabled()) {
          LOG.debug("Created new committed transaction with txn id " + committedTxn.getTxnId() +
              " and commit id " + committedTxn.getCommitId());
        }

        committedTxns.put(committedTxn.getTxnId(), committedTxn);
        // Record all of the commit ids in the lockQueues so other transactions can quickly look
        // it up when getting locks.
        for (HiveLock lock : txn.getHiveLocks()) {
          if (lock.getType() != LockType.SHARED_WRITE) continue;
          LockQueue queue = lockQueues.get(lock.getEntityLocked());
          if (LOG.isDebugEnabled()) {
            LOG.debug(lock.getEntityLocked().toString() + " has max commit id of " +
                queue.maxCommitId);
          }
          queue.maybeSetMaxCommitId(committedTxn.getCommitId());
          if (LOG.isDebugEnabled()) {
            LOG.debug("Set " + lock.getEntityLocked().toString() + " max commit id to " +
                queue.maxCommitId);
          }
        }

      } else {
        waitForWal = wal.queueForgetTransactions(Collections.singletonList(txn));
        if (LOG.isDebugEnabled()) {
          LOG.debug("Forgetting transaction " + txn.getTxnId() +
              " as it is committed and held no write locks");
        }
      }
    } catch (IOException e) {
      throw new RuntimeException("LockKeeper.close doesn't throw, how did this happen?", e);
    }
    try {
      waitForWal.get();
    } catch (InterruptedException|ExecutionException e) {
      LOG.error("Unable to record transaction commit in the WAL", e);
      selfDestruct();
    }
  }

  @Override
  public void heartbeat(HeartbeatRequest ids) throws NoSuchTxnException, NoSuchLockException,
      TxnAbortedException, MetaException {
    assert !ids.isSetLockid() : "Fail, we don't heartbeat locks anymore!";
    checkAlive();

    if (LOG.isDebugEnabled()) LOG.debug("Heartbeating txn " + ids.getTxnid());
    try (LockKeeper lk = new LockKeeper(masterLock.readLock())) {
      OpenTransaction txn = openTxns.get(ids.getTxnid());
      if (txn == null) throwAbortedOrNonExistent(ids.getTxnid(), "abort");
      txn.setLastHeartbeat(System.currentTimeMillis());
      // Don't write this down to the database.  There's no value.
    } catch (IOException e) {
      throw new RuntimeException("LockKeeper.close doesn't throw, how did this happen?", e);
    }
  }

  @Override
  public HeartbeatTxnRangeResponse heartbeatTxnRange(HeartbeatTxnRangeRequest rqst) throws
      MetaException {
    checkAlive();
    // TODO
    throw new UnsupportedOperationException();
  }

  @Override
  public LockResponse lock(LockRequest rqst) throws NoSuchTxnException, TxnAbortedException,
      MetaException {
    assert rqst.isSetTxnid() : "All locks must be associated with a transaction now";
    checkAlive();
    if (LOG.isDebugEnabled()) {
      LOG.debug("Requesting locks for transaction " + rqst.getTxnid());
      for (LockComponent component : rqst.getComponent()) {
        LOG.debug("entity: " + component.getDbname() +
            (component.isSetTablename() ? component.getTablename() : "") +
            (component.isSetPartitionname() ? component.getPartitionname() : ""));
      }
    }
    List<LockComponent> components = rqst.getComponent();
    HiveLock[] hiveLocks = new HiveLock[components.size()];
    OpenTransaction txn;
    Future<?> lockCheckerRun;
    Future<Integer> waitForWal;
    try (LockKeeper lk = new LockKeeper(masterLock.writeLock())) {
      txn = openTxns.get(rqst.getTxnid());
      if (txn == null) throwAbortedOrNonExistent(rqst.getTxnid(), "obtain lock in");

      for (int i = 0; i < components.size(); i++) {
        EntityKey key = new EntityKey(components.get(i));
        hiveLocks[i] =
            new HiveLock(nextLockId.getAndIncrement(), rqst.getTxnid(), key, components.get(i).getType());
        // Add to the appropriate DTP queue
        assureQueueExists(key);
        lockQueues.get(hiveLocks[i].getEntityLocked()).queue.put(hiveLocks[i].getLockId(),
            hiveLocks[i]);
        lockQueuesToCheck.add(hiveLocks[i].getEntityLocked());
      }
      // Run the lock checker to see if we can acquire these locks
      lockCheckerRun = threadPool.submit(lockChecker);
      txn.addLocks(hiveLocks);

      waitForWal = wal.queueLockRequest(rqst, Arrays.asList(hiveLocks));
    } catch (IOException e) {
      throw new RuntimeException("LockKeeper.close doesn't throw, how did this happen?", e);
    }

    // First, see if our locks acquired immediately using the return from our submission to the
    // thread queue.
    LockResponse rsp = null;
    try (LockKeeper lk = new LockKeeper(masterLock.readLock())) {
      try {
        lockCheckerRun.get(lockPollTimeout, TimeUnit.MILLISECONDS);
      } catch (TimeoutException e) {
        // Fall through to wait case.
      }

      if (checkMyLocks(hiveLocks) == LockState.ACQUIRED) {
        LOG.debug("Locks acquired immediately, returning");
        rsp = new LockResponse(rqst.getTxnid(),
            org.apache.hadoop.hive.metastore.api.LockState.ACQUIRED);
      } else {
        // We didn't acquire right away, so long poll.  We won't wait forever, but we can wait a few
        // seconds to avoid the clients banging away every few hundred milliseconds to see if their
        // locks have acquired.
        LOG.debug("Locks did not acquire immediately, waiting...");
        rsp = waitForLocks(hiveLocks);
      }
    } catch (IOException e) {
      throw new RuntimeException("LockKeeper.close doesn't throw, how did this happen?", e);
    } catch (InterruptedException|ExecutionException e) {
      LOG.error("LogChecker blew up", e);
      selfDestruct();
    }

    try {
      waitForWal.get();
    } catch (InterruptedException|ExecutionException e) {
      LOG.error("Unable to record lock request in the WAL", e);
      selfDestruct();
    }
    return rsp;
  }

  /**
   * Wait for a bit to see if a set of locks acquire.  This will wait on the lockChecker object
   * to signal that the queues should be checked.  It will only wait for up to lockPollTimeout
   * milliseconds.
   * @param hiveLocks locks to wait for.
   * @return The state of the locks, could be WAITING, ACQUIRED, or TXN_ABORTED
   * @throws IOException
   */
  private LockResponse waitForLocks(HiveLock[] hiveLocks) {
    LOG.debug("Waiting for locks");
    synchronized (lockChecker) {
      try {
        lockChecker.wait(lockPollTimeout);
      } catch (InterruptedException e) {
        LOG.warn("Interupted while waiting for locks", e);
        // Still go ahead and check our status and return it.
      }
    }
    try (LockKeeper lk = new LockKeeper(masterLock.readLock())) {
      return new LockResponse(hiveLocks[0].getTxnId(), checkMyLocks(hiveLocks));
    } catch (IOException e) {
      throw new RuntimeException("LockKeeper.close doesn't throw, how did this happen?", e);
    }
  }

  /**
   * See if a set of locks have acquired.  You MUST hold the read lock before entering this method.
   * @param hiveLocks set of locks to check
   * @return state of checked locks
   */
  private LockState checkMyLocks(HiveLock[] hiveLocks) {
    for (HiveLock lock : hiveLocks) {
      if (lock.getState() == LockState.WAITING) {
        LOG.debug("Some of our locks still in waiting state");
        return LockState.WAITING;
      }
      if (lock.getState() != LockState.ACQUIRED) {
        LOG.error("Found a lock in an unexpected state " + lock.getState());
        throw new RuntimeException("Lock not in waiting or acquired state, not sure what to do");
      }
    }
    LOG.debug("All requested locks acquired");
    return LockState.ACQUIRED;
  }

  @Override
  public LockResponse checkLock(CheckLockRequest rqst) throws NoSuchTxnException,
      NoSuchLockException, TxnAbortedException, MetaException {
    checkAlive();
    // Locks must now be associated with a transaction
    if (!rqst.isSetTxnid()) {
      throw new NoSuchLockException("Locks must now be associated with a transaction");
    }
    // Find the locks associated with this transaction, so we know what to check
    HiveLock[] locks = null;
    try (LockKeeper lk = new LockKeeper(masterLock.readLock())) {
      OpenTransaction txn = openTxns.get(rqst.getTxnid());
      if (txn == null) throwAbortedOrNonExistent(rqst.getTxnid(), "check locks");
      locks = txn.getHiveLocks();
    } catch (IOException e) {
      throw new RuntimeException("LockKeeper.close doesn't throw, how did this happen?", e);
    }
    return waitForLocks(locks);
  }

  @Override
  public void unlock(UnlockRequest rqst) throws NoSuchLockException, TxnOpenException,
      MetaException {
    // This is no longer supported, because all locks should now be released by abort transaction
    // or commit transaction
    throw new NoSuchLockException("Locks must now be part of a transaction.  Unlocking " +
        "should only be done as part of a commit or abort transaction");
  }

  @Override
  public ShowLocksResponse showLocks(ShowLocksRequest rqst) throws MetaException {
    checkAlive();
    // Put a checkpoint on the WAL so that everything in there get's moved before we go look in
    // the database.  This is slower (since we're waiting for the WAL to clear) but
    // much easier than combining data from the database tables and the WAL.  Since this method
    // is used to satisfy 'show locks' (not an operation that needs to perform in under a second)
    // this should be fine.
    try {
      wal.waitForCheckpoint(walWaitForCheckpointTimeout, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      // This likely means that we're being shutdown anyway, so just return nothing
      LOG.warn("Received interrupt exception while waiting for checkpoint");
      return new ShowLocksResponse();
    } catch (TimeoutException e) {
      LOG.warn("Timed out waiting for WAL checkpoint");
      throw new MetaException("Timed out waiting for the WAL checkpoint");
    }
    return super.showLocks(rqst);
  }

  @Override
  public int numLocksInLockTable() throws SQLException, MetaException {
    checkAlive();
    // Put a checkpoint on the WAL so that everything in there get's moved before we go look in
    // the database.  This is slower (since we're waiting for the WAL to clear) but
    // much easier than combining data from the database tables and the WAL.  Since this method
    // is used in testing this should be fine.
    try {
      wal.waitForCheckpoint(walWaitForCheckpointTimeout, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      // This likely means that we're being shutdown anyway, so just return nothing
      LOG.warn("Received interrupt exception while waiting for checkpoint");
      return 0;
    } catch (TimeoutException e) {
      LOG.warn("Timed out waiting for WAL checkpoint");
      throw new MetaException("Timed out waiting for the WAL checkpoint");
    }
    return super.numLocksInLockTable();
  }

  @Override
  public void performWriteSetGC() {
    // If I understand correctly, this method is no longer needed as the background cleaner
    // thread will handle this.  So just override this as a NO-OP.
  }

  @Override
  public void performTimeOuts() {
    // I don't think I need this anymore as the background timeout thread will handle this
  }

  @Override
  public void addDynamicPartitions(AddDynamicPartitions rqst) throws NoSuchTxnException,
      TxnAbortedException, MetaException {
    checkAlive();
    if (LOG.isDebugEnabled()) {
      LOG.debug("Adding dynamic partitions for transaction " + rqst.getTxnid() + " table " +
          rqst.getDbname() + "." + rqst.getTablename());
      for (String part : rqst.getPartitionnames()) {
        LOG.debug("Partition: " + part);
      }
    }
    // This acquires the readLock (rather than the writeLock) because it won't modify the lock or
    // transaction structures themselves.  It will just add locks to an existing transaction.  If
    // another thread tries to abort or commit that same transaction, those do acquire the write
    // lock and thus won't clash with this.
    Future<Integer> waitForWal;
    try (LockKeeper lk = new LockKeeper(masterLock.readLock())) {
      // Add the locks to the appropriate transaction so that we know what things to compact and
      // so we know what partitions were touched by this change.  Don't put the locks in the dtps
      // because we're actually covered by the table lock.  Do increment the counters in the dtps.
      OpenTransaction txn = openTxns.get(rqst.getTxnid());
      if (txn == null) {
        throwAbortedOrNonExistent(rqst.getTxnid(), "add dynamic partitions");
      }

      List<String> partitionNames = rqst.getPartitionnames();
      HiveLock[] partitionsWrittenTo = new HiveLock[partitionNames.size()];
      for (int i = 0; i < partitionNames.size(); i++) {
        partitionsWrittenTo[i] = new HiveLock(nextLockId.getAndIncrement(), rqst.getTxnid(),
            new EntityKey(rqst.getDbname(), rqst.getTablename(), partitionNames.get(i)),
            LockType.SHARED_WRITE);
        partitionsWrittenTo[i].setState(LockState.ACQUIRED);
      }
      waitForWal = wal.queueLockAcquisition(Arrays.asList(partitionsWrittenTo));
      txn.addLocks(partitionsWrittenTo);
    } catch (IOException e) {
      throw new RuntimeException("LockKeeper.close doesn't throw, how did this happen?", e);
    }

    try {
      waitForWal.get();
    } catch (InterruptedException|ExecutionException e) {
      LOG.error("Unable to record lock acquisition in the WAL", e);
      selfDestruct();
    }
  }

  @Override
  public void cleanupRecords(final HiveObjectType type, final Database db, final Table table,
                             final Iterator<Partition> partitionIterator) throws MetaException {
    checkAlive();
    // There's no reason to block for this, as the main event of dropping the object has already
    // occurred in a separate transaction and cannot be undone.  So rather than optimizing for
    // speed by checking the WAL and then the db tables, we'll fire a future event that waits
    // until everything currently in the WAL is written out and then executes the cleanup just
    // against the DB tables.
    threadPool.execute(new FutureTask<>(new Callable<Integer>() {
      @Override
      public Integer call() throws Exception {
        wal.waitForCheckpoint(1, TimeUnit.MINUTES);
        TransactionManager.super.cleanupRecords(type, db, table, partitionIterator);
        return 1;
      }
    }));
  }

  @Override
  public void markCompacted(CompactionInfo info) throws MetaException {
    checkAlive();
    if (LOG.isDebugEnabled()) {
      LOG.debug("Cleaning up after compaction of " + info.dbname + "." + info.tableName +
          (info.partName == null ? "" : "." + info.partName));
    }

    // Minor compactions don't strain out aborted records.
    if (info.isMajorCompaction()) {
      // Find any aborted locks that can now be forgotten, since we have compacted the
      // associated entity.
      EntityKey compacted = new EntityKey(info.dbname, info.tableName, info.partName);
      Future<Integer> waitForWal = null;
      try (LockKeeper lk = new LockKeeper(masterLock.readLock())) {
        List<AbortedTransaction> abortedTxns = abortedWrites.get(compacted);
        if (abortedTxns != null && abortedTxns.size() > 0) {
          List<HiveLock> toBeForgotten = new ArrayList<>();
          for (AbortedTransaction abortedTxn : abortedTxns) {
            // Find any locks that match this
            Map<EntityKey, HiveLock> compactableLocks = abortedTxn.getCompactableLocks();
            if (compactableLocks != null) {
              if (LOG.isDebugEnabled()) {
                LOG.debug("Fogetting aborted lock for txn " + abortedTxn.getTxnId() + " on entity "
                    + compacted.toString());
              }
              toBeForgotten.add(compactableLocks.get(compacted));
              compactableLocks.remove(compacted);
            }
          }
          waitForWal = wal.queueForgetLocks(toBeForgotten);
        }
      } catch (IOException e) {
        throw new RuntimeException("LockKeeper.close doesn't throw, how did this happen?", e);
      }
      try {
        if (waitForWal != null) waitForWal.get();
      } catch (InterruptedException|ExecutionException e) {
        LOG.error("Unable to record forgetting locks in the WAL", e);
        selfDestruct();
      }
    }

    // We still need to call CompactionTxnHandler.markCompacted so it can make the change in the
    // database.
    super.markCompacted(info);
  }

  private void throwAbortedOrNonExistent(long id, String attempedAction)
      throws TxnAbortedException, NoSuchTxnException {
    if (abortedTxns.containsKey(id)) {
      throw new TxnAbortedException("Attempt to " + attempedAction + " aborted transaction " + id);
    }
    throw new NoSuchTxnException("Attempt to " + attempedAction + " non-existent transaction" + id);
  }

  @Override
  public List<CompactionInfo> findReadyToClean() throws MetaException {
    checkAlive();
    // Before we allow something to be cleaned we need to assure that we don't have any active
    // read locks.  So first get the list from CompactionTxnHander and then double check it
    // against our structures.
    List<CompactionInfo> toClean = super.findReadyToClean();
    if (toClean != null && toClean.size() > 0) {
      long minOpenTxn;
      try (LockKeeper lk = new LockKeeper(masterLock.readLock())) {
        minOpenTxn = findMinOpenTxn();
      } catch (IOException e) {
        throw new RuntimeException("LockKeeper.close doesn't throw, how did this happen?", e);
      }

      List<CompactionInfo> approved = new ArrayList<>(toClean.size());
      for (CompactionInfo ci : toClean) {
        if (ci.highestTxnId < minOpenTxn) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Approving compaction for cleaning " + ci.getFullPartitionName());
          }
          approved.add(ci);
        }
      }
      toClean = approved;
    }
    return toClean;
  }

  // This method assumes you are holding the read lock.
  private long findMinOpenTxn() {
    long minOpenTxn = nextTxnId - 1;
    for (Long txnId : openTxns.keySet()) {
      minOpenTxn = Math.min(txnId, minOpenTxn);
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Found minimum open transaction of " + minOpenTxn);
    }
    return minOpenTxn;
  }

  // You MUST hold the write lock to call this method
  private void assureQueueExists(EntityKey key) {
    if (!lockQueues.containsKey(key)) lockQueues.put(key, new LockQueue());
  }

  private void checkAlive() throws MetaException {
    if (hesDeadJim) {
      throw new MetaException("The TransactionManager has died, awaiting regeneration");
    }
  }

  // This assumes that no one else is running when it is, thus it doesn't grab locks.
  private void recover() {
    LOG.info("Beginning recovery...");
    // First recover the WAL, so that all records are in the DB proper
    try {
      wal.start();

      try (Connection conn = getDbConn(Connection.TRANSACTION_READ_COMMITTED)) {
        try (Statement stmt = conn.createStatement()) {
          // Figure out the next transaction id we should be using.
          String sql = "select MAX(txn_id) from TXNS";
          LOG.debug("Going to execute query " + sql);
          ResultSet rs = stmt.executeQuery(sql);
          if (rs.next()) {
            nextTxnId = rs.getLong(1) + 1;
          } else {
            nextTxnId = 1;
          }

          // Figure out the next lock id we should use
          sql = "select MAX(HL_LOCK_EXT_ID) from HIVE_LOCKS";
          LOG.debug("Going to execute query " + sql);
          rs = stmt.executeQuery(sql);
          if (rs.next()) {
            nextLockId.set(rs.getLong(1) + 1);
          } else {
            nextLockId.set(1);
          }

          if (LOG.isDebugEnabled()) {
            LOG.debug("Found next transaction id of " + nextTxnId + " and next lock id of " +
                nextLockId);
          }

          // Read all of the transactions into memory and place them in the appropriate queues.
          // Don't worry if we could forget them.  The cleaner threads will handle that.
          sql = "select txn_id, txn_state, txn_committed_id from TXNS order by txn_id";
          LOG.debug("Going to execute query " + sql);
          rs = stmt.executeQuery(sql);
          while (rs.next()) {
            long txnId = rs.getLong(1);
            switch (rs.getString(2).charAt(0)) {
              case TxnHandler.TXN_OPEN:
                if (LOG.isDebugEnabled()) LOG.debug("Recovering open transaction " + txnId);
                openTxns.put(txnId, new OpenTransaction(txnId));
                break;

              case TxnHandler.TXN_ABORTED:
                if (LOG.isDebugEnabled()) LOG.debug("Recovering aborted transaction " + txnId);
                abortedTxns.put(txnId, new AbortedTransaction(txnId));
                break;

              case TxnHandler.TXN_COMMITTED:
                if (LOG.isDebugEnabled()) LOG.debug("Recovering committed transaction " + txnId);
                committedTxns.put(txnId, new CommittedTransaction(txnId, rs.getLong(3)));
                break;

              default:
                throw new RuntimeException("Unknown transaction state " + rs.getString(2));
            }
          }

          // Read all of the locks and associate them with the appropriate transactions.
          // Forgetting of locks is done by the WAL so no need to worry about that here.
          // Ordering by lock id to make sure we get locks back in the queues in the right order
          sql = "select hl_txnid, hl_lock_ext_id, hl_db, hl_table, hl_partition, hl_lock_state, " +
              "hl_lock_type from HIVE_LOCKS order by hl_lock_ext_id";
          LOG.debug("Going to execute query " + sql);
          rs = stmt.executeQuery(sql);
          Map<Long, List<HiveLock>> openTxnLocks = new HashMap<>();
          Map<Long, List<HiveLock>> abortedTxnLocks = new HashMap<>();
          while (rs.next()) {
            long txnId = rs.getLong(1);
            long lockId = rs.getLong(2);
            EntityKey entityKey = new EntityKey(rs.getString(3), rs.getString(4), rs.getString(5));
            LockType lockType;
            switch (rs.getString(6).charAt(0)) {
              case TxnHandler.LOCK_SEMI_SHARED:
                lockType = LockType.SHARED_WRITE;
                break;
              case TxnHandler.LOCK_EXCLUSIVE:
                lockType = LockType.EXCLUSIVE;
                break;
              case TxnHandler.LOCK_INTENTION:
                lockType = LockType.INTENTION;
                break;
              case TxnHandler.LOCK_SHARED:
                lockType = LockType.SHARED_READ;
                break;
              default: throw new RuntimeException("Unknown lock type " + rs.getString(6));
            }

            HiveLock hiveLock = new HiveLock(lockId, txnId, entityKey, lockType);
            assureQueueExists(entityKey);
            switch (rs.getString(5).charAt(0)) {
              case TxnHandler.LOCK_ABORTED:
                getLockList(abortedTxnLocks, txnId).add(hiveLock);
                break;

              case TxnHandler.LOCK_RELEASED:
                // Don't have to queue this lock, but we do have to set the current max commit id
                // for the appropriate queue.
                lockQueues.get(entityKey).maybeSetMaxCommitId(committedTxns.get(txnId)
                    .getCommitId());
                break;

              case TxnHandler.LOCK_WAITING:
              case TxnHandler.LOCK_ACQUIRED:
                getLockList(openTxnLocks, txnId).add(hiveLock);
                lockQueues.get(entityKey).queue.put(hiveLock.getLockId(), hiveLock);
                break;

              default:
                throw new RuntimeException("Unknown lock state " + rs.getString(5));
            }
          }

          // Put each of the locks into their transactions
          addLocksToTransactions(openTxnLocks, openTxns, "acquired and/or waiting", "open");
          addLocksToTransactions(abortedTxnLocks, abortedTxns, "aborted", "aborted");
        }
      }

      LOG.info("Recovery completed.");
    } catch (SQLException e) {
      LOG.error("Failed to recover, dying", e);
      throw new RuntimeException(e);
    }
  }

  private List<HiveLock> getLockList(Map<Long, List<HiveLock>> map, long txnId) {
    List<HiveLock> lockList = map.get(txnId);
    if (lockList == null) {
      lockList = new ArrayList<>();
      map.put(txnId, lockList);
    }
    return lockList;
  }

  private void addLocksToTransactions(Map<Long, List<HiveLock>> txnLockList,
                                      Map<Long, ? extends HiveTransaction> transactionMap,
                                      String lockStates, String txnState) {
    for (Map.Entry<Long, List<HiveLock>> entry : txnLockList.entrySet()) {
      HiveTransaction txn = transactionMap.get(entry.getKey());
      if (txn == null) {
        StringBuilder msg = new StringBuilder("Found ")
            .append(lockStates)
            .append(" locks with no associated ")
            .append(txnState)
            .append(" transaction, we are in trouble!  Transaction id ")
            .append(entry.getKey())
            .append(" lock ids ");
        for (HiveLock hiveLock : entry.getValue()) msg.append(hiveLock.getLockId());
        LOG.error(msg.toString());
        throw new RuntimeException(msg.toString());
      }
      txn.addLocks(entry.getValue().toArray(new HiveLock[entry.getValue().size()]));
    }
  }

  @VisibleForTesting static class LockQueue {
    final SortedMap<Long, HiveLock> queue;
    long maxCommitId;

    LockQueue() {
      queue = new TreeMap<>();
      maxCommitId = 0;
    }

    void maybeSetMaxCommitId(long commitId) {
      maxCommitId = Math.max(maxCommitId, commitId);
    }
  }

  private final Runnable lockChecker = new Runnable() {
    @Override
    public void run() {
      LOG.debug("Checking to see if we can promote any locks");
      List<EntityKey> keys = null;

      try (LockKeeper lk = new LockKeeper(masterLock.writeLock())) {
        if (lockQueuesToCheck.size() > 0) {
          keys = lockQueuesToCheck;
          lockQueuesToCheck = new ArrayList<>();
        }
      } catch (IOException e) {
        throw new RuntimeException("LockKeeper.close doesn't throw, how did this happen?", e);
      }

      if (keys != null) {
        List<HiveLock> toAcquire = new ArrayList<>();
        final Set<Long> writeConflicts = new HashSet<>();
        Future<Integer> waitForWal;
        try (LockKeeper lk = new LockKeeper(masterLock.readLock())) {
          // Many keys may have been added to the queue, grab them all so we can do this just
          // once.
          for (EntityKey key : keys) {
            LockQueue queue = lockQueues.get(key);
            HiveLock lastLock = null;
            for (HiveLock lock : queue.queue.values()) {
              if (lock.getState() == LockState.WAITING) {
                // See if we can acquire this lock
                if (lastLock == null || twoLocksCompatible(lastLock.getType(), lock.getType())) {
                  // Before deciding we can acquire it we have to assure that we don't have a
                  // lost update problem where another transaction not in the acquiring
                  // transaction's read set has written to the same entity.  If so, abort the
                  // acquiring transaction.  Only do this if this is also a write lock.  It's
                  // ok if we're reading old information, as this isn't serializable.
                  if (lock.getType() == LockType.SHARED_WRITE &&
                      lockQueues.get(lock.getEntityLocked()).maxCommitId > lock.getTxnId()) {
                    LOG.warn("Transaction " + lock.getTxnId() +
                        " attempted to obtain shared write lock for " +
                        lock.getEntityLocked().toString() + " but that entity more recently " +
                        "updated with transaction with commit id " +
                        lockQueues.get(lock.getEntityLocked()).maxCommitId
                        + " so later transaction will be aborted.");
                    writeConflicts.add(lock.getTxnId());
                  } else {
                    toAcquire.add(lock);
                    if (LOG.isDebugEnabled()) {
                      LOG.debug("Adding lock " + lock.getTxnId() + "." + lock.getLockId() +
                          " to list of locks to acquire");
                    }
                  }
                } else {
                  if (LOG.isDebugEnabled()) {
                    LOG.debug("Not acquiring lock " + lock.getTxnId() + "." + lock.getLockId() +
                        " of type " + lock.getType().toString() + " as lock " + lastLock.getTxnId()
                        + "." + lastLock.getLockId() + " of type " + lastLock.getType() + " is ahead of" +
                        " it in state " + lastLock.getState().toString());
                  }
                  // If we can't acquire then nothing behind us can either
                  // TODO prove to yourself this is true
                  break;
                }
              }
              lastLock = lock;
            }
          }
          // TODO I think I can get away with this because I'm not changing the structures just
          // the values of the locks themselves.  It's possible another reader would see an
          // intermittent state where some of the locks are acquired and some aren't, but the
          // worst that should happen there is they wait a bit when they don't have to.  That
          // seems better than locking everyone out while I do all this.
          for (HiveLock lock : toAcquire) {
            lock.setState(LockState.ACQUIRED);
          }
          waitForWal = wal.queueLockAcquisition(toAcquire);
        } catch (IOException e) {
          throw new RuntimeException("LockKeeper.close doesn't throw, how did this happen?", e);
        } // Now outside read lock

        // TODO I think I can do this after I'm out of the lock.  But I want to wait for the
        // results so that this thread does not run again and rediscover these as an issue.
        // Abort the transactions that ran into write conflicts.  We logged this above, so no
        // need to log it here.  Do it in a separate thread as this could take a bit and we
        // have other things to do.
        if (writeConflicts.size() > 0) {
          for (long txnId : writeConflicts) {
            abortTxnInternal(openTxns.get(txnId));
          }
        }

        try {
          waitForWal.get();
        } catch (InterruptedException|ExecutionException e) {
          LOG.error("Unable to record lock acquisition in the WAL", e);
          selfDestruct();
        }

        // Notify any waiters to go look for their locks
        synchronized (this) {
          this.notifyAll();
        }
      }
    }

    private boolean twoLocksCompatible(LockType holder, LockType requester) {
      return lockCompatibilityTable[holder.ordinal()][requester.ordinal()];
    }
  };

  // Detect deadlocks in the lock graph
  private Runnable deadlockDetector = new Runnable() {
    // Rather than follow the general pattern of go through all the entries and find potentials
    // and then remove all potentials this thread kills a deadlock as soon as it sees it.
    // Otherwise we'd likely be too aggressive and kill all participants in the deadlock.  If a
    // deadlock is detected it immediately schedules another run of itself so that it doesn't end
    // up taking minutes to find many deadlocks
    @Override
    public void run() {
      LOG.debug("Looking for deadlocks");

      OpenTransaction deadlocked = lookForCycles();
      if (deadlocked != null) {
        abortTxnInternal(openTxns.get(deadlocked.getTxnId()));
        threadPool.submit(this);
      }
    }

    private OpenTransaction lookForCycles() {
      try (LockKeeper lk = new LockKeeper(masterLock.readLock())) {
        // We're looking only for transactions that have 1+ acquired locks and 1+ waiting locks
        for (OpenTransaction txn : openTxns.values()) {
          boolean sawAcquired = false, sawWaiting = false;
          for (HiveLock lock : txn.getHiveLocks()) {
            if (lock.getState() == LockState.WAITING) {
              sawWaiting = true;
            } else if (lock.getState() == LockState.ACQUIRED) {
              sawAcquired = true;
            }
          }
          // Only check if we have both acquired and waiting locks.  A transaction might be in
          // a cycle without that, but it won't be key to the cycle without it.
          if (sawAcquired && sawWaiting) {
            if (lookForDeadlock(txn.getTxnId(), txn, true)) {
              LOG.warn("Detected deadlock, aborting transaction " + txn.getTxnId() +
                  " to resolve it");
              // It's easiest to always kill this one rather than try to figure out where in
              // the graph we can remove something and break the cycle.  Given that which txn
              // we examine first is mostly random this should be ok (I hope).
              return txn;
            }
          }
        }
      } catch (IOException e) {
        throw new RuntimeException("LockKeeper.close doesn't throw, how did this happen?", e);
      }
      return null;
    }

    /**
     * This looks for cycles in the lock graph.  It remembers the first transaction we started
     * looking at, and if it gets back to that transaction then it returns true.
     * @param initialTxnId Starting transaction in the graph traversal.
     * @param currentTxn Current transaction in the graph traversal.
     * @param initial Whether this call is the first time this is called, so initialTxn should
     *                equals currentTxn
     * @return true if a cycle is detected.
     */
    private boolean lookForDeadlock(long initialTxnId, OpenTransaction currentTxn,
                                    boolean initial) {
      if (!initial && initialTxnId == currentTxn.getTxnId()) return true;
      for (HiveLock lock : currentTxn.getHiveLocks()) {
        if (lock.getState() == LockState.WAITING) {
          // We need to look at all of the locks ahead of this lock in it's queue
          for (HiveLock predecessor :
              lockQueues.get(lock.getEntityLocked()).queue.headMap(lock.getTxnId()).values()) {
            if (lookForDeadlock(initialTxnId, openTxns.get(predecessor.getTxnId()), false)) {
              return true;
            }
          }
        }
      }
      return false;
    }
  };

  // Look for any transactions that have timed out.
  private Runnable timedOutCleaner = new Runnable() {
    @Override
    public void run() {
      LOG.debug("Running timeout cleaner");
      // First get the read lock and find all of the potential timeouts.
      List<OpenTransaction> potentials = new ArrayList<>();
      long now = System.currentTimeMillis();
      long txnTimeout =
          conf.getTimeVar(HiveConf.ConfVars.HIVE_TXN_TIMEOUT, TimeUnit.MILLISECONDS);
      try (LockKeeper lk = new LockKeeper(masterLock.readLock())) {
        for (OpenTransaction txn : openTxns.values()) {
          if (txn.getLastHeartbeat() + txnTimeout < now) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("Adding " + txn.getTxnId() + " to list of potential timeouts");
            }
            potentials.add(txn);
          }
        }
      } catch (IOException e) {
        throw new RuntimeException("LockKeeper.close doesn't throw, how did this happen?", e);
      }

      // Now go back through the potentials list, holding the write lock, and remove any that
      // still haven't heartbeat
      try (LockKeeper lk = new LockKeeper(masterLock.writeLock())) {
        for (OpenTransaction txn : potentials) {
          if (txn.getLastHeartbeat() + txnTimeout < now) {
            LOG.info("Aborting transaction " + txn.getTxnId() + " due to heartbeat timeout");
            abortTxnInternal(txn);
          }
        }
      } catch (IOException e) {
        LOG.warn("Caught exception aborting transaction", e);
      }
    }
  };

  // This looks through the list of committed transactions and figures out what can be
  // forgotten.
  private Runnable committedTxnCleaner = new Runnable() {
    @Override
    public void run() {
      try {
        LOG.debug("Running committed transaction cleaner");
        Set<Long> forgetableCommitIds = new HashSet<>();
        List<CommittedTransaction> forgetableTxns = new ArrayList<>();
        List<Long> forgetableTxnIds = new ArrayList<>();
        Map<LockQueue, Long> forgetableDtps = new HashMap<>();
        long minOpenTxn;
        try (LockKeeper lk = new LockKeeper(masterLock.readLock())) {
          minOpenTxn = findMinOpenTxn();
          for (CommittedTransaction txn : committedTxns.values()) {
            // Look to see if all open transactions have a txnId greater than this txn's commitId
            if (txn.getCommitId() <= minOpenTxn) {
              if (LOG.isDebugEnabled()) {
                LOG.debug("Adding " + txn.getTxnId() + " to list of forgetable transactions");
              }
              forgetableTxns.add(txn);
              forgetableTxnIds.add(txn.getTxnId());
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
        if (forgetableTxns.size() > 0) {
          try (LockKeeper lk = new LockKeeper(masterLock.writeLock())) {
            committedTxns.keySet().removeAll(forgetableTxnIds);
            wal.queueForgetTransactions(forgetableTxns);
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
      } catch (IOException e) {
        LOG.warn("Caught exception cleaning committed transactions", e);
      }
    }
  };

  // Keep the lockQueues and abortedWrites maps from growing indefinitely by removing any entries
  // with empty lists.
  private Runnable queueShrinker = new Runnable() {
    @Override
    public void run() {
      // Rather than hold the write lock while we walk the entire set of queues we walk the
      // queues under the read lock, remembering what we've found, and then grab the write lock
      // only when we're done and know what to remove.  Before we removing we have to check again
      // to make sure another thread didn't start using the empty list in the meantime.
      List<EntityKey> emptyQueues = new ArrayList<>();
      List<EntityKey> emptyAbortedWrites = new ArrayList<>();
      try (LockKeeper lk = new LockKeeper(masterLock.readLock())) {
        for (Map.Entry<EntityKey, LockQueue> entry : lockQueues.entrySet()) {
          if (entry.getValue().queue.size() == 0 && entry.getValue().maxCommitId == 0) {
            emptyQueues.add(entry.getKey());
          }
        }

        for (Map.Entry<EntityKey, List<AbortedTransaction>> entry : abortedWrites.entrySet()) {
          if (entry.getValue().size() == 0) {
            emptyAbortedWrites.add(entry.getKey());
          }
        }

      } catch (IOException e) {
        throw new RuntimeException("LockKeeper.close doesn't throw, how did this happen?", e);
      }

      if (emptyAbortedWrites.size() > 0 || emptyQueues.size() > 0) {
        try (LockKeeper lk = new LockKeeper(masterLock.writeLock())) {
          for (EntityKey entityKey : emptyQueues) {
            LockQueue queue = lockQueues.get(entityKey);
            if (queue.queue.size() == 0 && queue.maxCommitId == 0) {
              lockQueues.remove(entityKey);
            }
          }

          for (EntityKey entityKey : emptyAbortedWrites) {
            if (abortedWrites.get(entityKey).size() == 0) {
              abortedWrites.remove(entityKey);
            }
          }
        } catch (IOException e) {
          throw new RuntimeException("LockKeeper.close doesn't throw, how did this happen?", e);
        }
      }
    }
  };

  // A thread that checks for aborted transactions with no more aborted locks to track and
  // removes them.
  private Runnable abortedTxnForgetter = new Runnable() {
    @Override
    public void run() {
      List<AbortedTransaction> forgettable = new ArrayList<>();
      try (LockKeeper lk = new LockKeeper(masterLock.readLock())) {
        for (AbortedTransaction aborted : abortedTxns.values()) {
          if (aborted.fullyCompacted()) forgettable.add(aborted);
        }
      } catch (IOException e) {
        throw new RuntimeException("LockKeeper.close doesn't throw, how did this happen?", e);
      }

      if (forgettable.size() > 0) {
        try (LockKeeper lk = new LockKeeper(masterLock.writeLock())) {
          for (AbortedTransaction aborted : forgettable) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("Forgetting aborted transaction " + aborted.getTxnId());
            }
            abortedTxns.remove(aborted.getTxnId());
          }
          wal.queueForgetTransactions(forgettable);
        } catch (IOException e) {
          throw new RuntimeException("LockKeeper.close doesn't throw, how did this happen?", e);
        }
      }
    }
  };

  // We don't want to expose the internal structures, even for tests to look at.  But they need
  // some way to see them.  So provide methods that will make point in time copies.
  @VisibleForTesting Map<Long, OpenTransaction> copyOpenTxns() {
    return new HashMap<>(openTxns);
  }

  @VisibleForTesting Map<Long, AbortedTransaction> copyAbortedTxns() {
    return new HashMap<>(abortedTxns);
  }

  @VisibleForTesting Map<Long, CommittedTransaction> copyCommittedTxns() {
    return new HashMap<>(committedTxns);
  }

  @VisibleForTesting Map<EntityKey, LockQueue> copyLockQueues() {
    return new HashMap<>(lockQueues);
  }

  @VisibleForTesting List<EntityKey> copyLockQueuesToCheck() {
    return new ArrayList<>(lockQueuesToCheck);
  }

  @VisibleForTesting Map<EntityKey, List<AbortedTransaction>> copyAbortedWrites() {
    return new HashMap<>(abortedWrites);
  }

  // When unit testing is set we don't run all the threads in the background.  Give the unit
  // tests the ability to force run some of the threads
  @VisibleForTesting void forceAbortedTxnForgetter() {
    abortedTxnForgetter.run();
  }

  @VisibleForTesting void forceQueueShrinker() {
    queueShrinker.run();
  }

  @VisibleForTesting void forceCommittedTxnCleaner() {
    committedTxnCleaner.run();
  }

  @VisibleForTesting void forceTimedOutCleaner() {
    timedOutCleaner.run();
  }

  @VisibleForTesting void forceDeadlockDetector() {
    deadlockDetector.run();
  }
}
