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
import org.apache.hadoop.hive.metastore.api.LockLevel;
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

import javax.annotation.Nonnull;
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
import java.util.TreeMap;
import java.util.TreeSet;
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

  // TODO - make creation of intention locks automatic, as we need it all the time anyway

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

  @VisibleForTesting static HiveConf getHiveConf() {
    return conf;
  }
  /**
   * Get the transaction manager instance.  In the case where the transaction manager runs into
   * an error and dies this will generate a new one.  So it is best to call this each time rather
   * than keeping a reference.
   * @return a reference to the TransactionManager singleton.
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

  // This is just a typedef, but I get tired of typing all the generics everywhere
  @VisibleForTesting static class LockQueue extends TreeMap<Long, HiveLock> {
  }

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

  // Map of committed transactions by transaction id
  private Map<Long, CommittedTransaction> committedTxnsByTxnId;

  // Map of committed transactions, but it is a map of commit id to committed transaction, not
  // transaction id.
  private Map<Long, CommittedTransaction> committedTxnsByCommitId;

  // A structure to store the locks according to which database/table/partition they lock.
  // The sorted map is keyed by lock id
  private Map<EntityKey, LockQueue> lockQueues;

  // Lock queues that should be checked for whether a lock can be acquired.
  private List<EntityKey> lockQueuesToCheck;

  // Map of objects that have (possibly) been written to by aborted transactions.  This helps us
  // know where to look after a compaction to see which aborted transactions we can forget.
  private Map<EntityKey, List<AbortedTransaction>> abortedWrites;

  // Map of entities that have been written to in committed transactions.  The set is of commit
  // ids, because what we care about is at what point the transaction committed.
  private Map<EntityKey, Set<Long>> committedWrites;

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
  // This is set to true once we've self destructed, since Java doesn't allow delete(this)
  private boolean hesDeadJim;
  private int maxOpenTxns;
  private Class<? extends WriteSetRetriever> writeSetRetriever;

  private TransactionManager() {
    LOG.info("Initializing the TransactionManager...");
    // Initialize TxnHandler.
    super.setConf(conf);
    hesDeadJim = false;
    masterLock = new ReentrantReadWriteLock();
    openTxns = new HashMap<>();
    abortedTxns = new HashMap<>();
    committedTxnsByTxnId = new HashMap<>();
    committedTxnsByCommitId = new HashMap<>();
    abortedWrites = new HashMap<>();
    committedWrites = new HashMap<>();
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
    lockCompatibilityTable[LockType.SHARED_WRITE.ordinal()][LockType.SHARED_WRITE.ordinal()] = true;
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
      threadPool.scheduleAtFixedRate(txnForgetter, period + rand.nextInt((int)period),
          period, TimeUnit.MILLISECONDS);

      period = conf.getTimeVar(HiveConf.ConfVars.TXNMGR_INMEM_LOCK_QUEUE_SHRINKER_THREAD_PERIOD,
          TimeUnit.MILLISECONDS);
      threadPool.scheduleAtFixedRate(queueShrinker, period + rand.nextInt((int)period), period,
          TimeUnit.MILLISECONDS);

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
   * It is also useful for tests that want to force the transaction manager to rebuild itself and
   * force new state.
   * @param lastWords Log message to write out before we die.
   * @param t Exception that was received that is leading to self destruction.
   * @return a RuntimeException.  This is returned rather than thrown for two reasons.  One, it
   * works better with the compiler if callers of this class explicitly throw, since the compiler
   * doesn't look through to this call and see that it always does.  Two, it works better for
   * unit tests that want a way to completely shutdown the TransactionManager but don't want to
   * have exceptions thrown in the process.
   */
  RuntimeException selfDestruct(String lastWords, Throwable t) {
    if (t == null) LOG.error(lastWords);
    else LOG.error(lastWords, t);
    hesDeadJim = true;
    threadPool.shutdownNow(); // This will terminate the threads in the WAL as well.
    synchronized (TransactionManager.class) {
      self = null;
    }
    return new RuntimeException("Transaction manager self destructed.");
  }

  RuntimeException selfDestruct(String lastWords) {
    return selfDestruct(lastWords, null);
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
      throw selfDestruct("Unable to record transaction open in the WAL", e);
    }
    return new OpenTxnsResponse(Collections.singletonList(txn.getTxnId()));
  }

  private OpenTxnsResponse openMultipleTxns(OpenTxnRequest rqst) throws MetaException {
    checkAlive();
    // TODO
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
      txn = assureTxnOpen(rqst.getTxnid(), "abort");
    } catch (IOException e) {
      throw new RuntimeException("LockKeeper.close doesn't throw, how did this happen?", e);
    }
    abortTxnInternal(txn);
  }

  // This call will acquire the write lock.  So you need to either have it or have no locks.
  private void abortTxnInternal(OpenTransaction openTxn) {
    HiveLock[] locks = openTxn.getHiveLocks();
    AbortedTransaction abortedTxn = new AbortedTransaction(openTxn);
    Future<Integer> waitForWal;
    try (LockKeeper lk = new LockKeeper(masterLock.writeLock())) {
      waitForWal = wal.queueAbortTxn(openTxn);
      openTxns.remove(openTxn.getTxnId());
      // We only need to remember this transaction in the aborted queue if it had some shared
      // write locks.  Otherwise, it can't have done anything we care about remembering.
      if (locks != null && locks.length > 0) {
        for (HiveLock lock : locks) {
          lockQueues.get(lock.getEntityLocked()).remove(lock.getLockId());
          lockQueuesToCheck.add(lock.getEntityLocked());
          // Remember that we might have written to this object
          if (lock.getType() == LockType.SHARED_WRITE) {
            List<AbortedTransaction> aborted = abortedWrites.get(lock.getEntityLocked());
            if (aborted == null) {
              aborted = new ArrayList<>();
              abortedWrites.put(lock.getEntityLocked(), aborted);
            }
            aborted.add(abortedTxn);
          }
        }
      }
      // Only remember the aborted transaction if it may have written data
      if (abortedTxn.getWriteSets() != null) abortedTxns.put(abortedTxn.getTxnId(), abortedTxn);
    } catch (IOException e) {
      throw new RuntimeException("LockKeeper.close doesn't throw, how did this happen?", e);
    }

    if (locks != null && locks.length > 0) {
      LOG.debug("Requesting lockChecker run");
      waitForLockChecker = threadPool.submit(lockChecker);
    }

    try {
      waitForWal.get();
    } catch (InterruptedException|ExecutionException e) {
      throw selfDestruct("Unable to record transaction abort in the WAL", e);
    }
  }

  @Override
  public void abortTxns(AbortTxnsRequest rqst) throws NoSuchTxnException, MetaException {
    checkAlive();
    // TODO
    throw new UnsupportedOperationException();
  }

  // TODO handle dynamic partition locks where we'll have both table and partition locks and we
  // need to not check conflicts on the table level.
  @Override
  public void commitTxn(CommitTxnRequest rqst) throws NoSuchTxnException, TxnAbortedException,
      MetaException {
    try {
      LOG.debug("Received request to commit transaction " + rqst.getTxnid());
      // Step 1, figure out if there are any potential conflicts for which we need to go fetch
      // write sets.  Do this under the read lock.
      OpenTransaction openTxn;
      final CommittedTransaction committedTxn;
      Map<EntityKey, List<Future<CommittedTransaction>>> futurePotentialConflicts = new HashMap<>();
      Map<EntityKey, Future<CommittedTransaction>> myFuture = new HashMap<>();
      try (LockKeeper lk = new LockKeeper(masterLock.readLock())) {
        openTxn = assureTxnOpen(rqst.getTxnid(), "commit");
        // We need to create the committedTxn so we can generate the writeSets.
        committedTxn = new CommittedTransaction(openTxn, nextTxnId);
        if (committedTxn.getWriteSets() != null) {
          LOG.debug("Our write sets are not null");
          for (Map.Entry<EntityKey, Set<WriteSetRecordIdentifier>> entry : committedTxn.getWriteSets().entrySet()) {

            Set<Long> potentialConflicts = committedWrites.get(entry.getKey());
            if (potentialConflicts == null) continue;
            LOG.debug("potential conflicts are not null");

            // Now we need to check each potential conflict.  We do this in parallel in the
            // background.  It's possible another commit has already fetched these.
            boolean sawAtLeastOne = false;
            for (long possibleConflictingCommitId : potentialConflicts) {
              if (possibleConflictingCommitId > committedTxn.getTxnId() &&
                  possibleConflictingCommitId <= committedTxn.getCommitId()) {
                LOG.debug("Found possible conflicting transaction with commit id " +
                    possibleConflictingCommitId);
                CommittedTransaction possibleConflict =
                    committedTxnsByCommitId.get(possibleConflictingCommitId);
                Set<WriteSetRecordIdentifier> theirRecordIds =
                    possibleConflict.getWriteSets().get(entry.getKey());
                if (theirRecordIds == null) {
                  LOG.debug("Scheduling fetch of records ");
                  // We have to go read their records
                  List<Future<CommittedTransaction>> list =
                      futurePotentialConflicts.get(entry.getKey());
                  if (list == null) {
                    list = new ArrayList<>();
                    futurePotentialConflicts.put(entry.getKey(), list);
                  }
                  list.add(scheduleWriteSetRetriever(possibleConflict, entry.getKey()));
                  sawAtLeastOne = true;
                }
              }
            }
            // Make sure we only fetch our own if we saw a potential conflict
            if (entry.getValue() == null && sawAtLeastOne) {
              LOG.debug("Scheduling fetch of records for our own writeSets");
              myFuture.put(entry.getKey(), scheduleWriteSetRetriever(committedTxn, entry.getKey()));
            }
          }
        }
      } catch (IOException e) {
        throw new RuntimeException("LockKeeper.close doesn't throw, how did this happen?", e);
      }

      // Step 2, go fetch any write sets, not under the read lock.
      Map<EntityKey, List<CommittedTransaction>> potentialConflicts =
          new HashMap<>(futurePotentialConflicts.size());
      try {
        for (EntityKey entityKey : myFuture.keySet()) {
          LOG.debug("Checking for conflicts in " + entityKey);
          myFuture.get(entityKey).get();
          for (Future<CommittedTransaction> potentialConflictingTxn : futurePotentialConflicts.get(entityKey)) {
            List<CommittedTransaction> list = potentialConflicts.get(entityKey);
            if (list == null) {
              list = new ArrayList<>();
              potentialConflicts.put(entityKey, list);
            }
            list.add(potentialConflictingTxn.get());
          }
        }
      } catch (InterruptedException e) {
        throw selfDestruct("Interuppted while reading record ids from storage, guessing this " +
            "means it is time to die", e);
      } catch (ExecutionException e) {
        LOG.warn("Failed to read recordIds from storage, aborting transaction " +
            rqst.getTxnid() + "because we cannot tell if there were conflicts or not.");
        abortTxnInternal(openTxn);
        throw new TxnAbortedException("We cannot tell if a previous transaction had a write " +
            "conflict with your transaction");
      }

      // Step 3, do the join of write sets to check for any conflicts.  We do this outside the read
      // lock.
      for (EntityKey entityKey : myFuture.keySet()) {
        Set<WriteSetRecordIdentifier> myIds = committedTxn.getWriteSets().get(entityKey);
        assert myIds != null;
        for (CommittedTransaction potential : potentialConflicts.get(entityKey)) {
          Set<WriteSetRecordIdentifier> theirIds = potential.getWriteSets().get(entityKey);
          assert theirIds != null;
          for (WriteSetRecordIdentifier myId : myIds) {
            if (theirIds.contains(myId)) {
              // We've found a conflict, we're done.
              LOG.info("Found that txn " + committedTxn.getTxnId() + " with commitId " +
                  committedTxn.getCommitId() + " had conflict with transaction " +
                  potential.getTxnId() + " with commitId " + potential.getCommitId() +
                  ".  Conflicting record: " + myId);
              abortTxnInternal(openTxn);
              throw new TxnAbortedException("A previous transaction had a write conflict with " +
                  "your transaction");
            }
          }
        }
      }

      // Step 4, get the write lock and actually commit the transaction.  We need to first check that
      // the transaction is still open.
      Future<Integer> waitForWal = null;
      try (LockKeeper lk = new LockKeeper(masterLock.writeLock())) {
        // We need to refetch the open transaction to guarantee that it's still open.
        openTxn = assureTxnOpen(rqst.getTxnid(), "commit");

        // Check that the commit ID hasn't moved.  If it has, make sure the committing
        // transaction didn't potentially conflict with this one.  If it did, give up and start over.
        if (committedTxn.getWriteSets() != null && nextTxnId > committedTxn.getCommitId()) {
          for (long otherCommitId = committedTxn.getCommitId() + 1; otherCommitId <= nextTxnId;
               otherCommitId++) {
            CommittedTransaction other = committedTxnsByCommitId.get(otherCommitId);
            if (other != null) {
              for (EntityKey entityKey : committedTxn.getWriteSets().keySet()) {
                if (other.getWriteSets().get(entityKey) != null) {
                  // Bummer, give up and start over
                  LOG.debug("CommitId moved while we checked conflicts and a commit done since then" +
                      " looks like it might conflict, so starting commit process over");
                  throw new RetryException();
                }
              }
            }
          }
        }

        HiveLock[] locks = openTxn.getHiveLocks();
        if (locks != null) {
          for (HiveLock lock : locks) {
            lockQueues.get(lock.getEntityLocked()).remove(lock.getLockId());
            lockQueuesToCheck.add(lock.getEntityLocked());
          }
          // Request a lockChecker run since we've released locks.
          LOG.debug("Requesting lockChecker run");
          waitForLockChecker = threadPool.submit(lockChecker);
        }

        openTxns.remove(rqst.getTxnid());
        // We only need to remember the transaction if it had write locks.  If it's read only or
        // DDL we can forget it.
        if (committedTxn.getWriteSets() != null) {
          // There's no need to move the transaction counter ahead
          waitForWal = wal.queueCommitTxn(committedTxn);

          // We'll need to remember this transaction for a bit
          committedTxnsByTxnId.put(rqst.getTxnid(), committedTxn);
          committedTxnsByCommitId.put(committedTxn.getCommitId(), committedTxn);

          if (LOG.isDebugEnabled()) {
            LOG.debug("Created new committed transaction with txn id " + rqst.getTxnid() +
                " and commitId " + committedTxn.getCommitId());
          }

          // Add our writes to committedWrites
          for (EntityKey entityKey : committedTxn.getWriteSets().keySet()) {
            Set<Long> commits = committedWrites.get(entityKey);
            if (commits == null) {
              commits = new TreeSet<>();
              committedWrites.put(entityKey, commits);
            }
            commits.add(committedTxn.getCommitId());
          }
        } else {
          waitForWal = wal.queueForgetTransactions(Collections.singletonList(openTxn));
          if (LOG.isDebugEnabled()) {
            LOG.debug("Forgetting transaction " + rqst.getTxnid() +
                " as it is committed and held no write locks");
          }
        }
      } catch (IOException e) {
        throw new RuntimeException("LockKeeper.close doesn't throw, how did this happen?", e);
      }
      try {
        if (waitForWal != null) waitForWal.get();
      } catch (InterruptedException | ExecutionException e) {
        throw selfDestruct("Unable to record transaction commit in the WAL", e);
      }
    } catch (RetryException e) {
      // This means we had to unwind it and try again
      commitTxn(rqst);
    }
  }

  private Future<CommittedTransaction>
  scheduleWriteSetRetriever(final CommittedTransaction committedTxn, final EntityKey entityKey) {
    try {
      if (writeSetRetriever == null) {
        String className = conf.getVar(HiveConf.ConfVars.TXNMGR_INMEM_WRITE_SET_RETRIEVER_IMPL);
        writeSetRetriever = (Class<? extends WriteSetRetriever>)Class.forName(className);

      }
      final WriteSetRetriever retriever = writeSetRetriever.newInstance();
      retriever.setEntityKey(entityKey);
      retriever.setTxnId(committedTxn.getTxnId());
      return threadPool.submit(new Callable<CommittedTransaction>() {
        @Override
        public CommittedTransaction call() throws Exception {
          retriever.readRecordIds();
          committedTxn.getWriteSets().put(entityKey, retriever.getRecordIds());
          return committedTxn;
        }
      });
    } catch (ClassNotFoundException|IllegalAccessException|InstantiationException e) {
      String className = conf.getVar(HiveConf.ConfVars.TXNMGR_INMEM_WRITE_SET_RETRIEVER_IMPL);
      throw selfDestruct("Unable to instantiate WriteSetRetriever " + className, e);
    }
  }

  static class DummyWriteSetRetriever extends WriteSetRetriever {
    @Override
    public void readRecordIds() throws IOException {
      recordIds = new HashSet<>();

    }
  }

  @Override
  public void heartbeat(HeartbeatRequest ids) throws NoSuchTxnException, NoSuchLockException,
      TxnAbortedException, MetaException {
    assert !ids.isSetLockid() : "Fail, we don't heartbeat locks anymore!";
    checkAlive();

    if (LOG.isDebugEnabled()) LOG.debug("Heartbeating txn " + ids.getTxnid());
    try (LockKeeper lk = new LockKeeper(masterLock.readLock())) {
      OpenTransaction txn = assureTxnOpen(ids.getTxnid(), "heartbeat");
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
    List<LockComponent> components = addIntentionLocks(rqst.getComponent());
    HiveLock[] hiveLocks = new HiveLock[components.size()];
    OpenTransaction txn;
    Future<?> lockCheckerRun;
    Future<Integer> waitForWal;
    try (LockKeeper lk = new LockKeeper(masterLock.writeLock())) {
      txn = assureTxnOpen(rqst.getTxnid(), "obtain lock in");

      for (int i = 0; i < components.size(); i++) {
        EntityKey key = new EntityKey(components.get(i));
        hiveLocks[i] =
            new HiveLock(nextLockId.getAndIncrement(), rqst.getTxnid(), key, components.get(i).getType());
        // Add to the appropriate DTP queue
        LockQueue queue = getQueue(hiveLocks[i].getEntityLocked());
        queue.put(hiveLocks[i].getLockId(), hiveLocks[i]);
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
    try {
      lockCheckerRun.get(lockPollTimeout, TimeUnit.MILLISECONDS);
    } catch (TimeoutException e) {
      // Fall through to wait case.
    } catch (InterruptedException|ExecutionException e) {
      throw selfDestruct("lockChecker threw exception", e);
    }

    if (checkMyLocks(hiveLocks) == LockState.ACQUIRED) {
      LOG.debug("Locks acquired immediately, returning");
      rsp = new LockResponse(rqst.getTxnid(), LockState.ACQUIRED);
    } else {
      // We didn't acquire right away, so long poll.  We won't wait forever, but we can wait a few
      // seconds to avoid the clients banging away every few hundred milliseconds to see if their
      // locks have acquired.
      LOG.debug("Locks did not acquire immediately, waiting...");
      rsp = waitForLocks(hiveLocks);
    }

    try {
      waitForWal.get();
    } catch (InterruptedException|ExecutionException e) {
      throw selfDestruct("Unable to record lock request in the WAL", e);
    }
    return rsp;
  }

  private List<LockComponent> addIntentionLocks(List<LockComponent> requested) {
    // For each table, add an intention lock for the db that contains it
    // For each partition, add an intention lock for the db and table that contain it
    // Make a copy of the list, as I'm not sure thrift uses expandable lists by default.
    List<LockComponent> allLocks = new ArrayList<>(requested);
    for (LockComponent lc : requested) {
      LockComponent intention;
      switch (lc.getLevel()) {
        case PARTITION:
          intention = new LockComponent(LockType.INTENTION, LockLevel.TABLE, lc.getDbname());
          intention.setTablename(lc.getTablename());
          allLocks.add(intention);
          // Fall through intentional

        case TABLE:
          intention = new LockComponent(LockType.INTENTION, LockLevel.DB, lc.getDbname());
          allLocks.add(intention);
          // Fall through intentional

        case DB:
          break;

        default:
          throw new RuntimeException("Unknown lock level " + lc.getLevel());
      }
    }
    return allLocks;
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
      OpenTransaction txn = assureTxnOpen(rqst.getTxnid(), "check locks");
      locks = txn.getHiveLocks();
      // Check initially as our locks may have already acquired
      if (checkMyLocks(locks) == LockState.ACQUIRED) {
        return new LockResponse(rqst.getTxnid(), LockState.ACQUIRED);
      }
    } catch (IOException e) {
      throw new RuntimeException("LockKeeper.close doesn't throw, how did this happen?", e);
    }
    // The locks haven't acquired yet, but wait a bit before we return and see if we get lucky.
    // This long polling reduces the network load on the metastore.
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
      // because we're actually covered by the table lock.
      OpenTransaction txn = assureTxnOpen(rqst.getTxnid(), "add dynamic partitions");

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
      throw selfDestruct("Unable to record lock acquisition in the WAL", e);
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
      EntityKey compacted = new EntityKey(info);
      try (LockKeeper lk = new LockKeeper(masterLock.readLock())) {
        List<AbortedTransaction> aborted = abortedWrites.remove(compacted);
        if (aborted != null && aborted.size() > 0) {
          for (AbortedTransaction abortedTxn : aborted) {
            abortedTxn.clearWriteSet(compacted);
          }
        }
      } catch (IOException e) {
        throw new RuntimeException("LockKeeper.close doesn't throw, how did this happen?", e);
      }
    }

    // We still need to call CompactionTxnHandler.markCompacted so it can make the change in the
    // database.
    super.markCompacted(info);
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
          // We also need to check that we don't have any write sets we're tracking that might
          // need to reference this
          Set<Long> commitIds = committedWrites.get(new EntityKey(ci));
          if (commitIds == null) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("Approving compaction for cleaning " + ci.getFullPartitionName());
            }
            approved.add(ci);
          } else {
            // It still might be ok, we just need to check that the highest txn it's going to
            // compact is less than any of the txn ids in our write sets
            boolean allClear = true;
            for (long commitId : commitIds) {
              if (committedTxnsByCommitId.get(commitId).getTxnId() <= ci.highestTxnId) {
                allClear = false;
                break;
              }
            }
            if (allClear) {
              if (LOG.isDebugEnabled()) {
                LOG.debug("Approving compaction for cleaning " + ci.getFullPartitionName());
              }
              approved.add(ci);
            } else {
              if (LOG.isDebugEnabled()) {
                LOG.debug("Witholding cleaning for " + ci.getFullPartitionName() + " because we " +
                    "still have some committed transactions with record sets in that range");
              }
            }
          }
        }
      }
      toClean = approved;
    }
    return toClean;
  }

  private @Nonnull OpenTransaction assureTxnOpen(long id, String attemptedAction)
      throws NoSuchTxnException, TxnAbortedException {
    OpenTransaction txn = openTxns.get(id);
    if (txn == null) {
      if (abortedTxns.containsKey(id)) {
        throw new TxnAbortedException("Attempt to " + attemptedAction + " aborted transaction " + id);
      }
      throw new NoSuchTxnException("Attempt to " + attemptedAction + " non-existent transaction" + id);
    }
    return txn;
  }

  // This method assumes you are holding the read lock.  We could avoid this by making openTxns a
  // SortedMap rather than a HashMap.  But all the operations that need this are out of the
  // critical path, so it is better to let those in the critical path be O(1) and these less
  // critical ones be O(n) rather than forcing things in the critical path to be O(ln(n)) so
  // these can be O(1).
  private long findMinOpenTxn() {
    long minOpenTxn = nextTxnId;
    for (Long txnId : openTxns.keySet()) {
      minOpenTxn = Math.min(txnId, minOpenTxn);
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Found minimum open transaction of " + minOpenTxn);
    }
    return minOpenTxn;
  }

  // You MUST hold the write lock to call this method
  private LockQueue getQueue(EntityKey key) {
    LockQueue lockQueue = lockQueues.get(key);
    if (lockQueue == null) {
      lockQueue = new LockQueue();
      lockQueues.put(key, lockQueue);
    }
    return lockQueue;
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
            LockQueue queue = getQueue(entityKey);
            assert rs.getString(5).charAt(0) == TxnHandler.LOCK_WAITING ||
                rs.getString(5).charAt(0) == TxnHandler.LOCK_ACQUIRED;
            getLockList(openTxnLocks, txnId).add(hiveLock);
            queue.put(hiveLock.getLockId(), hiveLock);
          }

          // Put each of the locks into their transactions
          addLocksToTransactions(openTxnLocks, openTxns, "acquired and/or waiting", "open");
          for (Map.Entry<Long, List<HiveLock>> entry : openTxnLocks.entrySet()) {
            OpenTransaction txn = openTxns.get(entry.getKey());
            if (txn == null) {
              StringBuilder msg = new StringBuilder("Found acquired or waiting locks with no ")
                  .append("associated open transaction, we are in trouble!  Transaction id ")
                  .append(entry.getKey())
                  .append(" lock ids ");
              for (HiveLock hiveLock : entry.getValue()) {
                msg.append(hiveLock.getLockId());
                msg.append(", ");
              }
              throw selfDestruct(msg.toString());
            }
            txn.addLocks(entry.getValue().toArray(new HiveLock[entry.getValue().size()]));
          }

          // Use the transactions component table to rebuild write sets for aborted transactions
          if (abortedTxns.size() > 0) {
            StringBuilder buf = new StringBuilder("select tc_txnid, tc_database, tc_table, tc_partition")
                .append(" from TXN_COMPONENTS")
                .append(" where tc_txnid in (");
            boolean first = true;
            for (long txnId : abortedTxns.keySet()) {
              if (first) first = false;
              else buf.append(", ");
              buf.append(txnId);
            }
            buf.append(')');

            if (LOG.isDebugEnabled()) LOG.debug("Going to execute query " + buf.toString());
            rs = stmt.executeQuery(buf.toString());

            while (rs.next()) {
              AbortedTransaction abortedTxn = abortedTxns.get(rs.getLong(1));
              EntityKey entityKey = new EntityKey(rs.getString(3), rs.getString(4), rs.getString(5));
              abortedTxn.addWriteSet(entityKey);
              List<AbortedTransaction> writes = abortedWrites.get(entityKey);
              if (writes == null) {
                writes = new ArrayList<>();
                abortedWrites.put(entityKey, writes);
              }
              writes.add(abortedTxn);
            }
          }

          // Use the WriteSet table to rebuild information about committed transactions
          sql = "select ws_txnid, ws_commit_id, ws_database, ws_table, ws_partition from WRITE_SET";
          if (LOG.isDebugEnabled()) LOG.debug("Going to execute query " + sql);
          rs = stmt.executeQuery(sql);
          while (rs.next()) {
            long txnId = rs.getLong(1);
            long commitId = rs.getLong(2);
            CommittedTransaction committedTxn = committedTxnsByTxnId.get(txnId);
            if (committedTxn == null) {
              committedTxn = new CommittedTransaction(txnId, commitId);
              committedTxnsByTxnId.put(txnId, committedTxn);
              committedTxnsByCommitId.put(commitId, committedTxn);
            }
            EntityKey entityKey = new EntityKey(rs.getString(3), rs.getString(4), rs.getString(5));
            committedTxn.addWriteSet(entityKey);
            Set<Long> commitIds = committedWrites.get(entityKey);
            if (commitIds == null) {
              commitIds = new HashSet<>();
              committedWrites.put(entityKey, commitIds);
            }
            commitIds.add(commitId);
          }
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
        List<HiveLock> acquired = new ArrayList<>();
        Future<Integer> waitForWal = null;
        try (LockKeeper lk = new LockKeeper(masterLock.readLock())) {
          // Many keys may have been added to the queue, grab them all so we can do this just
          // once.
          for (EntityKey key : keys) {
            LockQueue queue = lockQueues.get(key);
            HiveLock lastLock = null;
            for (HiveLock lock : queue.values()) {
              if (lock.getState() == LockState.WAITING) {
                // See if we can acquire this lock
                if (lastLock == null || twoLocksCompatible(lastLock.getType(), lock.getType())) {
                  // TODO I think I can get away with this because I'm not changing the structures just
                  // the values of the locks themselves.  It's possible another reader would see an
                  // intermittent state where some of the locks are acquired and some aren't, but the
                  // worst that should happen there is they wait a bit when they don't have to.  That
                  // seems better than locking everyone out while I do all this.
                  lock.setState(LockState.ACQUIRED);
                  acquired.add(lock);
                  if (LOG.isDebugEnabled()) {
                    LOG.debug("Adding lock " + lock.getTxnId() + "." + lock.getLockId() +
                        " to list of locks to acquire");
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
          if (acquired.size() > 0) waitForWal = wal.queueLockAcquisition(acquired);
        } catch (IOException e) {
          throw new RuntimeException("LockKeeper.close doesn't throw, how did this happen?", e);
        } // Now outside read lock

        if (waitForWal != null) {
          try {
            waitForWal.get();
          } catch (InterruptedException | ExecutionException e) {
            throw selfDestruct("Unable to record lock acquisition in the WAL", e);
          }
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
            LOG.debug("Found a potential deadlock component in transaction " + txn.getTxnId());
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
              lockQueues.get(lock.getEntityLocked()).headMap(lock.getLockId()).values()) {
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

  // Keep the lockQueues and abortedWrites maps from growing indefinitely by removing any entries
  // with empty lists.
  private Runnable queueShrinker = new Runnable() {
    @Override
    public void run() {
      // Rather than hold the write lock while we walk the entire set of queues we walk the
      // queues under the read lock, remembering what we've found, and then grab the write lock
      // only when we're done and know what to remove.  Before removing we have to check again
      // to make sure another thread didn't start using the empty list in the meantime.
      List<EntityKey> emptyQueues = new ArrayList<>();
      List<EntityKey> emptyAbortedWrites = new ArrayList<>();
      List<EntityKey> emptyCommittedWrites = new ArrayList<>();
      try (LockKeeper lk = new LockKeeper(masterLock.readLock())) {
        for (Map.Entry<EntityKey, LockQueue> entry : lockQueues.entrySet()) {
          if (entry.getValue().size() == 0) {
            emptyQueues.add(entry.getKey());
          }
        }

        for (Map.Entry<EntityKey, List<AbortedTransaction>> entry : abortedWrites.entrySet()) {
          if (entry.getValue().size() == 0) {
            emptyAbortedWrites.add(entry.getKey());
          }
        }

        for (Map.Entry<EntityKey, Set<Long>> entry : committedWrites.entrySet()) {
          if (entry.getValue().size() == 0) {
            emptyCommittedWrites.add(entry.getKey());
          }
        }

      } catch (IOException e) {
        throw new RuntimeException("LockKeeper.close doesn't throw, how did this happen?", e);
      }

      try (LockKeeper lk = new LockKeeper(masterLock.writeLock())) {
        for (EntityKey entityKey : emptyQueues) {
          // Double check, someone may have added something while we were waiting for the write lock
          if (lockQueues.get(entityKey).size() == 0) lockQueues.remove(entityKey);
        }

        for (EntityKey entityKey : emptyAbortedWrites) {
          // Double check, someone may have added something while we were waiting for the write lock
          if (abortedWrites.get(entityKey).size() == 0) abortedWrites.remove(entityKey);
        }

        for (EntityKey entityKey : emptyCommittedWrites) {
          // Double check, someone may have added something while we were waiting for the write lock
          if (committedWrites.get(entityKey).size() == 0) committedWrites.remove(entityKey);
        }

      } catch (IOException e) {
        throw new RuntimeException("LockKeeper.close doesn't throw, how did this happen?", e);
      }

    }
  };

  // A thread that checks for transactions that we can forget.  Aborted transactions can be
  // forgotten when all their writeSets have been compacted.  Committed transactions can be
  // forgotten when their commitId < minOpenTxnId.
  private Runnable txnForgetter = new Runnable() {
    @Override
    public void run() {
      List<AbortedTransaction> forgettableAborted = new ArrayList<>();
      List<CommittedTransaction> forgettableCommitted = new ArrayList<>();
      long minOpenTxn;
      try (LockKeeper lk = new LockKeeper(masterLock.readLock())) {
        for (AbortedTransaction aborted : abortedTxns.values()) {
          if (aborted.fullyCompacted()) forgettableAborted.add(aborted);
        }
        minOpenTxn = findMinOpenTxn();
        for (CommittedTransaction committedTxn : committedTxnsByTxnId.values()) {
          if (committedTxn.getCommitId() <= minOpenTxn) {
            forgettableCommitted.add(committedTxn);
          }
        }
      } catch (IOException e) {
        throw new RuntimeException("LockKeeper.close doesn't throw, how did this happen?", e);
      }

      if (forgettableAborted.size() > 0 || forgettableCommitted.size() > 0) {
        try (LockKeeper lk = new LockKeeper(masterLock.writeLock())) {
          for (AbortedTransaction aborted : forgettableAborted) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("Forgetting aborted transaction " + aborted.getTxnId());
            }
            abortedTxns.remove(aborted.getTxnId());
          }
          for (CommittedTransaction committedTxn : forgettableCommitted) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("Forgetting committed transaction " + committedTxn.getTxnId());
            }
            committedTxnsByTxnId.remove(committedTxn.getTxnId());
            committedTxnsByCommitId.remove(committedTxn.getCommitId());

            // Clean out committedWrites as well
            Map<EntityKey, Set<WriteSetRecordIdentifier>> writeSets = committedTxn.getWriteSets();
            if (writeSets != null) {
              for (EntityKey entityKey : writeSets.keySet()) {
                Set<Long> commitIds = committedWrites.get(entityKey);
                if (commitIds != null) commitIds.remove(committedTxn.getCommitId());
              }
            }
          }
          wal.queueForgetTransactions(forgettableAborted);
        } catch (IOException e) {
          throw new RuntimeException("LockKeeper.close doesn't throw, how did this happen?", e);
        }
      }
    }
  };

  //
  //

  /**
   * We don't want to expose the internal structures, even for tests to look at.  But they need
   * some way to see them.  So provide methods that will make point in time copies.
   * @return copy of open transactions
   */
  @VisibleForTesting Map<Long, OpenTransaction> copyOpenTxns() {
    return new HashMap<>(openTxns);
  }

  @VisibleForTesting Map<Long, AbortedTransaction> copyAbortedTxns() {
    return new HashMap<>(abortedTxns);
  }

  @VisibleForTesting Map<Long, CommittedTransaction> copyCommittedTxnsByTxnId() {
    return new HashMap<>(committedTxnsByTxnId);
  }

  @VisibleForTesting Map<Long, CommittedTransaction> copyCommittedTxnsByCommitId() {
    return new HashMap<>(committedTxnsByCommitId);
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

  @VisibleForTesting Map<EntityKey, Set<Long>> copyCommittedWrites() {
    return new HashMap<>(committedWrites);
  }

  /**
   * When unit testing is set we don't run all the threads in the background.  Give the unit
   * tests the ability to force run some of the threads
   */
  @VisibleForTesting void forceTxnForgetter() {
    txnForgetter.run();
  }

  @VisibleForTesting void forceQueueShrinker() {
    queueShrinker.run();
  }

  @VisibleForTesting void forceTimedOutCleaner() {
    timedOutCleaner.run();
  }

  @VisibleForTesting void forceDeadlockDetector() {
    deadlockDetector.run();
  }

  /**
   * Make visible to the unit tests when the lockChecker has finished running so they know when
   * to check the lock states without sleeps that will certainly go wrong on a busy system.
   */
  @VisibleForTesting Future<?> waitForLockChecker;

  /**
   * A test class that will always return the same single row in record ids, thus (hopefully)
   * creating a conflict when checking writeSets.
   */
  @VisibleForTesting static class AlwaysConflictingRetriever extends WriteSetRetriever {
    @Override
    public void readRecordIds() throws IOException {
      recordIds.add(new WriteSetRecordIdentifier(1, 1, 1));
    }
  }

}
