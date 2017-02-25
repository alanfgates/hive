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

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.AbortTxnRequest;
import org.apache.hadoop.hive.metastore.api.CommitTxnRequest;
import org.apache.hadoop.hive.metastore.api.GetOpenTxnsResponse;
import org.apache.hadoop.hive.metastore.api.HeartbeatRequest;
import org.apache.hadoop.hive.metastore.api.LockComponent;
import org.apache.hadoop.hive.metastore.api.LockRequest;
import org.apache.hadoop.hive.metastore.api.LockResponse;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchLockException;
import org.apache.hadoop.hive.metastore.api.NoSuchTxnException;
import org.apache.hadoop.hive.metastore.api.OpenTxnRequest;
import org.apache.hadoop.hive.metastore.api.OpenTxnsResponse;
import org.apache.hadoop.hive.metastore.api.TxnAbortedException;
import org.apache.hadoop.hive.metastore.txn.CompactionTxnHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
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
 */
public class TransactionManager extends CompactionTxnHandler {

  static final private Logger LOG = LoggerFactory.getLogger(TransactionManager.class.getName());

  // This lock needs to be acquired in write mode only when modifying the structures that store
  // locks and transactions (e.g. opening, aborting, committing txns, adding locks).  To modify
  // locks or transactions (e.g. heartbeat) it is only needed in the read mode.  Anything looking
  // at locks or transactions should acquire it in the read mode.
  static private ReadWriteLock masterLock;

  // BEGIN THINGS PROTECTED BY masterLock
  // A list of all active transactions.
  static private Map<Long, OpenTransaction> openTxns;

  // List of aborted transactions, kept in memory for efficient reading when readers need a valid
  // transaction list.
  static private Map<Long, AbortedTransaction> abortedTxns;

  // A set of all committed transactions.
  static private Set<CommittedTransaction> committedTxns;

  // A structure to store the locks according to which database/table/partition they lock.
  static private Map<EntityKey, LockQueue> lockQueues;

  // Lock queues that should be checked for whether a lock can be acquired.
  static private List<EntityKey> lockQueuesToCheck;

  static private IdGenerator txnIdGenerator;
  static private IdGenerator lockIdGenerator;

  // END THINGS PROTECTED BY masterLock

  static private ScheduledThreadPoolExecutor threadPool;

  // Track what locks types are compatible.  First array is holder, second is requester
  private static boolean[][] lockCompatibilityTable;

  private HiveConf conf;
  private WriteAheadLog wal;

  public void setConf(HiveConf configuration) {
    conf = configuration;
    // Initialization done here, since it's where we have the opportunity
    staticInit();
    LOG.info("Initializing the TransactionManager...");
    wal = new NoopWal(); // TODO - make this configurable

    LOG.info("TransactionManager initialization compelte");
  }

  public HiveConf getConf() {
    return conf;
  }

  private static synchronized void staticInit() {
    if (masterLock == null) {
      LOG.info("Doing static initialization");
      masterLock = new ReentrantReadWriteLock();
      openTxns = new HashMap<>();
      abortedTxns = new HashMap<>();
      committedTxns = new HashSet<>();
      lockQueues = new HashMap<>();
      lockQueuesToCheck = new ArrayList<>();

      // TODO Handle reading initial nextTxnId from the database
      final long initialNextTxn = 1;
      txnIdGenerator = new IdGenerator() {
        long nextVal = initialNextTxn;

        @Override
        public long next() {
          return nextVal++;
        }

        @Override
        public long[] next(int num) {
          long[] ids = new long[num];
          for (int i = 0; i < num; i++) ids[i] = nextVal++;
          return ids;
        }

        @Override
        public long current() {
          return nextVal;
        }
      };

      // This can always start new.  Locks don't have any relationship across transactions
      lockIdGenerator = new IdGenerator() {
        long nextVal = 1;

        @Override
        public long next() {
          return nextVal++;
        }

        @Override
        public long[] next(int num) {
          long[] ids = new long[num];
          for (int i = 0; i < num; i++) ids[i] = nextVal++;
          return ids;
        }

        @Override
        public long current() {
          return nextVal;
        }
      };

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
      // TODO make base size of thread pool execute configurable
      threadPool = new ScheduledThreadPoolExecutor(10);
      // TODO make maximum size of thread pool execute configurable
      threadPool.setMaximumPoolSize(20);

      LOG.info("Static initialization complete");
    }
  }

  public TransactionManager() {

  }

  /**
   * Do a graceful shutdown.  This does not bother to flush the queues are try to get everything
   * on disk, as the WAL will already have it recorded.  It just handles shutting down resources
   * like the thread pools.
   */
  public void shutdown() {
    LOG.info("Shutting down the TransactionManager...");
    threadPool.shutdown();
    LOG.info("TransactionManager shutdown complete");
  }

  @Override
  public OpenTxnsResponse openTxns(OpenTxnRequest rqst) throws MetaException {
    // 99.9% of the time we're only opening one txn.  Special case it to avoid needing to create
    // extra arrays, lists, etc.
    if (rqst.getNum_txns() > 1) return openMultipleTxns(rqst);
    LOG.debug("Opening a transaction");

    Future<WriteAheadLog> waitForWal;
    OpenTransaction txn;
    try (LockKeeper lk = new LockKeeper(masterLock.writeLock())) {
      txn = new OpenTransaction(txnIdGenerator);
      openTxns.put(txn.getTxnId(), txn);

      waitForWal = wal.queueOpenTxn(txn.getTxnId(), rqst);
    } catch (IOException e) {
      // This is only here because LockKeeper.close has to throw an IOException because Closeable
      // .close does.  But in reality it never will, so this should never happen.
      throw new RuntimeException("This should never happen", e);
    }

    try {
      waitForWal.get();
    } catch (InterruptedException|ExecutionException e) {
      // This means we failed to record it in the WAL.  We could try to nicely unwind everything.
      //  But more realistically this likely means we're screwed and should just die.
      LOG.error("Unable to record transaction open in the WAL", e);
      throw new RuntimeException(e);
    }
    return new OpenTxnsResponse(Collections.singletonList(txn.getTxnId()));
  }

  private OpenTxnsResponse openMultipleTxns(OpenTxnRequest rqst) throws MetaException {
    throw new UnsupportedOperationException();
  }

  @Override
  public GetOpenTxnsResponse getOpenTxns() throws MetaException {
    LOG.debug("Getting open transactions");
    try (LockKeeper lk = new LockKeeper(masterLock.readLock())) {
      GetOpenTxnsResponse rsp =
          new GetOpenTxnsResponse(txnIdGenerator.current(), openTxns.keySet());
      rsp.getOpen_txns().addAll(abortedTxns.keySet());
      return rsp;
    } catch (IOException e) {
      // This is only here because LockKeeper.close has to throw an IOException because Closeable
      // .close does.  But in reality it never will, so this should never happen.
      throw new RuntimeException("This should never happen", e);
    }
  }

  @Override
  public void abortTxn(AbortTxnRequest rqst) throws NoSuchTxnException, MetaException,
      TxnAbortedException {
    if (LOG.isDebugEnabled()) LOG.debug("Aborting transaction " + rqst.getTxnid());
    OpenTransaction txn;
    try (LockKeeper lk = new LockKeeper(masterLock.readLock())) {
      txn = openTxns.get(rqst.getTxnid());
      if (txn == null) throwAbortedOrNonExistent(rqst.getTxnid(), "abort");
    } catch (IOException e) {
      // This is only here because LockKeeper.close has to throw an IOException because Closeable
      // .close does.  But in reality it never will, so this should never happen.
      throw new RuntimeException("This should never happen", e);
    }
    abortTxnInternal(txn);
  }

  // This call will acquire the write lock.  So you need to either have it or have no locks.
  private void abortTxnInternal(OpenTransaction openTxn) {
    HiveLock[] locks = openTxn.getHiveLocks();
    AbortedTransaction abortedTxn = new AbortedTransaction(openTxn);
    Future<WriteAheadLog> waitForWal;
    try (LockKeeper lk = new LockKeeper(masterLock.writeLock())) {
      openTxns.remove(openTxn.getTxnId());
      // Don't need to track potential compactions, existing CompactionTxnHandler will do that
      // for us
      abortedTxns.put(abortedTxn.getTxnId(), abortedTxn);
      waitForWal = wal.queueAbortTxn(openTxn);
      if (locks != null && locks.length > 0) {
        for (HiveLock lock : locks) {
          lock.setState(LockState.TXN_ABORTED);
          lockQueues.get(lock.getEntityLocked()).queue.remove(lock.getLockId());
          lockQueuesToCheck.add(lock.getEntityLocked());
        }
      }
    } catch (IOException e) {
      // This is only here because LockKeeper.close has to throw an IOException because Closeable
      // .close does.  But in reality it never will, so this should never happen.
      throw new RuntimeException("This should never happen", e);
    }

    if (locks != null && locks.length > 0) {
      LOG.debug("Requesting lockChecker run");
      threadPool.execute(lockChecker);
    }

    try {
      waitForWal.get();
    } catch (InterruptedException|ExecutionException e) {
      // This means we failed to record it in the WAL.  We could try to nicely unwind everything.
      //  But more realistically this likely means we're screwed and should just die.
      LOG.error("Unable to record transaction abort in the WAL", e);
      throw new RuntimeException(e);
    }
  }

  @Override
  public void commitTxn(CommitTxnRequest rqst) throws NoSuchTxnException, TxnAbortedException,
      MetaException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Committing txn " + rqst.getTxnid());
    }
    Future<WriteAheadLog> waitForWal;
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
      waitForWal = wal.queueCommitTxn(txn);
      // We only need to remember the transaction if it had write locks.  If it's read only or
      // DDL we can forget it.
      if (sawWriteLock) {
        // There's no need to move the transaction counter ahead
        CommittedTransaction committedTxn = new CommittedTransaction(txn, txnIdGenerator);

        if (LOG.isDebugEnabled()) {
          LOG.debug("Created new committed transaction with txn id " + committedTxn.getTxnId() +
              " and commit id " + committedTxn.getCommitId());
        }

        committedTxns.add(committedTxn);
        // Record all of the commit ids in the lockQueues so other transactions can quickly look
        // it up when getting locks.
        for (HiveLock lock : txn.getHiveLocks()) {
          if (lock.getType() != LockType.SHARED_WRITE) continue;
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

      } else {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Forgetting transaction " + txn.getTxnId() +
              " as it is committed and held no write locks");
        }
      }
    } catch (IOException e) {
      // This is only here because LockKeeper.close has to throw an IOException because Closeable
      // .close does.  But in reality it never will, so this should never happen.
      throw new RuntimeException("This should never happen", e);
    }
    try {
      waitForWal.get();
    } catch (InterruptedException|ExecutionException e) {
      // This means we failed to record it in the WAL.  We could try to nicely unwind everything.
      //  But more realistically this likely means we're screwed and should just die.
      LOG.error("Unable to record transaction abort in the WAL", e);
      throw new RuntimeException(e);
    }
  }

  @Override
  public void heartbeat(HeartbeatRequest ids) throws NoSuchTxnException, NoSuchLockException,
      TxnAbortedException, MetaException {
    assert !ids.isSetLockid() : "Fail, we don't heartbeat locks anymore!";

    if (LOG.isDebugEnabled()) LOG.debug("Heartbeating txn " + ids.getTxnid());
    try (LockKeeper lk = new LockKeeper(masterLock.readLock())) {
      OpenTransaction txn = openTxns.get(ids.getTxnid());
      if (txn == null) throwAbortedOrNonExistent(ids.getTxnid(), "abort");
      txn.setLastHeartbeat(System.currentTimeMillis());
      // Don't write this down to the database.  There's no value.
    } catch (IOException e) {
      // This is only here because LockKeeper.close has to throw an IOException because Closeable
      // .close does.  But in reality it never will, so this should never happen.
      throw new RuntimeException("This should never happen", e);
    }
  }

  @Override
  public LockResponse lock(LockRequest rqst) throws NoSuchTxnException, TxnAbortedException,
      MetaException {
    assert rqst.isSetTxnid() : "All locks must be associated with a transaction now";
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
    Future<WriteAheadLog> waitForWal;
    try (LockKeeper lk = new LockKeeper(masterLock.writeLock())) {
      txn = openTxns.get(rqst.getTxnid());
      if (txn == null) throwAbortedOrNonExistent(rqst.getTxnid(), "obtain lock in");

      for (int i = 0; i < components.size(); i++) {
        EntityKey key = new EntityKey(components.get(i));
        hiveLocks[i] = new HiveLock(lockIdGenerator, rqst.getTxnid(), key,
            thriftTypeToType(components.get(i).getType()));
        // Add to the appropriate DTP queue
        assureQueueExists(key);
        lockQueues.get(hiveLocks[i].getEntityLocked()).queue.put(hiveLocks[i].getLockId(),
            hiveLocks[i]);
        lockQueuesToCheck.add(hiveLocks[i].getEntityLocked());
      }
      // Run the lock checker to see if we can acquire these locks
      lockCheckerRun = threadPool.submit(lockChecker);
      txn.addLocks(hiveLocks);

      waitForWal = wal.queueLockRequest(rqst);
    } catch (IOException e) {
      // This is only here because LockKeeper.close has to throw an IOException because Closeable
      // .close does.  But in reality it never will, so this should never happen.
      throw new RuntimeException("This should never happen", e);
    }

    // First, see if our locks acquired immediately using the return from our submission to the
    // thread queue.
    LockResponse rsp;
    try (LockKeeper lk = new LockKeeper(masterLock.readLock())) {
      try {
        // TODO make configurable
        lockCheckerRun.get(1000, TimeUnit.MILLISECONDS);
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
      waitForWal.get();
    } catch (IOException e) {
      // This is only here because LockKeeper.close has to throw an IOException because Closeable
      // .close does.  But in reality it never will, so this should never happen.
      throw new RuntimeException("This should never happen", e);
    } catch (InterruptedException|ExecutionException e) {
      // This means we failed to record it in the WAL.  We could try to nicely unwind everything.
      //  But more realistically this likely means we're screwed and should just die.
      LOG.error("Unable to record transaction abort in the WAL", e);
      throw new RuntimeException(e);
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
      // This is only here because LockKeeper.close has to throw an IOException because Closeable
      // .close does.  But in reality it never will, so this should never happen.
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


  private void throwAbortedOrNonExistent(long id, String attempedAction)
      throws TxnAbortedException, NoSuchTxnException {
    if (abortedTxns.containsKey(id)) {
      throw new TxnAbortedException("Attempt to " + attempedAction + " aborted transaction " + id);
    }
    throw new NoSuchTxnException("Attempt to " + attempedAction + " non-existent transaction" + id);
  }

  private LockType thriftTypeToType(org.apache.hadoop.hive.metastore.api.LockType thriftType) {
    switch (thriftType) {
    case SHARED_READ: return LockType.SHARED_READ;
    case SHARED_WRITE: return LockType.SHARED_WRITE;
    case EXCLUSIVE: return LockType.EXCLUSIVE;
    // TODO add intention locks
    default: throw new RuntimeException("Unknown lock type " + thriftType);
    }
  }

  // You MUST hold the write lock to call this method
  private void assureQueueExists(EntityKey key) {
    if (!lockQueues.containsKey(key)) lockQueues.put(key, new LockQueue());
  }

  private static class LockQueue {
    final SortedMap<Long, HiveLock> queue;
    long maxCommitId;

    public LockQueue() {
      queue = new TreeMap<>();
      maxCommitId = 0;
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
        // This is only here because LockKeeper.close has to throw an IOException because Closeable
        // .close does.  But in reality it never will, so this should never happen.
        throw new RuntimeException("This should never happen", e);
      }

      if (keys != null) {
        List<HiveLock> toAcquire = new ArrayList<>();
        final Set<Long> writeConflicts = new HashSet<>();
        Future<WriteAheadLog> waitForWal;
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
          // This is only here because LockKeeper.close has to throw an IOException because Closeable
          // .close does.  But in reality it never will, so this should never happen.
          throw new RuntimeException("This should never happen", e);
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
          // This means we failed to record it in the WAL.  We could try to nicely unwind everything.
          //  But more realistically this likely means we're screwed and should just die.
          LOG.error("Unable to record transaction abort in the WAL", e);
          throw new RuntimeException(e);
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

}
