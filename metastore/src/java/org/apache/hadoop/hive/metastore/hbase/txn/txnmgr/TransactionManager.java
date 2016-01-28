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
import org.apache.hadoop.hive.metastore.api.GetOpenTxnsResponse;
import org.apache.hadoop.hive.metastore.api.HeartbeatTxnRangeRequest;
import org.apache.hadoop.hive.metastore.api.HeartbeatTxnRangeResponse;
import org.apache.hadoop.hive.metastore.api.LockComponent;
import org.apache.hadoop.hive.metastore.api.LockRequest;
import org.apache.hadoop.hive.metastore.api.LockResponse;
import org.apache.hadoop.hive.metastore.api.LockState;
import org.apache.hadoop.hive.metastore.api.LockType;
import org.apache.hadoop.hive.metastore.api.OpenTxnRequest;
import org.apache.hadoop.hive.metastore.api.OpenTxnsResponse;
import org.apache.hadoop.hive.metastore.hbase.HbaseMetastoreProto;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

// TODO
// MAJOR TODO
// What are the threading characteristics of a co-processor?  Is it already single threaded or
// can I expect a thread per caller?


public class TransactionManager {

  // Track what locks types are compatible.  First array is holder, second is requester
  private static boolean[][] lockCompatibilityTable;

  private static TransactionManager self;

  // This lock needs to be acquired in write mode only when modifying structures (opening,
  // aborting, committing txns, adding locks).  To modify structures (ie unlock, heartbeat) it is
  // only needed in the read mode.  Anything looking at this structure should acquire it in the
  // read mode.
  private final ReadWriteLock masterLock;

  // A list of all active transactions.  Obtain the globalLock before changing this list.  You
  // can read this list without obtaining the global lock.
  private final Map<Long, HiveTransaction> openTxns;

  // List of aborted transactions, kept in memory for efficient reading when readers need a valid
  // transaction list.
  private final Map<Long, HiveTransaction> abortedTxns;

  private final HiveConf conf;

  // Protected by synchronized code section on openTxn;
  private long nextTxnId;

  // Protected by synchronized code section on getLockId;
  private long nextLockId;

  // A structure to store the locks according to which database/table/partition they lock.
  private Map<DTPKey, DTPLockQueue> dtps;

  private Thread timeoutThread, deadlockThread;

  public static synchronized TransactionManager getTransactionManager(HiveConf conf) {
    if (self == null) {
      self = new TransactionManager(conf);
    }
    return self;
  }

  private TransactionManager(HiveConf conf) {
    masterLock = new ReentrantReadWriteLock();
    openTxns = new HashMap<>();
    abortedTxns = new HashMap<>();
    dtps = new HashMap<>(10000);
    this.conf = conf;
    try {
      recover();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    timeoutThread = new Thread(timedOutCleaner);
    timeoutThread.setDaemon(true);
    timeoutThread.start();

    deadlockThread = new Thread(deadlockDetector);
    deadlockThread.setDaemon(true);
    deadlockThread.start();
  }


  public OpenTxnsResponse openTxns(OpenTxnRequest rqst) throws IOException {
    try (LockKeeper lk = new LockKeeper(masterLock.writeLock())) {
      for (int i = 0; i < rqst.getNum_txns(); i++) {
        HiveTransaction txn = new HiveTransaction(nextTxnId++);
        openTxns.put(txn.getId(), txn);
      }

      // TODO write entries to HBase.

      return null;
    }
  }

  public GetOpenTxnsResponse getOpenTxns() throws IOException {
    try (LockKeeper lk = new LockKeeper(masterLock.readLock())) {
      Set<Long> ids = new HashSet<>(openTxns.size() + abortedTxns.size());
      ids.addAll(openTxns.keySet());
      ids.addAll(abortedTxns.keySet());
      return new GetOpenTxnsResponse(nextTxnId, ids);
    }
  }

  public void abortTxn(AbortTxnRequest abortTxnRequest) throws IOException {
    try (LockKeeper lk = new LockKeeper(masterLock.writeLock())) {
      HiveTransaction txn = openTxns.get(abortTxnRequest.getTxnid());
      // TODO someone else may have altered the state of the transaction while we waited for the
      // lock, deal with that.
      abortTxn(txn);
    }
  }

  // You must own the master write lock before entering this method.
  private void abortTxn(HiveTransaction txn) throws IOException {
    HiveLock[] locks = txn.getHiveLocks();
    if (locks != null) {
      for (HiveLock lock : locks) {
        lock.setState(HbaseMetastoreProto.Transaction.Lock.LockState.ABORTED);
        lock.getDtpQueue().hiveLocks.remove(lock.getId());
      }
    }

    // TODO move to history table in HBase

    // TODO remove from current table in HBase

    // Move the entry to the aborted txns list
    openTxns.remove(txn.getId());
    txn.setState(HbaseMetastoreProto.Transaction.TxnState.ABORTED);
    abortedTxns.put(txn.getId(), txn);
  }

  public void commitTxn(CommitTxnRequest commitTxnRequest) throws IOException {
    try (LockKeeper lk = new LockKeeper(masterLock.writeLock())) {
      HiveTransaction txn = openTxns.get(commitTxnRequest.getTxnid());
      // TODO someone else may have altered the state of the transaction while we waited for the
      // lock, deal with that.
      HiveLock[] locks = txn.getHiveLocks();
      if (locks != null) {
        for (HiveLock lock : locks) {
          lock.setState(HbaseMetastoreProto.Transaction.Lock.LockState.RELEASED);
          lock.getDtpQueue().hiveLocks.remove(lock.getId());
        }
      }

      // TODO move to history table in HBase

      // TODO remove from current table in HBase

      openTxns.remove(txn.getId());
      txn.setState(HbaseMetastoreProto.Transaction.TxnState.COMMITTED);
    }
  }

  public HeartbeatTxnRangeResponse heartbeat(HeartbeatTxnRangeRequest rqst) throws IOException {
    long now = System.currentTimeMillis();
    Set<Long> aborted = new HashSet<>();
    Set<Long> noSuch = new HashSet<>();
    try (LockKeeper lk = new LockKeeper(masterLock.readLock())) {
      for (long txnId = rqst.getMin(); txnId < rqst.getMax(); txnId++) {
        HiveTransaction txn = openTxns.get(txnId);
        if (txn != null) {
          txn.setLastHeartbeat(now);
          // TODO record in HBase
        } else {
          txn = abortedTxns.get(txnId);
          if (txn == null) noSuch.add(txnId);
          else aborted.add(txnId);
        }
      }
    }
    return new HeartbeatTxnRangeResponse(aborted, noSuch);
  }

  public LockResponse lock(LockRequest rqst) throws IOException {
    List<LockComponent> components = rqst.getComponent();
    HiveLock[] hiveLocks = new HiveLock[components.size()];
    HiveTransaction txn = null;
    try (LockKeeper lk = new LockKeeper(masterLock.writeLock())) {
      txn = openTxns.get(rqst.getTxnid());
      // TODO someone else may have altered the state of the transaction while we waited for the
      // lock, deal with that.
      for (int i = 0; i < components.size(); i++) {
        LockComponent component = components.get(i);
        hiveLocks[i] = new HiveLock(nextLockId++, rqst.getTxnid(),
            translateLockType(component.getType()),
            findDTPQueue(component.getDbname(), component.getTablename(),
                component.getPartitionname()));
        // Add to the appropriate DTP queue
        hiveLocks[i].getDtpQueue().hiveLocks.put(hiveLocks[i].getId(), hiveLocks[i]);
      }
      txn.addLocks(hiveLocks);
    }

    // Must have the read lock before going into checkLocks
    try (LockKeeper lk = new LockKeeper(masterLock.readLock())) {
      return checkLocks(txn.getId(), hiveLocks);
    }

  }

  private HbaseMetastoreProto.Transaction.Lock.LockType translateLockType(LockType thriftType) {
    // TODO
    return null;
  }

  DTPLockQueue findDTPQueue(String db, String table, String part) throws IOException {
    DTPKey key = new DTPKey(db, table, part);
    DTPLockQueue queue = dtps.get(key);
    if (queue == null) {
      queue = new DTPLockQueue(key);
      dtps.put(key, queue);
    }
    return queue;
  }

  public LockResponse checkLocks(CheckLockRequest rqst) throws IOException {
    try (LockKeeper lk = new LockKeeper(masterLock.readLock())) {
      HiveTransaction txn = openTxns.get(rqst.getTxnid());
      // TODO someone else may have altered the state of the transaction while we waited for the
      // lock, deal with that.
      HiveLock[] hiveLocks = txn.getHiveLocks();
      return checkLocks(txn.getId(), hiveLocks);
    }
  }

  // Assumes the master read lock is held
  private LockResponse checkLocks(long txnId, HiveLock[] locks) {
    // TODO handle lock promotion
    // TODO I don't think using txnId for lock state the way I'm trying to do here will work,
    // I'll have to have an artificial lock id I send the client.
    for (HiveLock lock : locks) {
      Iterator<HiveLock> iter = lock.getDtpQueue().hiveLocks.values().iterator();
      // Walk the list, looking for anything in front of me that blocks me.
      while (iter.hasNext()) {
        HiveLock current = iter.next();
        if (current.equals(lock)) {
          // We haven't seen anything that blocks us, so we're good
          break;
        }
        if (current.getState() == HbaseMetastoreProto.Transaction.Lock.LockState.RELEASED) {
          // This lock has been released, we can ignore it
          continue;
        }
        if (current.getState() == HbaseMetastoreProto.Transaction.Lock.LockState.WAITING) {
          // Something in front of us is waiting, so we're done
          return new LockResponse(txnId, LockState.WAITING);
        }
        if (current.getState() == HbaseMetastoreProto.Transaction.Lock.LockState.ACQUIRED) {
          // See if we're compatible.  If we are, we have to keep looking as something else may
          // be in the way.  If we're not, we know we're done
          if (!twoLocksCompatible(current.getType(), lock.getType())) {
            return new LockResponse(txnId, LockState.WAITING);
          }
        }
        throw new RuntimeException("Logic error, how did we get here?");
      }
    }

    // If we've gotten this far, it means all our locks can acquire, so do it.
    for (HiveLock lock : locks) {
      lock.setState(HbaseMetastoreProto.Transaction.Lock.LockState.ACQUIRED);
      // TODO modify state in HBase.
    }
    return new LockResponse(txnId, LockState.ACQUIRED);
  }

  private boolean twoLocksCompatible(HbaseMetastoreProto.Transaction.Lock.LockType holder,
                                     HbaseMetastoreProto.Transaction.Lock.LockType requester) {
    if (lockCompatibilityTable == null) {
      // TODO not at all sure I have intention locks correct in this table
      lockCompatibilityTable = new boolean[HbaseMetastoreProto.Transaction.Lock.LockType.values().length][HbaseMetastoreProto.Transaction.Lock.LockType.values().length];
      Arrays.fill(lockCompatibilityTable[HbaseMetastoreProto.Transaction.Lock.LockType.EXCLUSIVE_VALUE], false);
      lockCompatibilityTable[HbaseMetastoreProto.Transaction.Lock.LockType.SHARED_WRITE_VALUE][HbaseMetastoreProto.Transaction.Lock.LockType.EXCLUSIVE_VALUE] = false;
      lockCompatibilityTable[HbaseMetastoreProto.Transaction.Lock.LockType.SHARED_WRITE_VALUE][HbaseMetastoreProto.Transaction.Lock.LockType.SHARED_WRITE_VALUE] = false;
      lockCompatibilityTable[HbaseMetastoreProto.Transaction.Lock.LockType.SHARED_WRITE_VALUE][HbaseMetastoreProto.Transaction.Lock.LockType.SHARED_READ_VALUE] = true;
      lockCompatibilityTable[HbaseMetastoreProto.Transaction.Lock.LockType.SHARED_WRITE_VALUE][HbaseMetastoreProto.Transaction.Lock.LockType.INTENTION_VALUE] = true;
      lockCompatibilityTable[HbaseMetastoreProto.Transaction.Lock.LockType.SHARED_READ_VALUE][HbaseMetastoreProto.Transaction.Lock.LockType.EXCLUSIVE_VALUE] = false;
      lockCompatibilityTable[HbaseMetastoreProto.Transaction.Lock.LockType.SHARED_READ_VALUE][HbaseMetastoreProto.Transaction.Lock.LockType.SHARED_WRITE_VALUE] = true;
      lockCompatibilityTable[HbaseMetastoreProto.Transaction.Lock.LockType.SHARED_READ_VALUE][HbaseMetastoreProto.Transaction.Lock.LockType.SHARED_READ_VALUE] = true;
      lockCompatibilityTable[HbaseMetastoreProto.Transaction.Lock.LockType.SHARED_READ_VALUE][HbaseMetastoreProto.Transaction.Lock.LockType.INTENTION_VALUE] = true;
      lockCompatibilityTable[HbaseMetastoreProto.Transaction.Lock.LockType.INTENTION_VALUE][HbaseMetastoreProto.Transaction.Lock.LockType.EXCLUSIVE_VALUE] = false;
      lockCompatibilityTable[HbaseMetastoreProto.Transaction.Lock.LockType.INTENTION_VALUE][HbaseMetastoreProto.Transaction.Lock.LockType.SHARED_WRITE_VALUE] = true;
      lockCompatibilityTable[HbaseMetastoreProto.Transaction.Lock.LockType.INTENTION_VALUE][HbaseMetastoreProto.Transaction.Lock.LockType.SHARED_READ_VALUE] = true;
      lockCompatibilityTable[HbaseMetastoreProto.Transaction.Lock.LockType.INTENTION_VALUE][HbaseMetastoreProto.Transaction.Lock.LockType.INTENTION_VALUE] = true;
    }
    return lockCompatibilityTable[holder.getNumber()][requester.getNumber()];
  }

  public void unlock(long txnId, Set<Long> lockIds) throws IOException {
    // Unlock doesn't remove anything from the structure.  It should only be called by the agent
    // so it shouldn't conflict with checkLocks.  Anyone aborting the lock would acquire the
    // write lock so we shouldn't conflict there either.  So just the read lock should be fine.
    try (LockKeeper lk = new LockKeeper(masterLock.readLock())) {
      HiveTransaction txn = openTxns.get(txnId);
      // TODO someone else may have altered the state of the transaction while we waited for the
      // lock, deal with that.
      HiveLock[] hiveLocks = new HiveLock[lockIds.size()];

      int i = 0;
      for (HiveLock lock : txn.getHiveLocks()) {
        if (lockIds.contains(lock.getId())) {
          lock.setState(HbaseMetastoreProto.Transaction.Lock.LockState.RELEASED);
          lock.getDtpQueue().hiveLocks.remove(lock.getId());
        }
      }
      // TODO update HBase
    }
  }

  // TODO add dynamic partitions

  private void recover() throws IOException {
    // No locking is required here because we're still in the constructor and we're guaranteed no
    // one else is muddying the waters.
    // TODO get existing transactions from HBase
    List<HbaseMetastoreProto.Transaction> hbaseTxns = null;
    if (hbaseTxns != null) {
      for (HbaseMetastoreProto.Transaction hbaseTxn : hbaseTxns) {
        // Before the locks are created we need to go create the appropriate DTP queue for each
        // lock, since lock creation involves giving the lock a reference to its DTP queue.  It's
        // harder to do this later anyway because the hive lock doesn't track the DTP info so
        // we'd have to search based on lock ids.
        List<HbaseMetastoreProto.Transaction.Lock> hbaseLocks = hbaseTxn.getLocksList();
        if (hbaseLocks != null) {
          for (HbaseMetastoreProto.Transaction.Lock hbaseLock : hbaseLocks) {
            // findDTPQueue creates the queue if it doesn't already exist
            findDTPQueue(hbaseLock.getDb(), hbaseLock.getTable(), hbaseLock.getPartition());
          }
        }

        HiveTransaction hiveTxn = new HiveTransaction(hbaseTxn, this);
        if (hiveTxn.getState() == HbaseMetastoreProto.Transaction.TxnState.ABORTED) {
          abortedTxns.put(hiveTxn.getId(), hiveTxn);
        } else if (hiveTxn.getState() == HbaseMetastoreProto.Transaction.TxnState.OPEN) {
          openTxns.put(hiveTxn.getId(), hiveTxn);
          for (HiveLock hiveLock : hiveTxn.getHiveLocks()) {
            if (hiveLock.getState() == HbaseMetastoreProto.Transaction.Lock.LockState.WAITING ||
                hiveLock.getState() == HbaseMetastoreProto.Transaction.Lock.LockState.ACQUIRED) {
              hiveLock.getDtpQueue().hiveLocks.put(hiveLock.getId(), hiveLock);
            }
          }
        } else {
          throw new RuntimeException("Logic error, how did we get here?");
        }
      }
    }
  }

  // TODO search for deadlocks

  public static class LockKeeper implements Closeable {
    final Lock lock;

    public LockKeeper(Lock lock) {
      this.lock = lock;
    }

    @Override
    public void close() throws IOException {
      lock.unlock();
    }
  }

  static class DTPKey {
    final String db;
    final String table;
    final String part;
    private int hashCode = Integer.MIN_VALUE;

    DTPKey(String db, String table, String part) {
      this.db = db;
      this.table = table;
      this.part = part;
    }

    @Override
    public int hashCode() {
      if (hashCode == Integer.MIN_VALUE) {
        // db should never be null
        hashCode = db.hashCode();
        if (table != null) hashCode = hashCode * 31 + table.hashCode();
        if (part != null) hashCode = hashCode * 31 + part.hashCode();
      }
      return hashCode;
    }

    @Override
    public boolean equals(Object obj) {
      if (!(obj instanceof DTPKey)) return false;
      DTPKey other = (DTPKey)obj;
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
  }

  static class DTPLockQueue {
    final DTPKey key;
    final SortedMap<Long, HiveLock> hiveLocks;

    DTPLockQueue(DTPKey key) {
      this.key = key;
      hiveLocks = new TreeMap<>();
    }
  }

  private Runnable timedOutCleaner = new Runnable() {
    @Override
    public void run() {
      // Set initial sleep time long so we have time after recovery for things to heartbeat.
      int sleepTime = 60000;
      long timeout = conf.getIntVar(HiveConf.ConfVars.HIVE_TXN_TIMEOUT);
      while (true) {
        // Sleep first, so we don't kill things right after recovery, give them a chance to
        // heartbeat if we've been out of it for a few seconds.
        try {
          Thread.sleep(sleepTime);
        } catch (InterruptedException e) {
          // Ignore
        }
        sleepTime = 5000;
        long now = System.currentTimeMillis();
        try (LockKeeper lk = new LockKeeper(masterLock.readLock())) {
          for (HiveTransaction txn : openTxns.values()) {
            if (txn.getLastHeartbeat() + timeout < now) {
              // Must release the read lock and acquire the write lock before aborting a
              // transaction.
              masterLock.readLock().unlock();
              try (LockKeeper lk1 = new LockKeeper(masterLock.writeLock())) {
                abortTxn(txn);
              }
              // TODO Not sure if we should re-acquire the read lock here to avoid an error when
              // going through finally.  It's silly if we have to as we don't actually need the
              // lock.
              // Don't keep going, as we've altered the structure we're reading through.  Instead
              // set our sleep timer very low so that we run again soon.
              sleepTime = 1;
              break;
            }
          }
        } catch (IOException e) {
          // TODO probably want to log this and ignore
        }
      }
    }
  };

  private Runnable deadlockDetector = new Runnable() {
    @Override
    public void run() {
      int sleepTime = 2000;
      while (true) {
        try {
          Thread.sleep(sleepTime);
        } catch (InterruptedException e) {
          // Ignore
        }
        sleepTime = 2000;
        try (LockKeeper lk = new LockKeeper(masterLock.readLock())) {
          // We're looking only for transactions that have 1+ acquired locks and 1+ waiting locks
          Map<Long, PotentialDeadlock> potentials = new HashMap<>();
          for (HiveTransaction txn : openTxns.values()) {
            Set<Long> acquired = new HashSet<>();
            List<HiveLock> waiting = new ArrayList<>();
            for (HiveLock lock : txn.getHiveLocks()) {
              if (lock.getState() == HbaseMetastoreProto.Transaction.Lock.LockState.WAITING) {
                waiting.add(lock);
              } else if (lock.getState() == HbaseMetastoreProto.Transaction.Lock.LockState.ACQUIRED) {
                acquired.add(lock.getId());
              }
            }
            if (acquired.size() > 0 && waiting.size() > 0) {
              potentials.put(txn.getId(), new PotentialDeadlock(acquired, waiting));
            }
          }
          // TODO - this no work.  We need a full depth first traversal to find loops, not just
          // two step loops as the following does.
          // Now, look through the potentials and see if any of them are wedged on each other
          for (Map.Entry<Long, PotentialDeadlock> potential : potentials.entrySet()) {
            // For each one it is waiting for, go see if that transaction is waiting on us and
            // has the lock we're waiting for.
            for (HiveLock waitingFor : potential.getValue().waitingLocks) {
              Iterator<HiveLock> iter = waitingFor.getDtpQueue().hiveLocks.values().iterator();
              while (iter.hasNext()) {
                HiveLock holder = iter.next();
                if (holder.getState() == HbaseMetastoreProto.Transaction.Lock.LockState.ACQUIRED) {
                  // See if this transaction is in our potential set
                  PotentialDeadlock otherHalf = potentials.get(holder.getTxnId());
                  if (otherHalf != null) {
                    // Ok, there is at least the possibility.  We need to see if the other half
                    // is waiting on a lock our initial potential has acquired.
                    for (HiveLock otherHalfLock : otherHalf.waitingLocks) {
                      if (potential.getValue().acquiredLocks.contains(otherHalfLock.getId())) {
                        // Bingo, found one.  Now, abort whichever of these transactions has the
                        // higher id number.  To do that we'll have to release the read lock and
                        // acquire the write lock, so after that we'll set the sleep time low and
                        // start over again.
                        long victim = Math.max(potential.getKey(), holder.getTxnId());
                        masterLock.readLock().unlock();
                        // TODO LOG this as an ERROR
                        try (LockKeeper lk1 = new LockKeeper(masterLock.writeLock())) {
                          abortTxn(openTxns.get(victim));
                        }
                        sleepTime = 1;
                        break;
                      }
                    }
                  }
                }
              }
              if (sleepTime == 1) break;
            }
            if (sleepTime == 1) break;
          }
          if (sleepTime == 1) break;
        } catch (IOException e) {
          // TODO probably want to log this and ignore
        }
      }
    }
  };

  private static class PotentialDeadlock {
    final Set<Long> acquiredLocks;
    final List<HiveLock> waitingLocks;

    PotentialDeadlock(Set<Long> acquiredLocks, List<HiveLock> waitingLocks) {
      this.acquiredLocks = acquiredLocks;
      this.waitingLocks = waitingLocks;
    }
  }

}
