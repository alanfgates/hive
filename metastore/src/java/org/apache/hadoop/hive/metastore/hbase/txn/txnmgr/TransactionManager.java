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
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

// TODO
// MAJOR TODO
// What are the threading characteristics of a co-processor?  Is it already single threaded or
// can I expect a thread per caller?


public class TransactionManager {

  // Track what locks types are compatible.  First array is holder, second is requester
  private static boolean[][] lockCompatibilityTable;

  private static TransactionManager self;

  // This lock needs to be acquired when readers want to determine the valid transaction list.
  // Writers (any thread changing transactions) only need to acquire it when adding or removing
  // transactions from the txns List.  All other operations can just acquire a lock on the
  // appropriate transaction.
  private final ReadWriteLock txnLock;

  // This lock needs to be acquired only when creating new locks.
  private final Lock lockLock;

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

  private Thread timer;

  public static synchronized TransactionManager getTransactionManager(HiveConf conf) {
    if (self == null) {
      self = new TransactionManager(conf);
    }
    return self;
  }

  private TransactionManager(HiveConf conf) {
    txnLock = new ReentrantReadWriteLock();
    lockLock = new ReentrantLock();
    openTxns = new HashMap<>(); // TODO Does this need to be sorted?
    abortedTxns = new HashMap<>();
    dtps = new ConcurrentHashMap<>(10000, 0.75f, 1);
    this.conf = conf;
    try {
      recover();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    timer = new Thread(timedOutCleaner);
    timer.setDaemon(true);
    timer.start();
  }


  public OpenTxnsResponse openTxns(OpenTxnRequest rqst) throws IOException {
    try (LockKeeper lk = new LockKeeper(txnLock.writeLock())) {
      for (int i = 0; i < rqst.getNum_txns(); i++) {
        HiveTransaction txn = new HiveTransaction(nextTxnId++);
        openTxns.put(txn.getId(), txn);
      }

      // TODO write entries to HBase.

      return null;
    }
  }

  public GetOpenTxnsResponse getOpenTxns() throws IOException {
    try (LockKeeper lk = new LockKeeper(txnLock.readLock())) {
      Set<Long> ids = new HashSet<>(openTxns.size() + abortedTxns.size());
      ids.addAll(openTxns.keySet());
      ids.addAll(abortedTxns.keySet());
      return new GetOpenTxnsResponse(nextTxnId, ids);
    }
  }

  public void abortTxn(AbortTxnRequest abortTxnRequest) throws IOException {
    HiveTransaction txn = openTxns.get(abortTxnRequest.getTxnid());
    abortTxn(txn);
  }

  private void abortTxn(HiveTransaction txn) throws IOException {
    // TODO deal with non-existent transaction
    Lock[] dtpLocks = null;
    try (LockKeeper lk = new LockKeeper(txn.getTxnLock())) {
      // TODO someone else may have altered the state of the transaction while we waited for the
      // lock, deal with that.
      HiveLock[] locks = txn.getHiveLocks();
      if (locks != null) {
        dtpLocks = new Lock[locks.length];
        // We avoid deadlocks because the locks are always in sorted order
        for (int i = 0; i < locks.length; i++) {
          dtpLocks[i] = locks[i].getDtpQueue().lock;
          dtpLocks[i].lock();
        }
        // Don't change any HiveLocks until we've acquired all the DTP locks.
        for (HiveLock lock : locks) {
          lock.setState(HbaseMetastoreProto.Transaction.Lock.LockState.ABORTED);
          // This is not very efficient, but I'm keeping the locks in a queue so we can iterate
          // over them in both directions efficiently.  If it turns out this is not useful we
          // should switch the locks to a LinkedHashSet which preserves insertion order but still
          // supports efficient random removal.
          lock.getDtpQueue().hiveLocks.remove(lock);
        }
      }

      // TODO move to history table in HBase

      // TODO remove from current table in HBase

      // Move the entry to the aborted txns list
      openTxns.remove(txn.getId());
      txn.setState(HbaseMetastoreProto.Transaction.TxnState.ABORTED);
      abortedTxns.put(txn.getId(), txn);
    } finally {
      if (dtpLocks != null) {
        for (Lock dtpLock : dtpLocks) dtpLock.unlock();
      }
    }
  }

  // This assumes that the txnLock and the lock on the particular transaction are already held.
  private void innerAbort(HiveTransaction txn) {

  }

  public void commitTxn(CommitTxnRequest commitTxnRequest) throws IOException {
    HiveTransaction txn = openTxns.get(commitTxnRequest.getTxnid());
    // TODO deal with non-existent transaction
    Lock[] dtpLocks = null;
    try (LockKeeper lk = new LockKeeper(txn.getTxnLock())) {
      // TODO someone else may have altered the state of the transaction while we waited for the
      // lock, deal with that.
      HiveLock[] locks = txn.getHiveLocks();
      if (locks != null) {
        dtpLocks = new Lock[locks.length];
        // We avoid deadlocks because the locks are always in sorted order
        for (int i = 0; i < locks.length; i++) {
          dtpLocks[i] = locks[i].getDtpQueue().lock;
          dtpLocks[i].lock();
        }
        // Don't change any HiveLocks until we've acquired all the DTP locks.
        for (HiveLock lock : locks) {
          lock.setState(HbaseMetastoreProto.Transaction.Lock.LockState.RELEASED);
          // This is not very efficient, but I'm keeping the locks in a queue so we can iterate
          // over them in both directions efficiently.  If it turns out this is not useful we
          // should switch the locks to a LinkedHashSet which preserves insertion order but still
          // supports efficient random removal.
          lock.getDtpQueue().hiveLocks.remove(lock);
        }
      }

      // TODO move to history table in HBase

      // TODO remove from current table in HBase

      openTxns.remove(txn.getId());
      txn.setState(HbaseMetastoreProto.Transaction.TxnState.COMMITTED);
    } finally {
      if (dtpLocks != null) {
        for (Lock dtpLock : dtpLocks) dtpLock.unlock();
      }
    }
  }

  // TODO heartbeat
  public HeartbeatTxnRangeResponse heartbeat(HeartbeatTxnRangeRequest rqst) throws IOException {
    long now = System.currentTimeMillis();
    Set<Long> aborted = new HashSet<>();
    Set<Long> noSuch = new HashSet<>();
    for (long txnId = rqst.getMin(); txnId < rqst.getMax(); txnId++) {
      HiveTransaction txn = openTxns.get(txnId);
      if (txn != null) {
        try (LockKeeper lk = new LockKeeper(txn.getTxnLock())) {
          // It's possible that someone else aborted or completed this txn before we got the lock
          switch (txn.getState()) {
          case ABORTED:
            aborted.add(txnId);
            break;

          case COMMITTED:
            noSuch.add(txnId);
            break;

          case OPEN:
            txn.setLastHeartbeat(now);
            // TODO record in HBase
            break;
          }
        }
      } else {
        txn = abortedTxns.get(txnId);
        if (txn == null) noSuch.add(txnId);
        else aborted.add(txnId);
      }
    }
    return new HeartbeatTxnRangeResponse(aborted, noSuch);
  }

  public LockResponse lock(LockRequest rqst) throws IOException {
    HiveTransaction txn = openTxns.get(rqst.getTxnid());
    // TODO deal with non-existent transaction
    List<LockComponent> components = rqst.getComponent();
    HiveLock[] hiveLocks = new HiveLock[components.size()];
    // TODO I have a race condition here since I get the lock ids before I put them
    try (LockKeeper lk = new LockKeeper(txn.getTxnLock())) {
      // TODO someone else may have altered the state of the transaction while we waited for the
      // lock, deal with that.
      try (LockKeeper lk1 = new LockKeeper(lockLock)) {
        for (int i = 0; i < components.size(); i++) {
          LockComponent component = components.get(i);
          hiveLocks[i] = new HiveLock(nextLockId++, rqst.getTxnid(),
              translateLockType(component.getType()),
              findDTPQueue(component.getDbname(), component.getTablename(),
                  component.getPartitionname()));
          Arrays.sort(hiveLocks);
        }
        txn.addLocks(hiveLocks);

        // Add the locks to the appropriate DTP queues, acquire all those locks first.
        for (HiveLock lock : hiveLocks) {
          lock.getDtpQueue().lock.lock();
        }
        for (HiveLock lock : hiveLocks) {
          lock.getDtpQueue().hiveLocks.add(lock);
        }
        // I've incremented the lock id and inserted into the dtp queues, I can now release the
        // lockLock
      }

      // TODO record state in HBase.

      return checkLocks(txn.getId(), hiveLocks);

    } finally {
      for (HiveLock lock : hiveLocks) {
        if (lock != null) lock.getDtpQueue().lock.unlock();
      }
    }
  }

  private HbaseMetastoreProto.Transaction.Lock.LockType translateLockType(LockType thriftType) {
    // TODO
    return null;
  }

  DTPLockQueue findDTPQueue(String db, String table, String part) throws IOException {
    DTPKey key = new DTPKey(db, table, part);
    DTPLockQueue queue = null;
    queue = dtps.get(key);
    if (queue == null) {
      queue = new DTPLockQueue(key);
      dtps.put(key, queue);
    }
    return queue;
  }

  public LockResponse checkLocks(CheckLockRequest rqst) throws IOException {
    HiveTransaction txn = openTxns.get(rqst.getTxnid());
    HiveLock[] hiveLocks = null;
    // TODO deal with non-existent transaction
    try (LockKeeper lk = new LockKeeper(txn.getTxnLock())) {
      // TODO someone else may have altered the state of the transaction while we waited for the
      // lock, deal with that.

      hiveLocks = txn.getHiveLocks();
      // Acquire all the DTP locks we need
      for (HiveLock lock : hiveLocks) {
        lock.getDtpQueue().lock.lock();
      }
       return checkLocks(txn.getId(), hiveLocks);
    } finally {
      if (hiveLocks != null) {
        for (HiveLock lock : hiveLocks) {
          lock.getDtpQueue().lock.unlock();
        }
      }
    }
  }

  // Assumes the txn lock is held and required DTP locks are held, don't enter without these
  private LockResponse checkLocks(long txnId, HiveLock[] locks) {
    // TODO handle lock promotion
    // TODO I don't think using txnId for lock state the way I'm trying to do here will work,
    // I'll have to have an artificial lock id I send the client.
    for (HiveLock lock : locks) {
      Iterator<HiveLock> iter = lock.getDtpQueue().hiveLocks.iterator();
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
      lockCompatibilityTable =
          new boolean[HbaseMetastoreProto.Transaction.Lock.LockType.values().length]
              [HbaseMetastoreProto.Transaction.Lock.LockType.values().length];
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
    HiveTransaction txn = openTxns.get(txnId);
    HiveLock[] hiveLocks = new HiveLock[lockIds.size()];
    // TODO deal with non-existent transaction
    try (LockKeeper lk = new LockKeeper(txn.getTxnLock())) {
      // TODO someone else may have altered the state of the transaction while we waited for the
      // lock, deal with that.

      // Find our locks, make sure we keep them in the sorted order so we don't deadlock
      int i = 0;
      for (HiveLock lock : txn.getHiveLocks()) {
        if (lockIds.contains(lock)) hiveLocks[i++] = lock;
      }

      // Acquire all the DTP locks we need
      for (HiveLock lock : hiveLocks) {
        lock.getDtpQueue().lock.lock();
      }

      for (HiveLock lock : hiveLocks) {
        lock.setState(HbaseMetastoreProto.Transaction.Lock.LockState.RELEASED);
        // TODO update HBase
      }
    } finally {
      for (HiveLock lock : hiveLocks) {
        if (lock != null) lock.getDtpQueue().lock.unlock();
      }
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
        // harder to do this later anyway because the hive lock doesn't track the DTP info and
        // we've sorted the lock array to make sure it's in the right order, so we'd have to
        // search based on lock ids.
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
            hiveLock.getDtpQueue().hiveLocks.add(hiveLock);
          }
        } else {
          throw new RuntimeException("Logic error, how did we get here?");
        }
      }
    }
  }

  // TODO clean timed out txns
  // TODO if we've recently recovered don't run the cleaner for a bit to avoid killing txns that
  // haven't been able to heartbeat because we were down.

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
    final Lock lock;
    final Set<HiveLock> hiveLocks;

    DTPLockQueue(DTPKey key) {
      this.key = key;
      lock = new ReentrantLock();
      hiveLocks = new TreeSet<>(dtpLockComparator);
    }
  }

  private static Comparator<HiveLock> dtpLockComparator = new Comparator<HiveLock>() {
    @Override
    public int compare(HiveLock o1, HiveLock o2) {
      // Our locks should never be null (famous last words)
      if (o1.getId() < o1.getId()) return -1;
      else if (o1.getId() > o2.getId()) return 1;
      else return 0;
    }
  };

  private Runnable timedOutCleaner = new Runnable() {
    @Override
    public void run() {
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
        try (LockKeeper lk = new LockKeeper(txnLock.readLock())) {
          for (HiveTransaction txn : openTxns.values()) {
            try (LockKeeper lk1 = new LockKeeper(txn.getTxnLock())) {
              if (txn.getLastHeartbeat() + timeout < now) {
                // Locks are re-entrant, so it's ok that we already own them.
                abortTxn(txn);
              }
            }
          }
        } catch (IOException e) {
          // shouldn't ever happen
          throw new RuntimeException("Logic error", e);
        }
      }
    }
  };

}
