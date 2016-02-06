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
import org.apache.hadoop.hive.metastore.hbase.HBaseReadWrite;
import org.apache.hadoop.hive.metastore.hbase.HbaseMetastoreProto;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class TransactionManager {

  // TODO prove out compaction operations
  // TODO solve lost update problem
  // TODO convert to coprocessor service
  // TODO connect to actual HBase storage
  // TODO fix bug in lock acquire that won't acquire ahead of wait if it can
  // TODO see if we can make lock acquition active instead of passive
  // TODO remove the use of HiveConf, this won't have access to it, need to find another way to pass the required values.
  // TODO handle refusing new transactions once we reach a certain size.
  // TODO handle lock promotion


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

  // Keep it in a set for now, though we're going to need a better structure than this
  private final Set<HiveTransaction> committedTxns;

  private final HiveConf conf;

  // Protected by synchronized code section on openTxn;
  private long nextTxnId;

  // Protected by synchronized code section on getLockId;
  private long nextLockId;

  // A structure to store the locks according to which database/table/partition they lock.
  private Map<DTPKey, DTPQueue> dtps;

  private HBaseReadWrite hbase;

  // A queue used to signal to the lockChecker which dtp queues it should look in.
  private BlockingQueue<DTPKey> lockQueuesToCheck;

  public static synchronized TransactionManager getTransactionManager(HiveConf conf) {
    if (self == null) {
      self = new TransactionManager(conf);
    }
    return self;
  }

  private TransactionManager(HiveConf conf) {
    masterLock = new ReentrantReadWriteLock();
    // Don't set the values here, as for efficiency in iteration we want the size to match as
    // closely as possible to the actual number of entries.
    openTxns = new HashMap<>();
    abortedTxns = new HashMap<>();
    committedTxns = new HashSet<>();
    dtps = new HashMap<>(10000);
    lockQueuesToCheck = new LinkedBlockingDeque<>();
    this.conf = conf;
    try {
      recover();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    for (Runnable service : serivces) {
      Thread t = new Thread(service);
      t.setDaemon(true);
      t.start();
    }
  }


  public HbaseMetastoreProto.OpenTxnsResponse openTxns(HbaseMetastoreProto.OpenTxnsRequest rqst)
      throws IOException {
    try (LockKeeper lk = new LockKeeper(masterLock.writeLock())) {
      List<HbaseMetastoreProto.Transaction> hbaseTxns = new ArrayList<>();
      HbaseMetastoreProto.OpenTxnsResponse.Builder rspBuilder =
          HbaseMetastoreProto.OpenTxnsResponse.newBuilder();
      for (int i = 0; i < rqst.getNumTxns(); i++) {
        HiveTransaction txn = new OpenHiveTransaction(nextTxnId++);
        openTxns.put(txn.getId(), txn);

        HbaseMetastoreProto.Transaction.Builder builder = HbaseMetastoreProto.Transaction
            .newBuilder()
            .setId(txn.getId())
            .setTxnState(txn.getState())
            .setUser(rqst.getUser())
            .setHostname(rqst.getHostname());
        if (rqst.hasAgentInfo()) {
          builder.setAgentInfo(rqst.getAgentInfo());
        }
        hbaseTxns.add(builder.build());
        rspBuilder.addTxnIds(txn.getId());
      }
      getHBase().putTransactions(hbaseTxns);


      return rspBuilder.build();
    }
  }

  public HbaseMetastoreProto.GetOpenTxnsResponse getOpenTxns() throws IOException {
    try (LockKeeper lk = new LockKeeper(masterLock.readLock())) {
      return HbaseMetastoreProto.GetOpenTxnsResponse.newBuilder()
          .setHighWaterMark(nextTxnId)
          .addAllOpenTransactions(openTxns.keySet())
          .addAllAbortedTransactions(abortedTxns.keySet())
          .build();
    }
  }

  public void abortTxn(HbaseMetastoreProto.TransactionId txnId) throws IOException {
    try (LockKeeper lk = new LockKeeper(masterLock.writeLock())) {
      HiveTransaction txn = openTxns.get(txnId.getId());
      if (txn == null) throw new IOException("Bad, unable to find txn " + txnId.getId());
      if (txn.getState() == HbaseMetastoreProto.TxnState.COMMITTED) {
        throw new IOException("Bad, it committed before we managed to abort");
      }
      if (txn.getState() == HbaseMetastoreProto.TxnState.ABORTED) {
        // Nothing to do here
        return;
      }
      abortTxn(txn);
    }
  }

  // You must own the master write lock before entering this method.
  private void abortTxn(HiveTransaction txn) throws IOException {
    HiveLock[] locks = txn.getHiveLocks();
    List<DTPKey> queuesToCheck = new ArrayList<>();
    if (locks != null) {
      for (HiveLock lock : locks) {
        lock.setState(HbaseMetastoreProto.LockState.TXN_ABORTED);
        lock.getDtpQueue().queue.remove(lock.getId());
        // It's ok to put these in the queue now even though we're still removing things because
        // we hold the master write lock.  The lockChecker won't be able to run until we've
        // released that lock anyway.
        try {
          lockQueuesToCheck.put(lock.getDtpQueue().key);
        } catch (InterruptedException e) {
          throw new IOException(e);
        }
      }
    }

    // Move the entry to the aborted txns list
    openTxns.remove(txn.getId());
    if (txn.hasWriteLocks()) {
      txn = new AbortedHiveTransaction(txn);
      abortedTxns.put(txn.getId(), txn);

      List<HBaseReadWrite.PotentialCompactionEntity> pces = new ArrayList<>();

      // This is where protocol buffers suck.  Since they're read only we have to make a whole
      // new copy
      HbaseMetastoreProto.Transaction hbaseTxn = getHBase().getTransaction(txn.getId());
      HbaseMetastoreProto.Transaction.Builder txnBuilder =
          HbaseMetastoreProto.Transaction.newBuilder(hbaseTxn);
      txnBuilder.clearLocks();
      txnBuilder.setTxnState(HbaseMetastoreProto.TxnState.ABORTED);
      List<HbaseMetastoreProto.Transaction.Lock> hbaseLocks = hbaseTxn.getLocksList();
      for (HbaseMetastoreProto.Transaction.Lock hbaseLock : hbaseLocks) {
        // We only need to remember the shared_write locks, all the rest can be ignored as they
        // aren't a part of compaction.
        if (hbaseLock.getType() == HbaseMetastoreProto.LockType.SHARED_WRITE) {
          HbaseMetastoreProto.Transaction.Lock.Builder lockBuilder =
              HbaseMetastoreProto.Transaction.Lock.newBuilder(hbaseLock);
          // We need to mark them not compacted so that at recovery we know
          // which partitions/table we still need to look at compacting.
          lockBuilder.setCompacted(false);
          txnBuilder.addLocks(lockBuilder);
          pces.add(new HBaseReadWrite.PotentialCompactionEntity(hbaseLock.getDb(), hbaseLock
              .getTable(), hbaseLock.getPartition()));
        }
      }
      getHBase().putTransaction(txnBuilder.build());
      getHBase().putPotentialCompactions(txn.getId(), pces);
    } else {
      // There's no reason to remember this txn anymore, it either had no locks or was read only.
      getHBase().deleteTransaction(txn.getId());
    }
  }

  public void commitTxn(HbaseMetastoreProto.TransactionId txnId) throws IOException {
    try (LockKeeper lk = new LockKeeper(masterLock.writeLock())) {
      HiveTransaction txn = openTxns.get(txnId.getId());
      if (txn == null) throw new IOException("Bad, unable to find txn " + txnId.getId());
      if (txn.getState() == HbaseMetastoreProto.TxnState.COMMITTED) return;
      if (txn.getState() == HbaseMetastoreProto.TxnState.ABORTED) {
        throw new IOException("Bad, trying to commit aborted txn");
      }
      HiveLock[] locks = txn.getHiveLocks();
      if (locks != null) {
        for (HiveLock lock : locks) {
          lock.setState(HbaseMetastoreProto.LockState.RELEASED);
          lock.getDtpQueue().queue.remove(lock.getId());
          // It's ok to put these in the queue now even though we're still removing things because
          // we hold the master write lock.  The lockChecker won't be able to run until we've
          // released that lock anyway.
          try {
            lockQueuesToCheck.put(lock.getDtpQueue().key);
          } catch (InterruptedException e) {
            throw new IOException(e);
          }
        }
      }

      openTxns.remove(txn.getId());
      // We only need to remember the transaction if it had write locks.  If it's read only or
      // DDL we can forget it.
      if (txn.hasWriteLocks()) {
        // There's no need to move the transaction counter ahead
        txn = new CommittedHiveTransaction(txn, nextTxnId);
        committedTxns.add(txn);
        List<HBaseReadWrite.PotentialCompactionEntity> pces = new ArrayList<>();

        HbaseMetastoreProto.Transaction hbaseTxn = getHBase().getTransaction(txn.getId());
        HbaseMetastoreProto.Transaction.Builder txnBuilder =
            HbaseMetastoreProto.Transaction.newBuilder(hbaseTxn);
        txnBuilder.setTxnState(HbaseMetastoreProto.TxnState.COMMITTED);
        txnBuilder.setCommitId(txn.getCommitId());
        txnBuilder.clearLocks();
        List<HbaseMetastoreProto.Transaction.Lock> hbaseLocks = hbaseTxn.getLocksList();
        for (HbaseMetastoreProto.Transaction.Lock hbaseLock : hbaseLocks) {
          // We only need to remember the shared_write locks, all the rest can be ignored as they
          // aren't of interest for building write sets
          if (hbaseLock.getType() == HbaseMetastoreProto.LockType.SHARED_WRITE) {
            HbaseMetastoreProto.Transaction.Lock.Builder lockBuilder =
                HbaseMetastoreProto.Transaction.Lock.newBuilder(hbaseLock);
            txnBuilder.addLocks(lockBuilder);
            pces.add(new HBaseReadWrite.PotentialCompactionEntity(hbaseLock.getDb(), hbaseLock
                .getTable(), hbaseLock.getPartition()));
          }
        }
        getHBase().putTransaction(txnBuilder.build());
        getHBase().putPotentialCompactions(txn.getId(), pces);
      } else {
        getHBase().deleteTransaction(txn.getId());
      }
    }
  }

  public HbaseMetastoreProto.HeartbeatTxnRangeResponse heartbeat(HbaseMetastoreProto.HeartbeatTxnRangeRequest rqst)
      throws IOException {
    long now = System.currentTimeMillis();
    HbaseMetastoreProto.HeartbeatTxnRangeResponse.Builder builder =
        HbaseMetastoreProto.HeartbeatTxnRangeResponse.newBuilder();

    try (LockKeeper lk = new LockKeeper(masterLock.readLock())) {
      for (long txnId = rqst.getMinTxn(); txnId <= rqst.getMaxTxn(); txnId++) {
        HiveTransaction txn = openTxns.get(txnId);
        if (txn != null) {
          txn.setLastHeartbeat(now);
          // Don't set the value in HBase, we don't track it there for efficiency
        } else {
          txn = abortedTxns.get(txnId);
          if (txn == null) builder.addNoSuch(txnId);
          else builder.addAborted(txnId);
        }
      }
    }
    return builder.build();
  }

  public void lock(HbaseMetastoreProto.LockRequest rqst) throws IOException {
    List<HbaseMetastoreProto.LockComponent> components = rqst.getComponentsList();
    HiveLock[] hiveLocks = new HiveLock[components.size()];
    HiveTransaction txn;
    try (LockKeeper lk = new LockKeeper(masterLock.writeLock())) {
      txn = openTxns.get(rqst.getTxnId());
      if (txn == null) throw new IOException("No such txn, " + rqst.getTxnId());
      if (txn.getState() != HbaseMetastoreProto.TxnState.OPEN) {
        throw new IOException("Can't request locks on a non-open txn");
      }
      for (int i = 0; i < components.size(); i++) {
        HbaseMetastoreProto.LockComponent component = components.get(i);
        hiveLocks[i] = new HiveLock(nextLockId++, rqst.getTxnId(), component.getType(),
            findDTPQueue(component.getDb(), component.getTable(), component.getPartition()));
        // Add to the appropriate DTP queue
        hiveLocks[i].getDtpQueue().queue.put(hiveLocks[i].getId(), hiveLocks[i]);
        try {
          lockQueuesToCheck.put(hiveLocks[i].getDtpQueue().key);
        } catch (InterruptedException e) {
          throw new IOException(e);
        }
      }
      txn.addLocks(hiveLocks);
    }
  }

  DTPQueue findDTPQueue(String db, String table, String part) throws IOException {
    DTPKey key = new DTPKey(db, table, part);
    DTPQueue queue = dtps.get(key);
    if (queue == null) {
      queue = new DTPQueue(key);
      dtps.put(key, queue);
    }
    return queue;
  }

  /*
  private LockResponse checkLocks(long txnId, HiveLock[] locks) {
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
        if (current.getState() == HbaseMetastoreProto.LockState.RELEASED) {
          // This lock has been released, we can ignore it
          continue;
        }
        if (current.getState() == HbaseMetastoreProto.LockState.WAITING) {
          // Something in front of us is waiting, so we're done
          return new LockResponse(txnId, LockState.WAITING);
        }
        if (current.getState() == HbaseMetastoreProto.LockState.ACQUIRED) {
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
      lock.setState(HbaseMetastoreProto.LockState.ACQUIRED);
      // TODO modify state in HBase.
    }
    return new LockResponse(txnId, LockState.ACQUIRED);
  }
  */

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
  public void addDynamicPartitions(HbaseMetastoreProto.AddDynamicPartitions parts) throws IOException {
    // This needs to acquire the write lock because it might add entries to dtps, which is
    // unfortunate.
    try (LockKeeper lk = new LockKeeper(masterLock.writeLock())) {
      // Add the locks to the appropriate transaction so that we know what things to compact and
      // so we know what partitions were touched by this change.  Don't put the locks in the dtps
      // because we're actually covered by the table lock.  Do increment the counters in the dtps.
      HiveTransaction txn = openTxns.get(parts.getTxnId());
      if (txn == null) throw new IOException("No such transaction " + parts.getTxnId());
      if (txn.getState() != HbaseMetastoreProto.TxnState.OPEN) {
        throw new IOException("Attempt to add dynamic partitions to aborted or committed txn");
      }

      HbaseMetastoreProto.Transaction hbaseTxn = getHBase().getTransaction(parts.getTxnId());
      HbaseMetastoreProto.Transaction.Builder txnBuilder =
          HbaseMetastoreProto.Transaction.newBuilder(hbaseTxn);

      List<String> partitionNames = parts.getPartitionsList();
      HiveLock[] partitionsWrittenTo = new HiveLock[partitionNames.size()];
      for (int i = 0; i < partitionNames.size(); i++) {
        partitionsWrittenTo[i] = new HiveLock(-1, parts.getTxnId(),
            HbaseMetastoreProto.LockType.SHARED_WRITE,
            findDTPQueue(parts.getDb(), parts.getTable(), partitionNames.get(i)));
        // Set the lock in released state so it doesn't get put in the DTP queue on recovery
        partitionsWrittenTo[i].setState(HbaseMetastoreProto.LockState.RELEASED);

        txnBuilder.addLocks(HbaseMetastoreProto.Transaction.Lock.newBuilder()
            .setId(-1)
            .setType(HbaseMetastoreProto.LockType.SHARED_WRITE)
            .setState(HbaseMetastoreProto.LockState.RELEASED)
            .setDb(parts.getDb())
            .setTable(parts.getTable())
            .setPartition(partitionNames.get(i)));
      }
      txn.addLocks(partitionsWrittenTo);


      getHBase().putTransaction(txnBuilder.build());
    }
  }

  // Called once all of the entities written to in an aborted txn have been compacted
  public void removeCompletelyCompactedAbortedTxn(HbaseMetastoreProto.TransactionId txnId)
      throws IOException {
    try (LockKeeper lk = new LockKeeper(masterLock.writeLock())) {
      abortedTxns.remove(txnId.getId());
      getHBase().deleteTransaction(txnId.getId());
    }
  }

  private void forgetCommittedTxn(long txnId) throws IOException {
    try (LockKeeper lk = new LockKeeper(masterLock.writeLock())) {
      committedTxns.remove(txnId);
      getHBase().deleteTransaction(txnId);
    }
  }

  private void recover() throws IOException {
    // No locking is required here because we're still in the constructor and we're guaranteed no
    // one else is muddying the waters.
    // TODO get existing transactions from HBase
    List<HbaseMetastoreProto.Transaction> hbaseTxns = null;
    if (hbaseTxns != null) {
      for (HbaseMetastoreProto.Transaction hbaseTxn : hbaseTxns) {
        switch (hbaseTxn.getTxnState()) {
        case ABORTED:
          HiveTransaction hiveTxn = new AbortedHiveTransaction(hbaseTxn);
          abortedTxns.put(hiveTxn.getId(), hiveTxn);
          break;

        case OPEN:
          hiveTxn = new OpenHiveTransaction(hbaseTxn, this);
          openTxns.put(hiveTxn.getId(), hiveTxn);
          break;

        case COMMITTED:
          hiveTxn = new CommittedHiveTransaction(hbaseTxn, this);
          committedTxns.add(hiveTxn);
          break;
        }
      }
    }
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

    DTPKey(String db, String table, String part) {
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

  static class DTPQueue {
    final SortedMap<Long, HiveLock> queue;
    final DTPKey key; // backpointer to our key so that from the lock we can find the dtp

    public DTPQueue(DTPKey key) {
      queue = new TreeMap<>();
      this.key = key;
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
          for (HiveTransaction txn : openTxns.values()) {
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
                // It's easiest to always kill this one rather than try to figure out where in
                // the graph we can remove something and break the cycle.  Given that which txn
                // we examine first is mostly random this should be ok (I hope).
                masterLock.readLock().unlock();
                // TODO LOG this as an ERROR
                try (LockKeeper lk1 = new LockKeeper(masterLock.writeLock())) {
                  abortTxn(openTxns.get(txn.getId()));
                }
                // We've released the readlock and messed with the data structures we're
                // traversing, so don't keep going.  Instead set our sleep time very low so we
                // run again soon.
                sleepTime = 1;
                break;
              }
            }
          }
        } catch (IOException e) {
          // TODO probably want to log this and ignore
        }
      }
    }

    private boolean lookForDeadlock(long firstHalf, HiveTransaction txn, boolean initial) {
      if (!initial && firstHalf == txn.getId()) return true;
      for (HiveLock lock : txn.getHiveLocks()) {
        if (lock.getState() == HbaseMetastoreProto.LockState.WAITING) {
          // TODO deal with missing txn
          return lookForDeadlock(firstHalf, openTxns.get(lock.getTxnId()), false);
        }
      }
      return false;
    }
  };

  private Runnable dtpsShrinker = new Runnable() {
    @Override
    public void run() {
      while (true) {
        try {
          Thread.sleep(60000);
        } catch (InterruptedException e) {
          // Ignore
        }
        List<DTPKey> empties = new ArrayList<>();
        // First get the read lock and find all currently empty keys.  Then release the read
        // lock, get the write lock and remove all those keys, checking again that they are
        // truly empty.
        try (LockKeeper lk = new LockKeeper(masterLock.readLock())) {
          for (Map.Entry<DTPKey, DTPQueue> entry : dtps.entrySet()) {
            if (entry.getValue().queue.size() == 0) empties.add(entry.getKey());
          }
        } catch (IOException e) {
          // TODO probably want to log this and ignore
        }

        if (empties.size() > 0) {
          try (LockKeeper lk = new LockKeeper(masterLock.writeLock())) {
            for (DTPKey key : empties) {
              DTPQueue queue = dtps.get(key);
              if (queue.queue.size() == 0) {
                dtps.remove(key);
              }
            }
          } catch (IOException e) {
            // TODO probably want to log this and ignore
          }
        }
      }
    }
  };

  private Runnable committedTxnCleaner = new Runnable() {
    @Override
    public void run() {
      while (true) {
        try {
          Thread.sleep(2000);
        } catch (InterruptedException e) {
          // Ignore
        }
        List<Long> forgetable = new ArrayList<>();
        long minOpenTxn = Long.MAX_VALUE;
        try (LockKeeper lk = new LockKeeper(masterLock.readLock())) {
          for (Long txnId : openTxns.keySet()) {
            minOpenTxn = Math.min(txnId, minOpenTxn);
          }
          for (HiveTransaction txn : committedTxns) {
            if (txn.getCommitId() < minOpenTxn) {
              // There are no open transactions left that don't already see this transaction in
              // their read set, so we can forget it.
              forgetable.add(txn.getId());
            }
          }
        } catch (IOException e) {
          // TODO probably want to log this and ignore
        }
        try (LockKeeper lk1 = new LockKeeper(masterLock.writeLock())) {
          for (Long txnId : forgetable) {
            forgetCommittedTxn(txnId);
          }
        } catch (IOException e) {
          // TODO probably want to log this and ignore
        }
      }
    }
  };

  private Runnable lockChecker = new Runnable() {
    @Override
    public void run() {
      while (true) {
        try {
          DTPKey key = lockQueuesToCheck.take();
          List<HiveLock> toNofify = new ArrayList<>();
          try (LockKeeper lk = new LockKeeper(masterLock.readLock())) {
            DTPQueue queue = dtps.get(key);
            HiveLock lastLock = null;
            for (HiveLock lock : queue.queue.values()) {
              if (lock.getState() == HbaseMetastoreProto.LockState.WAITING) {
                // See if we can acquire this lock
                if (lastLock == null || twoLocksCompatible(lastLock.getType(), lock.getType())) {
                  toNofify.add(lock);
                } else {
                  // If we can't acquire then nothing behind us can either
                  // TODO prove to yourself this is true
                  break;
                }

              }
              lastLock = lock;
            }
          } // Now outside read lock
          for (HiveLock lock : toNofify) {
            // TODO figure out how to notify requester
          }
        } catch (Exception ie) {
          // TODO probably want to log this and ignore
        }
      }

    }
  };

  private Runnable[] serivces = {timedOutCleaner, deadlockDetector, dtpsShrinker,
                                 committedTxnCleaner, lockChecker};

}
