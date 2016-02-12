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

  // TODO handle refusing new transactions once we reach a certain size.
  // TODO add logging
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
  private BlockingQueue<EntityKey> lockQueuesToCheck;

  // Keep track of all our service threads so we can stop them when we shut down.
  private List<Thread> serviceThreads;

  // Flag to tell threads to keep going
  private boolean go;

  public TransactionManager() {

  }

  @Override
  public void start(CoprocessorEnvironment coprocessorEnvironment) throws IOException {
    masterLock = new ReentrantReadWriteLock();
    // Don't set the values here, as for efficiency in iteration we want the size to match as
    // closely as possible to the actual number of entries.
    openTxns = new HashMap<>();
    abortedTxns = new HashMap<>();
    committedTxns = new HashSet<>();
    lockQueues = new HashMap<>(10000);
    lockQueuesToCheck = new LinkedBlockingDeque<>();
    conf = coprocessorEnvironment.getConfiguration();
    try {
      recover();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    go = true;
    serviceThreads = new ArrayList<>(services.length);
    for (Runnable service : services) {
      Thread t = new Thread(service);
      t.setDaemon(true);
      t.start();
      serviceThreads.add(t);
    }
  }

  @Override
  public void stop(CoprocessorEnvironment coprocessorEnvironment) throws IOException {
    go = false;
    for (Thread t : serviceThreads) {
      t.interrupt();
      try {
        t.wait(1000);
      } catch (InterruptedException e) {
        // TODO should probably log
      }
    }
  }

  @Override
  public Service getService() {
    return this;
  }

  @Override
  public void openTxns(RpcController controller, HbaseMetastoreProto.OpenTxnsRequest request,
                       RpcCallback<HbaseMetastoreProto.OpenTxnsResponse> done) {
    HbaseMetastoreProto.OpenTxnsResponse response = null;
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
    HbaseMetastoreProto.TransactionResult response = null;
    try (LockKeeper lk = new LockKeeper(masterLock.writeLock())) {
      OpenHiveTransaction txn = openTxns.get(request.getId());
      if (txn == null) {
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

  // You must own the master write lock before entering this method.

  /**
   * Abort a transaction.  You must own the master write lock before entering this method.  You
   * must NOT be in an HBase transaction.
   * @param txn transaction to abort
   * @throws IOException
   */
  private void abortTxn(OpenHiveTransaction txn)
      throws IOException {
    HiveLock[] locks = txn.getHiveLocks();
    List<EntityKey> queuesToCheck = new ArrayList<>();
    if (locks != null) {
      for (HiveLock lock : locks) {
        lock.setState(HbaseMetastoreProto.LockState.TXN_ABORTED);
        lockQueues.get(lock.getEntityLocked()).queue.remove(lock.getId());
        // It's ok to put these in the queue now even though we're still removing things because
        // we hold the master write lock.  The lockChecker won't be able to run until we've
        // released that lock anyway.
        try {
          lockQueuesToCheck.put(lock.getEntityLocked());
        } catch (InterruptedException e) {
          throw new IOException(e);
        }
      }
    }

    // Move the entry to the aborted txns list
    openTxns.remove(txn.getId());
    if (txn.hasWriteLocks()) {
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
          pces.add(new HBaseReadWrite.PotentialCompactionEntity(hbaseLock.getDb(), hbaseLock
              .getTable(), hbaseLock.getPartition()));
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
      getHBase().deleteTransaction(txn.getId());
    }
  }

  @Override
  public void commitTxn(RpcController controller, HbaseMetastoreProto.TransactionId request,
                        RpcCallback<HbaseMetastoreProto.TransactionResult> done) {
    HbaseMetastoreProto.TransactionResult response = null;
    boolean shouldCommit = false;
    getHBase().begin();
    try (LockKeeper lk = new LockKeeper(masterLock.writeLock())) {
      OpenHiveTransaction txn = openTxns.get(request.getId());
      if (txn == null) {
        // TODO Log
        response = HbaseMetastoreProto.TransactionResult.newBuilder()
            .setState(HbaseMetastoreProto.TxnStateChangeResult.NO_SUCH_TXN)
            .build();
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
          // It's ok to put these in the queue now even though we're still removing things because
          // we hold the master write lock.  The lockChecker won't be able to run until we've
          // released that lock anyway.
          try {
            lockQueuesToCheck.put(lock.getEntityLocked());
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
        CommittedHiveTransaction committedTxn = new CommittedHiveTransaction(txn, nextTxnId);

        committedTxns.add(committedTxn);
        // Record all of the commit ids in the dtps so other transactions can quickly look it up
        // when getting locks.
        for (HiveLock lock : txn.getHiveLocks()) {
          LockQueue queue = lockQueues.get(lock.getEntityLocked());
          queue.gtCommitId = Math.max(queue.gtCommitId, committedTxn.getCommitId());
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
            pces.add(new HBaseReadWrite.PotentialCompactionEntity(hbaseLock.getDb(), hbaseLock
                .getTable(), hbaseLock.getPartition()));
          }
        }
        getHBase().putTransaction(txnBuilder.build());
        getHBase().putPotentialCompactions(committedTxn.getId(), pces);
      } else {
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
    HbaseMetastoreProto.LockResponse response = null;
    List<HbaseMetastoreProto.LockComponent> components = request.getComponentsList();
    HiveLock[] hiveLocks = new HiveLock[components.size()];
    OpenHiveTransaction txn;
    try {
      try (LockKeeper lk = new LockKeeper(masterLock.writeLock())) {
        txn = openTxns.get(request.getTxnId());
        if (txn == null) {
          done.run(HbaseMetastoreProto.LockResponse.newBuilder()
                 .setState(HbaseMetastoreProto.LockState.TXN_ABORTED)
                 .build());
          return;
        }
        if (txn.getState() != HbaseMetastoreProto.TxnState.OPEN) {
          suicide("Way bad, non-open txn in open list");
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
          try {
            lockQueuesToCheck.put(hiveLocks[i].getEntityLocked());
          } catch (InterruptedException e) {
            throw new IOException(e);
          }
        }
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
    HbaseMetastoreProto.LockResponse response = null;
    try {
      OpenHiveTransaction txn = null;
      try (LockKeeper lk = new LockKeeper(masterLock.readLock())) {
        txn = openTxns.get(request.getId());
      }
      if (txn == null) {
        // TODO LOG
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
    long waitUntil = System.currentTimeMillis() + 5000;
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
          return HbaseMetastoreProto.LockResponse.newBuilder()
              .setState(HbaseMetastoreProto.LockState.ACQUIRED)
              .build();
        }
        now = System.currentTimeMillis();
        if (now < waitUntil) lockChecker.wait(waitUntil - now);
      }

      // If we got here it means we waited too long.
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
        // TODO LOG
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
    try {
      // First, go find the potential compaction that matches this compaction.  That has the map
      // to which transactions we now need to modify or forget.
      HbaseMetastoreProto.PotentialCompaction potential =
          getHBase().getPotentialCompaction(request.getDb(), request.getTable(),
              request.hasPartition() ? request.getPartition() : null);
      // It is possible for the potential to be null, as the user could have requested a compaction
      // when there was nothing really to do.
      if (potential != null) {
        List<Long> compactedTxns = new ArrayList<>(); // All txns that were compacted
        // Txns in the potential that weren't compacted as part of this compaction.  We need to
        // track this so we can properly write the potential back.
        List<Long> uncompactedTxns = new ArrayList<>();
        for (long txnId : potential.getTxnIdsList()) {
          // We only need to worry about this transaction if it's writes were compacted (that
          // is, it's <= the highestTxnId)
          if (txnId <= request.getHighestTxnId()) {
            compactedTxns.add(txnId);
          } else {
            uncompactedTxns.add(txnId);
          }
        }
        if (compactedTxns.size() > 0) {
          // Transactions that have had all the tables/partitions they touched compacted.
          List<Long> fullyCompacted = new ArrayList<>();
          // Transactions that have had locks marked as compacted but are not themselves fully
          // compacted yet.
          Map<Long, HiveLock> modifiedTxns = new HashMap<>();
          // We need the write lock because we might remove entries from abortedTxns and
          // delete from Transactions and rewrite or remove PotentialCompactions
          try (LockKeeper lk = new LockKeeper(masterLock.writeLock())) {
            for (long txnId : compactedTxns) {
              AbortedHiveTransaction txn = abortedTxns.remove(txnId);
              // The transaction could be null if this was a completed transaction and not an
              // aborted one.
              if (txn != null) {
                // Mark the lock on that partition in this aborted txn as compacted.
                HiveLock compactedLock = txn.compactLock(new EntityKey(request.getDb(),
                    request.getTable(), request.hasPartition() ? request.getPartition() : null));
                if (txn.fullyCompacted()) {
                  fullyCompacted.add(txnId);
                } else {
                  modifiedTxns.put(txnId, compactedLock);
                }
              }
            }

            if (fullyCompacted.size() > 0) getHBase().deleteTransactions(fullyCompacted);
            if (modifiedTxns.size() > 0) {
              List<HbaseMetastoreProto.Transaction> toModify =
                  getHBase().getTransactions(modifiedTxns.keySet());
              // New transaction objects we're writing back to HBase
              List<HbaseMetastoreProto.Transaction> toWrite = new ArrayList<>(modifiedTxns.size());
              for (HbaseMetastoreProto.Transaction hbaseTxn : toModify) {
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
            boolean shouldCommit = false;
            getHBase().begin();
            try {
              if (uncompactedTxns.size() > 0) {
                // Rewrite the potential compaction to remove txns that have been compacted out
                HbaseMetastoreProto.PotentialCompaction.Builder potentialBldr =
                    HbaseMetastoreProto.PotentialCompaction.newBuilder(potential);
                potentialBldr.clearTxnIds();
                potentialBldr.addAllTxnIds(uncompactedTxns);
                getHBase().putPotentialCompaction(potentialBldr.build());
              } else {
                // This was the last transaction in this potential compaction, so remove it
                getHBase().deletePotentialCompaction(potential.getDb(), potential.getTable(),
                    potential.hasPartition() ? potential.getPartition() : null);
              }
              shouldCommit = true;
            } finally {
              if (shouldCommit) getHBase().commit();
              else getHBase().rollback();
            }
          }
        }
      }
    } catch (IOException e) {
      ResponseConverter.setControllerException(controller, e);
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
    return minOpenTxn;
  }

  private void suicide(String logMsg) {
    throw new RuntimeException("Killing self due to " + logMsg);
    // TODO - figure out how to kill this instance of the co-processor without taking down the
    // region server so we force a recovery
  }

  private void recover() throws IOException {
    // No locking is required here because we're still in the constructor and we're guaranteed no
    // one else is muddying the waters.
    // Get existing transactions from HBase
    try {
      List<HbaseMetastoreProto.Transaction> hbaseTxns = getHBase().scanTransactions();
      if (hbaseTxns != null) {
        for (HbaseMetastoreProto.Transaction hbaseTxn : hbaseTxns) {
          switch (hbaseTxn.getTxnState()) {
          case ABORTED:
            AbortedHiveTransaction abortedTxn = new AbortedHiveTransaction(hbaseTxn, this);
            abortedTxns.put(abortedTxn.getId(), abortedTxn);
            break;

          case OPEN:
            OpenHiveTransaction openTxn = new OpenHiveTransaction(hbaseTxn, this);
            openTxns.put(openTxn.getId(), openTxn);
            break;

          case COMMITTED:
            for (HbaseMetastoreProto.Transaction.Lock hbaseLock : hbaseTxn.getLocksList()) {
              LockQueue queue = findOrCreateLockQueue(hbaseLock.getDb(), hbaseLock.getTable(),
                  hbaseLock.hasPartition() ? hbaseLock.getPartition() : null).getSecond();
              queue.gtCommitId = Math.max(queue.gtCommitId, hbaseTxn.getCommitId());
            }
            committedTxns.add(new CommittedHiveTransaction(hbaseTxn));
            break;
          }
        }
      }
      nextTxnId = getHBase().readCurrentSequence(HBaseReadWrite.TXN_SEQUENCE);
      nextLockId = getHBase().readCurrentSequence(HBaseReadWrite.LOCK_SEQUENCE);
    } finally {
      // It's a read only operation, so always commit
      getHBase().commit();
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
    long gtCommitId; // commit id of highest transaction that wrote to this DTP

    private LockQueue() {
      queue = new TreeMap<>();
      gtCommitId = 0;
    }
  }

  private Runnable timedOutCleaner = new Runnable() {
    @Override
    public void run() {
      // Set initial sleep time long so we have time after recovery for things to heartbeat.
      int sleepTime = 60000;
      long timeout = HiveConf.getIntVar(conf, HiveConf.ConfVars.HIVE_TXN_TIMEOUT);
      while (go) {
        // Sleep first, so we don't kill things right after recovery, give them a chance to
        // heartbeat if we've been out of it for a few seconds.
        try {
          Thread.sleep(sleepTime);
        } catch (InterruptedException e) {
          // Go back through the while loop, as this might mean it's time to quit
          continue;
        }
        sleepTime = 5000;
        // First get the read lock and find all of the potential timeouts.
        List<OpenHiveTransaction> potentials = new ArrayList<>();
        long now = System.currentTimeMillis();
        try (LockKeeper lk = new LockKeeper(masterLock.readLock())) {
          for (OpenHiveTransaction txn : openTxns.values()) {
            if (txn.getLastHeartbeat() + timeout < now) {
              potentials.add(txn);
            }
          }
        } catch (IOException e) {
          // TODO log it
        }

        // Now go back through the potentials list, holding the write lock, and remove any that
        // still haven't heartbeat
        try (LockKeeper lk = new LockKeeper(masterLock.writeLock())) {
          for (OpenHiveTransaction txn : potentials) {
            if (txn.getLastHeartbeat() + timeout < now) {
              abortTxn(txn);
            }
          }
        } catch (IOException e) {
          // TODO probably want to log this and ignore
        }
      }
    }
  };

  private Runnable deadlockDetector = new Runnable() {
    // Rather than follow the general pattern of go through all the entries and find potentials
    // and then remove all potentials this thread kills a deadlock as soon as it sees it and then
    // starts looking all over again.  Otherwise we'd likely be too aggressive and kill all
    // participants in the deadlock.
    @Override
    public void run() {
      int sleepTime = 2000;
      while (go) {
        try {
          Thread.sleep(sleepTime);
        } catch (InterruptedException e) {
          // Go back through the while loop, as this might mean it's time to quit
          continue;
        }
        sleepTime = 2000;
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
                sleepTime = 10;
                break;
              }
            }
          }
        } catch (IOException e) {
          // TODO probably want to log this and ignore
        }
      }
    }

    private boolean lookForDeadlock(long firstHalf, OpenHiveTransaction txn, boolean initial) {
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
      while (go) {
        try {
          Thread.sleep(60000);
        } catch (InterruptedException e) {
          // Go back through the while loop, as this might mean it's time to quit
          continue;
        }
        List<EntityKey> empties = new ArrayList<>();
        // First get the read lock and find all currently empty keys.  Then release the read
        // lock, get the write lock and remove all those keys, checking again that they are
        // truly empty.
        try (LockKeeper lk = new LockKeeper(masterLock.readLock())) {
          for (Map.Entry<EntityKey, LockQueue> entry : lockQueues.entrySet()) {
            if (entry.getValue().queue.size() == 0 && entry.getValue().gtCommitId == 0) {
              empties.add(entry.getKey());
            }
          }
        } catch (IOException e) {
          // TODO probably want to log this and ignore
        }

        if (empties.size() > 0) {
          try (LockKeeper lk = new LockKeeper(masterLock.writeLock())) {
            for (EntityKey key : empties) {
              LockQueue queue = lockQueues.get(key);
              if (queue.queue.size() == 0 && queue.gtCommitId == 0) {
                lockQueues.remove(key);
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
    // This looks through the list of committed transactions and figures out what can be
    // forgotten.
    @Override
    public void run() {
      while (go) {
        try {
          Thread.sleep(2000);
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
                forgetableIds.add(txn.getId());
                forgetableTxns.add(txn);
                forgetableCommitIds.add(txn.getCommitId());
              }
            }
            // For any of these found transactions, see if we can remove them from the dtps.
            // This is important because it enables us eventually to shrink the dtps
            for (LockQueue queue : lockQueues.values()) {
              if (forgetableCommitIds.contains(queue.gtCommitId)) {
                forgetableDtps.put(queue, queue.gtCommitId);
              }
            }
          }
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
              if (entry.getKey().gtCommitId == entry.getValue()) {
                entry.getKey().gtCommitId = 0;
              }
            }
          }
        } catch (InterruptedException e) {
          // Go back through the while loop, as this might mean it's time to quit
          continue;
        } catch (IOException e) {
          // TODO Log and continue
        }
      }
    }
  };

  private Runnable lockChecker = new Runnable() {
    @Override
    public void run() {
      while (go) {
        try {
          List<EntityKey> keys = new ArrayList<>();
          keys.add(lockQueuesToCheck.take());
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
                    // acquiring transaction.
                    long latestTxnId = lockQueues.get(lock.getEntityLocked()).gtCommitId;
                    if (latestTxnId > lock.getTxnId()) {
                      LOG.info("Transaction " + lock.getTxnId() + " attempted to obtain lock for " +
                          lock.getEntityLocked().toString() + " but that entity more recently " +
                          "updated with transaction with commit id " + latestTxnId
                          + " so later transaction will be aborted.");
                      writeConflicts.add(lock.getTxnId());
                    } else {
                      OpenHiveTransaction txn = openTxns.get(lock.getTxnId());
                      if (txn == null || txn.getState() != HbaseMetastoreProto.TxnState.OPEN) {
                        // TODO LOG this, don't throw
                      } else {
                        acquiringTxns.add(openTxns.get(lock.getTxnId()));
                        toAcquire.put(lock.getId(), lock);
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
            // Abort the transactions that ran into write conflicts
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
          this.notifyAll();

        } catch (InterruptedException ie) {
          // go back through the loop and make sure it's not time to stop
          continue;

        } catch (Exception ie) {
          // TODO probably want to log this and ignore
        }
      }
    }
  };

  private Runnable[] services = {timedOutCleaner, deadlockDetector, dtpsShrinker,
                                 committedTxnCleaner, lockChecker};

}
