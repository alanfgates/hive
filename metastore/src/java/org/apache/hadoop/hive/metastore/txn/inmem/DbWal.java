/**
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
import org.apache.hadoop.hive.metastore.api.DataOperationType;
import org.apache.hadoop.hive.metastore.api.LockComponent;
import org.apache.hadoop.hive.metastore.api.LockRequest;
import org.apache.hadoop.hive.metastore.api.LockType;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.OpenTxnRequest;
import org.apache.hadoop.hive.metastore.txn.SQLGenerator;
import org.apache.hadoop.hive.metastore.txn.TxnHandler;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TMemoryBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;


// TODO we should try an implementation of this that writes directly to the DB tables instead of
// going through the WAL and see how much performance difference it makes.  One one hand with the
// WAL the SQL operations are very simple (just insert, shouldn't require locking) and thus
// should be fast.  On the other hand we're already single threading write operations to the
// database through this class so there should not be lock contention in the DB.

// TODO if we do stick with the WAL explore generating the TW_ID ourselves rather than relying on
// a sequence to minimize locking.
/**
 * A real WAL implementation.
 *
 * In order to assure that operations are written in the order they are received the generated
 * tasks are put into a blocking queue, which is read by a single thread that applies the writes.
 */
public class DbWal implements WriteAheadLog {
  static final private Logger LOG = LoggerFactory.getLogger(DbWal.class.getName());

  private final DataSource connPool;
  private final ScheduledThreadPoolExecutor threadPool;
  private final HiveConf conf;
  private SQLGenerator sqlGenerator;
  private BlockingQueue<FutureTask<?>> writeQueue;
  private int numRecordsFromWalToDb;

  public DbWal(DataSource connPool, ScheduledThreadPoolExecutor threadPool, HiveConf conf) {
    this.connPool = connPool;
    this.threadPool = threadPool;
    this.conf = conf;
    writeQueue = new LinkedBlockingQueue<>();
  }

  @Override
  public Future<Integer> queueOpenTxn(final long txnId, final OpenTxnRequest rqst) {
    FutureTask<Integer> result = new FutureTask<>(new Callable<Integer>() {
      @Override
      public Integer call() throws Exception {
        try (Connection conn = connPool.getConnection()) {
          // Set auto-commit to on as we'll just be inserting one row, hopefully very quickly.
          conn.setAutoCommit(true);
          conn.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);

          String sql = "insert into TXN_WAL (TW_TYPE, TW_RECORDED_AT, TW_TXNID, TW_OPEN_TXN_RQST)" +
              " values (?, ?, ?, ?)";

          if (LOG.isDebugEnabled()) LOG.debug("Going to prepare statement " + sql);

          try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setInt(1, EntryType.OPEN_TXN.ordinal());
            stmt.setLong(2, sqlGenerator.getDbTime(conn));
            stmt.setLong(3, txnId);
            stmt.setBytes(3, serialize(rqst));
            return stmt.executeUpdate();
          }
        }
      }
    });
    writeQueue.add(result);
    return result;
  }

  @Override
  public Future<Integer> queueAbortTxn(final OpenTransaction openTxn) {
    FutureTask<Integer> result = new FutureTask<>(new Callable<Integer>() {
      @Override
      public Integer call() throws Exception {
        try (Connection conn = connPool.getConnection()) {
          // Set auto-commit to on as we'll just be inserting one row, hopefully very quickly.
          conn.setAutoCommit(true);
          conn.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);

          String sql = "insert into TXN_WAL (TW_TYPE, TW_RECORDED_AT, TW_TXNID)" +
              " values (?, ?, ?)";

          if (LOG.isDebugEnabled()) LOG.debug("Going to prepare statement " + sql);

          try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setInt(1, EntryType.ABORT_TXN.ordinal());
            stmt.setLong(2, sqlGenerator.getDbTime(conn));
            stmt.setLong(3, openTxn.getTxnId());
            return stmt.executeUpdate();
          }
        }
      }
    });
    writeQueue.add(result);
    return result;
  }

  @Override
  public Future<Integer> queueCommitTxn(final CommittedTransaction committedTxn) {
    FutureTask<Integer> result = new FutureTask<>(new Callable<Integer>() {
      @Override
      public Integer call() throws Exception {
        try (Connection conn = connPool.getConnection()) {
          // Set auto-commit to on as we'll just be inserting one row, hopefully very quickly.
          conn.setAutoCommit(true);
          conn.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);

          String sql = "insert into TXN_WAL (TW_TYPE, TW_RECORDED_AT, TW_TXNID, TW_COMMIT_ID)" +
              " values (?, ?, ?, ?)";

          if (LOG.isDebugEnabled()) LOG.debug("Going to prepare statement " + sql);

          try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setInt(1, EntryType.COMMIT_TXN.ordinal());
            stmt.setLong(2, sqlGenerator.getDbTime(conn));
            stmt.setLong(3, committedTxn.getTxnId());
            stmt.setLong(4, committedTxn.getCommitId());
            return stmt.executeUpdate();
          }
        }
      }
    });
    writeQueue.add(result);
    return result;
  }

  @Override
  public Future<Integer> queueLockRequest(final LockRequest rqst,
                                                final List<HiveLock> newLocks) {
    FutureTask<Integer> result = new FutureTask<>(new Callable<Integer>() {
      @Override
      public Integer call() throws Exception {
        try (Connection conn = connPool.getConnection()) {
          // Set auto-commit to on as we'll just be inserting one row, hopefully very quickly.
          conn.setAutoCommit(true);
          conn.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);

          String sql = "insert into TXN_WAL (TW_TYPE, TW_RECORDED_AT, TW_LOCK_RQST, TW_LOCKS)" +
              " values (?, ?, ?, ?)";

          if (LOG.isDebugEnabled()) LOG.debug("Going to prepare statement " + sql);

          try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setInt(1, EntryType.REQUEST_LOCKS.ordinal());
            stmt.setLong(2, sqlGenerator.getDbTime(conn));
            stmt.setBytes(3, serialize(rqst));
            stmt.setBytes(4, serialize(newLocks));
            return stmt.executeUpdate();
          }
        }
      }
    });
    writeQueue.add(result);
    return result;
  }

  @Override
  public Future<Integer> queueLockAcquisition(final List<HiveLock> acquiredLocks) {
    FutureTask<Integer> result = new FutureTask<>(new Callable<Integer>() {
      @Override
      public Integer call() throws Exception {
        try (Connection conn = connPool.getConnection()) {
          // Set auto-commit to on as we'll just be inserting one row, hopefully very quickly.
          conn.setAutoCommit(true);
          conn.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);

          String sql = "insert into TXN_WAL (TW_TYPE, TW_RECORDED_AT, TW_LOCKS)" +
              " values (?, ?, ?)";

          if (LOG.isDebugEnabled()) LOG.debug("Going to prepare statement " + sql);

          try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setInt(1, EntryType.ACQUIRE_LOCKS.ordinal());
            stmt.setLong(2, sqlGenerator.getDbTime(conn));
            stmt.setBytes(3, serialize(acquiredLocks));
            return stmt.executeUpdate();
          }
        }
      }
    });
    writeQueue.add(result);
    return result;
  }

  @Override
  public Future<Integer> queueForgetLocks(final List<HiveLock> locksToForget) {
    FutureTask<Integer> result = new FutureTask<>(new Callable<Integer>() {
      @Override
      public Integer call() throws Exception {
        try (Connection conn = connPool.getConnection()) {
          // Set auto-commit to on as we'll just be inserting one row, hopefully very quickly.
          conn.setAutoCommit(true);
          conn.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);

          String sql = "insert into TXN_WAL (TW_TYPE, TW_RECORDED_AT, TW_LOCKS)" +
              " values (?, ?, ?)";

          if (LOG.isDebugEnabled()) LOG.debug("Going to prepare statement " + sql);

          try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setInt(1, EntryType.FORGET_LOCKS.ordinal());
            stmt.setLong(2, sqlGenerator.getDbTime(conn));
            stmt.setBytes(3, serialize(locksToForget));
            return stmt.executeUpdate();
          }
        }
      }
    });
    writeQueue.add(result);
    return result;
  }

  @Override
  public Future<Integer> queueForgetTransactions(final List<? extends HiveTransaction> txns) {
    // Rather than serialize transactions, we add a record for each transaction.  This avoids
    // need to serialize a list of transactions
    FutureTask<Integer> result = new FutureTask<>(new Callable<Integer>() {
      @Override
      public Integer call() throws Exception {
        try (Connection conn = connPool.getConnection()) {
          conn.setAutoCommit(false); // no auto commit on this one as we'll insert multiple records
          conn.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);

          String sql = "insert into TXN_WAL (TW_TYPE, TW_RECORDED_AT, TW_TXNID)" +
              " values (?, ?, ?)";

          long now = sqlGenerator.getDbTime(conn);

          if (LOG.isDebugEnabled()) LOG.debug("Going to prepare statement " + sql);

          try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            int rc = 0;
            for (HiveTransaction txn : txns) {
              stmt.setInt(1, EntryType.FORGET_TXN.ordinal());
              stmt.setLong(2, now);
              stmt.setLong(3, txn.getTxnId());
              rc += stmt.executeUpdate();
            }
            conn.commit();
            return rc;
          }
        }
      }
    });
    writeQueue.add(result);
    return result;
  }

  @Override
  public void start() throws SQLException {
    try (Connection conn = connPool.getConnection()) {
      sqlGenerator = new SQLGenerator(SQLGenerator.determineDatabaseProduct(conn), conf);
    }

    // First, call the walToTableMover to move any existing records.  We'll move way more records
    // at a time, but still keep it bound to avoid situations where the WAL gets so big that we
    // can't move it over in a single transaction and we're completely wedged.
    numRecordsFromWalToDb = 10000; // TODO -make configurable
    do {
      walToTableMover.run();
    } while (walCount() > 0);

    // Now set up for regular running
    numRecordsFromWalToDb = 100; // TODO -make configurable
    int period = 500; // TODO - make configurable
    Random rand = new Random();
    threadPool.scheduleAtFixedRate(walToTableMover, period + rand.nextInt(period), period,
        TimeUnit.MILLISECONDS);
    threadPool.execute(walWriter);
  }

  private byte[] serialize(TBase obj) throws TException {
    TMemoryBuffer buf = new TMemoryBuffer(1024);
    TProtocol protocol = new TCompactProtocol(buf);
    obj.write(protocol);
    byte[] serialized = new byte[buf.length()];
    buf.read(serialized, 0, buf.length());
    return serialized;
  }

  private void deserialize(TBase obj, byte[] serialized) throws TException {
    TMemoryBuffer buf = new TMemoryBuffer(serialized.length);
    buf.write(serialized, 0, serialized.length);
    TProtocol protocol = new TCompactProtocol(buf);
    obj.read(protocol);
  }

  private byte[] serialize(List<? extends Writable> writables) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream(1024 * writables.size());
    DataOutput out = new DataOutputStream(baos);
    WritableUtils.writeVInt(out, writables.size());
    for (Writable writable : writables) writable.write(out);
    return baos.toByteArray();
  }

  private <T extends Writable> List<T> deserialize(Class<T> clazz, byte[] serialized)
      throws IOException, IllegalAccessException, InstantiationException {
    DataInput in = new DataInputStream(new ByteArrayInputStream(serialized));
    int numEntries = WritableUtils.readVInt(in);
    List<T> deserialized = new ArrayList<>(numEntries);
    for (int i = 0; i < numEntries; i++) {
      T element = (T)clazz.newInstance();
      element.readFields(in);
      deserialized.add(element);
    }
    return deserialized;
  }

  private int walCount() throws SQLException {
    try (Connection conn = connPool.getConnection()) {
      try (Statement stmt = conn.createStatement()) {
        ResultSet rs = stmt.executeQuery("select count(*) from TXN_WAL");
        rs.next();
        return rs.getInt(1);
      }
    }
  }

  private Runnable walWriter = new Runnable() {
    @Override
    public void run() {
      while (true) {
        try {
          FutureTask<?> nextWrite = writeQueue.take();
          nextWrite.run();
        } catch (InterruptedException e) {
          LOG.error("Received interuption waiting for next write to WAL, dying");
        }
      }
    }
  };

  private static final int TW_ID_POS = 1;
  private static final int TW_TYPE_POS = 2;
  private static final int TW_RECORDED_AT_POS = 3;
  private static final int TW_TXNID_POS = 4;
  private static final int TW_COMMIT_ID_POS = 5;
  private static final int TW_OPEN_TXN_RQST_POS = 6;
  private static final int TW_LOCK_RQST_POS = 7;
  private static final int TW_LOCKS_POS = 8;

  private Runnable walToTableMover = new Runnable() {
    @Override
    public void run() {
      try {
        try (Connection conn = connPool.getConnection()) {
          // Turn auto-commit off because we have to make sure we move the record from the WAL to
          // the DB in a single transaction.
          conn.setAutoCommit(false);
          // READ_COMMITTED should be fine as no one should ever be updating WAL records
          conn.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);

          // Don't use select for update since there's no need, no one will be updating these
          // rows, and no one better be deleting them but this thread.
          // Select everything in one pass.  This is clunky because each move method is now
          // affected anytime we modify the table.  But it's better than doing a single select to
          // get the id and type and a subsequent one to get just the columns the move method
          // cares about.
          try {
            String sql = "select TW_ID, TW_TYPE, TW_RECORDED_AT, TW_TXNID, TW_COMMIT_ID, " +
                "TW_OPEN_TXN_RQST, TW_LOCK_RQST, TW_LOCKS  from TXN_WAL order by TW_ID";
            sql = sqlGenerator.addLimitClause(numRecordsFromWalToDb, sql);

            List<Long> entriesToRemove = new ArrayList<>(numRecordsFromWalToDb);
            try (Statement stmt = conn.createStatement()) {
              LOG.debug("Going to execute query " + sql);
              ResultSet rs = stmt.executeQuery(sql);

              while (rs.next()) {
                entriesToRemove.add(rs.getLong(TW_ID_POS));
                EntryType entryType = EntryType.fromInteger(rs.getInt(TW_TYPE_POS));
                long recordedAt = rs.getLong(TW_RECORDED_AT_POS);
                switch (entryType) {
                  case OPEN_TXN:
                    moveOpenTxn(conn, recordedAt, rs);
                    break;
                  case ABORT_TXN:
                    moveAbortTxn(conn, rs);
                    break;
                  case COMMIT_TXN:
                    moveCommitTxn(conn, rs);
                    break;
                  case REQUEST_LOCKS:
                    moveRequestLocks(conn, recordedAt, rs);
                    break;
                  case ACQUIRE_LOCKS:
                    moveAcquireLocks(conn, recordedAt, rs);
                    break;
                  case FORGET_LOCKS:
                    moveForgetLocks(conn, rs);
                    break;
                  case FORGET_TXN:
                    moveForgetTxn(conn, rs);
                    break;
                  default:
                    throw new RuntimeException("Unknown entry type " + entryType);
                }
              }
            }

            try (Statement stmt = conn.createStatement()) {
              StringBuilder buf = new StringBuilder("delete from TXN_WAL where TW_ID in (");
              boolean first = true;
              for (long wid : entriesToRemove) {
                if (first) first = false;
                else buf.append(", ");
                buf.append(wid);
              }
              buf.append(')');
              if (LOG.isDebugEnabled()) LOG.debug("Going to execute statement " + buf.toString());
              stmt.execute(buf.toString());
            }

            conn.commit();
          } catch (SQLException e) {
            LOG.error("Caught SQL exception when moving records from the WAL to tables", e);
            conn.rollback();
            throw e;
          }
        }
      } catch (Throwable t) {
        LOG.error("Caught exception in wal to table mover thread, thread dying, this will be " +
            "bad!", t);
        // TODO put in marker showing this is dead so we can take down the whole txn service
      }
    }

    private void moveOpenTxn(Connection conn, long recordedAt, ResultSet rs)
        throws SQLException, TException {
      try (Statement stmt = conn.createStatement()) {

        long txnId = rs.getLong(TW_TXNID_POS);
        OpenTxnRequest rqst = new OpenTxnRequest();
        deserialize(rqst, rs.getBytes(TW_OPEN_TXN_RQST_POS));
        String row = txnId + "," + SQLGenerator.quoteChar(TxnHandler.TXN_OPEN) + "," +
            recordedAt + "," + recordedAt + "," + SQLGenerator.quoteString(rqst.getUser()) +
            "," + SQLGenerator.quoteString(rqst.getHostname());
        List<String> queries = sqlGenerator.createInsertValuesStmt(
            "TXNS (txn_id, txn_state, txn_started, txn_last_heartbeat, txn_user, txn_host)",
            Collections.singletonList(row));
        for (String q : queries) {
          LOG.debug("Going to execute update <" + q + ">");
          stmt.execute(q);
        }
      }
    }

    private void moveAbortTxn(Connection conn, ResultSet rs) throws SQLException {
      long txnId = rs.getLong(TW_TXNID_POS);

      // Set the transaction to aborted/committed
      try (Statement stmt = conn.createStatement()) {
        StringBuilder buf = new StringBuilder("update TXNS set txn_state = ")
            .append(SQLGenerator.quoteChar(TxnHandler.TXN_ABORTED))
            .append(" where txn_id = ")
            .append(txnId);

        if (LOG.isDebugEnabled()) LOG.debug("Going to execute statement " + buf.toString());
        stmt.executeUpdate(buf.toString());

        // Move any shared write locks to aborted/released
        buf = new StringBuilder("update HIVE_LOCKS set hl_lock_state = ")
            .append(SQLGenerator.quoteChar(TxnHandler.LOCK_ABORTED))
            .append(" where hl_txnid = ")
            .append(txnId)
            .append(" and hl_lock_type = ")
            .append(SQLGenerator.quoteChar(TxnHandler.LOCK_SEMI_SHARED));
        if (LOG.isDebugEnabled()) LOG.debug("Going to execute statement " + buf.toString());
        stmt.executeUpdate(buf.toString());

        // Delete any other locks
        buf = new StringBuilder("delete from HIVE_LOCKS where hl_txnid = ")
            .append(txnId)
            .append(" and hl_lock_type <> ")
            .append(SQLGenerator.quoteChar(TxnHandler.LOCK_SEMI_SHARED));
        if (LOG.isDebugEnabled()) LOG.debug("Going to execute statement " + buf.toString());
        stmt.executeUpdate(buf.toString());
      }
    }

    private void moveCommitTxn(Connection conn, ResultSet rs) throws SQLException {
      long txnId = rs.getLong(TW_TXNID_POS);
      long commitId = rs.getLong(TW_COMMIT_ID_POS);

      // Set the transaction to aborted/committed
      try (Statement stmt = conn.createStatement()) {
        StringBuilder buf = new StringBuilder("update TXNS set txn_state = ")
            .append(SQLGenerator.quoteChar(TxnHandler.TXN_COMMITTED))
            .append(", txn_committed_id = ")
            .append(commitId)
            .append(" where txn_id = ")
            .append(txnId);

        if (LOG.isDebugEnabled()) LOG.debug("Going to execute statement " + buf.toString());
        stmt.executeUpdate(buf.toString());

        // Move any shared write locks to aborted/released
        buf = new StringBuilder("update HIVE_LOCKS set hl_lock_state = ")
            .append(SQLGenerator.quoteChar(TxnHandler.LOCK_RELEASED))
            .append(" where hl_txnid = ")
            .append(txnId)
            .append(" and hl_lock_type = ")
            .append(SQLGenerator.quoteChar(TxnHandler.LOCK_SEMI_SHARED));
        if (LOG.isDebugEnabled()) LOG.debug("Going to execute statement " + buf.toString());
        stmt.executeUpdate(buf.toString());

        // Delete any other locks
        buf = new StringBuilder("delete from HIVE_LOCKS where hl_txnid = ")
            .append(txnId)
            .append(" and hl_lock_type <> ")
            .append(SQLGenerator.quoteChar(TxnHandler.LOCK_SEMI_SHARED));
        if (LOG.isDebugEnabled()) LOG.debug("Going to execute statement " + buf.toString());
        stmt.executeUpdate(buf.toString());

        // Move TXN_COMPONENTS entries to COMPLETED_TXN_COMPONENTS
        String s = "insert into COMPLETED_TXN_COMPONENTS select tc_txnid, tc_database, tc_table, " +
            "tc_partition from TXN_COMPONENTS where tc_txnid = " + txnId;
        LOG.debug("Going to execute insert <" + s + ">");
        stmt.executeUpdate(s);
        s = "delete from TXN_COMPONENTS where tc_txnid = " + txnId;
        LOG.debug("Going to execute delete <" + s + ">");
        stmt.executeUpdate(s);

        // I don't put entries in the WRITE_SET table because we don't need that table anymore.
        // That is answered from memory by tracking the committed transactions.
      }
    }

    private void moveRequestLocks(Connection conn, long recordedAt, ResultSet rs) throws
        SQLException, TException, IllegalAccessException, IOException, InstantiationException {
      LockRequest rqst = new LockRequest();
      deserialize(rqst, rs.getBytes(TW_LOCK_RQST_POS));
      List<HiveLock> newLocks = deserialize(HiveLock.class, rs.getBytes(TW_LOCKS_POS));

      try (Statement stmt = conn.createStatement()) {
        List<String> rows = new ArrayList<>();

        // For each component in this lock request that we need to track, add an entry to the
        // txn_components table
        for (LockComponent lc : rqst.getComponent()) {
          boolean updateTxnComponents;
          switch (lc.getOperationType()) {
            case INSERT:
            case UPDATE:
            case DELETE:
              updateTxnComponents = !lc.isIsDynamicPartitionWrite();
              break;

            case SELECT:
              updateTxnComponents = false;
              break;

            default:
              //since we have an open transaction, only 4 values above are expected
              throw new IllegalStateException("Unexpected DataOperationType: " + lc.getOperationType()
                  + " agentInfo=" + rqst.getAgentInfo() + " " + rqst.getTxnid());
          }

          if (!updateTxnComponents) continue;

          String tblName = lc.getTablename();
          String partName = lc.getPartitionname();
          rows.add(rqst.getTxnid() + ", '" + lc.getDbname() + "', " +
                (tblName == null ? "null" : "'" + tblName + "'") + ", " +
                (partName == null ? "null" : "'" + partName + "'")+ "," +
                SQLGenerator.quoteString(TxnHandler.OpertaionType.fromDataOperationType(
                    lc.getOperationType()).toString()));
        }
        List<String> queries = sqlGenerator.createInsertValuesStmt(
            "TXN_COMPONENTS (tc_txnid, tc_database, tc_table, tc_partition, tc_operation_type)", rows);
        for(String query : queries) {
          LOG.debug("Going to execute update <" + query + ">");
          stmt.executeUpdate(query);
        }

        // Create entries in the Locks table
        rows = new ArrayList<>();
        Iterator<HiveLock> locks = newLocks.iterator();
        for (LockComponent lc : rqst.getComponent()) {
          assert locks.hasNext();
          HiveLock newLock = locks.next();
          if (lc.isSetOperationType() && lc.getOperationType() == DataOperationType.UNSET &&
              (conf.getBoolVar(HiveConf.ConfVars.HIVE_IN_TEST) || conf.getBoolVar(HiveConf.ConfVars.HIVE_IN_TEZ_TEST))) {
            //old version of thrift client should have (lc.isSetOperationType() == false) but they do not
            //If you add a default value to a variable, isSet() for that variable is true regardless of the where the
            //message was created (for object variables.  It works correctly for boolean vars, e.g. LockComponent.isAcid).
            //in test mode, upgrades are not tested, so client version and server version of thrift always matches so
            //we see UNSET here it means something didn't set the appropriate value.
            throw new IllegalStateException("Bug: operationType=" + lc.getOperationType() + " for component "
                + lc + " agentInfo=" + rqst.getAgentInfo());
          }
          LockType lockType = lc.getType();
          char lockChar;
          switch (lockType) {
            case EXCLUSIVE:
              lockChar = TxnHandler.LOCK_EXCLUSIVE;
              break;
            case SHARED_READ:
              lockChar = TxnHandler.LOCK_SHARED;
              break;
            case SHARED_WRITE:
              lockChar = TxnHandler.LOCK_SEMI_SHARED;
              break;
            case INTENTION:
              lockChar = TxnHandler.LOCK_INTENTION;
              break;
            default: throw new RuntimeException("Unknown lock type " + lockType);
          }
          // Always set internal lock id to zero as it's part of the primary key, but we don't
          // need it anymore.
          rows.add(newLock.getLockId() + ", 0," + rqst.getTxnid() + ", " +
              SQLGenerator.quoteString(lc.getDbname()) + ", " +
              SQLGenerator.valueOrNullLiteral(lc.getTablename()) + ", " +
              SQLGenerator.valueOrNullLiteral(lc.getPartitionname()) + ", " +
              SQLGenerator.quoteChar(TxnHandler.LOCK_WAITING) + ", " +
              SQLGenerator.quoteChar(lockChar) + ", " + recordedAt + ", " +
              SQLGenerator.valueOrNullLiteral(rqst.getUser()) + ", " +
              SQLGenerator.valueOrNullLiteral(rqst.getHostname()) + ", " +
              SQLGenerator.valueOrNullLiteral(rqst.getAgentInfo()));// + ")";
        }
        queries = sqlGenerator.createInsertValuesStmt(
            "HIVE_LOCKS (hl_lock_ext_id, hl_lock_int_id, hl_txnid, hl_db, " +
                "hl_table, hl_partition,hl_lock_state, hl_lock_type, " +
                "hl_last_heartbeat, hl_user, hl_host, hl_agent_info)", rows);

        for (String query : queries) {
          LOG.debug("Going to execute update <" + query + ">");
          stmt.executeUpdate(query);
        }
      }
    }

    private void moveAcquireLocks(Connection conn, long recordedAt, ResultSet rs)
        throws SQLException, IllegalAccessException, IOException, InstantiationException {
      List<HiveLock> acquiredLocks = deserialize(HiveLock.class, rs.getBytes(TW_LOCKS_POS));
      try (Statement stmt = conn.createStatement()) {
        StringBuilder buf = new StringBuilder("update HIVE_LOCKS set hl_lock_state = '")
            .append(TxnHandler.LOCK_ACQUIRED)
            .append("', hl_last_heartbeat = ")
            .append(recordedAt)
            .append(", hl_acquired_at = ")
            .append(recordedAt)
            .append(", where hl_lock_ext_id in (");
        boolean first = true;
        for (HiveLock acquired : acquiredLocks) {
          if (first) first = false;
          else buf.append(", ");
          buf.append(acquired.getLockId());
        }
        if (LOG.isDebugEnabled()) LOG.debug("Going to execute update <" + buf.toString() + ">");
        stmt.executeUpdate(buf.toString());
      }
    }

    private void moveForgetLocks(Connection conn, ResultSet rs) throws SQLException,
        IllegalAccessException, IOException, InstantiationException {
      List<HiveLock> toBeForgotten = deserialize(HiveLock.class, rs.getBytes(TW_LOCKS_POS));
      try (Statement stmt = conn.createStatement()) {
        StringBuilder buf = new StringBuilder("delete from HIVE_LOCKS where hl_lock_ext_id in (");
        boolean first = true;
        for (HiveLock lock : toBeForgotten) {
          if (first) first = false;
          else buf.append(", ");
          buf.append(lock.getLockId());
        }
        buf.append(")");
        if (LOG.isDebugEnabled()) LOG.debug("Going to execute delete <" + buf.toString() + ">");
        stmt.executeUpdate(buf.toString());
      }

    }

    private void moveForgetTxn(Connection conn, ResultSet rs) throws SQLException {
      long txnId = rs.getLong(TW_TXNID_POS);
      try (Statement stmt = conn.createStatement()) {
        // Forget the transaction
        String sql = "delete from TXNS where txn_id = " + txnId;
        LOG.debug("Going to execute delete <" + sql + ">");
        stmt.execute(sql);

        // Forget any associated locks
        sql = "delete from HIVE_LOCKS where hl_txnid = " + txnId;
        LOG.debug("Going to execute delete <" + sql + ">");
        stmt.execute(sql);
      }
    }
  };

}
