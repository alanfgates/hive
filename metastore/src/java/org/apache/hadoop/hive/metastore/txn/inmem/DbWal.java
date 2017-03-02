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
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * A real WAL implementation
 */
public class DbWal implements WriteAheadLog {
  static final private Logger LOG = LoggerFactory.getLogger(DbWal.class.getName());

  private final DataSource connPool;
  private final ScheduledThreadPoolExecutor threadPool;
  private final HiveConf conf;
  private SQLGenerator sqlGenerator;

  public DbWal(DataSource connPool, ScheduledThreadPoolExecutor threadPool, HiveConf conf) {
    this.connPool = connPool;
    this.threadPool = threadPool;
    this.conf = conf;
    int period = 500; // TODO - make configurable
    Random rand = new Random();
    threadPool.scheduleAtFixedRate(walToTableMover, period + rand.nextInt(period), period,
        TimeUnit.MILLISECONDS);
  }

  @Override
  public Future<WriteAheadLog> queueOpenTxn(final long txnId, final OpenTxnRequest rqst) {
    FutureTask<WriteAheadLog> result = new FutureTask<>(new Callable<WriteAheadLog>() {
      @Override
      public WriteAheadLog call() throws Exception {
        writeToDb(EntryType.OPEN_TXN, txnId, serialize(rqst), null);
        return DbWal.this;
      }
    });
    threadPool.execute(result);
    return result;
  }

  @Override
  public Future<WriteAheadLog> queueAbortTxn(final OpenTransaction openTxn) {
    FutureTask<WriteAheadLog> result = new FutureTask<>(new Callable<WriteAheadLog>() {
      @Override
      public WriteAheadLog call() throws Exception {
        writeToDb(EntryType.ABORT_TXN, openTxn.getTxnId(), null, null);
        return DbWal.this;
      }
    });
    threadPool.execute(result);
    return result;
  }

  @Override
  public Future<WriteAheadLog> queueCommitTxn(final OpenTransaction openTxn) {
    FutureTask<WriteAheadLog> result = new FutureTask<>(new Callable<WriteAheadLog>() {
      @Override
      public WriteAheadLog call() throws Exception {
        writeToDb(EntryType.COMMIT_TXN, openTxn.getTxnId(), null, null);
        return DbWal.this;
      }
    });
    threadPool.execute(result);
    return result;
  }

  @Override
  public Future<WriteAheadLog> queueLockRequest(final LockRequest rqst,
                                                final List<HiveLock> newLocks) {
    FutureTask<WriteAheadLog> result = new FutureTask<>(new Callable<WriteAheadLog>() {
      @Override
      public WriteAheadLog call() throws Exception {
        writeToDb(EntryType.REQUEST_LOCKS, null, serialize(rqst), serialize(newLocks));
        return DbWal.this;
      }
    });
    threadPool.execute(result);
    return result;
  }

  @Override
  public Future<WriteAheadLog> queueLockAcquisition(final List<HiveLock> acquiredLocks) {
    FutureTask<WriteAheadLog> result = new FutureTask<>(new Callable<WriteAheadLog>() {
      @Override
      public WriteAheadLog call() throws Exception {
        writeToDb(EntryType.ACQUIRE_LOCKS, null, null, serialize(acquiredLocks));
        return DbWal.this;
      }
    });
    threadPool.execute(result);
    return result;
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

  // txnId is Long (rather than long) so it can be null
  private void writeToDb(EntryType entryType, Long txnId, byte[] serializedRqst,
                         byte[] serializedLocks) throws SQLException, MetaException {
    assert txnId != null || serializedRqst != null;
    try (Connection conn = connPool.getConnection()) {
      // Set auto-commit to on as we'll just be inserting one row, hopefully very quickly.
      conn.setAutoCommit(true);
      conn.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);

      // TODO - suspect I need to change this to use SQLGenerator.createInsertValuesStmt
      StringBuilder buf = new StringBuilder("insert into TXN_WAL (TW_TYPE");
      if (txnId != null) buf.append(", TW_TXN_ID");
      if (serializedRqst != null) buf.append(", TW_REQUEST");
      if (serializedLocks != null) buf.append(", TW_LOCKS");
      buf.append(", TW_RECORDED_AT) values (?");
      if (txnId != null) buf.append(", ?");
      if (serializedRqst != null) buf.append(", ?");
      if (serializedLocks != null) buf.append(", ?");
      buf.append(", ?)");

      if (LOG.isDebugEnabled()) LOG.debug("Going to prepare statement " + buf.toString());

      try (PreparedStatement stmt = conn.prepareStatement(buf.toString())) {
        stmt.setInt(1, entryType.ordinal());
        int nextNum = 2;
        if (txnId != null) stmt.setLong(nextNum++, txnId);
        if (serializedRqst != null) stmt.setBytes(nextNum++, serializedRqst);
        if (serializedLocks != null) stmt.setBytes(nextNum++, serializedLocks);
        stmt.setLong(nextNum, sqlGenerator.getDbTime(conn));
        stmt.execute();
      }
    }
  }

  private void initializeSqlGenerator() throws SQLException {
    if (sqlGenerator == null) {
      try (Connection conn = connPool.getConnection()) {
        sqlGenerator = new SQLGenerator(SQLGenerator.determineDatabaseProduct(conn), conf);
      }
    }
  }

  private Runnable walToTableMover = new Runnable() {
    @Override
    public void run() {
      try {
        initializeSqlGenerator();

        try (Connection conn = connPool.getConnection()) {
          // Turn auto-commit off because we have to make sure we move the record from the WAL to
          // the DB in a single transaction.
          conn.setAutoCommit(false);
          // READ_COMMITTED should be fine as no one should ever be updating WAL records
          conn.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);

          // Don't use select for update since there's no need, no one will be updating these
          // rows, and no one better be deleting them but this thread.
          try {
            String sql = "select TW_ID, TW_TYPE, TW_TXN_ID, TW_REQUEST, TW_LOCKS, TW_RECORDED_AT " +
                "from TXN_WAL order by TW_ID";
            // TODO - make configurable
            int numRowsToMove = 10;
            sql = sqlGenerator.addLimitClause(numRowsToMove, sql);

            List<Long> entriesToRemove = new ArrayList<>(numRowsToMove);
            try (Statement stmt = conn.createStatement()) {
              LOG.debug("Going to execute query " + sql);
              ResultSet rs = stmt.executeQuery(sql);

              while (rs.next()) {
                entriesToRemove.add(rs.getLong(1));
                EntryType entryType = EntryType.fromInteger(rs.getInt(2));
                switch (entryType) {
                  case OPEN_TXN:
                    moveOpenTxn(conn, rs);
                    break;
                  case ABORT_TXN:
                    moveAbortTxn(conn, rs);
                    break;
                  case COMMIT_TXN:
                    moveCommitTxn(conn, rs);
                    break;
                  case REQUEST_LOCKS:
                    moveRequestLocks(conn, rs);
                    break;
                  case ACQUIRE_LOCKS:
                    moveAcquireLocks(conn, rs);
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

    private void moveOpenTxn(Connection conn, ResultSet rs) throws SQLException, TException {
      long txnId = rs.getLong(3);
      OpenTxnRequest rqst = new OpenTxnRequest();
      deserialize(rqst, rs.getBytes(4));
      long recordedAt = rs.getLong(6);

      try (Statement stmt = conn.createStatement()) {

        String row = txnId + "," + SQLGenerator.quoteChar(TxnHandler.TXN_OPEN) + "," + recordedAt + "," +
            recordedAt + "," + SQLGenerator.quoteString(rqst.getUser()) + "," +
            SQLGenerator.quoteString(rqst.getHostname());
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
      moveCommitOrAbortTxn(conn, rs, TxnHandler.TXN_ABORTED, TxnHandler.LOCK_ABORTED);
    }

    private void moveCommitTxn(Connection conn, ResultSet rs) throws SQLException {
      moveCommitOrAbortTxn(conn, rs, TxnHandler.TXN_COMMITTED, TxnHandler.LOCK_RELEASED);
    }

    private void moveCommitOrAbortTxn(Connection conn, ResultSet rs, char newTxnState,
                                      char newLockState) throws SQLException {
      long txnId = rs.getLong(3);

      // Set the transaction to aborted/committed
      try (Statement stmt = conn.createStatement()) {
        StringBuilder buf = new StringBuilder("update TXNS set txn_state = ")
            .append(SQLGenerator.quoteChar(newTxnState))
            .append(" where txn_id = ")
            .append(txnId);

        if (LOG.isDebugEnabled()) LOG.debug("Going to execute statement " + buf.toString());
        stmt.executeUpdate(buf.toString());
      }

      // Move any shared write locks to aborted/released
      try (Statement stmt = conn.createStatement()) {
        StringBuilder buf = new StringBuilder("update HIVE_LOCKS set hl_lock_state = ")
            .append(SQLGenerator.quoteChar(newLockState))
            .append(" where hl_txnid = ")
            .append(txnId)
            .append(" and hl_lock_type = ")
            .append(SQLGenerator.quoteChar(newTxnState));
        if (LOG.isDebugEnabled()) LOG.debug("Going to execute statement " + buf.toString());
        stmt.executeUpdate(buf.toString());
      }

      // Delete any other locks
      try (Statement stmt = conn.createStatement()) {
        StringBuilder buf = new StringBuilder("delete from HIVE_LOCKS where hl_txnid = ")
            .append(txnId)
            .append(" and hl_lock_type <> ")
            .append(SQLGenerator.quoteChar(TxnHandler.LOCK_SEMI_SHARED));
        if (LOG.isDebugEnabled()) LOG.debug("Going to execute statement " + buf.toString());
        stmt.executeUpdate(buf.toString());
      }
    }

    private void moveRequestLocks(Connection conn, ResultSet rs) throws SQLException, TException,
        IllegalAccessException, IOException, InstantiationException {
      LockRequest rqst = new LockRequest();
      deserialize(rqst, rs.getBytes(4));
      List<HiveLock> newLocks = deserialize(HiveLock.class, rs.getBytes(5));
      long recordedAt = rs.getLong(6);

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
      }

      // Create entries in the Locks table
      try (Statement stmt = conn.createStatement()) {
        List<String> rows = new ArrayList<>();
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
          char lockChar = 'z';
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
        List<String> queries = sqlGenerator.createInsertValuesStmt(
            "HIVE_LOCKS (hl_lock_ext_id, hl_lock_int_id, hl_txnid, hl_db, " +
                "hl_table, hl_partition,hl_lock_state, hl_lock_type, " +
                "hl_last_heartbeat, hl_user, hl_host, hl_agent_info)", rows);

        for (String query : queries) {
          LOG.debug("Going to execute update <" + query + ">");
          stmt.executeUpdate(query);
        }
      }
    }

    private void moveAcquireLocks(Connection conn, ResultSet rs)
        throws SQLException, IllegalAccessException, IOException, InstantiationException,
        MetaException {
      List<HiveLock> acquiredLocks = deserialize(HiveLock.class, rs.getBytes(4));
      long recordedAt = rs.getLong(6);
      try (Statement stmt = conn.createStatement()) {
        for (HiveLock acquired : acquiredLocks) {
          String s = "update HIVE_LOCKS set hl_lock_state = '" + TxnHandler.LOCK_ACQUIRED + "', " +
              "hl_last_heartbeat = " + recordedAt +
              ", hl_acquired_at = " + recordedAt +
              ",HL_BLOCKEDBY_EXT_ID=NULL,HL_BLOCKEDBY_INT_ID=null where hl_lock_ext_id = " +
              acquired.getLockId() + " and hl_txnid = " + acquired.getTxnId();
          LOG.debug("Going to execute update <" + s + ">");
          stmt.executeUpdate(s);
        }
      }
    }
  };

  // TODO write thread to move items from WAL to regular tables
}
