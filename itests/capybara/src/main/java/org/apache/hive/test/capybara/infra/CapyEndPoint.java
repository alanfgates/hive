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
package org.apache.hive.test.capybara.infra;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hive.hcatalog.streaming.AbstractRecordWriter;
import org.apache.hive.hcatalog.streaming.ConnectionError;
import org.apache.hive.hcatalog.streaming.HiveEndPoint;
import org.apache.hive.hcatalog.streaming.ImpersonationFailed;
import org.apache.hive.hcatalog.streaming.InvalidPartition;
import org.apache.hive.hcatalog.streaming.InvalidTable;
import org.apache.hive.hcatalog.streaming.PartitionCreationFailed;
import org.apache.hive.hcatalog.streaming.RecordWriter;
import org.apache.hive.hcatalog.streaming.SerializationError;
import org.apache.hive.hcatalog.streaming.StreamingConnection;
import org.apache.hive.hcatalog.streaming.StreamingException;
import org.apache.hive.hcatalog.streaming.TransactionBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;

public class CapyEndPoint extends HiveEndPoint {
  static final private Logger LOG = LoggerFactory.getLogger(CapyEndPoint.class.getName());

  // A copy of the conf from IntegrationTest, used when conf is null so we don't create a new one.
  private final HiveConf testConf;
  private final BenchmarkDataStore bench;
  private final TestTable testTable;

  public CapyEndPoint(BenchmarkDataStore bench, TestTable testTable, HiveConf testConf,
                      String metaStoreUri, List<String> partitionVals) {
    super(metaStoreUri, testTable.getDbName(), testTable.getTableName(), partitionVals);
    this.bench = bench;
    this.testTable = testTable;
    this.testConf = testConf;
  }

  @Override
  public StreamingConnection newConnection(boolean createPartIfNotExists) throws
      ConnectionError, InvalidPartition, InvalidTable, PartitionCreationFailed, ImpersonationFailed,
      InterruptedException {
    return this.newConnection(createPartIfNotExists, testConf, null);
  }

  @Override
  public StreamingConnection newConnection(boolean createPartIfNotExists, HiveConf conf,
                                           UserGroupInformation authenticatedUser) throws
      ConnectionError, InvalidPartition, InvalidTable, PartitionCreationFailed, ImpersonationFailed,
      InterruptedException {
    StreamingConnection hiveStream =
        super.newConnection(createPartIfNotExists, conf, authenticatedUser);
    return new CapyStreamingConnection(hiveStream);
  }

  private class CapyStreamingConnection implements StreamingConnection {
    private final StreamingConnection hiveStream;
    private Connection conn;

    public CapyStreamingConnection(StreamingConnection hiveStream) {
      this.hiveStream = hiveStream;
    }

    @Override
    public TransactionBatch fetchTransactionBatch(int numTransactionsHint, RecordWriter writer)
        throws ConnectionError, StreamingException, InterruptedException {
      TransactionBatch hiveBatch = hiveStream.fetchTransactionBatch(numTransactionsHint, writer);
      try {
        conn = bench.getJdbcConnection(false);
        return new CapyTransactionBatch(hiveBatch, conn, writer);
      } catch (Exception e) {
        throw new ConnectionError("Unable to get connection to benchmark or instantiate object " +
            "inspectors", e);
      }
    }

    @Override
    public void close() {
      hiveStream.close();
      try {
        conn.close();
      } catch (SQLException e) {
        throw new RuntimeException(e);
      }
    }
  }

  private class CapyTransactionBatch implements TransactionBatch {
    private final TransactionBatch hiveBatch;
    private final Connection conn;
    private final AbstractRecordWriter writer;
    private final DataSet.RowBuilder rowBuilder;
    private final StructObjectInspector rowInspector;
    private final ObjectInspector[] colInspectors;
    private final DataSet.Row partRow;
    private final PreparedStatement preparedStatement;

    public CapyTransactionBatch(TransactionBatch hiveBatch, Connection conn,
                                RecordWriter writer) throws SerializationError, SerDeException {
      this.hiveBatch = hiveBatch;
      this.conn = conn;
      this.writer = (AbstractRecordWriter)writer;
      rowBuilder = new DataSet.RowBuilder(testTable.getCombinedSchema());
      rowInspector =
          (StructObjectInspector)((AbstractRecordWriter) writer).getSerde().getObjectInspector();
      List<? extends StructField> fields = rowInspector.getAllStructFieldRefs();
      colInspectors = new ObjectInspector[fields.size()];
      for (int i = 0; i < colInspectors.length; i++) {
        colInspectors[i] = fields.get(i).getFieldObjectInspector();
      }

      if (partitionVals != null && partitionVals.size() > 0) {
        DataSet.RowBuilder partRowBuilder = new DataSet.RowBuilder(testTable.getPartCols());
        partRow = partRowBuilder.build();
        for (int i = 0; i < partRow.size(); i++) {
          String val = partitionVals.get(i) == null ? "NULL" : partitionVals.get(i);
          partRow.get(i).fromString(val, "NULL");
        }
      } else {
        partRow = null;
      }
      try {
        preparedStatement = testTable.getLoadingStatement(conn, bench);
      } catch (SQLException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public void beginNextTransaction() throws StreamingException, InterruptedException {
      hiveBatch.beginNextTransaction();
    }

    @Override
    public Long getCurrentTxnId() {
      return hiveBatch.getCurrentTxnId();
    }

    @Override
    public TxnState getCurrentTransactionState() {
      return hiveBatch.getCurrentTransactionState();
    }

    @Override
    public void commit() throws StreamingException, InterruptedException {
      hiveBatch.commit();
      try {
        conn.commit();
      } catch (SQLException e) {
        throw new StreamingException("Couldn't commit exception", e);
      }
    }

    @Override
    public void abort() throws StreamingException, InterruptedException {
      hiveBatch.abort();
      try {
        conn.rollback();
      } catch (SQLException e) {
        throw new StreamingException("Couldn't rollback exception", e);
      }
    }

    @Override
    public int remainingTransactions() {
      return hiveBatch.remainingTransactions();
    }

    @Override
    public void write(byte[] record) throws StreamingException, InterruptedException {
      hiveBatch.write(record);
      writeBenchmark(record);
    }

    @Override
    public void write(Collection<byte[]> records) throws StreamingException, InterruptedException {
      hiveBatch.write(records);
      for (byte[] record : records) writeBenchmark(record);
    }

    private void writeBenchmark(byte[] record) throws StreamingException {
      // Convert the record from whatever format it came in (we don't know what it is) to an
      // Object that Hive can parse with ObjectInspectors
      Object objRow = writer.encode(record);

      // Convert the Object to our Row format.  This may not fill up all of the columns because
      // it won't have the partitions columns.
      DataSet.Row row = rowBuilder.build();
      List<Object> objCols = rowInspector.getStructFieldsDataAsList(objRow);
      for (int i = 0; i < colInspectors.length; i++) {
        row.get(i).fromObject(colInspectors[i], objCols.get(i));
      }

      // If there are any partition values, append them here
      if (partRow != null) row.append(partRow);

      // Load it in via a prepared statement
      try {
        for (DataSet.Column col : row) col.load(preparedStatement);
        preparedStatement.executeUpdate();
      } catch (SQLException e) {
        throw new StreamingException("Unable to load into benchmark", e);
      }
    }

    @Override
    public void heartbeat() throws StreamingException {
      hiveBatch.heartbeat();
    }

    @Override
    public void close() throws StreamingException, InterruptedException {
      hiveBatch.close();
    }

  }
}
