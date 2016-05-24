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
import org.apache.hadoop.security.UserGroupInformation;
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
import org.apache.hive.test.capybara.iface.TestTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;

public class CapyEndPoint extends HiveEndPoint {
  static final private Logger LOG = LoggerFactory.getLogger(CapyEndPoint.class.getName());

  // A copy of the conf from IntegrationTest, used when conf is null so we don't create a new one.
  private final HiveConf hiveConf;
  private final HiveEndPoint testEndPoint, benchEndPoint;

  public CapyEndPoint(HiveEndPoint testEndPoint, HiveEndPoint benchEndPoint, TestTable testTable,
                      HiveConf testConf, List<String> partitionVals) {
    super(testConf.getVar(HiveConf.ConfVars.METASTOREURIS), testTable.getDbName(), testTable.getTableName(),
        partitionVals);
    this.testEndPoint = testEndPoint;
    this.benchEndPoint = benchEndPoint;
    this.hiveConf = testConf;
  }

  @Override
  public StreamingConnection newConnection(boolean createPartIfNotExists) throws
      ConnectionError, InvalidPartition, InvalidTable, PartitionCreationFailed, ImpersonationFailed,
      InterruptedException {
    return this.newConnection(createPartIfNotExists, hiveConf, null);
  }

  @Override
  public StreamingConnection newConnection(boolean createPartIfNotExists, HiveConf conf,
                                           UserGroupInformation authenticatedUser) throws
      ConnectionError, InvalidPartition, InvalidTable, PartitionCreationFailed, ImpersonationFailed,
      InterruptedException {
    return new CapyStreamingConnection(
        testEndPoint.newConnection(createPartIfNotExists, conf, authenticatedUser),
        benchEndPoint.newConnection(createPartIfNotExists, conf, authenticatedUser));
  }

  private class CapyStreamingConnection implements StreamingConnection {

    private final StreamingConnection testStreamingConn, benchStreamingConn;

    public CapyStreamingConnection(StreamingConnection testStreamingConn,
                                   StreamingConnection benchStreamingConn) {
      this.testStreamingConn = testStreamingConn;
      this.benchStreamingConn = benchStreamingConn;
    }

    @Override
    public TransactionBatch fetchTransactionBatch(int numTransactionsHint, RecordWriter writer)
        throws ConnectionError, StreamingException, InterruptedException {
      TransactionBatch testBatch = testStreamingConn.fetchTransactionBatch(numTransactionsHint, writer);
      TransactionBatch benchBatch = benchStreamingConn.fetchTransactionBatch(numTransactionsHint, writer);
      try {
        return new CapyTransactionBatch(testBatch, benchBatch);
      } catch (SerDeException e) {
        throw new StreamingException(e.getMessage(), e);
      }
    }

    @Override
    public void close() {
      testStreamingConn.close();
      benchStreamingConn.close();
    }
  }

  private class CapyTransactionBatch implements TransactionBatch {
    private final TransactionBatch testBatch;
    private final TransactionBatch benchBatch;

    public CapyTransactionBatch(TransactionBatch testBatch, TransactionBatch benchBatch)
        throws SerializationError, SerDeException {
      this.testBatch = testBatch;
      this.benchBatch = benchBatch;

    }

    @Override
    public void beginNextTransaction() throws StreamingException, InterruptedException {
      testBatch.beginNextTransaction();
      benchBatch.beginNextTransaction();
    }

    @Override
    public Long getCurrentTxnId() {
      return testBatch.getCurrentTxnId();
    }

    @Override
    public TxnState getCurrentTransactionState() {
      return testBatch.getCurrentTransactionState();
    }

    @Override
    public void commit() throws StreamingException, InterruptedException {
      testBatch.commit();
      benchBatch.commit();
    }

    @Override
    public void abort() throws StreamingException, InterruptedException {
      testBatch.abort();
      benchBatch.abort();
    }

    @Override
    public int remainingTransactions() {
      return testBatch.remainingTransactions();
    }

    @Override
    public void write(byte[] record) throws StreamingException, InterruptedException {
      testBatch.write(record);
      benchBatch.write(record);
    }

    @Override
    public void write(Collection<byte[]> records) throws StreamingException, InterruptedException {
      testBatch.write(records);
      benchBatch.write(records);
    }

    @Override
    public void heartbeat() throws StreamingException {
      testBatch.heartbeat();
      benchBatch.heartbeat();
    }

    @Override
    public void close() throws StreamingException, InterruptedException {
      testBatch.close();
      benchBatch.close();
    }

    @Override
    public boolean isClosed() {
      return testBatch.isClosed() && benchBatch.isClosed();
    }
  }
}
