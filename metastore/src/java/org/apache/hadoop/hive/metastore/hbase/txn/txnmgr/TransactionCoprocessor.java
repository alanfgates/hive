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

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.CoprocessorService;
import org.apache.hadoop.hbase.protobuf.ResponseConverter;
import org.apache.hadoop.hive.metastore.hbase.HbaseMetastoreProto;

import java.io.IOException;

/**
 * This forwards the calls to
 * {@link org.apache.hadoop.hive.metastore.hbase.txn.txnmgr.TransactionManager}.  It is separate
 * from it to enable unit testing without the Protocol Buffer framework.
 */
public class TransactionCoprocessor extends HbaseMetastoreProto.TxnMgr
    implements Coprocessor, CoprocessorService {
  // Don't ever grab this directly, always use getTxnMgr
  private static TransactionManager txnMgr = null;

  private Configuration conf;

  private TransactionManager getTxnMgr() throws IOException {
    return txnMgr;
  }

  private void restart(Configuration conf) throws IOException {
    synchronized (TransactionCoprocessor.class) {
      if (txnMgr != null) txnMgr.shutdown();
      txnMgr = new TransactionManager(conf);
    }
  }

  private void shutdown() throws IOException {
    synchronized (TransactionCoprocessor.class) {
      if (txnMgr != null) txnMgr.shutdown();
    }
  }

  @Override
  public void start(CoprocessorEnvironment coprocessorEnvironment) throws IOException {
    conf = coprocessorEnvironment.getConfiguration();
    synchronized (TransactionCoprocessor.class) {
      txnMgr = new TransactionManager(conf);
    }
  }

  @Override
  public void stop(CoprocessorEnvironment coprocessorEnvironment) throws IOException {
    shutdown();
  }

  @Override
  public Service getService() {
    return this;
  }

  @Override
  public void openTxns(RpcController controller, HbaseMetastoreProto.OpenTxnsRequest request,
                       RpcCallback<HbaseMetastoreProto.OpenTxnsResponse> done) {
    HbaseMetastoreProto.OpenTxnsResponse response = null;
    try {
      try {
        response = getTxnMgr().openTxns(request);
      } catch (SeverusPleaseException e) {
        restart(conf);
      }
    } catch (IOException e) {
      ResponseConverter.setControllerException(controller, e);
    }
    done.run(response);

  }

  @Override
  public void getOpenTxns(RpcController controller, HbaseMetastoreProto.Void request,
                          RpcCallback<HbaseMetastoreProto.GetOpenTxnsResponse> done) {
    HbaseMetastoreProto.GetOpenTxnsResponse response = null;
    try {
      try {
        response = getTxnMgr().getOpenTxns(request);
      } catch (SeverusPleaseException e) {
        restart(conf);
      }
    } catch (IOException e) {
      ResponseConverter.setControllerException(controller, e);
    }
    done.run(response);
  }

  @Override
  public void abortTxn(RpcController controller, HbaseMetastoreProto.TransactionId request,
                       RpcCallback<HbaseMetastoreProto.TransactionResult> done) {
    HbaseMetastoreProto.TransactionResult response = null;
    try {
      try {
        response = getTxnMgr().abortTxn(request);
      } catch (SeverusPleaseException e) {
        restart(conf);
      }
    } catch (IOException e) {
      ResponseConverter.setControllerException(controller, e);
    }
    done.run(response);
  }

  @Override
  public void commitTxn(RpcController controller, HbaseMetastoreProto.TransactionId request,
                        RpcCallback<HbaseMetastoreProto.TransactionResult> done) {
    HbaseMetastoreProto.TransactionResult response = null;
    try {
      try {
        response = getTxnMgr().commitTxn(request);
      } catch (SeverusPleaseException e) {
        restart(conf);
      }
    } catch (IOException e) {
      ResponseConverter.setControllerException(controller, e);
    }
    done.run(response);
  }

  @Override
  public void heartbeat(RpcController controller,
                        HbaseMetastoreProto.HeartbeatTxnRangeRequest request,
                        RpcCallback<HbaseMetastoreProto.HeartbeatTxnRangeResponse> done) {
    HbaseMetastoreProto.HeartbeatTxnRangeResponse response = null;
    try {
      try {
        response = getTxnMgr().heartbeat(request);
      } catch (SeverusPleaseException e) {
        restart(conf);
      }
    } catch (IOException e) {
      ResponseConverter.setControllerException(controller, e);
    }
    done.run(response);
  }

  @Override
  public void lock(RpcController controller, HbaseMetastoreProto.LockRequest request,
                   RpcCallback<HbaseMetastoreProto.LockResponse> done) {
    HbaseMetastoreProto.LockResponse response = null;
    try {
      try {
        response = getTxnMgr().lock(request);
      } catch (SeverusPleaseException e) {
        restart(conf);
      }
    } catch (IOException e) {
      ResponseConverter.setControllerException(controller, e);
    }
    done.run(response);
  }

  @Override
  public void checkLocks(RpcController controller, HbaseMetastoreProto.TransactionId request,
                         RpcCallback<HbaseMetastoreProto.LockResponse> done) {
    HbaseMetastoreProto.LockResponse response = null;
    try {
      try {
        response = getTxnMgr().checkLocks(request);
      } catch (SeverusPleaseException e) {
        restart(conf);
      }
    } catch (IOException e) {
      ResponseConverter.setControllerException(controller, e);
    }
    done.run(response);
  }

  @Override
  public void addDynamicPartitions(RpcController controller,
                                   HbaseMetastoreProto.AddDynamicPartitionsRequest request,
                                   RpcCallback<HbaseMetastoreProto.TransactionResult> done) {
    HbaseMetastoreProto.TransactionResult response = null;
    try {
      try {
        response = getTxnMgr().addDynamicPartitions(request);
      } catch (SeverusPleaseException e) {
        restart(conf);
      }
    } catch (IOException e) {
      ResponseConverter.setControllerException(controller, e);
    }
    done.run(response);
  }

  @Override
  public void cleanupAfterCompaction(RpcController controller,
                                     HbaseMetastoreProto.Compaction request,
                                     RpcCallback<HbaseMetastoreProto.Void> done) {
    HbaseMetastoreProto.Void response = null;
    try {
      try {
        response = getTxnMgr().cleanupAfterCompaction(request);
      } catch (SeverusPleaseException e) {
        restart(conf);
      }
    } catch (IOException e) {
      ResponseConverter.setControllerException(controller, e);
    }
    done.run(response);
  }

  @Override
  public void verifyCompactionCanBeCleaned(RpcController controller,
                                           HbaseMetastoreProto.CompactionList request,
                                           RpcCallback<HbaseMetastoreProto.CompactionList> done) {
    HbaseMetastoreProto.CompactionList response = null;
    try {
      try {
        response = getTxnMgr().verifyCompactionCanBeCleaned(request);
      } catch (SeverusPleaseException e) {
        restart(conf);
      }
    } catch (IOException e) {
      ResponseConverter.setControllerException(controller, e);
    }
    done.run(response);
  }

  @VisibleForTesting
  public TransactionManager backdoor() {
    return txnMgr;
  }

}
