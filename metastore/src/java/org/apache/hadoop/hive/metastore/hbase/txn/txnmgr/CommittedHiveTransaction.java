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

import org.apache.hadoop.hive.metastore.hbase.HbaseMetastoreProto;

import java.io.IOException;

public class CommittedHiveTransaction extends HiveTransaction {

  private final long commitId;

  /**
   * For use when creating a newly committed transaction.
   * @param openTxn open transaction that is moving to a committed state.
   * @param commitId id assigned at commit time.
   */
  CommittedHiveTransaction(OpenHiveTransaction openTxn, long commitId) {
    super(openTxn.getId());
    this.commitId = commitId;
  }

  /**
   * For use when recovering transactions from HBase.
   * @param hbaseTxn transaction record from HBase.
   * @throws IOException
   */
  CommittedHiveTransaction(HbaseMetastoreProto.Transaction hbaseTxn)
      throws IOException {
    super(hbaseTxn.getId());
    this.commitId = hbaseTxn.getCommitId();
  }

  HbaseMetastoreProto.TxnState getState() {
    return HbaseMetastoreProto.TxnState.COMMITTED;
  }

  long getCommitId() {
    return commitId;
  }
}
