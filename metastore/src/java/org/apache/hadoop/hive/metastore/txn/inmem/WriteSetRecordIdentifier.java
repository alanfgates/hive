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

/**
 * A representation of the ACID rowid.  The RecordIdentifier class used by the storage system
 * cannot be used here because it is defined in QL.  We need to look at whether it makes sense to
 * move that to the storage api so it can be shared here.
 */
public class WriteSetRecordIdentifier {

  final private long txnId;
  final private long bucketId;
  final private long rowId;

  public WriteSetRecordIdentifier(long txnId, long bucketId, long rowId) {
    this.txnId = txnId;
    this.bucketId = bucketId;
    this.rowId = rowId;
  }

  public long getTxnId() {
    return txnId;
  }

  public long getBucketId() {
    return bucketId;
  }

  public long getRowId() {
    return rowId;
  }

  @Override
  public int hashCode() {
    return (int)((txnId * 31 + bucketId) * 31 + rowId);
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof WriteSetRecordIdentifier)) return false;
    WriteSetRecordIdentifier that = (WriteSetRecordIdentifier)obj;
    return txnId == that.txnId && bucketId == that.bucketId && rowId == that.rowId;
  }

  @Override
  public String toString() {
    return "txnId: " + txnId + " bucketId: " + bucketId + " rowId: " + rowId;
  }
}
