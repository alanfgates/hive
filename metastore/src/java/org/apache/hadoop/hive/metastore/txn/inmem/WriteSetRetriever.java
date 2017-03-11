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

import java.io.IOException;
import java.util.Set;

/**
 * A class that retrieve write set information from storage.  These classes retrieve only
 * information about records that were updated or deleted.  We're not worried about inserts.
 */
public abstract class WriteSetRetriever {

  protected EntityKey entityKey;
  protected long txnId;
  protected Set<WriteSetRecordIdentifier> recordIds;

  protected WriteSetRetriever() {
  }

  protected void setEntityKey(EntityKey entityKey) {
    this.entityKey = entityKey;
  }

  protected void setTxnId(long txnId) {
    this.txnId = txnId;
  }

  Set<WriteSetRecordIdentifier> getRecordIds() {
    return recordIds;
  }

  /**
   * Go read the records off the file system.  It is assumed that this will take some time and it
   * will likely be called in a separate thread.
   * @throws IOException if something goes wrong reading the records.
   */
  public abstract void readRecordIds() throws IOException;
}
