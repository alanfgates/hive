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

import org.apache.hadoop.hive.metastore.api.LockState;
import org.apache.hadoop.hive.metastore.api.LockType;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

class HiveLock implements Writable {
  private long txnId;             // Can't be final due to Writable, but don't ever change this
  private long lockId;            // Can't be final due to Writable, but don't ever change this
  private EntityKey entityLocked; // Can't be final due to Writable, but don't ever change this
  private LockType type;          // Can't be final due to Writable, but don't ever change this
  private LockState state;

  /**
   * Only intended for use by the WAL when deserializing lock information, do not ever call this
   * directly.  The locks it creates lack most necessary information.
   */
  HiveLock() {
    entityLocked = null;
    type = null;
    state = null;
  }

  /**
   * Used to create a new lock.  The lock will be placed in the WAITING state.
   * @param lockId id of this lock
   * @param txnId transaction this  lock is part of
   * @param entityLocked the object that is locked
   * @param type type of lock
   */
  HiveLock(long lockId, long txnId, EntityKey entityLocked, LockType type) {
    this.txnId = txnId;
    this.lockId = lockId;
    this.entityLocked = entityLocked;
    this.type = type;
    state = LockState.WAITING;
  }

  public long getTxnId() {
    return txnId;
  }

  public long getLockId() {
    return lockId;
  }

  public EntityKey getEntityLocked() {
    return entityLocked;
  }

  public LockType getType() {
    return type;
  }

  public LockState getState() {
    return state;
  }

  public void setState(LockState state) {
    this.state = state;
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof HiveLock)) return false;
    HiveLock other = (HiveLock)o;
    return txnId == other.txnId && lockId == other.lockId;
  }

  @Override
  public int hashCode() {
    return (int)(txnId * 31 + lockId);
  }

  @Override
  public String toString() {
    return "entityLocked: " + entityLocked + " txnId: " + txnId + " lockId: " + lockId + " type "
        + type + " state " + state;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    WritableUtils.writeVLong(out, txnId);
    WritableUtils.writeVLong(out, lockId);
    entityLocked.write(out);
    WritableUtils.writeEnum(out, type);
    WritableUtils.writeEnum(out, state);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    txnId = WritableUtils.readVLong(in);
    lockId = WritableUtils.readVLong(in);
    entityLocked = new EntityKey();
    entityLocked.readFields(in);
    type = WritableUtils.readEnum(in, LockType.class);
    state = WritableUtils.readEnum(in, LockState.class);
  }
}
