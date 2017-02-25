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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.locks.Lock;

/**
 * A wrapper to make lock implement closable.  The constructor of the object does not return
 * until the lock is acquired.  Calling close will release the lock.
 */
public class LockKeeper implements Closeable {
  static final private Logger LOG = LoggerFactory.getLogger(TransactionManager.class.getName());

  private final Lock lock;

  public LockKeeper(Lock lock) {
    this.lock = lock;
    if (LOG.isDebugEnabled()) LOG.debug("Waiting for lock " + lock.getClass().getName());
    this.lock.lock();
    if (LOG.isDebugEnabled()) LOG.debug("Acquired lock " + lock.getClass().getName());
  }

  @Override
  public void close() throws IOException {
    lock.unlock();
    if (LOG.isDebugEnabled()) LOG.debug("Released lock " + lock.getClass().getName());
  }
}
