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

import com.google.common.collect.ForwardingQueue;

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Queue;

/**
 * An impelmentation of queue that is bounded in capacity.  When capacity is reached and new
 * elements are added, old elements are dropped off the back.
 */
public class BoundedQueue<T> extends ForwardingQueue<T> {
  private final int size;
  Queue<T> elements;

  public BoundedQueue(int size) {
    this.size = size;
    elements = new ArrayDeque<>(size);
  }

  @Override
  protected Queue<T> delegate() {
    return elements;
  }

  @Override
  public boolean add(T t) {
    while (elements.size() >= size) elements.poll();
    elements.add(t);
    return true;
  }

  @Override
  public boolean addAll(Collection<? extends T> c) {
    while (elements.size() + c.size() > size) elements.poll();
    elements.addAll(c);
    return true;
  }

  @Override
  public boolean offer(T t) {
    return add(t);
  }
}
