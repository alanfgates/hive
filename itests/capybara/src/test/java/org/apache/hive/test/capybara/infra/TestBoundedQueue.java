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

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Iterator;

public class TestBoundedQueue {

  @Test
  public void underCapacity() {
    BoundedQueue<Integer> queue = new BoundedQueue<>(10);
    queue.add(1);
    queue.add(2);

    Assert.assertEquals(2, queue.size());
    Iterator<Integer> iter = queue.iterator();
    Assert.assertEquals(1, (int)iter.next());
    Assert.assertEquals(2, (int)iter.next());
    Assert.assertFalse(iter.hasNext());

    Assert.assertEquals(1, (int)queue.poll());
    Assert.assertEquals(2, (int)queue.peek());
    Assert.assertEquals(2, (int)queue.poll());
    Assert.assertTrue(queue.isEmpty());
  }

  @Test
  public void atCapacity() {
    BoundedQueue<Integer> queue = new BoundedQueue<>(2);
    queue.add(1);
    queue.add(2);

    Assert.assertEquals(2, queue.size());
    Iterator<Integer> iter = queue.iterator();
    Assert.assertEquals(1, (int)iter.next());
    Assert.assertEquals(2, (int)iter.next());
    Assert.assertFalse(iter.hasNext());

    Assert.assertEquals(1, (int)queue.poll());
    Assert.assertEquals(2, (int)queue.poll());
    Assert.assertTrue(queue.isEmpty());
  }

  @Test
  public void overCapacity() {
    BoundedQueue<Integer> queue = new BoundedQueue<>(2);
    queue.add(1);
    queue.add(2);
    queue.add(3);
    queue.add(4);

    Assert.assertEquals(2, queue.size());
    Iterator<Integer> iter = queue.iterator();
    Assert.assertEquals(3, (int)iter.next());
    Assert.assertEquals(4, (int)iter.next());
    Assert.assertFalse(iter.hasNext());

    Assert.assertEquals(3, (int)queue.poll());
    Assert.assertEquals(4, (int)queue.poll());
    Assert.assertTrue(queue.isEmpty());
  }

  @Test
  public void addAll() {
    BoundedQueue<Integer> queue = new BoundedQueue<>(5);
    queue.addAll(Arrays.asList(1, 2, 3, 4));
    Assert.assertEquals(4, queue.size());
    Iterator<Integer> iter = queue.iterator();
    Assert.assertEquals(1, (int)iter.next());
    Assert.assertEquals(2, (int)iter.next());
    Assert.assertEquals(3, (int)iter.next());
    Assert.assertEquals(4, (int)iter.next());
    Assert.assertFalse(iter.hasNext());

    queue.addAll(Arrays.asList(5, 6, 7, 8));
    Assert.assertEquals(5, queue.size());
    Assert.assertEquals(4, (int)queue.poll());
    Assert.assertEquals(5, (int)queue.poll());
    Assert.assertEquals(6, (int)queue.poll());
    Assert.assertEquals(7, (int)queue.poll());
    Assert.assertEquals(8, (int)queue.poll());
    Assert.assertTrue(queue.isEmpty());
  }

  @Test
  public void offer() {
    BoundedQueue<Integer> queue = new BoundedQueue<>(2);
    queue.offer(1);
    queue.offer(2);

    Assert.assertEquals(2, queue.size());
    Iterator<Integer> iter = queue.iterator();
    Assert.assertEquals(1, (int)iter.next());
    Assert.assertEquals(2, (int)iter.next());
    Assert.assertFalse(iter.hasNext());

    queue.offer(3);
    queue.offer(4);

    Assert.assertEquals(3, (int)queue.poll());
    Assert.assertEquals(4, (int)queue.poll());
    Assert.assertTrue(queue.isEmpty());
  }
}
