/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hive.test.capybara.infra;

import org.apache.hive.test.capybara.iface.Benchmark;
import org.apache.hive.test.capybara.iface.BenchmarkDataStore;
import org.apache.hive.test.capybara.iface.ResultComparator;
import org.apache.hive.test.capybara.iface.TableComparator;

/**
 * A Benchmark that uses Derby to store the results and standard ResultComparators for comparison.
 */
public class DerbyBenchmark implements Benchmark {
  BenchmarkDataStore store;

  @Override
  public BenchmarkDataStore getBenchDataStore() {
    if (store == null) {
      store = new DerbyStore();
    }
    return store;
  }

  @Override
  public ResultComparator getResultComparator(boolean sort) {
    if (sort) return new SortingComparator();
    else return new NonSortingComparator();
  }

  @Override
  public TableComparator getTableComparator() {
    return new TableComparator();
  }
}
