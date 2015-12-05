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
package org.apache.hive.test.capybara.iface;

/**
 * A benchmark for testing results against.  This provides both the
 * {@link DataStore} and the
 * {@link ResultComparator} used to determine if a test was
 * successful.  Users can extend this, though in most cases one of the existing Benchmark
 * implementations should be sufficient.
 */
public interface Benchmark {

  /**
   * Return the DataStore to be used in this Benchmark.
   * @return DataStore
   */
  BenchmarkDataStore getBenchDataStore();

  /**
   * Get the ResultComparator to use with this Benchmark.
   * @param sort Whether the result sets should be sorted as part of doing the comparison.  If
   *             this is false it is assumed that the results are either already sorted or single
   *             valued.
   * @return ResultComparator
   */
  ResultComparator getResultComparator(boolean sort);

  /**
   * Get a ResultComparator that will compare data already in a table.  This is intended for use
   * with insert statements.  Note that it is generally slower for small data sets than
   * {@link #getResultComparator(boolean)} but much faster for large ones since it can run in the
   * cluster.
   * @return TableComparator
   */
  TableComparator getTableComparator();
}
