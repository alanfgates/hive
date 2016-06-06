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

import org.apache.hive.test.capybara.data.DataSet;
import org.apache.hive.test.capybara.infra.TestManager;

import java.io.IOException;

/**
 * Generate data for use by Hive and benchmarks.
 */
public abstract class DataGenerator {

  /**
   * Generate data.
   * @param table table to generate for
   * @param scale Scale of data to generate.  The unit is kilobytes.  As a general rule you
   *              should not set this as it prevents the system from scaling automatically.  You
   *              may need it occasionally for dimension or fixed sized tables.
   * @param pctNulls Percentage of values for each column that should be null.  It is assumed
   *                 there is an entry for every column in the table, not counting partition
   *                 columns.  Partition columns will not be given null values.  Passing null will
   *                 result in the default setting (1% of values per column)
   * @return Data to be loaded.
   */
  abstract public DataSet generateData(TestTable table, int scale, double[] pctNulls) throws
      IOException;

/**
 * Generate data.  This is equivalent to calling {@link #generateData(TestTable,int,double[])} with
 * scale fetched from {@link org.apache.hive.test.capybara.infra.TestConf#getScale} and pctNulls set
 * to null.
 * @param table table to generate for
 * @return Data to be loaded.
 */
  final public DataSet generateData(TestTable table) throws IOException {
    return generateData(table, TestManager.getTestManager().getTestConf().getScale(), null);
  }

  /**
   * Generate data.  This is equivalent to calling {@link #generateData(TestTable,int,double[])}
   * with pctNulls set to null.
   * @param table table to generate for
   * @param scale Scale of data to generate.  The unit is kilobytes.  As a general rule you
   *              should not set this as it prevents the system from scaling automatically.  You
   *              may need it occasionally for dimension or fixed sized tables.
   * @return Data to be loaded.
   */
  final public DataSet generateData(TestTable table, int scale) throws IOException {
    return generateData(table, scale, null);
  }

  /**
   * Return a copy of the DataGenerator.  The intent of this method is that it can be used by the
   * {@link org.apache.hive.test.capybara.infra.ClusterDataGenerator} to make multiple copies of
   * DataGenerator for use on the cluster.  The default implementation throws an error.
   * @param copyNum a unique number for this copy.
   * @return A copy of the DataGenerator.
   */
  protected DataGenerator copy(int copyNum) {
    throw new UnsupportedOperationException("This DataGenerator does not support copy");
  }
}
