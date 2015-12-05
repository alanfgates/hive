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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.junit.Assert;

import java.io.IOException;
import java.sql.SQLException;

/**
 * Compare results from Hive and a benchmark.  Any failures will result in a
 * {@link java.lang.AssertionError}.  If you expect a failure you should catch this error.
 */
public abstract class ResultComparator {

  public final static char DELIMITER = ',';
  static final private Logger LOG = LoggerFactory.getLogger(ResultComparator.class.getName());

  /**
   * Compare two DataSets.  The DataSets must have the same schemas (number of columns and type
   * of columns, column names are ignored) and each DataSet must have the same data.  Whether
   * "same" includes sorted in the same order is implementation dependent.
   * @param hiveResult DataSet from Hive.
   * @param benchmarkResult DataSet from the benchmark.
   * @throws SQLException
   * @throws IOException
   */
  public abstract void compare(DataSet hiveResult, DataSet benchmarkResult) throws SQLException,
      IOException;

 /**
   * Make sure the schemas of two results match.  Match here is defined as having the same or
   * compatible data types.  Names of columns are not checked.
   * @param hive DataSet from hive
   * @param bench DataSet from the benchmark
   * @throws java.sql.SQLException
   */
  protected void compareSchemas(DataSet hive, DataSet bench)
      throws SQLException {
    Assert.assertEquals("Different number of columns", bench.getSchema().size(),
        hive.getSchema().size());
    int numCols = hive.getSchema().size();
    for (int i = 0; i < numCols; i++) {
      String hiveName = hive.getSchema().get(i).getType();
      String benchName = bench.getSchema().get(i).getType();
      Assert.assertTrue("Found discrepency in metadata at column " + i,
          typeMatches(hiveName, benchName));
    }
  }

  /**
   * Determine whether types match.  For types with lengths, precision, or scale, these values
   * are ignored (ie varchar(32) == varchar(10) = true).  All integer types (bigint, int,
   * smallint, tinyint) are considered equal.  All floating types (double, float) are considered
   * equals.  All string types (char, varchar, string) are considered equal.
   * @param hiveType type name from hive
   * @param benchType type name from benchmark
   * @return true if equal, false otherwise
   */
  static boolean typeMatches(String hiveType, String benchType) {
    // Strip any length, scale, or precision off of the types.
    int openParend = hiveType.indexOf('(');
    if (openParend > -1) hiveType = hiveType.substring(0, openParend).toLowerCase();
    openParend = benchType.indexOf('(');
    if (openParend > -1) benchType = benchType.substring(0, openParend).toLowerCase();

    return hiveType.equals(benchType) ||
      isInteger(hiveType) && isInteger(benchType) ||
      isFloatingPoint(hiveType) && isFloatingPoint(benchType) ||
      isString(hiveType) && isString(benchType);
  }

  private static boolean isInteger(String type) {
    return type.equals("bigint") || type.equals("int") || type.equals("smallint") ||
        type.equals("tinyint");
  }

  private static boolean isFloatingPoint(String type) {
    return type.equals("float") || type.equals("double");
  }

  private static boolean isString(String type) {
    return type.equals("string") || type.equals("char") || type.equals("varchar");
  }
}
