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

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hive.test.capybara.data.DataSet;
import org.apache.hive.test.capybara.iface.DataGenerator;
import org.apache.hive.test.capybara.iface.TestTable;

import java.util.List;

/**
 * A DataGenerator that takes canned data a user gives and creates a table.  This data generator
 * ignores the scale and pctNulls passed to {@link #generateData}.
 */
public class StaticDataGenerator extends DataGenerator {

  private List<String> rows;
  private final String delimiter;

  /**
   *
   * @param rows  The data, as strings.  Integer types (bigint, int, etc.) need to be valid
   *              integers.  Floating point and fixed point types need to be valid numbers
   *              (scientific notation is ok).  Boolean should be true or false.  String length
   *              is not checked for varchar and char data.  Dates should be in the format
   *              'yyyy-mm-dd' and timestamps 'yyyy-mm-dd hh:mm:ss[.f]' where f is a fractional
   *              component of the seconds.  Binary types should be Base64 encoded strings, they
   *              will be decoded by {@link org.apache.commons.codec.binary.Base64}.
   * @param delimiter column delimiter
   */
  public StaticDataGenerator(List<String> rows, String delimiter) {
    this.rows = rows;
    this.delimiter = delimiter;
  }

  @Override
  public DataSet generateData(TestTable table, int scale, double[] pctNulls) {
    // This method can be used with or without a schema.  Without one it just creates a bunch of
    // string columns.  This is used by IntegrationTest.compareAgainst.
    List<FieldSchema> cols = null;
    if (table != null) {
      // Figure out the schema of the table
      cols = table.getCombinedSchema();
    }

    return (cols == null) ? new StringDataSet(rows, delimiter, "NULL") :
        new StringDataSet(cols, rows, delimiter, "NULL");
  }
}
