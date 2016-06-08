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

import org.apache.hive.test.capybara.data.DataSet;
import org.apache.hive.test.capybara.data.Row;
import org.apache.hive.test.capybara.iface.ResultComparator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.junit.Assert;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.PriorityQueue;

/**
 * Compare two data sets without sorting Hive.  We still sort the benchmark because every
 * database sorts things just a little differently (like NULLs first or last, collation, etc.).
 */
public class NonSortingComparator extends ResultComparator {
  static final private Logger LOG = LoggerFactory.getLogger(NonSortingComparator.class.getName());

  @Override
  public void compare(DataSet hive, DataSet bench) throws SQLException, IOException {
    compareSchemas(hive, bench);

    compare(hive.iterator(), sort(bench));
  }

  protected void compare(Iterator<Row> hiveIter, Iterator<Row> benchIter) {
    // Keep a bounded queue of records so that if there's a failure we can print some context
    // around the failed lines rather than just the individual lines.
    BoundedQueue<Row> hiveQueue = new BoundedQueue<>(25);
    BoundedQueue<Row> benchQueue = new BoundedQueue<>(25);

    int rowNum = 0;
    while (hiveIter.hasNext()) {
      Assert.assertTrue("Benchmark ran out of rows at " + rowNum + " but hive still has rows",
          benchIter.hasNext());
      rowNum++;
      Row hiveRow = hiveIter.next();
      Row benchRow = benchIter.next();
      hiveQueue.add(hiveRow);
      benchQueue.add(benchRow);

      if (!hiveRow.equals(benchRow)) {

        if (LOG.isDebugEnabled()) {
          LOG.debug("Schema of failing hive row is " + hiveRow.describe());
          LOG.debug("Schema of failing bench row is " + benchRow.describe());
          LOG.debug("Last 25 Hive rows:");
          while (!hiveQueue.isEmpty()) LOG.debug(hiveQueue.poll().toString(",", "NULL", "'"));
          LOG.debug("Last 25 Bench rows:");
          while (!benchQueue.isEmpty()) LOG.debug(benchQueue.poll().toString(",", "NULL", "'"));
        }


        Assert.fail("Mismatch at row " + rowNum + " hive row is <" +
            hiveRow.toString(",", "NULL", "") + "> bench row is <" +
            benchRow.toString(",", "NULL", "") + ">");
      }
    }
    Assert.assertFalse("Hive ran out of rows at " + rowNum + " but benchmark still has rows",
        benchIter.hasNext());
  }

  protected Iterator<Row> sort(DataSet data) {
    final PriorityQueue<Row> sorted = new PriorityQueue<>();
    Iterator<Row> iter = data.iterator();
    while (iter.hasNext()) {
      sorted.add(iter.next());
    }
    return new Iterator<Row>() {
      @Override
      public boolean hasNext() {
        return sorted.size() > 0;
      }

      @Override
      public Row next() {
        return sorted.poll();
      }

      @Override
      public void remove() {

      }
    };
  }
}
