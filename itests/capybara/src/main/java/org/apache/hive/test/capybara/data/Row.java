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
package org.apache.hive.test.capybara.data;

import java.util.Iterator;
import java.util.List;

/**
 * A row of data.  This is a list of Columns, along with methods for sorting, comparing,
 * hashing, and turning a row to a string.
 */
public class Row implements Iterable<Column>, Comparable<Row> {
  private List<Column> cols;

  Row(List<Column> c) {
    cols = c;
  }

  @Override
  public Iterator<Column> iterator() {
    return cols.iterator();
  }

  @Override
  public int compareTo(Row o) {
    for (int i = 0; i < cols.size(); i++) {
      if (i > o.cols.size()) return 1;
      Column thatCol = o.cols.get(i);
      int rc = cols.get(i).compareTo(thatCol);
      if (rc != 0) return rc;
    }
    // They may have been equal all along, but if there are more columns in the other row,
    // declare it greater.
    if (cols.size() < o.cols.size()) return -1;
    else return 0;
  }

  @Override
  public String toString() {
    return toString(",", "NULL", "");
  }

  /**
   * Convert a row to a String, controlling how various aspects of the row are printed out.
   * @param delimiter column delimiter
   * @param nullIndicator value to return for null
   * @param quotes quotes to use around string columns
   * @return row as a String.
   */
  public String toString(String delimiter, String nullIndicator, String quotes) {
    StringBuilder builder = new StringBuilder();
    for (int i = 0; i < cols.size(); i++) {
      if (i != 0) builder.append(delimiter);
      builder.append(cols.get(i).toString(nullIndicator, quotes));
    }
    return builder.toString();
  }

  /**
   * Number of columns in this row.
   * @return columns in row
   */
  public int size() {
    return cols.size();
  }

  /**
   * Get the column.  This is zero based even though column numbers are 1 based.
   * @param i zero based column reference
   * @return column
   */
  public Column get(int i) {
    return cols.get(i);
  }

  /**
   * Estimated length of this row.  Note that this is a rough estimate on the number of bytes
   * it takes to represent this in a binary format on disk.  Each row takes
   * significantly more space in memory due to container objects, etc.
   * @return estimated length of the row.
   */
  public int lengthInBytes() {
    int len = 0;
    for (Column col : cols) len += col.length();
    return len;
  }

  @Override
  public boolean equals(Object other) {
    if (other == null || !(other instanceof Row)) return false;
    return cols.equals(((Row)other).cols);
  }

  @Override
  public int hashCode() {
    return cols.hashCode();
  }

  /**
   * Append columns of another row to this row.
   * @param that row to append
   */
  public void append(Row that) {
    cols.addAll(that.cols);
  }

  /**
   * Describe the schema of this row.
   * @return description
   */
  public String describe() {
    StringBuilder builder = new StringBuilder("Schema<");
    boolean isFirst = true;
    for (Column c : cols) {
      if (isFirst) isFirst = false;
      else builder.append(",");
      builder.append(c.getClass().getSimpleName());
    }
    builder.append('>');
    return builder.toString();
  }
}
