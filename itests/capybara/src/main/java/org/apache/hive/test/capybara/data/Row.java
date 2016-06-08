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

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

/**
 * A row of data.  This is a list of Columns, along with methods for sorting, comparing,
 * hashing, and turning a row to a string.
 *
 * Rows implement Comparable&lt;Row&gt;.  This can be used whenever comparing against another
 * rows known to be of the same schema (e.g., when sorting output from a DataStore).  If you need
 * to compare against a row that <i>may</i> of a different type use
 * {@link #getComparator} instead.  This will produce a comparator that can handle the
 * differences in the rows.
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

  /**
   * Get a comparator for comparing this row with another row.  This method need not be called
   * for every row, but it should be called for each different type of row.  The returned
   * comparator will be specific to the row type being compared against.
   * @param other row to compare this row against
   * @return comparator
   * @throws java.sql.SQLException if it is not possible to compare these rows
   */
  public Comparator<Row> getComparator(Row other) throws SQLException {
    if (size() != other.size()) {
      throw new SQLException("Attempt to compare two rows of different size.  Row 1 has " + size()
          + " columns while row 2 has " + other.size());
    }
    final List<Comparator<Column>> colComparators = new ArrayList<>(size());
    boolean sawDifferentType = false;
    for (int i = 0; i < size(); i++) {
      if (cols.get(i).getClass().equals(other.cols.get(i).getClass())) {
        colComparators.add(equalityColComparator);
      } else {
        colComparators.add(cols.get(i).getComparator(other.cols.get(i)));
        sawDifferentType = true;
      }
    }
    if (sawDifferentType) {
      return new Comparator<Row>() {
        @Override
        public int compare(Row o1, Row o2) {
          for (int i = 0; i < colComparators.size(); i++) {
            int rc = colComparators.get(i).compare(o1.get(i), o2.get(i));
            if (rc != 0) return rc;
          }
          return 0;
        }
      };
    }
    else return equalityRowComparator;

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

  private static Comparator<Row> equalityRowComparator = new Comparator<Row>() {
    @Override
    public int compare(Row o1, Row o2) {
      return o1.compareTo(o2);
    }
  };

  private static Comparator<Column> equalityColComparator = new Comparator<Column>() {
    @Override
    public int compare(Column o1, Column o2) {
      return o1.compareTo(o2);
    }
  };

}
