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

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Comparator;

/**
 * A column value.  There are implementations for each Java object type containing column data.
 * This class provides methods for comparing, loading via JDBC, converting from JDBC result
 * sets, parsing from a string, and turning into a string.  For datatypes with differing
 * requirements for equals (float, double) and for bytes (which needs different definitions of
 * equals, sorting, hashing, and string parsing) this gives us a place to do it.
 *
 * Note that Columns themselves should
 * never be null.  Null in a column is represented by the contained val being null.
 *
 * Column implements Comparable&lt;Column&gt;.  This can be used for comparing two columns knows
 * to be of the same type.  If you're not sure if they are the same type you can call
 * {@link #getComparator} which will return a comparator specific to the column type you want to
 * compare to.
 */
public abstract class Column implements Comparable<Column> {
  /**
   * This column number is 1 based, since it is used for SQL calls.
   */
  protected final int colNum;
  protected Comparable val;

  protected Column(int cn) {
    val = null;
    colNum = cn;
  }

  /**
   * Load into a {@link java.sql.PreparedStatement}.
   * @param stmt PreparedStatement to load column into
   * @throws java.sql.SQLException
   */
  public abstract void load(PreparedStatement stmt) throws SQLException;

  /**
   * Parse this value from a string
   * @param str string representation
   * @param nullIndicator string value that indicates null
   */
  public abstract void fromString(String str, String nullIndicator);

  /**
   * Parse this value from a {@link java.sql.ResultSet}.
   * @param rs ResultSet to parse the column from.
   * @throws java.sql.SQLException
   */
  public abstract void fromResultSet(ResultSet rs) throws SQLException;

  public abstract void fromObject(ObjectInspector objectInspector, Object o);

  /**
   * Return the length of this column.  For String based and binary columns this is the length of
   * the actual data, not the max length possible (ie a varchar(32) columns with the value 'fred'
   * will return 4, not 32). For fixed length columns it will return an estimate of the size of
   * the column (ie 8 for long, 4 for int).
   * @return length
   */
  public abstract long length();

  public void set(Object val) {
    this.val = (Comparable)val;
  }

  public Comparable get() {
    return val;
  }

  public boolean isNull() {
    return val == null;
  }

  @Override
  public int compareTo(Column o) {
    if (o == null) return 1;
    if (val == null && o.val == null) return 0;
    if (val == null) return -1;
    if (o.val == null) return 1;
    return val.compareTo(o.val);
  }

  @Override
  public boolean equals(Object o) {
    if (o == null || !(o instanceof Column)) return false;
    Column that = (Column)o;
    if (val == null && that.val == null) return true;
    else if (val == null || that.val == null) return false;
    else return val.equals(that.val);
  }

  @Override
  public int hashCode() {
    if (val == null) return 0;
    else return val.hashCode();
  }

  public String toString(String nullIndicator, String quotes) {
    if (val == null) return nullIndicator;
    else return val.toString();
  }

  @Override
  public String toString() {
    return toString("NULL", "");
  }

  /**
   * Get a comparator for comparing to columns of other types.
   * @param other column to be compared to
   * @return comparator
   * @throws java.sql.SQLException if these two types are not comparable
   */
  public abstract Comparator<Column> getComparator(Column other) throws SQLException;

  protected Comparator<Column> buildColComparator(final Comparator<Comparable> comparator) {
    return new Comparator<Column>() {
      @Override
      public int compare(Column o1, Column o2) {
        if (o1 == null) return 1;
        if (o1.val == null && o2.val == null) return 0;
        if (o1.val == null) return -1;
        if (o2.val == null) return 1;
        return comparator.compare(o1.val, o2.val);
      }
    };
  }

  // For the most part I don't expect to need these, as hopefully we don't usually need to
  // break things out by type.  But they're useful for testing and a few other places.
  public long asLong() { throw new UnsupportedOperationException("This is not a bigint column"); }
  public int asInt() { throw new UnsupportedOperationException("This is not a int column"); }
  public short asShort() { throw new UnsupportedOperationException("This is not a smallint column"); }
  public byte asByte() { throw new UnsupportedOperationException("This is not a tinyint column"); }
  public float asFloat() { throw new UnsupportedOperationException("This is not a float column"); }
  public double asDouble() { throw new UnsupportedOperationException("This is not a double column"); }
  public BigDecimal asBigDecimal() { throw new UnsupportedOperationException("This is not a decimal column"); }
  public Date asDate() { throw new UnsupportedOperationException("This is not a date column"); }
  public Timestamp asTimestamp() { throw new UnsupportedOperationException("This is not a timestamp column"); }
  public String asString() { throw new UnsupportedOperationException("This is not a string column"); }
  public boolean asBoolean() { throw new UnsupportedOperationException("This is not a boolean column"); }
  public byte[] asBytes() { throw new UnsupportedOperationException("This is not a bytes column"); }
}
