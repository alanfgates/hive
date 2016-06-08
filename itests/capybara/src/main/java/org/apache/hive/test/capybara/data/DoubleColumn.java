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
package org.apache.hive.test.capybara.data;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;

import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Comparator;

class DoubleColumn extends Column {
  DoubleColumn(int colNum) {
    super(colNum);
  }

  @Override
  public void load(PreparedStatement stmt) throws SQLException {
    if (val == null) stmt.setNull(colNum, Types.DOUBLE);
    else stmt.setDouble(colNum, (Double) val);
  }

  @Override
  public void fromString(String str, String nullIndicator) {
    if (str.equals(nullIndicator)) val = null;
    else val = Double.valueOf(str);
  }

  @Override
  public void fromResultSet(ResultSet rs) throws SQLException {
    double hiveVal = rs.getDouble(colNum);
    if (rs.wasNull()) val = null;
    else val = hiveVal;
  }

  @Override
  public void fromObject(ObjectInspector objectInspector, Object o) {
    if (o == null) val = null;
    else val = ((DoubleObjectInspector)objectInspector).get(o);
  }

  @Override
  public long length() {
    return Double.SIZE / 8;
  }

  @Override
  public double asDouble() {
    return (Double)val;
  }

  @Override
  public boolean equals(Object other) {
    if (other == null || !(other instanceof DoubleColumn)) return false;
    DoubleColumn that = (DoubleColumn)other;
    if (val == null && that.val == null) return true;
    else if (val == null || that.val == null) return false;

    return checkBits((Double)val, (Double)that.val);
  }

  @Override
  public int compareTo(Column other) {
    // Override this since we're playing a little fast and loose with equals
    if (equals(other)) return 0;
    else return super.compareTo(other);
  }

  @Override
  public Comparator<Column> getComparator(Column other) throws SQLException {
    if (other instanceof ByteColumn) {
      return buildColComparator(new Comparator<Comparable>() {
        @Override
        public int compare(Comparable o1, Comparable o2) {
          Double val2 = Double.valueOf((Byte) o2);
          if (checkBits((Double) o1, val2)) return 0;
          else return ((Double) o1).compareTo(val2);
        }
      });
    } else if (other instanceof ShortColumn) {
      return buildColComparator(new Comparator<Comparable>() {
        @Override
        public int compare(Comparable o1, Comparable o2) {
          Double val2 = Double.valueOf((Short) o2);
          if (checkBits((Double)o1, val2)) return 0;
          else return ((Double)o1).compareTo(val2);
        }
      });
    } else if (other instanceof IntColumn) {
      return buildColComparator(new Comparator<Comparable>() {
        @Override
        public int compare(Comparable o1, Comparable o2) {
          Double val2 = Double.valueOf((Integer)o2);
          if (checkBits((Double)o1, val2)) return 0;
          else return ((Double)o1).compareTo(val2);
        }
      });
    } else if (other instanceof LongColumn) {
      return buildColComparator(new Comparator<Comparable>() {
        @Override
        public int compare(Comparable o1, Comparable o2) {
          Double val2 = Double.valueOf((Long) o2);
          if (checkBits((Double) o1, val2)) return 0;
          else return ((Double) o1).compareTo(val2);
        }
      });
    }
    if (other instanceof FloatColumn) {
      return buildColComparator(new Comparator<Comparable>() {
        @Override
        public int compare(Comparable o1, Comparable o2) {
          Double val2 = Double.valueOf((Float) o2);
          // We can't just use compareTo as we need to do our fuzzy bit masking first, because we
          // want "close enough" to be equal
          if (checkBits((Double) o1, val2)) return 0;
          else return ((Double) o1).compareTo(val2);
        }
      });
    } else if (other instanceof DecimalColumn) {
      // We'll move the BD to a double.  This is less accomodating in terms of size (since BD can
      // be much larger) but we want double's equal semantics because the odds of getting exact
      // equality here seem remote.
      return buildColComparator(new Comparator<Comparable>() {
        @Override
        public int compare(Comparable o1, Comparable o2) {
          Double val2 = ((BigDecimal)o2).doubleValue();
          if (checkBits((Double)o1, val2)) return 0;
          else return ((Double)o1).compareTo(val2);
        }
      });
    } else {
      throw new SQLException("Incompatible types, can't compare a double to a " +
          other.getClass().getSimpleName());
    }
  }

  static boolean checkBits(Double d1, Double d2) {

    // We want to be fuzzy in our comparisons, but just using a hard wired differential is hard
    // because we don't know the scale.  So look at the bits and mask out the last few, as
    // these are where the difference is likely to be.
    long bits1 = Double.doubleToLongBits(d1);
    long bits2 = Double.doubleToLongBits(d2);

    // Make sure the sign is the same
    if ((bits1 & 0x8000000000000000L) != (bits2 & 0x8000000000000000L)) return false;
    // Check the exponent
    if ((bits1 & 0x7ff0000000000000L) != (bits2 & 0x7ff0000000000000L)) return false;
    // Check the mantissa, but leave off the last eight bits
    return (bits1 & 0x000fffff00000000L) == (bits2 & 0x000fffff00000000L);
  }
}
