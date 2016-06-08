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
import org.apache.hadoop.hive.serde2.objectinspector.primitive.FloatObjectInspector;

import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Comparator;

class FloatColumn extends Column {
  FloatColumn(int colNum) {
    super(colNum);
  }

  @Override
  public void load(PreparedStatement stmt) throws SQLException {
    if (val == null) stmt.setNull(colNum, Types.FLOAT);
    else stmt.setFloat(colNum, (Float) val);
  }

  @Override
  public void fromString(String str, String nullIndicator) {
    if (str.equals(nullIndicator)) val = null;
    else val = Float.valueOf(str);
  }

  @Override
  public void fromResultSet(ResultSet rs) throws SQLException {
    float hiveVal = rs.getFloat(colNum);
    if (rs.wasNull()) val = null;
    else val = hiveVal;
  }

  @Override
  public void fromObject(ObjectInspector objectInspector, Object o) {
    if (o == null) val = null;
    else val = ((FloatObjectInspector)objectInspector).get(o);
  }

  @Override
  public long length() {
    return Float.SIZE / 8;
  }

  @Override
  public float asFloat() {
    return (Float)val;
  }

  @Override
  public boolean equals(Object other) {
    if (other == null || !(other instanceof FloatColumn)) return false;
    FloatColumn that = (FloatColumn)other;
    if (val == null && that.val == null) return true;
    else if (val == null || that.val == null) return false;

    return checkBits((Float)val, (Float)that.val);
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
          Float val2 = Float.valueOf((Byte) o2);
          if (checkBits((Float) o1, val2)) return 0;
          else return ((Float) o1).compareTo(val2);
        }
      });
    } else if (other instanceof ShortColumn) {
      return buildColComparator(new Comparator<Comparable>() {
        @Override
        public int compare(Comparable o1, Comparable o2) {
          Float val2 = Float.valueOf((Short)o2);
          if (checkBits((Float)o1, val2)) return 0;
          else return ((Float)o1).compareTo(val2);
        }
      });
    } else if (other instanceof IntColumn) {
        return buildColComparator(new Comparator<Comparable>() {
          @Override
          public int compare(Comparable o1, Comparable o2) {
            Float val2 = Float.valueOf((Integer)o2);
            if (checkBits((Float)o1, val2)) return 0;
            else return ((Float)o1).compareTo(val2);
          }
        });
    } else if (other instanceof LongColumn) {
      return buildColComparator(new Comparator<Comparable>() {
        @Override
        public int compare(Comparable o1, Comparable o2) {
          Float val2 = Float.valueOf((Long)o2);
          if (checkBits((Float)o1, val2)) return 0;
          else return ((Float)o1).compareTo(val2);
        }
      });
    } else if (other instanceof DoubleColumn) {
      return buildColComparator(new Comparator<Comparable>() {
        @Override
        public int compare(Comparable o1, Comparable o2) {
          Double val1 = Double.valueOf((Float) o1);
          // We can't just use compareTo as we need to do our fuzzy bit masking first, because we
          // want "close enough" to be equal
          if (DoubleColumn.checkBits(val1, (Double) o2)) return 0;
          else return val1.compareTo((Double)o2);
        }
      });
    } else if (other instanceof DecimalColumn) {
      // We'll move both to doubles.  This is less accomodating in terms of size (since BD can
      // be much larger) but we want double's equal semantics because the odds of getting exact
      // equality here seem remote.
      return buildColComparator(new Comparator<Comparable>() {
        @Override
        public int compare(Comparable o1, Comparable o2) {
          Double val2 = ((BigDecimal)o2).doubleValue();
          Double val1 = Double.valueOf((Float)o1);
          if (DoubleColumn.checkBits(val1, val2)) return 0;
          else return val1.compareTo(val2);
        }
      });
    } else {
      throw new SQLException("Incompatible types, can't compare a float to a " +
          other.getClass().getSimpleName());
    }
  }

  static boolean checkBits(Float f1, Float f2) {

    // We want to be fuzzy in our comparisons, but just using a hard wired differential is hard
    // because we don't know the scale.  So look at the bits and mask out the last few, as
    // these are where the difference is likely to be.
    int thisBits = Float.floatToIntBits(f1);
    int thatBits = Float.floatToIntBits(f2);

    // Make sure the sign is the same
    if ((thisBits & 0x80000000) != (thatBits & 0x80000000)) return false;
    // Check the exponent
    if ((thisBits & 0x7f800000) != (thatBits & 0x7f800000)) return false;
    // Check the mantissa, but leave off the last two bits
    return (thisBits & 0x007fff00) == (thatBits & 0x007fff00);
  }
}
