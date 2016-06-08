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
import org.apache.hadoop.hive.serde2.objectinspector.primitive.HiveDecimalObjectInspector;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Comparator;

class DecimalColumn extends Column {
  DecimalColumn(int colNum) {
    super(colNum);
  }

  @Override
  public void load(PreparedStatement stmt) throws SQLException {
    if (val == null) stmt.setNull(colNum, Types.DECIMAL);
    else stmt.setBigDecimal(colNum, (BigDecimal) val);
  }

  @Override
  public void fromString(String str, String nullIndicator) {
    if (str.equals(nullIndicator)) val = null;
    else val = new BigDecimal(str);
  }

  @Override
  public void fromResultSet(ResultSet rs) throws SQLException {
    BigDecimal hiveVal = rs.getBigDecimal(colNum);
    if (rs.wasNull()) val = null;
    else val = hiveVal;
  }

  @Override
  public void fromObject(ObjectInspector objectInspector, Object o) {
    if (o == null) val = null;
    else val = ((HiveDecimalObjectInspector)objectInspector).getPrimitiveJavaObject(o).bigDecimalValue();
  }

  @Override
  public long length() {
    return val == null ? 0 : ((BigDecimal)val).unscaledValue().bitLength() / 8;
  }

  @Override
  public BigDecimal asBigDecimal() {
    return (BigDecimal)val;
  }

  @Override
  public boolean equals(Object other) {
    if (other == null || !(other instanceof DecimalColumn)) return false;
    DecimalColumn that = (DecimalColumn)other;
    if (val == null && that.val == null) return true;
    else if (val == null || that.val == null) return false;

    return scaleMungingEquals((BigDecimal)val, (BigDecimal)that.val);
  }

  @Override
  public Comparator<Column> getComparator(Column other) throws SQLException {
    if (other instanceof ByteColumn) {
      return buildColComparator(new Comparator<Comparable>() {
        @Override
        public int compare(Comparable o1, Comparable o2) {
          BigDecimal val2 = new BigDecimal((Byte)o2);
          // Try to use our local equals first, since it's special
          if (scaleMungingEquals((BigDecimal) o1, val2)) return 0;
          else return ((BigDecimal)o1).compareTo(val2);
        }
      });
    } else if (other instanceof ShortColumn) {
      return buildColComparator(new Comparator<Comparable>() {
        @Override
        public int compare(Comparable o1, Comparable o2) {
          BigDecimal val2 = new BigDecimal((Short)o2);
          // Try to use our local equals first, since it's special
          if (scaleMungingEquals((BigDecimal) o1, val2)) return 0;
          else return ((BigDecimal)o1).compareTo(val2);
        }
      });
    } else if (other instanceof IntColumn) {
      return buildColComparator(new Comparator<Comparable>() {
        @Override
        public int compare(Comparable o1, Comparable o2) {
          BigDecimal val2 = new BigDecimal((Integer)o2);
          // Try to use our local equals first, since it's special
          if (scaleMungingEquals((BigDecimal) o1, val2)) return 0;
          else return ((BigDecimal)o1).compareTo(val2);
        }
      });
    } else if (other instanceof LongColumn) {
      return buildColComparator(new Comparator<Comparable>() {
        @Override
        public int compare(Comparable o1, Comparable o2) {
          BigDecimal val2 = new BigDecimal((Long)o2);
          // Try to use our local equals first, since it's special
          if (scaleMungingEquals((BigDecimal) o1, val2)) return 0;
          else return ((BigDecimal)o1).compareTo(val2);
        }
      });
    } else if (other instanceof FloatColumn) {
      // See comments on DoubleColumn below on why we're converting the BigDecimal
      return buildColComparator(new Comparator<Comparable>() {
        @Override
        public int compare(Comparable o1, Comparable o2) {
          Double val1 = ((BigDecimal)o1).doubleValue();
          Double val2 = Double.valueOf((Float)o2);
          if (DoubleColumn.checkBits(val1, val2)) return 0;
          else return val1.compareTo(val2);
        }
      });
    } else if (other instanceof DoubleColumn) {
      // We'll move the BD to a double.  This is less accomodating in terms of size (since BD can
      // be much larger) but we want double's equal semantics because the odds of getting exact
      // equality here seem remote.
      return buildColComparator(new Comparator<Comparable>() {
        @Override
        public int compare(Comparable o1, Comparable o2) {
          Double val1 = ((BigDecimal)o1).doubleValue();
          if (DoubleColumn.checkBits(val1, (Double)o2)) return 0;
          else return val1.compareTo((Double)o2);
        }
      });
    } else {
      throw new SQLException("Incompatible types, can't compare a BigDecimal to a " +
          other.getClass().getSimpleName());
    }
  }

  private static boolean scaleMungingEquals(BigDecimal bd1, BigDecimal bd2) {
    // Handle the fact that values may have different scales, since Hive is sloppy about not
    // appending trailing zeros.
    if (bd1.scale() == bd2.scale()) {
      return bd1.equals(bd2);
    } else {
      // TODO - I'm not sure this is the best choice.  This picks the
      // TODO - entry with the lowest scale, subtracts one, and compares both entries at that
      // TODO - scale.  (The subtraction of 1 is to handle rounding differences.)  This handles
      // TODO - the fact that Hive
      // TODO - doesn't append trailing zeros and that for different expressions the two data
      // TODO - stores may assign different scales (e.g. avg(decimal(10,2)) produces a
      // TODO - decimal(10,6)  in Hive and a decimal (10,4) in Derby.)  But it will obscure
      // TODO - cases where we'd like to check that scale is properly kept.
      int newScale = Math.min(bd1.scale(), bd2.scale()) - 1;
      BigDecimal newThisVal = bd1.setScale(newScale, RoundingMode.FLOOR);
      BigDecimal newThatVal = bd2.setScale(newScale, RoundingMode.FLOOR);
      return newThisVal.equals(newThatVal);
    }
  }
}
