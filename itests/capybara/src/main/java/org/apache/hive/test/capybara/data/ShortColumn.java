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
import org.apache.hadoop.hive.serde2.objectinspector.primitive.ShortObjectInspector;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Comparator;

class ShortColumn extends Column {
  ShortColumn(int colNum) {
    super(colNum);
  }

  @Override
  public void load(PreparedStatement stmt) throws SQLException {
    if (val == null) stmt.setNull(colNum, Types.SMALLINT);
    else stmt.setShort(colNum, (Short) val);
  }

  @Override
  public void fromString(String str, String nullIndicator) {
    if (str.equals(nullIndicator)) val = null;
    else val = Short.valueOf(str);
  }

  @Override
  public void fromResultSet(ResultSet rs) throws SQLException {
    short hiveVal = rs.getShort(colNum);
    if (rs.wasNull()) val = null;
    else val = hiveVal;
  }

  @Override
  public void fromObject(ObjectInspector objectInspector, Object o) {
    if (o == null) val = null;
    else val = ((ShortObjectInspector)objectInspector).get(o);
  }

  @Override
  public long length() {
    return Short.SIZE / 8;
  }

  @Override
  public short asShort() {
    return (Short)val;
  }

  @Override
  public Comparator<Column> getComparator(Column other) throws SQLException {
    if (other instanceof LongColumn) {
      return buildColComparator(new Comparator<Comparable>() {
        @Override
        public int compare(Comparable o1, Comparable o2) {
          Long val1 = Long.valueOf((Short)o1);
          return val1.compareTo((Long)o2);
        }
      });
    } else if (other instanceof IntColumn) {
      return buildColComparator(new Comparator<Comparable>() {
        @Override
        public int compare(Comparable o1, Comparable o2) {
          Integer val1 = Integer.valueOf((Short)o1);
          return val1.compareTo((Integer)o2);
        }
      });
    } else if (other instanceof ByteColumn) {
      return buildColComparator(new Comparator<Comparable>() {
        @Override
        public int compare(Comparable o1, Comparable o2) {
          Short val2 = Short.valueOf((Byte) o2);
          return o1.compareTo(val2);
        }
      });
    } else {
      throw new SQLException("Incompatible types, can't compare a short to a " +
          other.getClass().getSimpleName());
    }
  }
}
