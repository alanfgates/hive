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
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DateObjectInspector;

import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Comparator;

class DateColumn extends Column {
  DateColumn(int colNum) {
    super(colNum);
  }

  @Override
  public void load(PreparedStatement stmt) throws SQLException {
    if (val == null) stmt.setNull(colNum, Types.DATE);
    else stmt.setDate(colNum, (Date) val);
  }

  @Override
  public void fromString(String str, String nullIndicator) {
    if (str.equals(nullIndicator)) val = null;
    else val = Date.valueOf(str);
  }

  @Override
  public void fromResultSet(ResultSet rs) throws SQLException {
    Date hiveVal = rs.getDate(colNum);
    if (rs.wasNull()) val = null;
    else val = hiveVal;
  }

  @Override
  public void fromObject(ObjectInspector objectInspector, Object o) {
    if (o == null) val = null;
    else val = ((DateObjectInspector)objectInspector).getPrimitiveJavaObject(o);
  }

  @Override
  public long length() {
    return Long.SIZE / 8; // Since we can express this as a long, that must be enough bytes to
    // store it.
  }

  @Override
  public Date asDate() {
    return (Date)val;
  }

  @Override
  public Comparator<Column> getComparator(Column other) throws SQLException {
    if (other instanceof TimestampColumn) {
      return buildColComparator(new Comparator<Comparable>() {
        @Override
        public int compare(Comparable o1, Comparable o2) {
          Timestamp val1 = new Timestamp(((Date)o1).getTime());
          return val1.compareTo((Timestamp)o2);
        }
      });
    } else {
      throw new SQLException("Incompatible types, can't compare a date to a " +
          other.getClass().getSimpleName());
    }
  }
}
