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
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Comparator;

class StringColumn extends Column {
  StringColumn(int colNum) {
    super(colNum);
  }

  @Override
  public void load(PreparedStatement stmt) throws SQLException {
    if (val == null) stmt.setNull(colNum, Types.VARCHAR);
    else stmt.setString(colNum, (String) val);
  }

  @Override
  public void fromString(String str, String nullIndicator) {
    if (str.equals(nullIndicator)) val = null;
    else val = str;
  }

  @Override
  public void fromResultSet(ResultSet rs) throws SQLException {
    String hiveVal = rs.getString(colNum);
    if (rs.wasNull()) val = null;
    else val = hiveVal;
  }

  @Override
  public void fromObject(ObjectInspector objectInspector, Object o) {
    if (o == null) val = null;
    else val = ((StringObjectInspector)objectInspector).getPrimitiveJavaObject(o);
  }

  @Override
  public long length() {
    if (val == null) return 0;
    else return ((String)val).length();
  }

  @Override
  public String toString(String nullIndicator, String quotes) {
    if (val == null) return nullIndicator;
    return new StringBuilder(quotes)
        .append(val.toString())
        .append(quotes)
        .toString();
  }

  @Override
  public String asString() {
    return (String)val;
  }

  @Override
  public Comparator<Column> getComparator(Column other) throws SQLException {
    // TODO everything can be stringified, so I should make this work with all the types except
    // maybe bytes.
    throw new SQLException("Incompatible types, can't compare a string to a " +
        other.getClass().getSimpleName());
  }
}
