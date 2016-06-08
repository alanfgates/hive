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

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BinaryObjectInspector;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Arrays;
import java.util.Comparator;

/**
 * This one's a bit of a special case because byte[] isn't comparable.  Also, we Base64
 * encode/decode when moving back and forth with String.  If we don't, Hive has problems when
 * we put the data in a text file because delimiters end up in the data.
 */
class BytesColumn extends Column {
  byte[] bVal;

  BytesColumn(int colNum) {
    super(colNum);
  }

  @Override
  public void load(PreparedStatement stmt) throws SQLException {
    if (val == null) stmt.setNull(colNum, Types.BLOB);
    else stmt.setBytes(colNum, bVal);
  }

  @Override
  public void fromString(String str, String nullIndicator) {
    if (str.equals(nullIndicator)) bVal = null;
    else bVal = Base64.decodeBase64(str);
  }

  @Override
  public void fromResultSet(ResultSet rs) throws SQLException {
    byte[] hiveVal = rs.getBytes(colNum);
    if (rs.wasNull()) bVal = null;
    else bVal = hiveVal;
  }

  @Override
  public void fromObject(ObjectInspector objectInspector, Object o) {
    if (o == null) bVal = null;
    else bVal = ((BinaryObjectInspector)objectInspector).getPrimitiveJavaObject(o);
  }

  @Override
  public long length() {
    if (bVal == null) return 0;
    else return bVal.length;
  }

  @Override
  public byte[] asBytes() {
    return bVal;
  }

  @Override
  public void set(Object val) {
    if (val == null) bVal = null;
    else if (val instanceof byte[]) bVal = (byte[])val;
    else throw new RuntimeException("Attempt to set ByteColumn to non-byte[] value " +
          val.getClass().getName());
  }

  @Override
  public boolean isNull() {
    return bVal == null;
  }

  @Override
  public int compareTo(Column o) {
    if (o == null || !(o instanceof BytesColumn)) return 1;
    BytesColumn that = (BytesColumn)o;
    if (bVal == null && that.bVal == null) return 0;
    if (bVal == null) return -1;
    if (that.bVal == null) return 1;
    for (int i = 0; i < bVal.length; i++) {
      if (i >= that.bVal.length) return 1;
      if (bVal[i] < that.bVal[i]) return -1;
      if (bVal[i] > that.bVal[i]) return 1;
    }
    // We know the bytes are the same for the bytes they both have, but that might be longer
    if (that.bVal.length > bVal.length) return -1;
    else return 0;
  }

  @Override
  public boolean equals(Object o) {
    if (o == null || !(o instanceof BytesColumn)) return false;
    return Arrays.equals(bVal, ((BytesColumn) o).bVal);
  }

  @Override
  public int hashCode() {
    if (bVal == null) return 0;
    else return Arrays.hashCode(bVal);
  }

  @Override
  public String toString(String nullIndicator, String quotes) {
    if (bVal == null) return nullIndicator;
    else return Base64.encodeBase64URLSafeString(bVal);
  }

  @Override
  public Comparator<Column> getComparator(Column other) throws SQLException {
    throw new SQLException("Incompatible types, can't compare a bytes to a " +
        other.getClass().getSimpleName());
  }
}
