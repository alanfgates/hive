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
package org.apache.hive.test.capybara.infra;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hive.test.capybara.data.Column;
import org.apache.hive.test.capybara.data.DataSet;
import org.apache.hive.test.capybara.data.Row;
import org.apache.hive.test.capybara.data.RowBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * A DataSet wrapped around a {@link java.sql.ResultSet}.  Unforunately we cannot be lazy in
 * turning the provided ResultSet into a List&lt;Object[]&gt; because the caller may close the
 * statement after handing us this, which will close the ResultSet.
 */
public class ResultSetDataSet extends DataSet {
  static final private Logger LOG = LoggerFactory.getLogger(ResultSetDataSet.class.getName());

  public ResultSetDataSet(ResultSet rs) throws SQLException {
    this(rs, Long.MAX_VALUE);
  }

  public ResultSetDataSet(ResultSet rs, long limit) throws SQLException {
    super(resultSetMetaDataToSchema(rs));
    rows = new ArrayList<>();

    RowBuilder builder = new RowBuilder(schema);
    for (long rownum = 0; rownum < limit && rs.next(); rownum++) {
      Row row = builder.build();
      for (Column col : row) {
        col.fromResultSet(rs);
      }
      rows.add(row);
    }
  }

  private static List<FieldSchema> resultSetMetaDataToSchema(ResultSet rs) throws SQLException {
    ResultSetMetaData meta = rs.getMetaData();
    List<FieldSchema> cols = new ArrayList<>(meta.getColumnCount());
    StringBuilder logMsg = new StringBuilder();
    for (int i = 0; i < meta.getColumnCount(); i++) {
      String type;
      switch (meta.getColumnType(i+1)) {
      case Types.BIGINT:
        type = "bigint";
        break;
      case Types.INTEGER:
        type = "int";
        break;
      case Types.SMALLINT:
        type = "smallint";
        break;
      case Types.TINYINT:
        type = "tinyint";
        break;
      case Types.REAL:
      case Types.FLOAT:
        type = "float";
        break;
      case Types.DOUBLE:
        type = "double";
        break;
      case Types.NUMERIC:
      case Types.DECIMAL:
        type = "decimal(" + meta.getPrecision(i+1) + "," + meta.getScale(i+1) + ")";
        break;
      case Types.DATE:
        type = "date";
        break;
      case Types.TIMESTAMP:
        type = "timestamp";
        break;
      case Types.VARCHAR:
        type = "varchar(" + meta.getPrecision(i+1) + ")";
        break;
      case Types.CHAR:
        type = "char(" + meta.getPrecision(i+1) + ")";
        break;
      case Types.BOOLEAN:
      case Types.BIT:  // postgress maps boolean to this
        type = "boolean";
        break;
      case Types.BLOB:
      case Types.BINARY:
        type = "binary";
        break;

      default:
        throw new RuntimeException("Unknown data type: " + meta.getColumnType(i+1) + " name: " +
            meta.getColumnTypeName(i + 1));
      }
      FieldSchema fs = new FieldSchema(meta.getColumnName(i+1), type, "");
      if (LOG.isDebugEnabled()) {
        logMsg.append(fs.getName())
            .append(':')
            .append(fs.getType())
            .append(',');
      }

      cols.add(fs);
    }
    LOG.debug("Obtained schema from ResultSetMetaData: " + logMsg.toString());
    return cols;

  }

  @Override
  public Iterator<Row> iterator() {
    return rows.iterator();
  }
}
