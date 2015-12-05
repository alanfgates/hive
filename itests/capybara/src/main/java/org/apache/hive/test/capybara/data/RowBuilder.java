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

import org.apache.hadoop.hive.metastore.api.FieldSchema;

import java.util.ArrayList;
import java.util.List;

/**
 * A class to build new rows.  This understands the layout of the row it will build.  It is
 * useful because we can build the template once and then quickly generate new rows each time
 * we need one.
 */
public class RowBuilder {
  private final List<ColBuilder> builders;
  private final int colOffset; // This is needed for partition columns, because their column number
  // doesn't match their position in the schema that is passed.

  /**
   * A constructor for regular columns.
   * @param schema list of FieldSchemas that describe the row
   */
  public RowBuilder(List<FieldSchema> schema) {
    this(schema, 0);
  }

  /**
   * A constructor for building a RowBuilder for partition columns.
   * @param schema list of FieldSchemas that describe the row
   * @param offset number of non-partition columns in the table, used to figure out the column
   *               number for each partition column.
   */
  public RowBuilder(List<FieldSchema> schema, int offset) {
    builders = new ArrayList<>(schema.size());
    for (FieldSchema fs : schema) builders.add(getColBuilderFromFieldSchema(fs));
    colOffset = offset;
  }

  public Row build() {
    List<Column> cols = new ArrayList<>(builders.size());
    // Add 1 to the column number so that it works with SQL calls.
    for (int i = 0; i < builders.size(); i++) cols.add(builders.get(i).build(i + colOffset + 1));
    return new Row(cols);
  }

  /**
   * Builders for individual column types.
   */
  interface ColBuilder {
    /**
     *
     * @param colNum 1 based reference to column number.
     * @return A column instance of the appropriate type
     */
    abstract Column build(int colNum);
  }

  private static ColBuilder longColBuilder = new ColBuilder() {
    @Override
    public Column build(int colNum) {
      return new LongColumn(colNum);
    }
  };

  private static ColBuilder intColBuilder = new ColBuilder() {
    @Override
    public Column build(int colNum) {
      return new IntColumn(colNum);
    }
  };

  private static ColBuilder shortColBuilder = new ColBuilder() {
    @Override
    public Column build(int colNum) {
      return new ShortColumn(colNum);
    }
  };

  private static ColBuilder byteColBuilder = new ColBuilder() {
    @Override
    public Column build(int colNum) {
      return new ByteColumn(colNum);
    }
  };

  private static ColBuilder floatColBuilder = new ColBuilder() {
    @Override
    public Column build(int colNum) {
      return new FloatColumn(colNum);
    }
  };

  private static ColBuilder doubleColBuilder = new ColBuilder() {
    @Override
    public Column build(int colNum) {
      return new DoubleColumn(colNum);
    }
  };

  private static ColBuilder decimalColBuilder = new ColBuilder() {
    @Override
    public Column build(int colNum) {
      return new DecimalColumn(colNum);
    }
  };

  private static ColBuilder dateColBuilder = new ColBuilder() {
    @Override
    public Column build(int colNum) {
      return new DateColumn(colNum);
    }
  };

  private static ColBuilder timestampColBuilder = new ColBuilder() {
    @Override
    public Column build(int colNum) {
      return new TimestampColumn(colNum);
    }
  };

  private static ColBuilder stringColBuilder = new ColBuilder() {
    @Override
    public Column build(int colNum) {
      return new StringColumn(colNum);
    }
  };

  private static ColBuilder charColBuilder = new ColBuilder() {
    @Override
    public Column build(int colNum) {
      return new CharColumn(colNum);
    }
  };

  private static ColBuilder varcharColBuilder = new ColBuilder() {
    @Override
    public Column build(int colNum) {
      return new VarcharColumn(colNum);
    }
  };

  private static ColBuilder booleanColBuilder = new ColBuilder() {
    @Override
    public Column build(int colNum) {
      return new BooleanColumn(colNum);
    }
  };

  private static ColBuilder bytesColBuilder = new ColBuilder() {
    @Override
    public Column build(int colNum) {
      return new BytesColumn(colNum);
    }
  };

  private static ColBuilder getColBuilderFromFieldSchema(FieldSchema schema) {
    String colType = schema.getType();
    if (colType.equalsIgnoreCase("bigint")) return longColBuilder;
    else if (colType.toLowerCase().startsWith("int")) return intColBuilder;
    else if (colType.equalsIgnoreCase("smallint")) return shortColBuilder;
    else if (colType.equalsIgnoreCase("tinyint")) return byteColBuilder;
    else if (colType.equalsIgnoreCase("float")) return floatColBuilder;
    else if (colType.equalsIgnoreCase("double")) return doubleColBuilder;
    else if (colType.toLowerCase().startsWith("decimal")) return decimalColBuilder;
    else if (colType.equalsIgnoreCase("date")) return dateColBuilder;
    else if (colType.equalsIgnoreCase("timestamp")) return timestampColBuilder;
    else if (colType.equalsIgnoreCase("string")) return stringColBuilder;
    else if (colType.toLowerCase().startsWith("char")) return charColBuilder;
    else if (colType.toLowerCase().startsWith("varchar")) return varcharColBuilder;
    else if (colType.equalsIgnoreCase("boolean")) return booleanColBuilder;
    else if (colType.equalsIgnoreCase("binary")) return bytesColBuilder;
    else throw new RuntimeException("Unknown column type " + colType);
  }
}
