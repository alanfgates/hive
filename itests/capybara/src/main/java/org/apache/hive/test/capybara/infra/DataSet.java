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

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BinaryObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BooleanObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.ByteObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DateObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.FloatObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.HiveCharObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.HiveDecimalObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.HiveVarcharObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.ShortObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.TimestampObjectInspector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * <p>A data set.  This class encapsulates a set of data.  The data could be the result of a query
 * against a {@link org.apache.hive.test.capybara.infra.DataStore} or generating data from a
 * {@link org.apache.hive.test.capybara.DataGenerator}.</p>
 *
 * <p>Interally this class contains all the logic for handling rows, columns, and different
 * datatypes.  The intent is to factor all of the datatype specific code into this class so that
 * other classes in capybara can be type agnostic.
 * </p>
 */
public abstract class DataSet implements Iterable<DataSet.Row> {
  static final private Logger LOG = LoggerFactory.getLogger(DataSet.class.getName());

  static private int nextId = 1;

  protected List<FieldSchema> schema;
  protected List<Row> rows;
  // This value will only be set if the DataSet has been stored on the cluster.
  private Path clusterLocation;
  private transient int id = 0; // Unique identifier for this DataSet

  /**
   *
   * @param schema column schema for this dataset.
   */
  protected DataSet(List<FieldSchema> schema) {
    this.schema = schema;
  }

  /**
   * Get the schema for this data set.
   *
   * @return schema
   */
  public List<FieldSchema> getSchema() {
    return schema;
  }

  /**
   * Set the schema for a DataSet.  This is necessary because we get some DataSets as a
   * List&lt;String&gt; and thus we do not know the schema.
   * @param schema the schema to set
   */
  public void setSchema(List<FieldSchema> schema) {
    this.schema = schema;
  }

  /**
   * Return the DataSet as a set of strings, one for each row.  This is useful for printing to
   * logs, etc.
   * @param delimiter String to use to delimit columns
   * @param nullIndicator String to write in for nulls.
   * @param quotes String to use to quote String columns.
   * @return iterator of Strings
   */
  public Iterator<String> stringIterator(final String delimiter, final String nullIndicator,
                                         final String quotes) {
    final Iterator<Row> outerIter = iterator();
    return new Iterator<String>() {
      @Override
      public boolean hasNext() {
        return outerIter.hasNext();
      }

      @Override
      public String next() {
        Row row = outerIter.next();
        return row == null ? null : row.toString(delimiter, nullIndicator, quotes);
      }

      @Override
      public void remove() {
      }
    };
  }

  /**
   * Provide a Path where this data is stored.  This will be the directory, there may be multiple
   * files in the directory.  A particular implemenation of DataSet may choose not to support
   * writing itself out to HDFS.
   * @param clusterLocation URI for this data.
   */
  public void setClusterLocation(Path clusterLocation) {
    this.clusterLocation = clusterLocation;
  }

  /**
   * Get the the Path where this is stored.  If this is null, then the data is only in memory.
   * @return storage location
   */
  public Path getClusterLocation() {
    return clusterLocation;
  }

  /**
   * Dump the contents of the DataSet into a local file in java.io.tmpdir.
   * @param label A label to be used in the file name
   * @return name of the file the data was dumped into.
   */
  String dumpToFile(String label) throws IOException {
    String fileName = new StringBuilder(System.getProperty("java.io.tmpdir"))
        .append(System.getProperty("file.separator"))
        .append("capy_dataset_dump_")
        .append(label)
        .append('_')
        .append(uniqueId())
        .toString();
    File file = new File(fileName);
    FileWriter writer = new FileWriter(file);
    Iterator<String> iter = stringIterator(",", "NULL", "'");
    while (iter.hasNext()) writer.write(iter.next());
    writer.close();
    return fileName;
  }

  /**
   * Get a unique identifier for this DataSet.  This is useful for classes handling multiple
   * DataSets that need to be able to distinguish them (like in a Map).  This identifier is only
   * guaranteed to be unique within this JVM.  Treat it as transient.
   * @return a unique id for this DataSet
   */
  int uniqueId() {
    if (id == 0) {
      id = incrementId();
    }
    return id;
  }

  /**
   * Get the total size of the dataset.  This is for testing purposes and shouldn't be used in
   * general.  It can be expensive, as it forces an iteration through the data set.
   * @return size of the dataset.
   */
  @VisibleForTesting
  long lengthInBytes() {
    long len = 0;
    for (Row r : this) len += r.lengthInBytes();
    return len;
  }

  private static synchronized int incrementId() {
    return nextId++;
  }

  /**
   * A row of data.  This is a list of Columns, along with methods for sorting, comparing,
   * hashing, and turning a row to a string.
   */
  protected static class Row implements Iterable<Column>, Comparable<Row> {
    private List<Column> cols;

    private Row(List<Column> c) {
      cols = c;
    }

    @Override
    public Iterator<Column> iterator() {
      return cols.iterator();
    }

    @Override
    public int compareTo(Row o) {
      for (int i = 0; i < cols.size(); i++) {
        if (i > o.cols.size()) return 1;
        Column thatCol = o.cols.get(i);
        int rc = cols.get(i).compareTo(thatCol);
        if (rc != 0) return rc;
      }
      // They may have been equal all along, but if there are more columns in the other row,
      // declare it greater.
      if (cols.size() < o.cols.size()) return -1;
      else return 0;
    }

    @Override
    public String toString() {
      return toString(",", "NULL", "");
    }

    /**
     * Convert a row to a String, controlling how various aspects of the row are printed out.
     * @param delimiter column delimiter
     * @param nullIndicator value to return for null
     * @param quotes quotes to use around string columns
     * @return row as a String.
     */
    String toString(String delimiter, String nullIndicator, String quotes) {
      StringBuilder builder = new StringBuilder();
      for (int i = 0; i < cols.size(); i++) {
        if (i != 0) builder.append(delimiter);
        builder.append(cols.get(i).toString(nullIndicator, quotes));
      }
      return builder.toString();
    }

    /**
     * Number of columns in this row.
     * @return columns in row
     */
    int size() {
      return cols.size();
    }

    /**
     * Get the column.  This is zero based even though column numbers are 1 based.
     * @param i zero based column reference
     * @return column
     */
    Column get(int i) {
      return cols.get(i);
    }

    /**
     * Estimated length of this row.  Note that this is a rough estimate on the number of bytes
     * it takes to represent this in a binary format on disk.  Each row takes
     * significantly more space in memory due to container objects, etc.
     * @return estimated length of the row.
     */
    int lengthInBytes() {
      int len = 0;
      for (Column col : cols) len += col.length();
      return len;
    }

    @Override
    public boolean equals(Object other) {
      if (other == null || !(other instanceof Row)) return false;
      return cols.equals(((Row)other).cols);
    }

    @Override
    public int hashCode() {
      return cols.hashCode();
    }

    /**
     * Append columns of another row to this row.
     * @param that row to append
     */
    void append(Row that) {
      cols.addAll(that.cols);
    }
  }

  /**
   * A column value.  There are implementations for each Java object type containing column data.
   * This class provides methods for comparing, loading via JDBC, converting from JDBC result
   * sets, parsing from a string, and turning into a string.  For datatypes with differing
   * requirements for equals (float, double) and for bytes (which needs different definitions of
   * equals, sorting, hashing, and string parsing) this gives us a place to do it.
   *
   * Note that Columns themselves should
   * never be null.  Null in a column is represented by the contained val being null.
   */
  protected abstract static class Column implements Comparable<Column> {
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
     * @throws SQLException
     */
    abstract void load(PreparedStatement stmt) throws SQLException;

    /**
     * Parse this value from a string
     * @param str string representation
     * @param nullIndicator string value that indicates null
     */
    abstract void fromString(String str, String nullIndicator);

    /**
     * Parse this value from a {@link java.sql.ResultSet}.
     * @param rs ResultSet to parse the column from.
     * @throws SQLException
     */
    abstract void fromResultSet(ResultSet rs) throws SQLException;

    abstract void fromObject(ObjectInspector objectInspector, Object o);

    /**
     * Return the length of this column.  For String based and binary columns this is the length of
     * the actual data, not the max length possible (ie a varchar(32) columns with the value 'fred'
     * will return 4, not 32). For fixed length columns it will return an estimate of the size of
     * the column (ie 8 for long, 4 for int).
     * @return length
     */
    abstract long length();

    void set(Object val) {
      this.val = (Comparable)val;
    }

    boolean isNull() {
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

    // For the most part I don't expect to need these, as hopefully we don't usually need to
    // break things out by type.  But they're useful for testing and a few other places.
    long asLong() { throw new UnsupportedOperationException("This is not a bigint column"); }
    int asInt() { throw new UnsupportedOperationException("This is not a int column"); }
    short asShort() { throw new UnsupportedOperationException("This is not a smallint column"); }
    byte asByte() { throw new UnsupportedOperationException("This is not a tinyint column"); }
    float asFloat() { throw new UnsupportedOperationException("This is not a float column"); }
    double asDouble() { throw new UnsupportedOperationException("This is not a double column"); }
    BigDecimal asBigDecimal() { throw new UnsupportedOperationException("This is not a decimal column"); }
    Date asDate() { throw new UnsupportedOperationException("This is not a date column"); }
    Timestamp asTimestamp() { throw new UnsupportedOperationException("This is not a timestamp column"); }
    String asString() { throw new UnsupportedOperationException("This is not a string column"); }
    boolean asBoolean() { throw new UnsupportedOperationException("This is not a boolean column"); }
    byte[] asBytes() { throw new UnsupportedOperationException("This is not a bytes column"); }
  }

  protected static class LongColumn extends Column {
    LongColumn(int colNum) {
      super(colNum);
    }

    @Override
    void load(PreparedStatement stmt) throws SQLException {
      if (val == null) stmt.setNull(colNum, Types.BIGINT);
      else stmt.setLong(colNum, (Long)val);
    }

    @Override
    void fromString(String str, String nullIndicator) {
      if (str.equals(nullIndicator)) val = null;
      else val = Long.valueOf(str);
    }

    @Override
    void fromResultSet(ResultSet rs) throws SQLException {
      long hiveVal = rs.getLong(colNum);
      if (rs.wasNull()) val = null;
      else val = hiveVal;
    }

    @Override
    void fromObject(ObjectInspector objectInspector, Object o) {
      if (o == null) val = null;
      else val = ((LongObjectInspector)objectInspector).get(o);
    }

    @Override
    long length() {
      return Long.SIZE / 8;
    }

    @Override
    long asLong() {
      return (Long)val;
    }
  }

  protected static class IntColumn extends Column {
    IntColumn(int colNum) {
      super(colNum);
    }

    @Override
    void load(PreparedStatement stmt) throws SQLException {
      if (val == null) stmt.setNull(colNum, Types.INTEGER);
      else stmt.setInt(colNum, (Integer) val);
    }

    @Override
    void fromString(String str, String nullIndicator) {
      if (str.equals(nullIndicator)) val = null;
      else val = Integer.valueOf(str);
    }

    @Override
    void fromResultSet(ResultSet rs) throws SQLException {
      int hiveVal = rs.getInt(colNum);
      if (rs.wasNull()) val = null;
      else val = hiveVal;
    }

    @Override
    void fromObject(ObjectInspector objectInspector, Object o) {
      if (o == null) val = null;
      else val = ((IntObjectInspector)objectInspector).get(o);
    }

    @Override
    long length() {
      return Integer.SIZE / 8;
    }

    @Override
    int asInt() {
      return (Integer)val;
    }
  }

  protected static class ShortColumn extends Column {
    ShortColumn(int colNum) {
      super(colNum);
    }

    @Override
    void load(PreparedStatement stmt) throws SQLException {
      if (val == null) stmt.setNull(colNum, Types.SMALLINT);
      else stmt.setShort(colNum, (Short) val);
    }

    @Override
    void fromString(String str, String nullIndicator) {
      if (str.equals(nullIndicator)) val = null;
      else val = Short.valueOf(str);
    }

    @Override
    void fromResultSet(ResultSet rs) throws SQLException {
      short hiveVal = rs.getShort(colNum);
      if (rs.wasNull()) val = null;
      else val = hiveVal;
    }

    @Override
    void fromObject(ObjectInspector objectInspector, Object o) {
      if (o == null) val = null;
      else val = ((ShortObjectInspector)objectInspector).get(o);
    }

    @Override
    long length() {
      return Short.SIZE / 8;
    }

    @Override
    short asShort() {
      return (Short)val;
    }
  }

  protected static class ByteColumn extends Column {
    ByteColumn(int colNum) {
      super(colNum);
    }

    @Override
    void load(PreparedStatement stmt) throws SQLException {
      if (val == null) stmt.setNull(colNum, Types.TINYINT);
      else stmt.setByte(colNum, (Byte) val);
    }

    @Override
    void fromString(String str, String nullIndicator) {
      if (str.equals(nullIndicator)) val = null;
      else val = Byte.valueOf(str);
    }

    @Override
    void fromResultSet(ResultSet rs) throws SQLException {
      byte hiveVal = rs.getByte(colNum);
      if (rs.wasNull()) val = null;
      else val = hiveVal;
    }

    @Override
    void fromObject(ObjectInspector objectInspector, Object o) {
      if (o == null) val = null;
      else val = ((ByteObjectInspector)objectInspector).get(o);
    }

    @Override
    long length() {
      return Byte.SIZE / 8;
    }

    @Override
    byte asByte() {
      return (Byte)val;
    }
  }

  protected static class FloatColumn extends Column {
    FloatColumn(int colNum) {
      super(colNum);
    }

    @Override
    void load(PreparedStatement stmt) throws SQLException {
      if (val == null) stmt.setNull(colNum, Types.FLOAT);
      else stmt.setFloat(colNum, (Float) val);
    }

    @Override
    void fromString(String str, String nullIndicator) {
      if (str.equals(nullIndicator)) val = null;
      else val = Float.valueOf(str);
    }

    @Override
    void fromResultSet(ResultSet rs) throws SQLException {
      float hiveVal = rs.getFloat(colNum);
      if (rs.wasNull()) val = null;
      else val = hiveVal;
    }

    @Override
    void fromObject(ObjectInspector objectInspector, Object o) {
      if (o == null) val = null;
      else val = ((FloatObjectInspector)objectInspector).get(o);
    }

    @Override
    long length() {
      return Float.SIZE / 8;
    }

    @Override
    float asFloat() {
      return (Float)val;
    }

    @Override
    public boolean equals(Object other) {
      if (other == null || !(other instanceof FloatColumn)) return false;
      FloatColumn that = (FloatColumn)other;
      if (val == null && that.val == null) return true;
      else if (val == null || that.val == null) return false;

      // We want to be fuzzy in our comparisons, but just using a hard wired differential is hard
      // because we don't know the scale.  So look at the bits and mask out the last few, as
      // these are where the difference is likely to be.
      int thisBits = Float.floatToIntBits((Float)val);
      int thatBits = Float.floatToIntBits((Float)that.val);

      // Make sure the sign is the same
      if ((thisBits & 0x80000000) != (thatBits & 0x80000000)) return false;
      // Check the exponent
      if ((thisBits & 0x7f800000) != (thatBits & 0x7f800000)) return false;
      // Check the mantissa, but leave off the last two bits
      return (thisBits & 0x007fff00) == (thatBits & 0x007fff00);
    }

    @Override
    public int compareTo(Column other) {
      // Override this since we're playing a little fast and loose with equals
      if (equals(other)) return 0;
      else return super.compareTo(other);
    }
  }

  protected static class DoubleColumn extends Column {
    DoubleColumn(int colNum) {
      super(colNum);
    }

    @Override
    void load(PreparedStatement stmt) throws SQLException {
      if (val == null) stmt.setNull(colNum, Types.DOUBLE);
      else stmt.setDouble(colNum, (Double) val);
    }

    @Override
    void fromString(String str, String nullIndicator) {
      if (str.equals(nullIndicator)) val = null;
      else val = Double.valueOf(str);
    }

    @Override
    void fromResultSet(ResultSet rs) throws SQLException {
      double hiveVal = rs.getDouble(colNum);
      if (rs.wasNull()) val = null;
      else val = hiveVal;
    }

    @Override
    void fromObject(ObjectInspector objectInspector, Object o) {
      if (o == null) val = null;
      else val = ((DoubleObjectInspector)objectInspector).get(o);
    }

    @Override
    long length() {
      return Double.SIZE / 8;
    }

    @Override
    double asDouble() {
      return (Double)val;
    }

    @Override
    public boolean equals(Object other) {
      if (other == null || !(other instanceof DoubleColumn)) return false;
      DoubleColumn that = (DoubleColumn)other;
      if (val == null && that.val == null) return true;
      else if (val == null || that.val == null) return false;

      // We want to be fuzzy in our comparisons, but just using a hard wired differential is hard
      // because we don't know the scale.  So look at the bits and mask out the last few, as
      // these are where the difference is likely to be.
      long thisBits = Double.doubleToLongBits((Double)val);
      long thatBits = Double.doubleToLongBits((Double)that.val);

      // Make sure the sign is the same
      if ((thisBits & 0x8000000000000000L) != (thatBits & 0x8000000000000000L)) return false;
      // Check the exponent
      if ((thisBits & 0x7ff0000000000000L) != (thatBits & 0x7ff0000000000000L)) return false;
      // Check the mantissa, but leave off the last eight bits
      return (thisBits & 0x000fffff00000000L) == (thatBits & 0x000fffff00000000L);
    }

    @Override
    public int compareTo(Column other) {
      // Override this since we're playing a little fast and loose with equals
      if (equals(other)) return 0;
      else return super.compareTo(other);
    }
  }

  protected static class DecimalColumn extends Column {
    DecimalColumn(int colNum) {
      super(colNum);
    }

    @Override
    void load(PreparedStatement stmt) throws SQLException {
      if (val == null) stmt.setNull(colNum, Types.DECIMAL);
      else stmt.setBigDecimal(colNum, (BigDecimal) val);
    }

    @Override
    void fromString(String str, String nullIndicator) {
      if (str.equals(nullIndicator)) val = null;
      else val = new BigDecimal(str);
    }

    @Override
    void fromResultSet(ResultSet rs) throws SQLException {
      BigDecimal hiveVal = rs.getBigDecimal(colNum);
      if (rs.wasNull()) val = null;
      else val = hiveVal;
    }

    @Override
    void fromObject(ObjectInspector objectInspector, Object o) {
      if (o == null) val = null;
      else val = ((HiveDecimalObjectInspector)objectInspector).getPrimitiveJavaObject(o).bigDecimalValue();
    }

    @Override
    long length() {
      return val == null ? 0 : ((BigDecimal)val).unscaledValue().bitLength() / 8;
    }

    @Override
    BigDecimal asBigDecimal() {
      return (BigDecimal)val;
    }

    @Override
    public boolean equals(Object other) {
      if (other == null || !(other instanceof DecimalColumn)) return false;
      DecimalColumn that = (DecimalColumn)other;
      if (val == null && that.val == null) return true;
      else if (val == null || that.val == null) return false;

      // Handle the fact that values may have different scales, since Hive is sloppy about not
      // appending trailing zeros.
      BigDecimal thisVal = (BigDecimal)val;
      BigDecimal thatVal = (BigDecimal)that.val;
      if (thisVal.scale() == thatVal.scale()) {
        return thisVal.equals(thatVal);
      } else {
        // TODO - I'm not sure this is the best choice.  This picks the
        // TODO - entry with the lowest scale, subtracts one, and compares both entries at that
        // TODO - scale.  (The subtraction of 1 is to handle rounding differences.)  This handles
        // TODO - the fact that Hive
        // TODO - doesn't append trailing zeros and that for different expressions the two data
        // TODO - stores may assign different scales (e.g. avg(decimal(10,2)) produces a
        // TODO - decimal(10,6)  in Hive and a decimal (10,4) in Derby.)  But it will obscure
        // TODO - cases where we'd like to check that scale is properly kept.
        int newScale = Math.min(thisVal.scale(), thatVal.scale()) - 1;
        BigDecimal newThisVal = thisVal.setScale(newScale, RoundingMode.FLOOR);
        BigDecimal newThatVal = thatVal.setScale(newScale, RoundingMode.FLOOR);
        return newThisVal.equals(newThatVal);
      }
    }
  }

  protected static class DateColumn extends Column {
    DateColumn(int colNum) {
      super(colNum);
    }

    @Override
    void load(PreparedStatement stmt) throws SQLException {
      if (val == null) stmt.setNull(colNum, Types.DATE);
      else stmt.setDate(colNum, (Date) val);
    }

    @Override
    void fromString(String str, String nullIndicator) {
      if (str.equals(nullIndicator)) val = null;
      else val = Date.valueOf(str);
    }

    @Override
    void fromResultSet(ResultSet rs) throws SQLException {
      Date hiveVal = rs.getDate(colNum);
      if (rs.wasNull()) val = null;
      else val = hiveVal;
    }

    @Override
    void fromObject(ObjectInspector objectInspector, Object o) {
      if (o == null) val = null;
      else val = ((DateObjectInspector)objectInspector).getPrimitiveJavaObject(o);
    }

    @Override
    long length() {
      return Long.SIZE / 8; // Since we can express this as a long, that must be enough bytes to
      // store it.
    }

    @Override
    Date asDate() {
      return (Date)val;
    }
  }

  protected static class TimestampColumn extends Column {
    TimestampColumn(int colNum) {
      super(colNum);
    }

    @Override
    void load(PreparedStatement stmt) throws SQLException {
      if (val == null) stmt.setNull(colNum, Types.TIMESTAMP);
      else stmt.setTimestamp(colNum, (Timestamp) val);
    }

    @Override
    void fromString(String str, String nullIndicator) {
      if (str.equals(nullIndicator)) val = null;
      else val = Timestamp.valueOf(str);
    }

    @Override
    void fromResultSet(ResultSet rs) throws SQLException {
      Timestamp hiveVal = rs.getTimestamp(colNum);
      if (rs.wasNull()) val = null;
      else val = hiveVal;
    }

    @Override
    void fromObject(ObjectInspector objectInspector, Object o) {
      if (o == null) val = null;
      else val = ((TimestampObjectInspector)objectInspector).getPrimitiveJavaObject(o);
    }

    @Override
    long length() {
      return Long.SIZE / 8;
    }

    @Override
    Timestamp asTimestamp() {
      return (Timestamp)val;
    }
  }

  protected static class StringColumn extends Column {
    StringColumn(int colNum) {
      super(colNum);
    }

    @Override
    void load(PreparedStatement stmt) throws SQLException {
      if (val == null) stmt.setNull(colNum, Types.VARCHAR);
      else stmt.setString(colNum, (String) val);
    }

    @Override
    void fromString(String str, String nullIndicator) {
      if (str.equals(nullIndicator)) val = null;
      else val = str;
    }

    @Override
    void fromResultSet(ResultSet rs) throws SQLException {
      String hiveVal = rs.getString(colNum);
      if (rs.wasNull()) val = null;
      else val = hiveVal;
    }

    @Override
    void fromObject(ObjectInspector objectInspector, Object o) {
      if (o == null) val = null;
      else val = ((StringObjectInspector)objectInspector).getPrimitiveJavaObject(o);
    }

    @Override
    long length() {
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
    String asString() {
      return (String)val;
    }
  }

  // For the most part char and varchar can be handled identically to Strings.  The one case
  // where this isn't true is object inspectors.
  protected static class CharColumn extends StringColumn {
    public CharColumn(int colNum) {
      super(colNum);
    }

    @Override
    void fromObject(ObjectInspector objectInspector, Object o) {
      if (o == null) val = null;
      else val = ((HiveCharObjectInspector)objectInspector).getPrimitiveJavaObject(o).toString();
    }
  }

  protected static class VarcharColumn extends StringColumn {
    public VarcharColumn(int colNum) {
      super(colNum);
    }

    @Override
    void fromObject(ObjectInspector objectInspector, Object o) {
      if (o == null) val = null;
      else val = ((HiveVarcharObjectInspector)objectInspector).getPrimitiveJavaObject(o).toString();
    }
  }

  protected static class BooleanColumn extends Column {
    BooleanColumn(int colNum) {
      super(colNum);
    }

    @Override
    void load(PreparedStatement stmt) throws SQLException {
      if (val == null) stmt.setNull(colNum, Types.BOOLEAN);
      else stmt.setBoolean(colNum, (Boolean) val);
    }

    @Override
    void fromString(String str, String nullIndicator) {
      if (str.equals(nullIndicator)) val = null;
      else val = Boolean.valueOf(str);
    }

    @Override
    void fromResultSet(ResultSet rs) throws SQLException {
      boolean hiveVal = rs.getBoolean(colNum);
      if (rs.wasNull()) val = null;
      else val = hiveVal;
    }

    @Override
    void fromObject(ObjectInspector objectInspector, Object o) {
      if (o == null) val = null;
      else val = ((BooleanObjectInspector)objectInspector).get(o);
    }

    @Override
    long length() {
      return 1;
    }

    @Override
    boolean asBoolean() {
      return (Boolean)val;
    }
  }

  /**
   * This one's a bit of a special case because byte[] isn't comparable.  Also, we Base64
   * encode/decode when moving back and forth with String.  If we don't, Hive has problems when
   * we put the data in a text file because delimiters end up in the data.
   */
  protected static class BytesColumn extends Column {
    byte[] bVal;

    BytesColumn(int colNum) {
      super(colNum);
    }

    @Override
    void load(PreparedStatement stmt) throws SQLException {
      if (val == null) stmt.setNull(colNum, Types.BLOB);
      else stmt.setBytes(colNum, bVal);
    }

    @Override
    void fromString(String str, String nullIndicator) {
      if (str.equals(nullIndicator)) bVal = null;
      else bVal = Base64.decodeBase64(str);
    }

    @Override
    void fromResultSet(ResultSet rs) throws SQLException {
      byte[] hiveVal = rs.getBytes(colNum);
      if (rs.wasNull()) bVal = null;
      else bVal = hiveVal;
    }

    @Override
    void fromObject(ObjectInspector objectInspector, Object o) {
      if (o == null) bVal = null;
      else bVal = ((BinaryObjectInspector)objectInspector).getPrimitiveJavaObject(o);
    }

    @Override
    long length() {
      if (bVal == null) return 0;
      else return bVal.length;
    }

    @Override
    byte[] asBytes() {
      return bVal;
    }

    @Override
    void set(Object val) {
      if (val == null) bVal = null;
      else if (val instanceof byte[]) bVal = (byte[])val;
      else throw new RuntimeException("Attempt to set ByteColumn to non-byte[] value " +
            val.getClass().getName());
    }

    @Override
    boolean isNull() {
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
      return Arrays.equals(bVal, ((BytesColumn)o).bVal);
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
}

  /**
   * A class to build new rows.  This understands the layout of the row it will build.  It is
   * useful because we can build the template once and then quickly generate new rows each time
   * we need one.
   */
  static class RowBuilder {
    private final List<ColBuilder> builders;
    private final int colOffset; // This is needed for partition columns, because their column number
    // doesn't match their position in the schema that is passed.

    /**
     * A constructor for regular columns.
     * @param schema list of FieldSchemas that describe the row
     */
    RowBuilder(List<FieldSchema> schema) {
      this(schema, 0);
    }

    /**
     * A constructor for building a RowBuilder for partition columns.
     * @param schema list of FieldSchemas that describe the row
     * @param offset number of non-partition columns in the table, used to figure out the column
     *               number for each partition column.
     */
    RowBuilder(List<FieldSchema> schema, int offset) {
      builders = new ArrayList<>(schema.size());
      for (FieldSchema fs : schema) builders.add(getColBuilderFromFieldSchema(fs));
      colOffset = offset;
    }

    Row build() {
      List<Column> cols = new ArrayList<>(builders.size());
      // Add 1 to the column number so that it works with SQL calls.
      for (int i = 0; i < builders.size(); i++) cols.add(builders.get(i).build(i + colOffset + 1));
      return new Row(cols);
    }
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


