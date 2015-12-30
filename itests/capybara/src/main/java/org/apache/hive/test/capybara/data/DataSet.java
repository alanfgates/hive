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

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;

/**
 * <p>A data set.  This class encapsulates a set of data.  The data could be the result of a query
 * against a {@link org.apache.hive.test.capybara.iface.DataStore} or generating data from a
 * {@link org.apache.hive.test.capybara.iface.DataGenerator}.</p>
 *
 * <p>Interally this class contains all the logic for handling rows, columns, and different
 * datatypes.  The intent is to factor all of the datatype specific code into this class so that
 * other classes in capybara can be type agnostic.
 * </p>
 */
public abstract class DataSet implements Iterable<Row> {
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
  public String dumpToFile(String label) throws IOException {
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
  public int uniqueId() {
    if (id == 0) {
      id = incrementId();
    }
    return id;
  }

  /**
   * Determine whether this DataSet is empty (ie, it has no data).
   * @return true if empty, false
   */
  public boolean isEmpty() {
    return rows == null || rows.size() == 0;
  }

  /**
   * Get the total size of the dataset.  This is for testing purposes and shouldn't be used in
   * general.  It can be expensive, as it forces an iteration through the data set.
   * @return size of the dataset.
   */
  @VisibleForTesting
  public long lengthInBytes() {
    long len = 0;
    for (Row r : this) len += r.lengthInBytes();
    return len;
  }

  private static synchronized int incrementId() {
    return nextId++;
  }

}


