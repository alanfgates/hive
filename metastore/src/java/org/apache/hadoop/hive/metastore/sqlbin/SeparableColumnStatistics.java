/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.metastore.sqlbin;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsDesc;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.io.Writable;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * An in memory representation of the column statistics.  Often the optimizer only wants
 * statistics for a few columns.  By storing all statistics in one structure and deserializing
 * the whole thing we spend too much time deserializing the many columns we will never care about.
 * This class can be deserialized without requiring the statistics from every column to be
 * deserialized.  Instead each column is deserialized on demand.  Once the deserialization is
 * done it will be kept in case it is asked for again.
 */
class SeparableColumnStatistics {

  private ColumnStatisticsDesc statsDesc;
  private Map<String, ColStatsObjContainer> statsObjs;
  byte[] serialized;

  // For internal use only
  private SeparableColumnStatistics() {

  }

  /**
   * For use when deserializing.
   */
  SeparableColumnStatistics(byte[] serialized) {
    this.serialized = serialized;
  }

  /**
   * Construct from an existing ColumnStatistics object.
   * @param colStats existing stats to store.
   */
  SeparableColumnStatistics(ColumnStatistics colStats) {
    statsDesc = colStats.getStatsDesc();
    statsObjs = new HashMap<>(colStats.getStatsObjSize());
    for (ColumnStatisticsObj statsObj : colStats.getStatsObj()) {
      statsObjs.put(statsObj.getColName(), new ColStatsObjContainer(statsObj));
    }
  }

  ColumnStatisticsDesc getStatsDesc() throws IOException {
    if (statsDesc == null) deserialize();
    return statsDesc;
  }

  /**
   * Get statistics for a particular column.  They will be deserialized if they have not already
   * been.
   * @param colName column to get stats for
   * @return stats object, or null if stats for that column are not present.
   */
  ColumnStatisticsObj getStatsForCol(String colName) throws IOException {
    if (statsDesc == null) deserialize();
    ColStatsObjContainer container = statsObjs.get(colName);
    return (container == null) ? null : container.getColStatsObj();
  }

  /**
   * Return a trimmed version of this that contains only some columns.  The underlying data will
   * not be copied.  If every column in the current object is requested, then the current object
   * will be returned.  If none of the requested columns are present an empty object will be
   * returned.
   * @param colNames columns to include in the trimmed object.
   * @return possibly trimmed object
   */
  SeparableColumnStatistics trim(List<String> colNames) throws IOException {
    if (statsDesc == null) deserialize();
    if (colNames.size() == statsObjs.size()) {
      // Check to see if they've asked for everything we already have
      boolean sawMismatch = false;
      for (String colName : colNames) {
        if (!statsObjs.keySet().contains(colName)) {
          sawMismatch = true;
          break;
        }
      }
      if (!sawMismatch) return this;
    }

    SeparableColumnStatistics trimmed = new SeparableColumnStatistics();
    trimmed.statsDesc = this.statsDesc;
    trimmed.statsObjs = new HashMap<>(colNames.size());
    for (String colName : colNames) {
      if (statsObjs.keySet().contains(colName)) {
        trimmed.statsObjs.put(colName, statsObjs.get(colName));
      }
    }
    return trimmed;
  }

  /**
   * Build a new SeparableColumnStatistics object that is a union of this object and a
   * ColumnStatistics instance.
   * The underlying data will not be copied.  It is assumed that the two objects describe
   * statistics for the same object, that is they should have equal ColumnStatisticsDescriptors.
   * Entries from the passed in statistics will override entries in this objects map.
   * @param cs statistics to merge in
   * @return unioned statistics
   */
  SeparableColumnStatistics union(ColumnStatistics cs) throws IOException {
    if (statsDesc == null) deserialize();
    assert this.statsDesc.equals(cs.getStatsDesc());

    SeparableColumnStatistics unioned = new SeparableColumnStatistics();
    unioned.statsDesc = this.statsDesc;
    unioned.statsObjs = new HashMap<>(this.statsObjs);
    for (ColumnStatisticsObj cso : cs.getStatsObj()) {
      unioned.statsObjs.put(cso.getColName(), new ColStatsObjContainer(cso));
    }

    return unioned;
  }

  /**
   * Convert this object back to the thrift object.  This will force a full deserialization and
   * should only be used if we have to return the entire thrift object to the upper layers.
   * @return ColumnStatistics
   */
  ColumnStatistics asColumnStatistics() throws IOException {
    if (statsDesc == null) deserialize();
    ColumnStatistics cs = new ColumnStatistics();
    cs.setStatsDesc(this.statsDesc);
    for (ColStatsObjContainer container : this.statsObjs.values()) {
      cs.addToStatsObj(container.getColStatsObj());
    }
    return cs;
  }

  @VisibleForTesting
  boolean isSerialized(String colName) throws IOException {
    if (statsDesc == null) deserialize();
    return statsObjs.get(colName).isSerialized();
  }

  byte[] serialize() throws IOException {
    // Object is immutable, so if we've already serialized we can just return it that way.
    if (serialized != null) return serialized;

    ByteArrayOutputStream byteBuf = new ByteArrayOutputStream(1024 * statsObjs.size() + 1024);
    DataOutput dataOutput = new DataOutputStream(byteBuf);
    byte[] buf = PostgresKeyValue.serialize(statsDesc);
    dataOutput.writeInt(buf.length);
    dataOutput.write(buf);
    dataOutput.writeInt(statsObjs.size());
    for (Map.Entry<String, ColStatsObjContainer> entry : statsObjs.entrySet()) {
      // We write the name (even though it's in the ColumnStatisticsObj) so we don't have to
      // deserialize the whole thing when we read it back.
      dataOutput.writeInt(entry.getKey().length());
      dataOutput.writeBytes(entry.getKey());
      entry.getValue().write(dataOutput);
    }
    return byteBuf.toByteArray();
  }

  private void deserialize() throws IOException {
    DataInput dataInput = new DataInputStream(new ByteArrayInputStream(serialized));
    int len = dataInput.readInt();
    byte[] buf = new byte[len];
    dataInput.readFully(buf);
    statsDesc = new ColumnStatisticsDesc();
    PostgresKeyValue.deserialize(statsDesc, buf);

    int numContainers = dataInput.readInt();
    statsObjs = new HashMap<>(numContainers);
    for (int i = 0; i < numContainers; i++) {
      len = dataInput.readInt();
      buf = new byte[len];
      dataInput.readFully(buf);
      String name = new String(buf);
      ColStatsObjContainer container = new ColStatsObjContainer();
      container.readFields(dataInput);
      statsObjs.put(name, container);
    }
  }

  private static class ColStatsObjContainer implements Writable {

    // Don't access this directly, it is not guaranteed to be valid.
    private ColumnStatisticsObj statsObj;
    // Don't access this directly, it is not guaranteed to be valid.
    private byte[] serialized;

    ColStatsObjContainer() {

    }

    ColStatsObjContainer(ColumnStatisticsObj statsObj) {
      this.statsObj = statsObj;
    }

    ColumnStatisticsObj getColStatsObj() {
      if (statsObj == null) {
        assert serialized != null;
        statsObj = new ColumnStatisticsObj();
        PostgresKeyValue.deserialize(statsObj, serialized);
        serialized = null;
      }
      return statsObj;
    }

    private boolean isSerialized() {
      return statsObj == null;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
      if (serialized == null) {
        assert statsObj != null;
        serialized = PostgresKeyValue.serialize(statsObj);
      }
      dataOutput.writeInt(serialized.length);
      dataOutput.write(serialized);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
      int len = dataInput.readInt();
      assert len > 0;
      serialized = new byte[len];
      dataInput.readFully(serialized);
    }
  }



}
