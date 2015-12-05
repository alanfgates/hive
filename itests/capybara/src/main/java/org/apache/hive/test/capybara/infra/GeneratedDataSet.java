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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.metastore.api.FieldSchema;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * A generated DataSet.  This should never be used for data read out of a
 * {@link org.apache.hive.test.capybara.infra.DataStore}, but only for data generated by a
 * {@link org.apache.hive.test.capybara.DataGenerator}.  It makes assumptions about spilling and
 * remembering what rows it has that will go all wrong if you try to store data
 * from a DataStore in it.
 */
class GeneratedDataSet extends DataSet {

  static final private Logger LOG = LoggerFactory.getLogger(GeneratedDataSet.class.getName());

  private int currentSize;
  private int spillSize; // Size at which we spill the contents to disk.
  private DataStore hive;
  private DataStore bench;
  private Thread hiveDumper;
  private Thread benchDumper;


  GeneratedDataSet(List<FieldSchema> schema) {
    super(schema);
    rows = new ArrayList<>();
    spillSize = TestConf.getSpillSize();
  }

  @Override
  public Iterator<Row> iterator() {
    // If we've started spilling, we need to complete that before anyone reads from this.
    try {
      if (hiveDumper != null) hiveDumper.join();
      if (benchDumper != null) benchDumper.join();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
    return rows.iterator();
  }

  void addRow(Row row) {
    rows.add(row);
    currentSize += row.lengthInBytes();
    if (currentSize > spillSize) spill();
  }

  private void spill() {
    LOG.debug("Spilling rows because currentSize got to " + currentSize);
    try {
      if (hive == null) hive = TestManager.getTestManager().getClusterManager().getHive();
      if (bench == null) bench = TestManager.getTestManager().getBenchmark().getBenchDataStore();
      // Wait for previous instances of the spilling to complete before we start it again.  This
      // avoids building up too much in memory.
      if (hiveDumper != null) hiveDumper.join();
      if (benchDumper != null) benchDumper.join();
      hiveDumper = new Dumper(hive, new QueuedDataSet(this));
      benchDumper = new Dumper(bench, new QueuedDataSet(this));
      hiveDumper.start();
      benchDumper.start();
      currentSize = 0;
      // Don't clear the list, as we're using it to spill.
      rows = new ArrayList<>(rows.size());
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  private class Dumper extends Thread {
    final DataStore store;
    final DataSet data;
    IOException stashed;

    public Dumper(DataStore store, DataSet data) {
      this.store = store;
      this.data = data;
    }

    @Override
    public void run() {
      try {
        store.dumpToFileForImport(data);
      } catch (IOException e) {
        stashed = e;
      }
    }
  }

  private class QueuedDataSet extends DataSet {
    public QueuedDataSet(GeneratedDataSet outer) {
      super(outer.schema);
      rows = outer.rows;
    }

    @Override
    public Iterator<Row> iterator() {
      return rows.iterator();
    }
  }


}
