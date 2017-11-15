/*
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
package org.apache.hive.hcatalog.data.transfer;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hive.hcatalog.common.HCatException;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class TestHCatWriter {

  @Test
  public void metastoreConfigValues() {
    Map<String, String> cfgVals = new HashMap<>();
    cfgVals.put(HiveConf.ConfVars.HIVE_AUTO_SORTMERGE_JOIN.varname, "true");
    cfgVals.put(MetastoreConf.ConfVars.THRIFT_URIS.toString(), "def");
    cfgVals.put(MetastoreConf.ConfVars.THRIFT_CONNECTION_RETRIES.getHiveName(), "17");
    HCatWriter writer = new HCatWriterTester(cfgVals);

    Assert.assertTrue(HiveConf.getBoolVar(writer.conf, HiveConf.ConfVars.HIVE_AUTO_SORTMERGE_JOIN));
    Assert.assertEquals("def", MetastoreConf.getVar(writer.conf, MetastoreConf.ConfVars.THRIFT_URIS));
    Assert.assertEquals(17, MetastoreConf.getIntVar(writer.conf, MetastoreConf.ConfVars.THRIFT_CONNECTION_RETRIES));
  }


  private static class HCatWriterTester extends HCatWriter {
    public HCatWriterTester(Map<String, String> config) {
      super(null, config);
    }

    @Override
    public WriterContext prepareWrite() throws HCatException {
      return null;
    }

    @Override
    public void write(Iterator<HCatRecord> recordItr) throws HCatException {

    }

    @Override
    public void commit(WriterContext context) throws HCatException {

    }

    @Override
    public void abort(WriterContext context) throws HCatException {

    }
  }
}
