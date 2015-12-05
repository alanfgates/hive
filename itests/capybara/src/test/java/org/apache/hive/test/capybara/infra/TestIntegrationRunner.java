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

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.test.capybara.IntegrationTest;
import org.apache.hive.test.capybara.annotations.AcidOn;
import org.apache.hive.test.capybara.annotations.NoCli;
import org.apache.hive.test.capybara.annotations.NoJdbc;
import org.apache.hive.test.capybara.annotations.NoOrc;
import org.apache.hive.test.capybara.annotations.NoParquet;
import org.apache.hive.test.capybara.annotations.NoRcFile;
import org.apache.hive.test.capybara.annotations.NoSpark;
import org.apache.hive.test.capybara.annotations.NoTextFile;
import org.apache.hive.test.capybara.annotations.NoTez;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.sql.SQLException;

@NoSpark // Make sure no tests run when spark is set
public class TestIntegrationRunner extends IntegrationTest {

  @NoCli @Test public void testNoCli() {
    Assert.assertNotEquals("cli", TestConf.access());
  }

  @NoJdbc @Test public void testNoJdbc() {
    Assert.assertNotEquals("jdbc", TestConf.access());
  }

  @NoOrc @Test public void testNoOrc() {
    Assert.assertNotEquals("orc", TestConf.fileFormat());
  }

  @NoParquet @Test public void testNoParquet() {
    Assert.assertNotEquals("parquet", TestConf.fileFormat());
  }

  @NoRcFile @Test public void testNoRcFile() {
    Assert.assertNotEquals("rcfile", TestConf.fileFormat());
  }

  @NoTextFile @Test public void testTextFile() {
    Assert.assertNotEquals("text", TestConf.fileFormat());
  }

  @NoTez @Test public void testTez() {
    Assert.assertNotEquals("tez", TestConf.engine());
  }

  @AcidOn @Test public void testAcid() throws IOException, SQLException {
    Assert.assertTrue(getCurrentConf().getBoolVar(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY));
  }

  @Test public void testNoSpark() {
    Assert.assertNotEquals("spark", TestConf.engine());
    Assert.assertFalse(getCurrentConf().getBoolVar(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY));
  }

}
