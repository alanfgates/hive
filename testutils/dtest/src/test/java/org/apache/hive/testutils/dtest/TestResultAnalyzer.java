package org.apache.hive.testutils.dtest;

import org.junit.Assert;
import org.junit.Test;

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
public class TestResultAnalyzer {

  @Test
  public void unitTestLog() {
    ResultAnalyzer analyzer = new ResultAnalyzer();
    String[] log = {
        "[INFO] ------------------------------------------------------------------------",
        "[INFO] Building Hive Integration - Unit Tests 3.0.0-SNAPSHOT",
        "[INFO] ------------------------------------------------------------------------",
        "[WARNING] The POM for net.minidev:json-smart:jar:2.3-SNAPSHOT is missing, no dependency information available",
        "[WARNING] The POM for org.glassfish:javax.el:jar:3.0.1-b06-SNAPSHOT is missing, no dependency information available",
        "[WARNING] The POM for org.glassfish:javax.el:jar:3.0.1-b07-SNAPSHOT is missing, no dependency information available",
        "[WARNING] The POM for org.glassfish:javax.el:jar:3.0.1-b08-SNAPSHOT is missing, no dependency information available",
        "[INFO] Tests run: 4, Failures: 0, Errors: 0, Skipped: 0, Time elapsed: 27.497 s - in org.apache.hadoop.hive.ql.txn.compactor.TestCleanerWithReplication",
        "[INFO] Running org.apache.hadoop.hive.ql.txn.compactor.TestCompactor",
        "[INFO] Tests run: 15, Failures: 0, Errors: 0, Skipped: 0, Time elapsed: 207.35 s - in org.apache.hadoop.hive.ql.txn.compactor.TestCompactor",
        "[ERROR] Tests run: 11, Failures: 0, Errors: 1, Skipped: 1, Time elapsed: 328.082 s <<< FAILURE! - in org.apache.hadoop.hive.ql.TestAcidOnTez",
        "[ERROR] testGetSplitsLocks(org.apache.hadoop.hive.ql.TestAcidOnTez)  Time elapsed: 21.572 s  <<< ERROR!",
        "java.io.IOException: org.apache.hadoop.hive.ql.metadata.HiveException: java.io.IOException: java.lang.NullPointerException",
        "at org.apache.hadoop.hive.ql.exec.FetchTask.fetch(FetchTask.java:161)",
        "at org.apache.hadoop.hive.ql.Driver.getResults(Driver.java:2424)",
        "at org.apache.hadoop.hive.ql.reexec.ReExecDriver.getResults(ReExecDriver.java:215)",
        "at org.apache.hadoop.hive.ql.TestAcidOnTez.runStatementOnDriver(TestAcidOnTez.java:879)",
        "[ERROR] Tests run: 4, Failures: 1, Errors: 0, Skipped: 0, Time elapsed: 30.526 s <<< FAILURE! - in org.apache.hive.jdbc.TestActivePassiveHA",
        "[ERROR] testManualFailover(org.apache.hive.jdbc.TestActivePassiveHA)  Time elapsed: 1.665 s  <<< FAILURE!",
        "java.lang.AssertionError: expected:<true> but was:<false>",
        "at org.junit.Assert.fail(Assert.java:88)",
        "at org.junit.Assert.failNotEquals(Assert.java:743)",
        "at org.junit.Assert.assertEquals(Assert.java:118)",
        "at org.junit.Assert.assertEquals(Assert.java:144)"
    };
    for (String line : log) analyzer.analyzeLogLine("hive-dtest-1_itests-hive-unit", line);
    Assert.assertEquals(1, analyzer.getErrors().size());
    Assert.assertEquals("TestAcidOnTez.testGetSplitsLocks", analyzer.getErrors().get(0));
    Assert.assertEquals(1, analyzer.getFailed().size());
    Assert.assertEquals("TestActivePassiveHA.testManualFailover", analyzer.getFailed().get(0));
    Assert.assertEquals(32, analyzer.getSucceeded());
  }

  @Test
  public void qtestLog() {
    ResultAnalyzer analyzer = new ResultAnalyzer();
    String[] log = {
        "main:",
        "[delete] Deleting directory /root/hive/itests/qtest/target/tmp",
        "[delete] Deleting directory /root/hive/itests/qtest/target/testconf",
        "[delete] Deleting directory /root/hive/itests/qtest/target/warehouse",
        "[mkdir] Created dir: /root/hive/itests/qtest/target/tmp",
        "[mkdir] Created dir: /root/hive/itests/qtest/target/warehouse",
        "[mkdir] Created dir: /root/hive/itests/qtest/target/testconf",
        "[copy] Copying 19 files to /root/hive/itests/qtest/target/testconf",
        "[INFO] Executed tasks",
        "[INFO] Running org.apache.hadoop.hive.cli.TestNegativeCliDriver",
        "[ERROR] Tests run: 74, Failures: 1, Errors: 1, Skipped: 0, Time elapsed: 201.001 s <<< FAILURE! - in org.apache.hadoop.hive.cli.TestNegativeCliDriver",
        "[ERROR] testCliDriver[alter_notnull_constraint_violation](org.apache.hadoop.hive.cli.TestNegativeCliDriver)  Time elapsed: 2.699 s  <<< ERROR!",
        "org.apache.hadoop.hive.ql.exec.errors.DataConstraintViolationError: NOT NULL constraint violated!",
        "at org.apache.hadoop.hive.ql.udf.generic.GenericUDFEnforceNotNullConstraint.evaluate(GenericUDFEnforceNotNullConstraint.java:61)",
        "[ERROR] testCliDriver[alter_table_constraint_duplicate_pk](org.apache.hadoop.hive.cli.TestNegativeCliDriver)  Time elapsed: 0.428 s  <<< FAILURE!",
        "java.lang.AssertionError:",
        "Client Execution succeeded but contained differences (error code = 1) after executing alter_table_constraint_duplicate_pk.q",
        "11c11",
        "< FAILED: Execution Error, return code 1 from org.apache.hadoop.hive.ql.exec.DDLTask. MetaException(message: Primary key already exists for: hive.default.table1)",
        "---",
        "> FAILED: Execution Error, return code 1 from org.apache.hadoop.hive.ql.exec.DDLTask. MetaException(message: Primary key already exists for: default.table1)",
        "",
        "at org.junit.Assert.fail(Assert.java:88)",
        "at org.apache.hadoop.hive.ql.QTestUtil.failedDiff(QTestUtil.java:2166)"
    };
    for (String line : log) analyzer.analyzeLogLine("hive-dtest-1_itests-qtest_TestNegativeCliDriver_a_LF_a-t_RT_._S_", line);
    Assert.assertEquals(1, analyzer.getErrors().size());
    Assert.assertEquals("TestNegativeCliDriver.alter_notnull_constraint_violation", analyzer.getErrors().get(0));
    Assert.assertEquals(1, analyzer.getFailed().size());
    Assert.assertEquals("TestNegativeCliDriver.alter_table_constraint_duplicate_pk", analyzer.getFailed().get(0));
    Assert.assertEquals(72, analyzer.getSucceeded());
  }
}
