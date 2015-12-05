/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hive.test.capybara.examples;

import org.apache.hive.hcatalog.streaming.DelimitedInputWriter;
import org.apache.hive.hcatalog.streaming.HiveEndPoint;
import org.apache.hive.hcatalog.streaming.StreamingConnection;
import org.apache.hive.hcatalog.streaming.TransactionBatch;
import org.apache.hive.test.capybara.IntegrationTest;
import org.apache.hive.test.capybara.annotations.AcidOn;
import org.apache.hive.test.capybara.iface.TestTable;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExampleStreamingTest extends IntegrationTest {
  static final private Logger LOG = LoggerFactory.getLogger(ExampleStreamingTest.class.getName());

  private boolean running;

  @Test
  @AcidOn
  public void stream() throws Exception {
    // There's no way to validate results while streaming (since there's no guarantee that each
    // database is at the same point in committing the results.  But we can stream data in while
    // running queries (just to make sure the queries work) and occasionally pause and run
    // queries to verify we are getting the same results.
    TestTable target = TestTable.getBuilder("streamtarget")
        .addCol("a", "varchar(10)")
        .addCol("b", "int")
        .setBucketCols("a")
        .setNumBuckets(2)
        .setAcid(true)
        .build();
    String[] colNames = {"a", "b"};
    target.createTargetTable();
    String[] rows = new String[] {"abc,1", "def,2", "ghi,3", "jkl,4", "mno,5", "pqr,6", "stu,7",
                                  "vwx,8", "yz,9", "alpha,10"};

    HiveEndPoint endPoint = getHiveEndPoint(target, null);
    StreamingConnection conn = endPoint.newConnection(true, getConf());
    running = true;
    Runnable queryRunner = new Runnable() {
      @Override
      public void run() {
        while (running) {
          try {
            // Just run it on Hive, we're not interested in testing that the benchmark can read
            // while writing.
            runHive("select count(*) from streamtarget");
            Thread.sleep(1000);
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        }
      }
    };

    Thread queryThread = new Thread(queryRunner);
    queryThread.start();

    for (int i = 0; i < 5; i++) {
      TransactionBatch txnBatch = conn.fetchTransactionBatch(5,
          new DelimitedInputWriter(colNames, ",", endPoint, getConf(), ','));
      while (txnBatch.remainingTransactions() > 0) {
        txnBatch.beginNextTransaction();
        for (String row : rows) txnBatch.write(row.getBytes());
        txnBatch.commit();
      }
      txnBatch.close();
      runQuery("select count(*) from streamtarget");
      sortAndCompare();
    }
    running = false;
    conn.close();
    queryThread.join();
    runQuery("select count(*) from streamtarget");
    compare();
    //tableCompare(target);
  }
}
