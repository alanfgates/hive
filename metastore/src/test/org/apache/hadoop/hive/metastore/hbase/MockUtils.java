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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hadoop.hive.metastore.hbase;

import com.google.protobuf.RpcController;
import com.google.protobuf.Service;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.FileFormatProxy;
import org.apache.hadoop.hive.metastore.PartitionExpressionProxy;
import org.apache.hadoop.hive.metastore.api.FileMetadataExprType;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.hbase.txn.txnmgr.TransactionCoprocessor;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * Mock utilities for HBaseStore testing.  Extend this class to use the mock utilities and call
 * {@link #mockInit} once you have a config file.
 */
public class MockUtils {

  static final private Logger LOG = LoggerFactory.getLogger(MockUtils.class.getName());

  private Map<String, HTableInterface> tables = new HashMap<>();

  protected TransactionCoprocessor txnCoProc;

  /**
   * The default impl is in ql package and is not available in unit tests.
   */
  public static class NOOPProxy implements PartitionExpressionProxy {

    @Override
    public String convertExprToFilter(byte[] expr) throws MetaException {
      return null;
    }

    @Override
    public boolean filterPartitionsByExpr(List<String> partColumnNames,
        List<PrimitiveTypeInfo> partColumnTypeInfos, byte[] expr, String defaultPartitionName,
        List<String> partitionNames) throws MetaException {
      return false;
    }

    @Override
    public SearchArgument createSarg(byte[] expr) {
      return null;
    }

    @Override
    public FileMetadataExprType getMetadataType(String inputFormat) {
      return null;
    }

    @Override
    public FileFormatProxy getFileFormatProxy(FileMetadataExprType type) {
      return null;
    }
  }

  private void initTable(HTableInterface htable) throws Throwable {
    final SortedMap<String, Cell> rows = new TreeMap<>();
    Mockito.when(htable.get(Mockito.any(Get.class))).thenAnswer(new Answer<Result>() {
      @Override
      public Result answer(InvocationOnMock invocation) throws Throwable {
        Get get = (Get) invocation.getArguments()[0];
        Cell cell = rows.get(new String(get.getRow()));
        if (cell == null) {
          return new Result();
        } else {
          return Result.create(new Cell[]{cell});
        }
      }
    });

    Mockito.when(htable.get(Mockito.anyListOf(Get.class))).thenAnswer(new Answer<Result[]>() {
      @Override
      public Result[] answer(InvocationOnMock invocation) throws Throwable {
        @SuppressWarnings("unchecked")
        List<Get> gets = (List<Get>) invocation.getArguments()[0];
        Result[] results = new Result[gets.size()];
        for (int i = 0; i < gets.size(); i++) {
          Cell cell = rows.get(new String(gets.get(i).getRow()));
          Result result;
          if (cell == null) {
            result = new Result();
          } else {
            result = Result.create(new Cell[]{cell});
          }
          results[i] = result;
        }
        return results;
      }
    });

    // Filters not currently supported in scan
    Mockito.when(htable.getScanner(Mockito.any(Scan.class))).thenAnswer(
        new Answer<ResultScanner>() {
          @Override
          public ResultScanner answer(InvocationOnMock invocation) throws Throwable {
            Scan scan = (Scan) invocation.getArguments()[0];
            List<Result> results = new ArrayList<>();
            String start = new String(scan.getStartRow());
            String stop = new String(scan.getStopRow());
            if (start.length() == 0) {
              start = new String(new char[]{Character.MIN_VALUE});
            }
            if (stop.length() == 0) {
              stop = new String(new char[]{Character.MAX_VALUE});
            }
            SortedMap<String, Cell> sub = rows.subMap(start, stop);
            for (Map.Entry<String, Cell> e : sub.entrySet()) {
              results.add(Result.create(new Cell[]{e.getValue()}));
            }

            final Iterator<Result> iter = results.iterator();

            return new ResultScanner() {
              @Override
              public Result next() throws IOException {
                return null;
              }

              @Override
              public Result[] next(int nbRows) throws IOException {
                return new Result[0];
              }

              @Override
              public void close() {

              }

              @Override
              public Iterator<Result> iterator() {
                return iter;
              }
            };
          }
        });

    Mockito.doAnswer(new Answer<Void>() {
      @Override
      public Void answer(InvocationOnMock invocation) throws Throwable {
        Put put = (Put) invocation.getArguments()[0];
        rows.put(new String(put.getRow()), put.getFamilyCellMap().firstEntry().getValue().get(0));
        return null;
      }
    }).when(htable).put(Mockito.any(Put.class));

    Mockito.doAnswer(new Answer<Void>() {
      @Override
      public Void answer(InvocationOnMock invocationOnMock) throws Throwable {
        List<Put> puts = (List<Put>) invocationOnMock.getArguments()[0];
        for (Put put : puts) {
          rows.put(new String(put.getRow()), put.getFamilyCellMap().firstEntry().getValue().get(0));
        }
        return null;
      }
    }).when(htable).put(Mockito.anyListOf(Put.class));

    Mockito.when(htable.checkAndPut(Mockito.any(byte[].class), Mockito.any(byte[].class),
        Mockito.any(byte[].class), Mockito.any(byte[].class), Mockito.any(Put.class))).thenAnswer(
        new Answer<Boolean>() {

          @Override
          public Boolean answer(InvocationOnMock invocation) throws Throwable {
            // Always say it succeeded and overwrite
            Put put = (Put) invocation.getArguments()[4];
            rows.put(new String(put.getRow()),
                put.getFamilyCellMap().firstEntry().getValue().get(0));
            return true;
          }
        });

    Mockito.doAnswer(new Answer<Void>() {
      @Override
      public Void answer(InvocationOnMock invocation) throws Throwable {
        Delete del = (Delete) invocation.getArguments()[0];
        rows.remove(new String(del.getRow()));
        return null;
      }
    }).when(htable).delete(Mockito.any(Delete.class));

    Mockito.doAnswer(new Answer<Void>() {
      @Override
      public Void answer(InvocationOnMock invocationOnMock) throws Throwable {
        List<Delete> deletes = (List<Delete>) invocationOnMock.getArguments()[0];
        for (Delete delete : deletes) {
          rows.remove(new String(delete.getRow()));
        }
        return null;
      }
    }).when(htable).delete(Mockito.anyListOf(Delete.class));

    Mockito.when(htable.checkAndDelete(Mockito.any(byte[].class), Mockito.any(byte[].class),
        Mockito.any(byte[].class), Mockito.any(byte[].class),
        Mockito.any(Delete.class))).thenAnswer(
        new Answer<Boolean>() {

          @Override
          public Boolean answer(InvocationOnMock invocation) throws Throwable {
            // Always say it succeeded
            Delete del = (Delete) invocation.getArguments()[4];
            rows.remove(new String(del.getRow()));
            return true;
          }
        });

    Mockito.when(htable.coprocessorService(Mockito.any(Class.class), Mockito.any(byte[].class),
        Mockito.any(byte[].class), Mockito.any(Batch.Call.class)))
        .thenAnswer(
            new Answer<Map>() {
              @Override
              public Map answer(InvocationOnMock invocationOnMock) throws Throwable {
                Class<? extends Service> clazz =
                    (Class<? extends Service>)invocationOnMock.getArguments()[0];
                if (!clazz.equals(TransactionCoprocessor.class)) {
                  throw new RuntimeException("Only know how to do TransactionCoprocessor");
                }
                Batch.Call call = (Batch.Call)invocationOnMock.getArguments()[3];
                Object o = call.call(txnCoProc);
                return Collections.singletonMap((byte[])null, o);
              }
            }
        );
  }

  protected HBaseStore mockInit(Configuration conf) throws IOException {
    ((HiveConf) conf).setVar(ConfVars.METASTORE_EXPRESSION_PROXY_CLASS,
        NOOPProxy.class.getName());
    // Mock connection
    HBaseConnection hconn = Mockito.mock(HBaseConnection.class);
    Mockito.when(hconn.getHBaseTable(Mockito.anyString())).thenAnswer(
        new Answer<HTableInterface>() {
          @Override
          public HTableInterface answer(InvocationOnMock invocationOnMock) throws Throwable {
            String tableName = (String)invocationOnMock.getArguments()[0];
            HTableInterface table = tables.get(tableName);
            if (table == null) {
              table = Mockito.mock(HTableInterface.class);
              initTable(table);
              tables.put(tableName, table);
            }
            return table;
          }
        }
    );
    // Setup HBase Store
    HiveConf.setVar(conf, HiveConf.ConfVars.METASTORE_HBASE_CONNECTION_CLASS,
        HBaseReadWrite.TEST_CONN);
    HBaseReadWrite.setTestConnection(hconn);
    HBaseReadWrite.setConf(conf);
    HBaseStore store = new HBaseStore();
    store.setConf(conf);

    // Setup the txn coprocessor
    txnCoProc = new TransactionCoprocessor();
    CoprocessorEnvironment env = Mockito.mock(CoprocessorEnvironment.class);
    Mockito.when(env.getConfiguration()).thenReturn(conf);
    txnCoProc.start(env);
    return store;
  }

  protected void mockShutdown() throws IOException {
    CoprocessorEnvironment env = Mockito.mock(CoprocessorEnvironment.class);
    txnCoProc.stop(env);
  }

  protected RpcController getController() {
    RpcController controller = Mockito.mock(RpcController.class);
    // TODO Going to have to figure out what setControllerException calls and then mock that
    return controller;
  }
}
