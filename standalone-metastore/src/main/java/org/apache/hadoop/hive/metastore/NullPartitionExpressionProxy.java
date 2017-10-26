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
package org.apache.hadoop.hive.metastore;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.FileMetadataExprType;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;

import java.util.List;

/**
 * An implementation of PartitionExpressionProxy that does not actually support partition
 * expression proxies.  This is here to allow the Metastore to start without the Hive jars in place.
 */
public class NullPartitionExpressionProxy implements PartitionExpressionProxy {
  @Override
  public String convertExprToFilter(byte[] expr) throws MetaException {
    throw error();
  }

  @Override
  public boolean filterPartitionsByExpr(List<FieldSchema> partColumns, byte[] expr,
                                        String defaultPartitionName,
                                        List<String> partitionNames) throws MetaException {
    throw error();
  }

  @Override
  public FileMetadataExprType getMetadataType(String inputFormat) {
    throw error();
  }

  @Override
  public FileFormatProxy getFileFormatProxy(FileMetadataExprType type) {
    throw error();
  }

  @Override
  public SearchArgument createSarg(byte[] expr) {
    throw error();
  }

  private UnsupportedOperationException error() {
    return new UnsupportedOperationException("Partition expressions not supported.  If you want " +
        "these supported you must install Hive jars in your class path and then set " +
        "metastore.expression.proxy to " +
        "org.apache.hadoop.hive.ql.optimizer.ppr.PartitionExpressionForMetastore in your config " +
        "file");
  }
}
