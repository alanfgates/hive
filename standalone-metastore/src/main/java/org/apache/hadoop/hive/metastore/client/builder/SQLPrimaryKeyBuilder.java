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
package org.apache.hadoop.hive.metastore.client.builder;

import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.SQLPrimaryKey;

import java.util.ArrayList;
import java.util.List;

/**
 * Builder for {@link SQLPrimaryKey}.  Only requires what {@link ConstraintBuilder} requires.
 */
public class SQLPrimaryKeyBuilder extends ConstraintBuilder<SQLPrimaryKeyBuilder> {

  public SQLPrimaryKeyBuilder() {
    super.setChild(this);
  }

  // Just to translate
  public SQLPrimaryKeyBuilder setPrimaryKeyName(String name) {
    return setConstraintName(name);
  }

  public List<SQLPrimaryKey> build() throws MetaException {
    checkBuildable("primary_key");
    List<SQLPrimaryKey> pk = new ArrayList<>(columns.size());
    for (String colName : columns) {
      SQLPrimaryKey keyCol = new SQLPrimaryKey(dbName, tableName, colName, getNextSeq(),
          constraintName, enable, validate, rely);
      keyCol.setCatName(catName);
      pk.add(keyCol);
    }
    return pk;
  }
}
