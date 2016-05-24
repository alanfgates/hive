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

import org.apache.hive.test.capybara.data.DataSet;
import org.apache.hive.test.capybara.data.ResultCode;
import org.apache.hive.test.capybara.iface.TestTable;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.sql.Driver;
import java.sql.SQLException;
import java.util.Properties;

public class TestAnsiSqlStore {

  private AnsiSqlStore store = new AnsiSqlStore() {
    @Override
    protected String ifExists() {
      return null;
    }

    @Override
    public String getTableName(TestTable table) {
      return null;
    }

    @Override
    public Class<? extends Driver> getJdbcDriverClass() {
      return null;
    }

    @Override
    protected String markColumnPrimaryKey() {
      return null;
    }

    @Override
    public void loadData(TestTable table, DataSet rows) throws SQLException, IOException {

    }

    @Override
    protected String fileColumnDelimiter() {
      return null;
    }

    @Override
    protected String fileNull() {
      return null;
    }

    @Override
    protected String fileStringQuotes() {
      return null;
    }

    @Override
    protected String getTempTableCreate(String tableName) {
      return null;
    }

    @Override
    protected Properties connectionProperties() {
      return null;
    }

    @Override
    protected String connectionURL() {
      return null;
    }

    @Override
    protected SQLTranslator getTranslator() {
      return new SQLTranslator() {
        @Override
        protected String translateDataTypes(String hiveSql) {
          return hiveSql;
        }

        @Override
        protected String translateAlterTableRename(String tableName, String remainder) throws
            TranslationException {
          return null;
        }

        @Override
        protected char identifierQuote() {
          return '"';
        }
      };
    }

    @Override
    public void forceCreateTable(TestTable table) throws SQLException, IOException {

    }
  };

  @Test
  public void createTable() throws Exception {
    String hiveSql = "create table if not exists acid_uanp(a int, b varchar(128)) partitioned by " +
        "(c string) clustered by (a) into 2 buckets stored as orc TBLPROPERTIES ('transactional'='true')";
    // string isn't translated to varchar in this query because TestAnsiSqlStore has it's own
    // translator that doesn't handle that.  It would be translated by derby or postgres.
    Assert.assertEquals("create table if not exists acid_uanp (a int, b varchar(128), c string)",
        store.hiveSqlToAnsiSql(hiveSql));
  }

  @Test
  public void insert() throws Exception {
    String hiveSql = "insert into table acid_uanp partition (c = 'fred') values (1, 'boy')";
    Assert.assertEquals("insert into acid_uanp values (1, 'boy', 'fred')",
        store.hiveSqlToAnsiSql(hiveSql));
    Assert.assertFalse(store.failureOk);
  }

  @Test
  public void dropTable() throws Exception {
    String hiveSql = "drop table if exists acid_uanp";
    Assert.assertEquals("drop table if exists acid_uanp", store.hiveSqlToAnsiSql(hiveSql));
  }

  @Test
  public void emptySqlSucceeds() throws Exception {
    // Make sure a Hive SQL statement like alter database which is a NOP for the benchmark succeeds
    String hiveSQL = "alter database fred set owner user user1";
    Assert.assertEquals("", store.hiveSqlToAnsiSql(hiveSQL));
    Assert.assertEquals(ResultCode.SUCCESS, store.executeSql(hiveSQL).rc);
  }
}
