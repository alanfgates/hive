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
package org.apache.hive.test.capybara;

import org.apache.hive.test.capybara.data.DataSet;
import org.apache.hive.test.capybara.iface.DataGenerator;
import org.apache.hive.test.capybara.iface.TestTable;
import org.apache.hive.test.capybara.infra.RandomDataGenerator;
import org.apache.hive.test.capybara.infra.TestManager;
import org.apache.thrift.TException;

import java.io.IOException;
import java.sql.SQLException;

/**
 * Standard tables used in integration tests.  These tables tests are built to mimic the tests
 * available in Hive's qfile infrastructure, though they are not 1-1.  In particular the names
 * have been changed because Hive now looks for those specific names to prevent dropping them.
 */
public class TableTool {

  /**
   * Creates and populates the table <quote>alltypes</quote>.  The schema of alltypes is
   * (cvarchar varchar(120), cchar char(20), cstring string, cint int, cbigint bigint, csmallint
   * smallint, ctinyint tinyint, cfloat float, cdouble double, cdecimal decimal(10,2), cdate
   * date, ctimestamp timestamp, cboolean boolean).  This is similar to qfiles' alltypesorc table.
   * @throws TException
   * @throws SQLException
   */
  public static void createAllTypes() throws SQLException, IOException {
    TestTable t =  TestTable.getBuilder("alltypes")
        .addCol("cvarchar", "varchar(120)")
        .addCol("cchar", "char(20)")
        .addCol("cstring", "string")
        .addCol("cint", "int")
        .addCol("cbigint", "bigint")
        .addCol("csmallint", "smallint")
        .addCol("ctinyint", "tinyint")
        .addCol("cfloat", "float")
        .addCol("cdouble", "double")
        .addCol("cdecimal", "decimal(10,2)")
        .addCol("cdate", "date")
        .addCol("ctimestamp", "timestamp")
        .addCol("cboolean", "boolean")
        // .addCol("cbinary", "binary")  Binary doesn't work with Derby yet.
        .build();

    DataGenerator generator = new RandomDataGenerator(1);
    t.create();
    t.populate(generator);
  }

  /**
   * Creates and populates the table <quote>capysrc</quote>.  This is equivalent to the qfile
   * table src, but must be given a different name as Hive itself borks if you give it a table
   * with that name.  The schema is (k string, value string).
   * @throws TException
   * @throws SQLException
   */
  public static void createCapySrc() throws SQLException, IOException {
    TestTable t = TestTable.getBuilder("capysrc")
        .addCol("k", "string")
        .addCol("value", "string")
        .build();

    DataGenerator generator = new RandomDataGenerator(2);
    t.create();
    t.populate(generator);
  }

  /**
   * Creates and populates the table <quote>capysrcpart</quote>.  This is equilvalent to the
   * qfile table srcpart, but must be given a different name as Hive itself borks if you try to
   * create a table named srcpart. The schema is (k string, value string, ds string) where ds is
   * the partition column.
   * @throws SQLException
   * @throws IOException
   */
  public static void createCapySrcPart() throws SQLException, IOException {
    TestTable t = TestTable.getBuilder("capysrcpart")
        .addCol("k", "string")
        .addCol("value", "string")
        .addPartCol("ds", "string")
        .addPartCol("hr", "string")
        .setNumParts(partsFromScale())
        .build();

    DataGenerator generator = new RandomDataGenerator(3);
    t.create();
    t.populate(generator);
  }

  /**
   * Creates a populates a set of tables that are TPC-H like.  (It is TPC-H like in that it is a
   * very simplified version of TPC-H, with fewer tables, fewer columns per table, and only
   * single columned primary keys and foreign keys.  Also a few columns have been added or had
   * their datatypes modified in order to cover more datatypes.) These have the following schemas
   * and relationships:
   * facttable:
   * ph_lineitem (l_linenum bigint, l_orderkey bigint, l_partkey bigint,
   *    l_quantity integer, l_price decimal(10,2), l_shipdate date)
   * primarykey: l_linenum
   * foreignkeys:
   *   l_orderkey -> ph_order.o_orderkey
   *   l_partkey  -> ph_part.p_partkey
   *
   * dimension tables:
   * ph_customer (c_custkey bigint, c_name varchar(25), c_address varchar(100),
   *    c_acctbal decimal(20, 2))
   * primary key: c_custkey
   *
   * ph_order (o_orderkey bigint, o_custkey bigint, o_orderstatus char(1), o_orderdate timestamp,
   *    o_orderpriority tinyint)
   * primary key: o_orderkey
   * foreign keys:
   *   o_custkey -> ph_customer.c_custkey
   *
   * ph_part (p_partkey bigint, p_name varchar(55), p_size int, p_numinstock smallint,
   * p_backordered boolean)
   * primary key: p_partkey
   */
  public static void createPseudoTpch() throws IOException, SQLException {
    DataGenerator generator = new RandomDataGenerator('H');

    int dimScale = getDimScale();

    TestTable ph_part = TestTable.getBuilder("ph_part")
        .addCol("p_partkey", "bigint")
        .addCol("p_name", "varchar(55)")
        .addCol("p_size", "int")
        .addCol("p_numinstock", "smallint")
        .addCol("p_backordered", "boolean")
        .setPrimaryKey(new TestTable.Sequence(0))
        .build();
    ph_part.create();
    ph_part.setCacheData(true);
    ph_part.populate(generator, dimScale, null);
    DataSet partData = ph_part.getData();

    TestTable ph_customer = TestTable.getBuilder("ph_customer")
        .addCol("c_custkey", "bigint")
        .addCol("c_name", "varchar(55)")
        .addCol("c_address", "varchar(100)")
        .addCol("c_acctbal", "decimal(10,2)")
        .setPrimaryKey(new TestTable.Sequence(0))
        .build();
    ph_customer.create();
    ph_customer.populate(generator, dimScale, null);
    DataSet customerData = ph_customer.getData();

    TestTable ph_order = TestTable.getBuilder("ph_order")
        .addCol("o_orderkey", "bigint")
        .addCol("o_custkey", "bigint")
        .addCol("o_orderstatus", "char(1)")
        .addCol("o_orderdate", "timestamp")
        .addCol("o_orderpriority", "tinyint")
        .setPrimaryKey(new TestTable.Sequence(0))
        .addForeignKey(new TestTable.ForeignKey(customerData, 0, 1))
        .build();
    ph_order.create();
    ph_order.populate(generator, dimScale, null);
    DataSet orderData = ph_order.getData();

    TestTable ph_lineitem = TestTable.getBuilder("ph_lineitem")
        .addCol("l_linenum", "bigint")
        .addCol("l_orderkey", "bigint")
        .addCol("l_partkey", "bigint")
        .addCol("l_quantity", "int")
        .addCol("l_price", "decimal(10,2)")
        .addPartCol("l_shipdate", "date")
        .setPrimaryKey(new TestTable.Sequence(0))
        .addForeignKey(new TestTable.ForeignKey(orderData, 0, 1))
        .addForeignKey(new TestTable.ForeignKey(partData, 0, 2))
        .build();
    ph_lineitem.create();
    ph_lineitem.populate(generator);
  }

  private static int getDimScale() {
    return TestManager.getTestManager().getTestConf().getScale() / 100 + 1;
  }

  private static int partsFromScale() {
    return Math.max(2, TestManager.getTestManager().getTestConf().getScale() / (1024 * 512));
  }
}
