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
package org.apache.hadoop.hive.metastore.sqlbin;

import org.apache.hadoop.hive.cli.CliSessionState;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.ql.CommandNeedRetryException;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;
import org.apache.hadoop.hive.ql.security.SessionStateConfigUserAuthenticator;
import org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd
    .SQLStdHiveAuthorizerFactoryForTest;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class TestPostgresStoreSql {
  // If this property is defined, we'll know that the user intends to connect to an external
  // postgres, all tests will be ignored.
  private static PostgresStore store;
  // If you create any tables with partitions, you must add the corresponding partition table into
  // this list so that it gets dropped at test start time.
  private static List<String> tablesToDrop = new ArrayList<>();

  private static HiveConf conf;

  private Driver driver;

  @BeforeClass
  public static void connect() throws MetaException, SQLException {
    conf = new HiveConf();
    conf.setVar(HiveConf.ConfVars.DYNAMICPARTITIONINGMODE, "nonstrict");
    conf.setBoolVar(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY, false);
    // Setup so we can test SQL standard auth
    conf.setBoolVar(HiveConf.ConfVars.HIVE_TEST_AUTHORIZATION_SQLSTD_HS2_MODE, true);
    conf.setVar(HiveConf.ConfVars.HIVE_AUTHORIZATION_MANAGER,
        SQLStdHiveAuthorizerFactoryForTest.class.getName());
    conf.setVar(HiveConf.ConfVars.HIVE_AUTHENTICATOR_MANAGER,
        SessionStateConfigUserAuthenticator.class.getName());
    conf.setBoolVar(HiveConf.ConfVars.HIVE_AUTHORIZATION_ENABLED, true);
    conf.setVar(HiveConf.ConfVars.USERS_IN_ADMIN_ROLE, System.getProperty("user.name"));
    conf.setVar(HiveConf.ConfVars.HIVEMAPREDMODE,"nonstrict");
    store = PostgresKeyValue.connectForTest(conf, tablesToDrop);
  }

  @AfterClass
  public static void cleanup() throws SQLException {
    PostgresKeyValue.cleanupAfterTest(store, tablesToDrop);
  }

  @Before
  public void checkExternalPostgres() {
    Assume.assumeNotNull(System.getProperty(PostgresKeyValue.TEST_POSTGRES_JDBC));
    SessionState.start(new CliSessionState(conf));
    driver = new Driver(conf);
  }

  @Test
  public void query55() throws CommandNeedRetryException {
    String dbName = "query55_db";
    CommandProcessorResponse response = driver.run("drop database if exists " + dbName);
    Assert.assertEquals(response.getErrorMessage(), 0, response.getResponseCode());
    response = driver.run("create database if not exists " + dbName);
    Assert.assertEquals(response.getErrorMessage(), 0, response.getResponseCode());
    response = driver.run("use " + dbName);
    Assert.assertEquals(response.getErrorMessage(), 0, response.getResponseCode());
    response = driver.run("create table date_dim\n" +
        "(\n" +
        "    d_date_sk                 bigint,\n" +
        "    d_date_id                 string,\n" +
        "    d_date                    string,\n" +
        "    d_month_seq               int,\n" +
        "    d_week_seq                int,\n" +
        "    d_quarter_seq             int,\n" +
        "    d_year                    int,\n" +
        "    d_dow                     int,\n" +
        "    d_moy                     int,\n" +
        "    d_dom                     int,\n" +
        "    d_qoy                     int,\n" +
        "    d_fy_year                 int,\n" +
        "    d_fy_quarter_seq          int,\n" +
        "    d_fy_week_seq             int,\n" +
        "    d_day_name                string,\n" +
        "    d_quarter_name            string,\n" +
        "    d_holiday                 string,\n" +
        "    d_weekend                 string,\n" +
        "    d_following_holiday       string,\n" +
        "    d_first_dom               int,\n" +
        "    d_last_dom                int,\n" +
        "    d_same_day_ly             int,\n" +
        "    d_same_day_lq             int,\n" +
        "    d_current_day             string,\n" +
        "    d_current_week            string,\n" +
        "    d_current_month           string,\n" +
        "    d_current_quarter         string,\n" +
        "    d_current_year            string\n" +
        ")");
    Assert.assertEquals(response.getErrorMessage(), 0, response.getResponseCode());

    response = driver.run("create table item\n" +
        "(\n" +
        "    i_item_sk                 bigint,\n" +
        "    i_item_id                 string,\n" +
        "    i_rec_start_date          string,\n" +
        "    i_rec_end_date            string,\n" +
        "    i_item_desc               string,\n" +
        "    i_current_price           double,\n" +
        "    i_wholesale_cost          double,\n" +
        "    i_brand_id                int,\n" +
        "    i_brand                   string,\n" +
        "    i_class_id                int,\n" +
        "    i_class                   string,\n" +
        "    i_category_id             int,\n" +
        "    i_category                string,\n" +
        "    i_manufact_id             int,\n" +
        "    i_manufact                string,\n" +
        "    i_size                    string,\n" +
        "    i_formulation             string,\n" +
        "    i_color                   string,\n" +
        "    i_units                   string,\n" +
        "    i_container               string,\n" +
        "    i_manager_id              int,\n" +
        "    i_product_name            string\n" +
        ")");
    Assert.assertEquals(response.getErrorMessage(), 0, response.getResponseCode());

    response = driver.run("create table store_sales\n" +
        "(\n" +
        "    ss_sold_time_sk           bigint,\n" +
        "    ss_item_sk                bigint,\n" +
        "    ss_customer_sk            bigint,\n" +
        "    ss_cdemo_sk               bigint,\n" +
        "    ss_hdemo_sk               bigint,\n" +
        "    ss_addr_sk                bigint,\n" +
        "    ss_store_sk               bigint,\n" +
        "    ss_promo_sk               bigint,\n" +
        "    ss_ticket_number          bigint,\n" +
        "    ss_quantity               int,\n" +
        "    ss_wholesale_cost         double,\n" +
        "    ss_list_price             double,\n" +
        "    ss_sales_price            double,\n" +
        "    ss_ext_discount_amt       double,\n" +
        "    ss_ext_sales_price        double,\n" +
        "    ss_ext_wholesale_cost     double,\n" +
        "    ss_ext_list_price         double,\n" +
        "    ss_ext_tax                double,\n" +
        "    ss_coupon_amt             double,\n" +
        "    ss_net_paid               double,\n" +
        "    ss_net_paid_inc_tax       double,\n" +
        "    ss_net_profit             double\n" +
        ") partitioned by (ss_sold_date_sk bigint)");
    Assert.assertEquals(response.getErrorMessage(), 0, response.getResponseCode());

    response = driver.run("analyze table date_dim compute statistics for columns");
    Assert.assertEquals(response.getErrorMessage(), 0, response.getResponseCode());
    response = driver.run("analyze table item compute statistics for columns");
    Assert.assertEquals(response.getErrorMessage(), 0, response.getResponseCode());
    response = driver.run("analyze table store_sales compute statistics for columns");
    Assert.assertEquals(response.getErrorMessage(), 0, response.getResponseCode());

    response = driver.run("alter table store_sales add partition (ss_sold_date = 1)");
    Assert.assertEquals(response.getErrorMessage(), 0, response.getResponseCode());

    response = driver.run("select  i_brand_id brand_id, i_brand brand,\n" +
        "  sum(ss_ext_sales_price) ext_price\n" +
        " from date_dim, store_sales, item\n" +
        " where date_dim.d_date_sk = store_sales.ss_sold_date_sk\n" +
        "  and store_sales.ss_item_sk = item.i_item_sk\n" +
        "  and i_manager_id=36\n" +
        "  and d_moy=12\n" +
        "  and d_year=2001\n" +
        " group by i_brand, i_brand_id\n" +
        " order by ext_price desc, i_brand_id\n" +
        "limit 100 ");
    Assert.assertEquals(response.getErrorMessage(), 0, response.getResponseCode());
  }

}
