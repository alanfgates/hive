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
package org.apache.hive.test.capybara.infra;

import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.QueryPlan;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;

abstract class MiniHiveStoreBase extends HiveStore {
  static final private Logger LOG = LoggerFactory.getLogger(MiniClusterManager.class.getName());

  private ThreadLocal<Driver> driver;

  protected Driver getDriver() {
    if (driver == null) {
      driver = new ThreadLocal<Driver>() {
        @Override
        protected Driver initialValue() {
          // Make sure our conf file gets set
          SessionState.start(clusterManager.getHiveConf());
          assert SessionState.get() != null;
          return new Driver(clusterManager.getHiveConf());
        }
      };
    }
    return driver.get();
  }

  @Override
  public void tearDown() throws IOException {
    LOG.debug("Dropping created tables");
    super.tearDown();

    Exception caughtException = null;
    for (String dbNameTableName : registeredTables) {
      LOG.debug("Dropping table " + dbNameTableName);
      try {
        executeSql("drop table " + dbNameTableName);
      } catch (SQLException | IOException e) {
        LOG.error("Unable to drop table " + dbNameTableName, e);
        caughtException = e;
        // Keep going so we get everything shutdown, finally close it at the end
      }
    }
    if (caughtException != null) throw new RuntimeException(caughtException);

  }

  @Override
  public QueryPlan explain(String sql) {
    getDriver();
    if (driver.get().compile(sql) != 0) {
      Assert.fail("Failed to compile <" + sql + ">");
    }
    return driver.get().getPlan();
  }

  @Override
  public String getMetastoreUri() {
    return null;
  }
}
