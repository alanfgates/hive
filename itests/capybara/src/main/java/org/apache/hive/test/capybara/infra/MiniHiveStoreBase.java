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
import org.apache.hive.test.capybara.iface.ClusterManager;
import org.junit.Assert;

abstract class MiniHiveStoreBase extends HiveStore {
  private ThreadLocal<Driver> driver;

  public MiniHiveStoreBase(ClusterManager clusterManager) {
    super(clusterManager);
  }

  protected Driver getDriver() {
    if (driver == null) {
      driver = new ThreadLocal<Driver>() {
        @Override
        protected Driver initialValue() {
          assert conf != null;
          // Make sure our conf file gets set
          SessionState.start(conf);
          assert SessionState.get() != null;
          return new Driver(conf);
        }
      };
    }
    return driver.get();
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
