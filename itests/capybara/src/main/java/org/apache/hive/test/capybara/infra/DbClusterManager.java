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

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.test.capybara.iface.DataStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A cluster manager for handling "clusters" that are really a database
 */
public class DbClusterManager extends ClusterManagerBase {
  static final private Logger LOG = LoggerFactory.getLogger(ExternalClusterManager.class);

  private HiveConf conf;

  @Override
  public boolean remote() {
    return false;
  }

  @Override
  public DataStore getStore() {
    if (store == null) store = getClusterConf().getDataStore();
    return store;
  }

  @Override
  public HiveConf getHiveConf() {
    if (conf == null) conf = new HiveConf();
    return conf;
  }
}
