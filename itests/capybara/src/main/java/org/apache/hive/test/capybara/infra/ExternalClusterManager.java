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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.test.capybara.iface.DataStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manage external clusters.
 */
public class ExternalClusterManager extends ClusterManagerBase {
  static final private Logger LOG = LoggerFactory.getLogger(ExternalClusterManager.class);

  static final private String[] hadoopConfigFiles = {"core-site.xml", "hdfs-site.xml",
                                                     "mapred-site.xml", "yarn-site.xml"};
  static final private String hiveConfigFile = "hive-site.xml";

  @Override
  public boolean remote() {
    return true;
  }

  @Override
  public DataStore getStore() {
    if (store == null) {
      String access = getClusterConf().getAccess();
      if (access.equals(TestConf.ACCESS_CLI)) store = new ClusterCliHiveStore();
      else if (access.equals(TestConf.ACCESS_JDBC)) store = new ClusterJdbcHiveStore();
      else throw new RuntimeException("Unknown access method " + access);
    }
    return store;
  }

  @Override
  public HiveConf getHiveConf() {
    if (conf == null) {
      ClusterConf cc = getClusterConf();
      String hadoopHome = cc.getHadoopHome();
      if (hadoopHome == null) {
        throw new RuntimeException("You must define the property " + clusterType +
            TestConf.CLUSTER_HADOOP_HOME + " to run on a cluster");
      }
      String hiveHome = cc.getHiveHome();
      if (hiveHome == null) {
        throw new RuntimeException("You must define the property " + clusterType +
            TestConf.CLUSTER_HIVE_HOME + " to run on a cluster");
      }

      String hadoopConf = hadoopHome + System.getProperty("file.separator") + "conf"
          + System.getProperty("file.separator");
      // Build a configuration that doesn't read the default resources.
      Configuration base = new Configuration(false);
      HiveConf conf = new HiveConf(base, HiveConf.class);
      for (String hadoopConfigFile : hadoopConfigFiles) {
        Path p = new Path(hadoopConf + hadoopConfigFile);
        conf.addResource(p);
      }

      String hiveConf = hiveHome +  System.getProperty("file.separator") + "conf"
          + System.getProperty("file.separator") + hiveConfigFile;
      conf.addResource(new Path(hiveConf));
    }
    return conf;
  }
}
