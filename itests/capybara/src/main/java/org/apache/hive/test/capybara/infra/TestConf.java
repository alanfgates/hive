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

import com.google.common.annotations.VisibleForTesting;
import org.apache.hive.test.capybara.iface.ClusterManager;
import org.apache.hive.test.capybara.iface.DataStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Configuration for integration tests.  Configuration is done as a set of system properties.
 */
public class TestConf {

  // TODO Need a way to set a config var for all tests
  static final private Logger LOG = LoggerFactory.getLogger(TestConf.class);

  private static Properties defaults;

  private static final String CONF_FILE_PROPERTY = "capy.conf.file";
  private static final String CONF_FILE_DEFAULT = "./capy.properties";

  // Key names
  private static final String CLUSTER_GEN_THRESHOLD = "table_comparator.cluster_gen_threshold";
  private static final String FILE_FORMAT = "file_format";
  private static final String SCALE = "scale";
  @VisibleForTesting static final String SPILL_SIZE = "data_gen.spill_size";

  // Cluster type names
  public static final String BENCH_CLUSTER = "bench";
  public static final String TEST_CLUSTER = "test";

  // Key names inside a cluster
  @VisibleForTesting static final String CLUSTER_ACCESS = ".access";
  @VisibleForTesting static final String CLUSTER_CLUSTER_MANAGER = ".manager";
  private static final String CLUSTER_ENGINE = ".engine";
  static final String CLUSTER_HADOOP_CONF_DIR = ".hadoop.conf.dir";
  static final String CLUSTER_HIVE_HOME = ".hive.home";
  private static final String CLUSTER_JDBC_HOST = ".jdbc.host";
  private static final String CLUSTER_JDBC_DB = ".jdbc.db";
  private static final String CLUSTER_JDBC_PASSWD = ".jdbc.password";
  private static final String CLUSTER_JDBC_PORT = ".jdbc.port";
  private static final String CLUSTER_JDBC_USER = ".jdbc.user";
  private static final String CLUSTER_METASTORE = ".metastore";
  private static final String CLUSTER_NUM_TEZ_TASKS = ".tez.num_tasks";
  private static final String CLUSTER_SECURE = ".secure";
  private static final String CLUSTER_STORE = ".store";

  // Values
  static final String ACCESS_CLI = "cli";
  static final String ACCESS_JDBC = "jdbc";
  static final String ENGINE_MR = "mr";
  static final String ENGINE_SPARK = "spark";
  static final String ENGINE_TEZ = "tez";
  static final String FILE_FORMAT_ORC = "orc";
  static final String FILE_FORMAT_PARQUET = "parquet";
  static final String FILE_FORMAT_RC = "rc";
  static final String FILE_FORMAT_TEXT = "text";
  static final String METASTORE_HBASE = "hbase";
  static final String METASTORE_RDBMS = "rdbms";

  static {
    defaults = new Properties();
    defaults.setProperty(CLUSTER_GEN_THRESHOLD, Integer.toString(1024 * 1024 * 1024));
    defaults.setProperty(FILE_FORMAT, FILE_FORMAT_TEXT);
    defaults.setProperty(SCALE, "1");
    defaults.setProperty(SPILL_SIZE, Integer.toString(1024 * 1024 * 256));

    defaults.setProperty(BENCH_CLUSTER + CLUSTER_CLUSTER_MANAGER, DbClusterManager.class.getName());
    defaults.setProperty(BENCH_CLUSTER + CLUSTER_STORE, DerbyStore.class.getName());
    defaults.setProperty(TEST_CLUSTER + CLUSTER_CLUSTER_MANAGER, MiniClusterManager.class.getName());
    defaults.setProperty(TEST_CLUSTER + CLUSTER_STORE, MiniCliHiveStore.class.getName());
    for (String clusterType : new String[]{BENCH_CLUSTER, TEST_CLUSTER}) {
      defaults.setProperty(clusterType + CLUSTER_ACCESS, ACCESS_CLI);
      defaults.setProperty(clusterType + CLUSTER_ENGINE, ENGINE_MR);
      defaults.setProperty(clusterType + CLUSTER_JDBC_HOST, "localhost");
      defaults.setProperty(clusterType + CLUSTER_JDBC_USER, System.getProperty("user.name"));
      defaults.setProperty(clusterType + CLUSTER_METASTORE, METASTORE_RDBMS);
      defaults.setProperty(clusterType + CLUSTER_NUM_TEZ_TASKS, "2");
      defaults.setProperty(clusterType + CLUSTER_SECURE, "false");
    }
  }

  private final Properties properties;
  private final ClusterConf testClusterConf;
  private final ClusterConf benchClusterConf;
  private ClusterManager testCluster;
  private ClusterManager benchCluster;

  TestConf() {

    properties = new Properties(defaults);

    // Find and read the input file if there is one
    String confFile = System.getProperty(CONF_FILE_PROPERTY);
    InputStream in = null;
    if (confFile == null) {
      // See if we can find the default
      try {
        in = new FileInputStream(CONF_FILE_DEFAULT);
      } catch (FileNotFoundException e) {
        LOG.info("No configuration file specified, and config file not found in default location "
          + CONF_FILE_DEFAULT + ", using all default values");
      }
    } else {
      try {
        in = new FileInputStream(confFile);
      } catch (FileNotFoundException e) {
        LOG.error("Unable to find config file " + confFile + ", giving up");
        throw new RuntimeException("Unable to find config file " + confFile);
      }
    }
    try {
      if (in != null) properties.load(in);
    } catch (IOException e) {
      LOG.error("Failed to read properties file", e);
      throw new RuntimeException(e);
    }

    testClusterConf = new ClusterConfImpl(TEST_CLUSTER);
    benchClusterConf = new ClusterConfImpl(BENCH_CLUSTER);
  }


  /**
   * Get an instance of the class for our benchmark
   * @return benchmark clusterk
   */
  public ClusterManager getBenchCluster() {
    if (benchCluster == null) benchCluster = instantiateCluster(BENCH_CLUSTER + CLUSTER_CLUSTER_MANAGER);
    return benchCluster;
  }

  public ClusterConf getBenchClusterConf() {
    return benchClusterConf;
  }

  /**
   * Get the threshold at which data should be generated in the cluster rather than locally, in
   * bytes.
   * @return threshold
   */
  public int getClusterGenThreshold() {
    return Integer.valueOf(properties.getProperty(CLUSTER_GEN_THRESHOLD));
  }

  /**
   * Get File format to use in Hive for this test.
   * @return file format
   */
  public String getFileFormat() {
    return properties.getProperty(FILE_FORMAT);
  }

  /**
   * Scale to run the test at.  Units are megabytes.
   * @return scale
   */
  public int getScale() {
    return Integer.valueOf(properties.getProperty(SCALE));
  }

  /**
   * Threshold at which data will be spilled to disk during data generation.  In bytes.
   * @return spill threshold
   */
  public long getSpillSize() {
    return Long.valueOf(properties.getProperty(SPILL_SIZE));
  }

  /**
   * Get an instance of the class for our object under test
   * @return test cluster
   */
  public ClusterManager getTestCluster() {
    if (testCluster == null) testCluster = instantiateCluster(TEST_CLUSTER + CLUSTER_CLUSTER_MANAGER);
    return testCluster;
  }

  public ClusterConf getTestClusterConf() {
    return testClusterConf;
  }

  @VisibleForTesting
  Properties getProperties() {
    return properties;
  }

  private ClusterManager instantiateCluster(String propertyKey) {
    try {
      Class clusterMgrClass = Class.forName(properties.getProperty(propertyKey));
      return (ClusterManager) clusterMgrClass.newInstance();
    } catch (ClassNotFoundException|IllegalAccessException|InstantiationException e) {
      throw new RuntimeException(e);
    }
  }

  private class ClusterConfImpl implements ClusterConf {
    private final String clusterType;

    public ClusterConfImpl(String clusterType) {
      this.clusterType = clusterType;
    }

    @Override
    public String getAccess() {
      return properties.getProperty(clusterType + CLUSTER_ACCESS);

    }

    @Override
    public DataStore getDataStore() {
      try {
        Class dataStoreClass = Class.forName(properties.getProperty(clusterType + CLUSTER_STORE));
        return (DataStore)dataStoreClass.newInstance();
      } catch (ClassNotFoundException|IllegalAccessException|InstantiationException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public String getEngine() {
      return properties.getProperty(clusterType + CLUSTER_ENGINE);
    }

    @Override
    public String getHadoopConfDir() {
      return properties.getProperty(clusterType + CLUSTER_HADOOP_CONF_DIR);
    }

    @Override
    public String getHiveHome() {
      return properties.getProperty(clusterType + CLUSTER_HIVE_HOME);
    }

    @Override
    public String getJdbcDatabase() {
      return properties.getProperty(clusterType + CLUSTER_JDBC_DB);
    }

    @Override
    public String getJdbcHost() {
      return properties.getProperty(clusterType + CLUSTER_JDBC_HOST);
    }

    @Override
    public String getJdbcPasswd() {
      return properties.getProperty(clusterType + CLUSTER_JDBC_PASSWD);
    }

    @Override
    public String getJdbcPort() {
      String port = properties.getProperty(clusterType + CLUSTER_JDBC_PORT);
      if (port != null) return ":" + port;
      else return null;
    }

    @Override
    public String getJdbcUser() {
      return properties.getProperty(clusterType + CLUSTER_JDBC_USER);
    }

    @Override
    public String getMetastore() {
      return properties.getProperty(clusterType + CLUSTER_METASTORE);
    }

    @Override
    public int getNumTezTasks() {
      return Integer.valueOf(properties.getProperty(clusterType + CLUSTER_NUM_TEZ_TASKS));
    }

    @Override
    public boolean isSecure() {
      return properties.getProperty(clusterType + CLUSTER_SECURE).equalsIgnoreCase("true");
    }
  }

}
