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
import org.apache.hadoop.hive.conf.HiveConf;
import org.junit.Assert;

/**
 * Configuration for integration tests.  Configuration is done as a set of system properties.
 */
public class TestConf {

  /**
   * Property that controls how Hive is accessed in the tests.
   */
  public static final String ACCESS_PROPERTY = "hive.test.capybara.access";

  /**
   * Value to set to access Hive via the CLI.
   */
  public static final String ACCESS_CLI = "cli";

  /**
   * Value to set to access Hive via JDBC
   */
  public static final String ACCESS_JDBC = "jdbc";

  /**
   * Default access method for the tests.
   */
  public static final String ACCESS_DEFAULT = ACCESS_CLI;

  /**
   * Determine whether this test is being executed via the CLI or JDBC
   * @return cli or jdbc, depending on how this is being executed.
   */
  public static String access() {
    return System.getProperty(ACCESS_PROPERTY, ACCESS_DEFAULT).toLowerCase();
  }

  @VisibleForTesting static void setAccess(String access) {
    System.setProperty(ACCESS_PROPERTY, access);
  }

  /**
   * Property that controls which engine is used to execute Hive queries.
   */
  public static final String ENGINE_PROPERTY = "hive.test.capybara.engine";

  /**
   * Value to set to execute Hive queries using Tez. (Currently local tests fail when set.)
   */
  public static final String ENGINE_TEZ = "tez";

  /**
   * Value to set to get the default engine from Hive (currently MapReduce).
   */
  public static final String ENGINE_UNSPECIFIED = "default";

  /**
   * Default engine.
   */
  public static final String ENGINE_DEFAULT = ENGINE_UNSPECIFIED;

  /**
   * Determine execution engine for this test
   * @return default, tez or spark
   */
  public static String engine() {
    return System.getProperty(ENGINE_PROPERTY, ENGINE_DEFAULT).toLowerCase();
  }

  @VisibleForTesting
  static void setEngine(String engine) {
    System.setProperty(ENGINE_PROPERTY, engine);
  }

  /**
   * Property to set to control how many tasks Tez runs.  This only controls Tez in the local
   * (mini-cluster).  Tez on the cluster will be controlled by the configuration of the cluster.
   */
  public static final String TEZ_NUM_TASKS_PROPERTY = "hive.test.capybara.tez.num.tasks";

  /**
   * Default number of Tez tasks when run in the minicluster.
   */
  public static final String TEZ_NUM_TASKS_DEFAULT = "2";

  /**
   * Determine number of tasks to start in MiniTezCluster
   * @return numer of tasks
   */
  static int numTezTasks() {
    return Integer.valueOf(System.getProperty(TEZ_NUM_TASKS_PROPERTY, TEZ_NUM_TASKS_DEFAULT));
  }

  /**
   * Property to set to control which file format Hive uses by default.
   */
  public static final String FILE_FORMAT_PROPERTY = "hive.test.capybara.file.format";

  /**
   * Value to set to use ORC as the default file format.
   */
  public static final String FILE_FORMAT_ORC = "ORC";

  /**
   * Value to set to use Sequence as the default file format.
   */
  public static final String FILE_FORMAT_SEQUENCE = "SequenceFile";

  /**
   * Value to set to use Text as the default file format.
   */
  public static final String FILE_FORMAT_TEXT = "TextFile";

  /**
   * Value to set to use RCFile as the default file format.
   */
  public static final String FILE_FORMAT_RCFILE = "RCfile";

  /**
   * Default value to use for file format.
   */
  public static final String FILE_FORMAT_DEFAULT = FILE_FORMAT_ORC;

  /**
   * Determine default storage format for this test
   * @return orc, parquet, text, rcfile
   */
  public static String fileFormat() {
    String format = System.getProperty(FILE_FORMAT_PROPERTY, FILE_FORMAT_DEFAULT);
    // So Validator.validate returns null if everything is good, or error string if there's an
    // issue.  Wow, that's the weirdest interface I've seen in a while.
    Assert.assertNull(HiveConf.ConfVars.HIVEDEFAULTFILEFORMAT.getValidator().validate(format));
    return format;
  }

  /**
   * Property to set to control whether these tests use a secure cluster.
   */
  public static final String SECURITY_PROPERTY = "hive.test.capybara.security";

  /**
   * Value for running with security off.
   */
  public static final String SECURITY_NONSECURE = "nonsecure";

  /**
   * Default setting for security.
   */
  public static final String SECURITY_DEFAULT = SECURITY_NONSECURE;

  /**
   * Determine whether this test is being executed in secure or non-secure mode
   * @return secure or nonsecure
   */
  public static String security() {
    return System.getProperty(SECURITY_PROPERTY, SECURITY_DEFAULT).toLowerCase();
  }

  /**
   * Property to set to control which metastore implementation Hive uses.  This will only affect
   * the minicluster setup, as the metastore on a real cluster will be controlled by the cluster.
   */
  public static final String METASTORE_PROPERTY = "hive.test.capybara.metastore";

  /**
   * Value for running with a RDBMS metastore.
   */
  public static final String METASTORE_RDBMS = "rdbms";

  /**
   * Default value for the metastore.
   */
  public static final String METASTORE_DEFAULT = METASTORE_RDBMS;

  /**
   * Determine which metastore implementation to use.
   * @return rdbms or hbase.
   */
  public static String metastore() {
    return System.getProperty(METASTORE_PROPERTY, METASTORE_DEFAULT).toLowerCase();
  }

  /**
   * Property to set to control whether tests run locally in a minicluster or on a real cluster.
   * The values are boolean.  If set to true, you must also provide values for HADOOP_HOME and
   * HIVE_HOME via system properties (e.g. add -DHADOOP_HOME=/cluster/test/hadoop to your command
   * line).
   */
  public static final String USE_CLUSTER_PROPERTY ="hive.test.capybara.use.cluster";

  /**
   * Default value for running on a cluster.
   */
  public static final String USE_CLUSTER_DEFAULT = "false";

  /**
   * Determine whether the tests should run on a cluster.
   * @return true if they should run on a cluster.
   */
  public static boolean onCluster() {
    return Boolean.valueOf(System.getProperty(USE_CLUSTER_PROPERTY, USE_CLUSTER_DEFAULT));
  }

  /**
   * Property to set to control the scale the tests run at.  The unit of scale is a kilobyte.
   */
  public static final String SCALE_PROPERTY = "hive.test.capybara.scale";

  /**
   * Default value for the scale.  This is set for the local case and should definitely be set
   * higher if being run on a cluster.
   */
  public static final String SCALE_DEFAULT = "1";

  public static int getScale() {
    return Integer.valueOf(System.getProperty(SCALE_PROPERTY, SCALE_DEFAULT));
  }

  /**
   * Property to set to control when generated data is spilled to disk.  In bytes.
   */
  public static final String SPILL_SIZE_PROPERTY =  "hive.test.capybara.data.spill.size";

  /**
   * Default spill size.
   */
  public static final String SPILL_SIZE_DEFAULT = Integer.toString(1024 * 1024 * 256);

  public static int getSpillSize() {
    // Keep in mind that twice the spill size may be in memory at a time as it will be spilling
    // one batch while it is generating the next.
    return Integer.valueOf(System.getProperty(SPILL_SIZE_PROPERTY, SPILL_SIZE_DEFAULT));
  }

  /**
   * Property to set to control when data is generated on the cluster instead of on the local
   * machine.  This only applies when running tests on the cluster.
   */
  public static final String CLUSTERGEN_SIZE_PROPERTY =  "hive.test.capybara.data.clustergen.threshold";

  /**
   * Default size to switch to generating data on the cluster.  In bytes.
   */
  public static final String CLUSTERGEN_SIZE_DEFAULT = Integer.toString(1024 * 1024 * 1024);

  public static int getClusterGenThreshold() {
    return Integer.valueOf(System.getProperty(CLUSTERGEN_SIZE_PROPERTY, CLUSTERGEN_SIZE_DEFAULT));
  }
}
