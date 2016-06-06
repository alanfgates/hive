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

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.QueryPlan;
import org.apache.hive.test.capybara.data.FetchResult;
import org.apache.hive.test.capybara.data.ResultCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * An implementation of HiveStore that works on external clusters using the CLI.
 */
class ClusterCliHiveStore extends HiveStore {
  static final private Logger LOG = LoggerFactory.getLogger(ClusterCliHiveStore.class.getName());
  Map<String, String> envMap;

  /**
   * No-args constructor for use on the cluster.
   */
  ClusterCliHiveStore() {
    envMap = new HashMap<>();
    getEnv("HADOOP_HOME");
    getEnv("HIVE_HOME");
    envMap.put("JAVA_HOME", System.getProperty("java.home"));
  }

  @Override
  public FetchResult executeSql(String sql) throws SQLException, IOException {
    List<String> cmd = new ArrayList<>();
    cmd.add(new StringBuilder(envMap.get("HIVE_HOME"))
        .append(System.getProperty("file.separator"))
        .append("bin")
        .append(System.getProperty("file.separator"))
        .append("hive")
        .toString());
    // Add in the configuration values
    for (Map.Entry<String, String> entry : clusterManager.getConfVars().entrySet()) {
      cmd.add("--hiveconf");
      cmd.add(new StringBuilder(entry.getKey())
          .append('=')
          .append(entry.getValue())
          .toString());
    }
    cmd.add("-e");
    cmd.add(sql);

    String[] env = new String[envMap.size()];
    int i = 0;
    for (Map.Entry<String, String> entry : envMap.entrySet()) {
      env[i++] = new StringBuilder(entry.getKey())
          .append("=")
          .append(entry.getValue())
          .toString();
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Going to exec: " + StringUtils.join(cmd, " ") + " with env " +
          StringUtils.join(env, " "));
    }
    Process proc = Runtime.getRuntime().exec(cmd.toArray(new String[cmd.size()]), env);
    List<String> stdoutLines = new ArrayList<>();
    List<String> stderrLines = new ArrayList<>();
    Thread stdout = new Thread(getStreamPumper(new BufferedReader(new InputStreamReader(
        proc.getInputStream())), stdoutLines));
    Thread stderr = new Thread(getStreamPumper(new BufferedReader(new InputStreamReader(
        proc.getErrorStream())), stderrLines));
    stdout.start();
    stderr.start();
    try {
      proc.waitFor();
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
    int rc = proc.exitValue();
    if (rc != 0) {
      LOG.warn("Got non-zero return code from Hive " + rc + ", stderr is <" +
          StringUtils.join(stderrLines, "\n") + ">");
      return new FetchResult(ResultCode.NON_RETRIABLE_FAILURE);
    }
    LOG.debug("stderr from Hive invocation: " + StringUtils.join(stderrLines, "\n"));
    if (stdoutLines.size() > 0) {
      return new FetchResult(new StringDataSet(stdoutLines, "\t", "NULL"));
    } else {
      return new FetchResult(ResultCode.SUCCESS);
    }
  }

  @Override
  public QueryPlan explain(String sql) {
    return null;
  }

  @Override
  public Connection getJdbcConnection(boolean autoCommit) throws SQLException {
    return null;
  }

  @Override
  public Class<? extends Driver> getJdbcDriverClass() {
    return null;
  }

  @Override
  protected String connectionURL() {
    return null;
  }

  @Override
  protected Properties connectionProperties() {
    return null;
  }

  @Override
  public String getMetastoreUri() {
    return clusterManager.getHiveConf().getVar(HiveConf.ConfVars.METASTOREURIS);
  }

  private void getEnv(String var) {
    String val = System.getProperty(var);
    if (val == null || val.equals("")) {
      throw new RuntimeException("You must set " + var + " in your properties before using " +
          "Hive on a cluster");
    }
    envMap.put(var, val);
  }

  private Runnable getStreamPumper(final BufferedReader reader, final List<String> lines) {
    return new Runnable() {
      @Override
      public void run() {
        try {
          String line;
          while ((line = reader.readLine()) != null) lines.add(line);
        } catch (IOException e) {
          LOG.error("Stream Pumper thread caught IOException ", e);
          throw new RuntimeException(e);
        }

      }
    };
  }
}
