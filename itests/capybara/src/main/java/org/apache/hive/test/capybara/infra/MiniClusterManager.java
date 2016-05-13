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
import org.apache.hadoop.crypto.key.KeyProviderCryptoExtension;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.WindowsPathUtil;
import org.apache.hadoop.hive.shims.Utils;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hive.jdbc.miniHS2.MiniHS2;
import org.apache.hive.test.capybara.iface.ClusterManager;
import org.apache.tez.test.MiniTezCluster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Manager for mini-clusters
 */
public class MiniClusterManager implements ClusterManager {
  static final private Logger LOG = LoggerFactory.getLogger(MiniClusterManager.class.getName());

  private static final String DFS_DIR = "minidfs";
  private static final String TEZ_DIR = "apps_staging_dir";
  private Configuration conf;
  private MiniDFSCluster dfs;
  private MiniTezCluster tez;
  private HiveStore hive;
  private MiniHS2 hs2;
  private List<String> registeredTables = new ArrayList<>();
  private Map<String, String> confVars = new HashMap<>();

  @Override
  public void setup() throws IOException {
    File base = new File(baseDir() + DFS_DIR).getAbsoluteFile();

    FileUtil.fullyDelete(base);
    conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, base.getAbsolutePath());
    // If we are running with security off, we need to turn off permissions in HDFS as it messes
    // up MiniHS2
    // TODO - not clear if I need this.
    /*
    if (TestConf.security().equals(TestConf.SECURITY_NONSECURE)) {
      conf.setBoolean("dfs.permissions.enabled", false);
    }
    */

    String user;
    try {
      user = Utils.getUGI().getShortUserName();
    } catch (Exception e) {
      String msg = "Cannot obtain username: " + e;
      throw new IllegalStateException(msg, e);
    }
    conf.set("hadoop.proxyuser." + user + ".groups", "*");
    conf.set("hadoop.proxyuser." + user + ".hosts", "*");

    dfs = new MiniDFSCluster.Builder(conf).numDataNodes(4).format(true).build();

    // Need to set the client's KeyProvider to the NN's for JKS,
    // else the updates do not get flushed properly
    KeyProviderCryptoExtension keyProvider =  dfs.getNameNode().getNamesystem().getProvider();
    if (keyProvider != null) {
      dfs.getFileSystem().getClient().setKeyProvider(keyProvider);
    }

    String nameNodeUri = WindowsPathUtil.getHdfsUriString(getFileSystem().getUri().toString());
    Path warehouseDir = new Path(nameNodeUri,"/user/hive/warehouse");
    dfs.getFileSystem().mkdirs(warehouseDir);

    // Now, set the location for the metastore so it doesn't end up in the goofy pfile stuff
    LOG.debug("Setting warhouse dir to " + warehouseDir.toUri());
    HiveConf.setVar(conf, HiveConf.ConfVars.METASTOREWAREHOUSE, warehouseDir.toUri().toString());
    HiveConf.setBoolVar(conf, HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY, false);
    // Don't think I need to remember the URI, but keeping this here in case I do.
    // dfsURI = "hdfs://localhost:"+ dfs.getNameNodePort();

    // if this is running Tez, construct Tez mini-cluster
    if (TestConf.engine().equals(TestConf.ENGINE_TEZ)) {
      tez = new MiniTezCluster("hive", TestConf.numTezTasks());
      conf.set("mapred.tez.java.opts","-Xmx128m");
      conf.setInt("hive.tez.container.size", 128);
      conf.setBoolean("hive.merge.tezfiles", false);
      conf.set("hive.tez.java.opts", "-Dlog4j.configurationFile=tez-container-log4j2.xml -Dtez.container.log.level=INFO -Dtez.container.root.logger=CLA");
      conf.set("tez.am.launch.cmd-opts", "-Dlog4j.configurationFile=tez-container-log4j2.xml -Dtez.container.log.level=INFO -Dtez.container.root.logger=CLA");
      conf.set("hive.tez.input.format", "org.apache.hadoop.hive.ql.io.HiveInputFormat");
      conf.set("fs.defaultFS", nameNodeUri);
      conf.set("tez.am.log.level", "DEBUG");
      conf.set(MRJobConfig.MR_AM_STAGING_DIR, baseDir() + TEZ_DIR);
      HiveConf.setVar(conf, HiveConf.ConfVars.HIVE_EXECUTION_ENGINE, "tez");

      LOG.debug("Starting mini tez cluster");
      tez.init(conf);
      tez.start();
      LOG.debug("Starting complete");

      Configuration config = tez.getConfig();
      for (Map.Entry<String, String> pair: config) {
      	conf.set(pair.getKey(), pair.getValue());
      }

      Path jarPath = new Path("hdfs:///user/hive");
      Path hdfsPath = new Path("hdfs:///user/");

      try {
      	FileSystem fs = getFileSystem();
      	jarPath = fs.makeQualified(jarPath);
      	conf.set("hive.jar.directory", jarPath.toString());
      	fs.mkdirs(jarPath);
      	hdfsPath = fs.makeQualified(hdfsPath);
      	conf.set("hive.user.install.directory", hdfsPath.toString());
      	fs.mkdirs(hdfsPath);
      } catch (Exception e) {
      	LOG.error("failed setup: ", e);
      }

      conf.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, nameNodeUri);
    } else if (TestConf.engine().equals(TestConf.ENGINE_UNSPECIFIED)) {
      // ok, hope you know what you're doing
    } else {
      throw new RuntimeException("Unknown engine: " + TestConf.engine());
    }

    if (TestConf.access().equals(TestConf.ACCESS_JDBC)) {
      /*
      try {
        hs2 = new MiniHS2((HiveConf)conf, dfs.getFileSystem());
        hs2.start(Collections.<String, String>emptyMap());
      } catch (Exception e) {
        throw new IOException(e);
      }
      */
    }

    // TODO if this is running Hbase metastore, construct HBase mini-cluster
    // TODO if this is running secure, construct mini kdc
  }

  @Override
  public void tearDown() {
    Exception caughtException = null;
    for (String dbNameTableName : registeredTables) {
      try {
        getHive().fetchData("drop table " + dbNameTableName);
      } catch (SQLException | IOException e) {
        LOG.error("Unable to drop table " + dbNameTableName, e);
        caughtException = e;
        // Keep going so we get everything shutdown, finally close it at the end
      }
    }

    // tear down any mini-clusters we've constructed.
    if (dfs != null) dfs.shutdown();
    if (tez != null) tez.stop();
    if (hs2 != null) hs2.stop();
    if (caughtException != null) throw new RuntimeException(caughtException);
  }

  @Override
  public void beforeTest() throws IOException {

  }

  @Override
  public void afterTest() throws IOException {
    if (tez != null) tez.close();
    hive = null;
  }

  @Override
  public boolean remote() {
    return false;
  }

  @Override
  public FileSystem getFileSystem() throws IOException {
    return dfs.getFileSystem();
  }

  @Override
  public HiveStore getHive() {
    assert conf != null;
    if (hive == null) {
      String access = TestConf.access();
      if (access.equals(TestConf.ACCESS_CLI)) {
        hive = new MiniCliHiveStore(this);
      } else if (access.equals(TestConf.ACCESS_JDBC)) {
        hive = new MiniHS2HiveStore(this);
      } else {
        throw new RuntimeException("Unknown access method " + access);
      }
      hive.setConf(conf);
    }
    return hive;
  }

  @Override
  public String getJdbcURL() {
    if (hs2 == null) {
      throw new RuntimeException("No in JDBC mode!");
    }
    return null; //hs2.getJdbcURL();
  }

  @Override
  public void registerTable(String dbName, String tableName) {
    StringBuilder builder = new StringBuilder();
    if (dbName != null) {
      builder.append(dbName)
          .append('.');
    }
    builder.append(tableName);
    registeredTables.add(builder.toString());
  }

  @Override
  public void setConfVar(String var, String val) {
    confVars.put(var, val);
    conf.set(var, val);
  }

  @Override
  public Map<String, String> getConfVars() {
    return confVars;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;

  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  private String baseDir() {
    return System.getProperty("java.io.tmpdir") + System.getProperty("file.separator");
  }
}
