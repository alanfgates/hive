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
package org.apache.hadoop.hive.metastore;

import com.google.common.annotations.VisibleForTesting;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.metastore.api.SQLForeignKey;
import org.apache.hadoop.hive.metastore.api.SQLPrimaryKey;
import org.apache.hadoop.hive.metastore.hbase.HBaseStore;
import org.apache.hadoop.hive.metastore.sqlbin.PostgresStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.Function;
import org.apache.hadoop.hive.metastore.api.Index;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Role;
import org.apache.hadoop.hive.metastore.api.Table;

import java.io.File;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * A tool to take the contents of a Hive metastore and import it into an another one.  The target
 * is copying from ObjectStore based to HBaseStore or to PostgresStore based, though nothing in
 * the implementation depends on the two being of different types.  To use this the config files
 * for the source and destination stores must be in different directories.
 *
 * This tool can import an entire metastore or only selected objects.  When selecting objects it
 * is necessary to fully specify the object's name.  For example, if you want to import the table
 * T in the default database it needs to be identified as default.T.  The same is true for
 * functions.  When an object is specified, everything under that object will be imported (e.g.
 * if you select database D, then all tables and functions in that database will be
 * imported as well).
 *
 * At this point only tables and partitions are handled in parallel as it is assumed there are
 * relatively few of everything else.
 *
 * Note that whatever installation is required on the target must have already been done.  This
 * tool does not manage installation.
 */
public class DBCopy {

  static final private Logger LOG = LoggerFactory.getLogger(DBCopy.class.getName());

  public static int main(String[] args) {
    try {
      DBCopy tool = new DBCopy();
      int rv = tool.init(args);
      if (rv != 0) return rv;
      tool.run();
    } catch (Exception e) {
      System.err.println("Caught exception " + e.getClass().getName() + " with message <" +
          e.getMessage() + ">");
      return 1;
    }
    return 0;
  }

  private ThreadLocal<RawStore> srcStore;
  private ThreadLocal<RawStore> targetStore;

  private ThreadLocal<RawStore> getObjectStore(final HiveConf conf) {
    return new ThreadLocal<RawStore>() {
      @Override
      protected RawStore initialValue() {
        RawStore os = new ObjectStore();
        os.setConf(conf);
        return os;
      }
    };
  }

  private ThreadLocal<RawStore> getHBaseStore(final HiveConf conf) {
    return new ThreadLocal<RawStore>() {
      @Override
      protected RawStore initialValue() {
        RawStore hs = new HBaseStore();
        hs.setConf(conf);
        return hs;
      }
    };
  }

  private ThreadLocal<RawStore> getPostgresStore(final HiveConf conf) {
    return new ThreadLocal<RawStore>() {
      @Override
      protected RawStore initialValue() {
        RawStore hs = new PostgresStore();
        hs.setConf(conf);
        return hs;
      }
    };
  }

  private List<Database> dbs;
  private BlockingQueue<Table> partitionedTables;
  private BlockingQueue<String[]> tableNameQueue;
  private BlockingQueue<String[]> indexNameQueue;
  private BlockingQueue<PartQueueEntry> partQueue;
  private boolean writingToQueue, readersFinished;
  private boolean doKerberos, doAll;
  private List<String> rolesToImport, dbsToImport, tablesToImport, functionsToImport;
  private int parallel;
  private int batchSize;
  private boolean testing = false;

  private DBCopy() {}

  @VisibleForTesting
  public DBCopy(String... args) throws ParseException, MalformedURLException {
    testing = true;
    init(args);
  }

  private int init(String... args) throws ParseException, MalformedURLException {
    Options options = new Options();

    doAll = doKerberos = false;
    parallel = 1;
    batchSize = 1000;

    options.addOption(OptionBuilder
        .withLongOpt("all")
        .withDescription("Import the full metastore")
        .create('a'));

    options.addOption(OptionBuilder
        .withLongOpt("batchsize")
        .withDescription("Number of partitions to read and write in a batch, defaults to 1000")
            .hasArg()
            .create('b'));

    options.addOption(OptionBuilder
        .withLongOpt("source-config")
        .withDescription("Config file for source.")
        .hasArg()
        .create('c'));

    options.addOption(OptionBuilder
        .withLongOpt("target-config")
        .withDescription("Config file for target.")
        .hasArg()
        .create('C'));

    options.addOption(OptionBuilder
        .withLongOpt("database")
        .withDescription("Import a single database")
        .hasArgs()
        .create('d'));

    options.addOption(OptionBuilder
        .withLongOpt("help")
        .withDescription("You're looking at it")
        .create('h'));

    options.addOption(OptionBuilder
        .withLongOpt("function")
        .withDescription("Import a single function")
        .hasArgs()
        .create('f'));

    options.addOption(OptionBuilder
        .withLongOpt("kerberos")
        .withDescription("Import all kerberos related objects (master key, tokens)")
        .create('k'));

     options.addOption(OptionBuilder
        .withLongOpt("parallel")
        .withDescription("Parallel factor for loading (only applied to tables and partitions), " +
            "defaults to 1")
        .hasArg()
        .create('p'));

    options.addOption(OptionBuilder
        .withLongOpt("role")
        .withDescription("Import a single role")
        .hasArgs()
        .create('r'));

    options.addOption(OptionBuilder
        .withLongOpt("source-type")
        .withDescription("Type of the source database, valid options: object, hbase, postgres")
        .hasArg()
        .create('s'));

   options.addOption(OptionBuilder
        .withLongOpt("tables")
        .withDescription("Import a single tables")
        .hasArgs()
        .create('t'));

    options.addOption(OptionBuilder
        .withLongOpt("target-type")
        .withDescription("Type of the target database, valid options: object, hbase, postgres")
        .hasArg()
        .create('T'));

    CommandLine cli = new GnuParser().parse(options, args);

    // Make sure we have a source and target
    if (!testing &&
        (!cli.hasOption('c') || !cli.hasOption('C') || !cli.hasOption('s') || !cli.hasOption('T'))) {
      printHelp(options);
      return 1;
    }

    // Process help, if it was asked for, this must be done first
    if (cli.hasOption('h')) {
      printHelp(options);
      return 1;
    }

    boolean hasCmd = false;
    // Now process the other command line args
    if (cli.hasOption('a')) {
      hasCmd = true;
      doAll = true;
    }
    if (cli.hasOption('b')) {
      batchSize = Integer.parseInt(cli.getOptionValue('b'));
    }
    if (cli.hasOption('d')) {
      hasCmd = true;
      dbsToImport = Arrays.asList(cli.getOptionValues('d'));
    }
    if (cli.hasOption('f')) {
      hasCmd = true;
      functionsToImport = Arrays.asList(cli.getOptionValues('f'));
    }
    if (cli.hasOption('p')) {
      parallel = Integer.parseInt(cli.getOptionValue('p'));
    }
    if (cli.hasOption('r')) {
      hasCmd = true;
      rolesToImport = Arrays.asList(cli.getOptionValues('r'));
    }
    if (cli.hasOption('k')) {
      doKerberos = true;
    }
    if (cli.hasOption('t')) {
      hasCmd = true;
      tablesToImport = Arrays.asList(cli.getOptionValues('t'));
    }
    if (!hasCmd) {
      printHelp(options);
      return 1;
    }

    if (!testing) {
      srcStore = setupConnection(cli.getOptionValue('c'), cli.getOptionValue('s'));
      targetStore = setupConnection(cli.getOptionValue('C'), cli.getOptionValue('T'));
    }

    dbs = new ArrayList<>();
    // We don't want to bound the size of the table queue because we keep it all in memory
    partitionedTables = new LinkedBlockingQueue<>();
    tableNameQueue = new LinkedBlockingQueue<>();
    indexNameQueue = new LinkedBlockingQueue<>();

    // Bound the size of this queue so we don't get too much in memory.
    partQueue = new ArrayBlockingQueue<>(parallel * 2);
    return 0;
  }

  private void printHelp(Options options) {
    (new HelpFormatter()).printHelp("hbaseschematool", "", options,
        "Note that ANY values in your source and target configuration files that differ from " +
        "values in the hive-site.xml in your classpath MUST be explicitly specified.  Do not " +
        "count on default values as they will not override values from your classpath.");
  }

  private ThreadLocal<RawStore> setupConnection(String cfgFile, String storeType)
      throws MalformedURLException {
    LOG.info("Setting up connection for storeType " + storeType + " with conf " + cfgFile);
    File f = new File(cfgFile);
    HiveConf conf = new HiveConf();
    conf.addResource(f.toURI().toURL());
    if (storeType.equalsIgnoreCase("object")) {
      return getObjectStore(conf);
    } else if (storeType.equalsIgnoreCase("hbase")) {
      return getHBaseStore(conf);
    } else if (storeType.equalsIgnoreCase("postgres")) {
      return getPostgresStore(conf);
    } else {
      throw new RuntimeException("Unknown store type: " + storeType);
    }
  }

  @VisibleForTesting
  void run() throws MetaException, InstantiationException, IllegalAccessException,
      NoSuchObjectException, InvalidObjectException, InterruptedException {
    // Order here is crucial, as you can't add tables until you've added databases, etc.
    if (doAll || rolesToImport != null) {
      copyRoles();
    }
    if (doAll || dbsToImport != null) {
      copyDbs();
    }
    if (doAll || dbsToImport != null || tablesToImport != null) {
      copyTables();
      copyPartitions();
      copyIndexes();
    }
    if (doAll || dbsToImport != null || functionsToImport != null) {
      copyFunctions();
    }
    if (doAll || doKerberos) {
      copyKerberos();
    }
  }

  private void copyRoles() throws NoSuchObjectException, InvalidObjectException, MetaException {
    screen("Copying roles");
    List<String> toCopy = doAll ? srcStore.get().listRoleNames() : rolesToImport;
    for (String roleName : toCopy) {
      Role role = srcStore.get().getRole(roleName);
      screen("Copying role " + roleName);
      targetStore.get().addRole(roleName, role.getOwnerName());
    }
  }

  private void copyDbs() throws MetaException, NoSuchObjectException, InvalidObjectException {
    screen("Copying databases");
    List<String> toCopy = doAll ? srcStore.get().getAllDatabases() : dbsToImport;
    for (String dbName : toCopy) {
      Database db = srcStore.get().getDatabase(dbName);
      dbs.add(db);
      screen("Copying database " + dbName);
      targetStore.get().createDatabase(db);
    }
  }

  private void copyTables() throws MetaException, InvalidObjectException, InterruptedException {
    screen("Copying tables");

    // Start the parallel threads that will copy the tables
    Thread[] copiers = new Thread[parallel];
    writingToQueue = true;
    for (int i = 0; i < parallel; i++) {
      copiers[i] = new TableCopier();
      copiers[i].start();
    }

    // Put tables from the databases we copied into the queue
    for (Database db : dbs) {
      screen("Coyping tables in database " + db.getName());
      for (String tableName : srcStore.get().getAllTables(db.getName())) {
        tableNameQueue.put(new String[]{db.getName(), tableName});
      }
    }

    // Now put any specifically requested tables into the queue
    if (tablesToImport != null) {
      for (String compoundTableName : tablesToImport) {
        String[] tn = compoundTableName.split("\\.");
        if (tn.length != 2) {
          error(compoundTableName + " not in proper form.  Must be in form dbname.tablename.  " +
              "Ignoring this table and continuing.");
        } else {
          tableNameQueue.put(new String[]{tn[0], tn[1]});
        }
      }
    }
    writingToQueue = false;

    // Wait until we've finished adding all the tables
    for (Thread copier : copiers) copier.join();
 }

  private class TableCopier extends Thread {
    @Override
    public void run() {
      while (writingToQueue || tableNameQueue.size() > 0) {
        try {
          String[] name = tableNameQueue.poll(1, TimeUnit.SECONDS);
          if (name != null) {
            Table table = srcStore.get().getTable(name[0], name[1]);
            // If this has partitions, put it in the list to fetch partions for
            if (table.getPartitionKeys() != null && table.getPartitionKeys().size() > 0) {
              partitionedTables.put(table);
            }
            screen("Copying table " + name[0] + "." + name[1]);
            targetStore.get().createTable(table);

            // See if the table has any constraints, and if so copy those as well
            List<SQLPrimaryKey> pk =
                srcStore.get().getPrimaryKeys(table.getDbName(), table.getTableName());
            if (pk != null && pk.size() > 0) {
              LOG.debug("Found primary keys, adding them");
              targetStore.get().addPrimaryKeys(pk);
            }

            // Passing null as the target table name results in all of the foreign keys being
            // retrieved.
            List<SQLForeignKey> fks =
                srcStore.get().getForeignKeys(null, null, table.getDbName(), table.getTableName());
            if (fks != null && fks.size() > 0) {
              LOG.debug("Found foreign keys, adding them");
              targetStore.get().addForeignKeys(fks);
            }
          }
        } catch (InterruptedException | MetaException | InvalidObjectException e) {
          throw new RuntimeException(e);
        }
      }
    }
  }

  private void copyIndexes() throws MetaException, InvalidObjectException, InterruptedException {
    screen("Copying indexes");

    // Start the parallel threads that will copy the indexes
    Thread[] copiers = new Thread[parallel];
    writingToQueue = true;
    for (int i = 0; i < parallel; i++) {
      copiers[i] = new IndexCopier();
      copiers[i].start();
    }

    // Put indexes from the databases we copied into the queue
    for (Database db : dbs) {
      screen("Coyping indexes in database " + db.getName());
      for (String tableName : srcStore.get().getAllTables(db.getName())) {
        for (Index index : srcStore.get().getIndexes(db.getName(), tableName, -1)) {
          indexNameQueue.put(new String[]{db.getName(), tableName, index.getIndexName()});
        }
      }
    }

    // Now put any specifically requested tables into the queue
    if (tablesToImport != null) {
      for (String compoundTableName : tablesToImport) {
        String[] tn = compoundTableName.split("\\.");
        if (tn.length != 2) {
          error(compoundTableName + " not in proper form.  Must be in form dbname.tablename.  " +
              "Ignoring this table and continuing.");
        } else {
          for (Index index : srcStore.get().getIndexes(tn[0], tn[1], -1)) {
            indexNameQueue.put(new String[]{tn[0], tn[1], index.getIndexName()});
          }
        }
      }
    }

    writingToQueue = false;

    // Wait until we've finished adding all the tables
    for (Thread copier : copiers) copier.join();
 }

  private class IndexCopier extends Thread {
    @Override
    public void run() {
      while (writingToQueue || indexNameQueue.size() > 0) {
        try {
          String[] name = indexNameQueue.poll(1, TimeUnit.SECONDS);
          if (name != null) {
            Index index = srcStore.get().getIndex(name[0], name[1], name[2]);
            screen("Copying index " + name[0] + "." + name[1] + "." + name[2]);
            targetStore.get().addIndex(index);
          }
        } catch (InterruptedException | MetaException | InvalidObjectException e) {
          throw new RuntimeException(e);
        }
      }
    }
  }

  /* Partition copying is a little complex.  As we went through and copied the tables we put each
   * partitioned table into a queue.  We will now go through that queue and add partitions for the
   * tables.  We do the finding of partitions and writing of them separately and in parallel.
   * This way if there is one table with >> partitions then all of the others that skew won't
   * hurt us.  To avoid pulling all of the partitions for a table into memory, we batch up
   * partitions (by default in batches of 1000) and copy them over in batches.
   */
  private void copyPartitions() throws MetaException, NoSuchObjectException,
      InvalidObjectException, InterruptedException {
    screen("Copying partitions");
    readersFinished = false;
    Thread[] readers = new Thread[parallel];
    Thread[] writers = new Thread[parallel];
    for (int i = 0; i < parallel; i++) {
      readers[i] = new PartitionReader();
      readers[i].start();
      writers[i] = new PartitionWriter();
      writers[i].start();
    }

    for (Thread reader : readers) reader.join();
    readersFinished = true;

    // Wait until we've finished adding all the partitions
    for (Thread writer : writers) writer.join();
  }

  private class PartitionReader extends Thread {
    @Override
    public void run() {
      while (partitionedTables.size() > 0) {
        try {
          Table table = partitionedTables.poll(1, TimeUnit.SECONDS);
          if (table != null) {
            screen("Fetching partitions for table " + table.getDbName() + "." +
                table.getTableName());
            List<String> partNames =
                srcStore.get().listPartitionNames(table.getDbName(), table.getTableName(),
                    (short) -1);
            if (partNames.size() <= batchSize) {
              LOG.debug("Adding all partition names to queue for " + table.getDbName() + "." +
                  table.getTableName());
              partQueue.put(new PartQueueEntry(table.getDbName(), table.getTableName(), partNames));
            } else {
              int goUntil = partNames.size() % batchSize == 0 ? partNames.size() / batchSize :
                  partNames.size() / batchSize + 1;
              for (int i = 0; i < goUntil; i++) {
                int start = i * batchSize;
                int end = Math.min((i + 1) * batchSize, partNames.size());
                LOG.debug("Adding partitions " + start + " to " + end + " for " + table.getDbName()
                    + "." + table.getTableName());
                partQueue.put(new PartQueueEntry(table.getDbName(), table.getTableName(),
                    partNames.subList(start, end)));
              }
            }
          }
        } catch (InterruptedException | MetaException e) {
          throw new RuntimeException(e);
        }
      }
    }
  }

  private class PartitionWriter extends Thread {
    @Override
    public void run() {
      // This keeps us from throwing exceptions in our raw store calls
      Deadline.registerIfNot(1000000);
      while (!readersFinished || partQueue.size() > 0) {
        try {
          PartQueueEntry entry = partQueue.poll(1, TimeUnit.SECONDS);
          if (entry != null) {
            LOG.info("Writing partitions " + entry.dbName + "." + entry.tableName + "." +
                StringUtils.join(entry.partNames, ':'));
            // Fetch these partitions and write them to HBase
            Deadline.startTimer("hbaseimport");
            List<Partition> parts =
                srcStore.get().getPartitionsByNames(entry.dbName, entry.tableName,
                    entry.partNames);
            targetStore.get().addPartitions(entry.dbName, entry.tableName, parts);
            Deadline.stopTimer();
          }
        } catch (InterruptedException | MetaException | InvalidObjectException |
            NoSuchObjectException e) {
          throw new RuntimeException(e);
        }
      }
    }
  }

  private void copyFunctions() throws MetaException, NoSuchObjectException, InvalidObjectException {
    screen("Copying functions");
    // Copy any functions from databases we copied.
    for (Database db : dbs) {
      screen("Copying functions in database " + db.getName());
      for (String funcName : srcStore.get().getFunctions(db.getName(), "*")) {
        copyOneFunction(db.getName(), funcName);
      }
    }
    // Now do any specifically requested functions
    if (functionsToImport != null) {
      for (String compoundFuncName : functionsToImport) {
        String[] fn = compoundFuncName.split("\\.");
        if (fn.length != 2) {
          error(compoundFuncName + " not in proper form.  Must be in form dbname.funcname.  " +
              "Ignoring this function and continuing.");
        } else {
          copyOneFunction(fn[0], fn[1]);
        }
      }
    }
  }

  private void copyOneFunction(String dbName, String funcName) throws MetaException,
      InvalidObjectException {
    Function func = srcStore.get().getFunction(dbName, funcName);
    screen("Copying function " + dbName + "." + funcName);
    targetStore.get().createFunction(func);
  }

  private void copyKerberos() throws MetaException {
    screen("Copying kerberos related items");
    for (String tokenId : srcStore.get().getAllTokenIdentifiers()) {
      String token = srcStore.get().getToken(tokenId);
      targetStore.get().addToken(tokenId, token);
    }
    for (String masterKey : srcStore.get().getMasterKeys()) {
      targetStore.get().addMasterKey(masterKey);
    }
  }

  private void screen(String msg) {
    LOG.info(msg);
    System.out.println(msg);
  }

  private void error(String msg) {
    LOG.error(msg);
    System.err.println("ERROR:  " + msg);
  }

  @VisibleForTesting
  void setConnections(final RawStore src, final RawStore target) {
    if (!testing) throw new RuntimeException("Not valid to call this outside of a unit test");
    srcStore = getCopyingRawStore(src);
    targetStore = getCopyingRawStore(target);
  }

  private ThreadLocal<RawStore> getCopyingRawStore(final RawStore base) {
    return new ThreadLocal<RawStore>() {
      @Override
      protected RawStore initialValue() {
        RawStore rs;
        if (base instanceof ObjectStore) {
          rs = new ObjectStore();
        } else if (base instanceof HBaseStore) {
          rs = new HBaseStore();
        } else if (base instanceof PostgresStore) {
          rs = new PostgresStore();
        } else {
          throw new RuntimeException("Uknown base type " + base.getClass().getName());
        }
        rs.setConf(base.getConf());
        return rs;
      }
    };
  }

  private static class PartQueueEntry {
    final String dbName;
    final String tableName;
    final List<String> partNames;

    PartQueueEntry(String d, String t, List<String> p) {
      dbName = d;
      tableName = t;
      partNames = p;
    }
  }

}
