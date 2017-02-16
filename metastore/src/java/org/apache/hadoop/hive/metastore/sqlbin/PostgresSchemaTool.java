/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.metastore.sqlbin;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.RawStore;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.Function;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TSimpleJSONProtocol;
import org.apache.thrift.transport.TMemoryBuffer;

import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.util.List;

/**
 * A tool to dump contents from the Postgres binary store in human readable form.
 */
public class PostgresSchemaTool {

  private RawStore postgres;
  @VisibleForTesting PrintStream out;

  public static void main(String[] args) {

    try {
      PostgresSchemaTool tool = new PostgresSchemaTool();
      tool.run(tool.parse(args));
    } catch (Throwable t) {
      System.err.println("Failed, got exception: " + t.getMessage());
      t.printStackTrace(System.err);
    }

  }

  private static void usage(Options options) {
    (new HelpFormatter()).printHelp("postgresschematool", options);
  }

  @VisibleForTesting
  PostgresSchemaTool() {
    HiveConf conf = new HiveConf();
    postgres = new PostgresStore();
    postgres.setConf(conf);
  }

  @VisibleForTesting
  CommandLine parse(String... args) {
    Options options = new Options();

    options.addOption(OptionBuilder
        .withLongOpt("list-databases")
        .withDescription("List all of the databases")
        .create('d'));

    options.addOption(OptionBuilder
        .withLongOpt("show-table")
        .withDescription("Give details of a particular database")
        .hasArg()
        .create('D'));

    options.addOption(OptionBuilder
        .withLongOpt("list-functions")
        .withDescription("List all of the functions in the indicated database")
        .hasArg()
        .create('f'));

    options.addOption(OptionBuilder
        .withLongOpt("show-function")
        .withDescription("Give details of a particular function.  The name must be in the form " +
            "dbname.function")
        .hasArg()
        .create('F'));

    options.addOption(OptionBuilder
        .withLongOpt("help")
        .withDescription("You're looking at it")
        .create('h'));

    options.addOption(OptionBuilder
        .withLongOpt("list-partitions")
        .withDescription("List all of the partitions in the indicated table.  The table name must" +
            " be in the form dbname.tablename.")
        .hasArg()
        .create('p'));

    options.addOption(OptionBuilder
        .withLongOpt("show-partition")
        .withDescription("Give details of a particular partition.  The name must be in the form " +
            "dbname.tablename.partname")
        .hasArg()
        .create('P'));

    options.addOption(OptionBuilder
        .withLongOpt("list-tables")
        .withDescription("List all of the tables in the indicated database")
        .hasArg()
        .create('t'));

    options.addOption(OptionBuilder
        .withLongOpt("show-table")
        .withDescription("Give details of a particular table.  The name must be in the form " +
            "dbname.tablename")
        .hasArg()
        .create('T'));

    try {
      CommandLine cli = new GnuParser().parse(options, args);
      if (cli.hasOption('h')) {
        usage(options);
        return null;
      }
      return cli;
    } catch (ParseException e) {
      System.err.println("Parse Exception: " + e.getMessage());
      usage(options);
      return null;
    }
  }

  @VisibleForTesting
  void run(CommandLine cli) throws TException, UnsupportedEncodingException {
    if (cli == null) return;
    if (cli.hasOption('d')) {
      listDatabases();
    } else if (cli.hasOption('D')) {
      showDatabase(cli.getOptionValue('D'));
    } else if (cli.hasOption('f')) {
      listFunctions(cli.getOptionValue('f'));
    } else if (cli.hasOption('F')) {
      showFunction(cli.getOptionValue('F'));
    } else if (cli.hasOption('p')) {
      listPartitions(cli.getOptionValue('p'));
    } else if (cli.hasOption('P')) {
      showPartition(cli.getOptionValue('P'));
    } else if (cli.hasOption('t')) {
      listTables(cli.getOptionValue('t'));
    } else if (cli.hasOption('T')) {
      showTable(cli.getOptionValue('T'));
    }
  }

  private void listDatabases() throws MetaException {
    List<String> dbNames = postgres.getAllDatabases();
    for (String dbName : dbNames) out.println(dbName);
  }

  private void showDatabase(String dbName) throws TException, UnsupportedEncodingException {
    Database db = postgres.getDatabase(dbName);
    dumpThriftObject(db);
  }

  private void listFunctions(String dbName) throws MetaException {
    List<String> funcNames = postgres.getFunctions(dbName, null);
    for (String funcName : funcNames) out.println(funcName);
  }

  private void showFunction(String compoundName) throws TException, UnsupportedEncodingException {
    String[] name = parseCompoundName(compoundName);
    Function func = postgres.getFunction(name[0], name[1]);
    dumpThriftObject(func);
  }

  private void listTables(String dbName) throws MetaException {
    List<String> tableNames = postgres.getTables(dbName, null);
    for (String tableName : tableNames) out.println(tableName);
  }

  private void showTable(String compoundName) throws TException, UnsupportedEncodingException {
    String[] name = parseCompoundName(compoundName);
    Table table = postgres.getTable(name[0], name[1]);
    dumpThriftObject(table);
  }

  private void listPartitions(String compoundName) throws MetaException {
    String[] name = parseCompoundName(compoundName);
    List<String> partNames = postgres.listPartitionNames(name[0], name[1], (short)-1);
    for (String partName : partNames) out.println(partName);
  }

  private void showPartition(String compoundName) throws TException, UnsupportedEncodingException {
    String[] name = parseCompoundName(compoundName);
    Partition part =
        postgres.getPartition(name[0], name[1], Warehouse.getPartValuesFromPartName(name[2]));
    dumpThriftObject(part);
  }

  private String[] parseCompoundName(String compoundName) {
    return compoundName.split("\\.");
  }

  private void dumpThriftObject(TBase obj) throws TException, UnsupportedEncodingException {
    TMemoryBuffer buf = new TMemoryBuffer(1000);
    TProtocol protocol = new TSimpleJSONProtocol(buf);
    obj.write(protocol);
    out.println(buf.toString("UTF-8"));
  }

}


