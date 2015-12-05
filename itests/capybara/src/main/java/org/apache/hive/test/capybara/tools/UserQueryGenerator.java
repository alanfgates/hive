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
package org.apache.hive.test.capybara.tools;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.BinaryColumnStatsData;
import org.apache.hadoop.hive.metastore.api.BooleanColumnStatsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.DateColumnStatsData;
import org.apache.hadoop.hive.metastore.api.Decimal;
import org.apache.hadoop.hive.metastore.api.DecimalColumnStatsData;
import org.apache.hadoop.hive.metastore.api.DoubleColumnStatsData;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.LongColumnStatsData;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StringColumnStatsData;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * <p>A tool to collect information on tables based on user queries and then write code to test
 * those queries.</p>
 *
 * <p>To use this tool:</p>
 * <ol>
 *   <li>If this is the first time you have used the tool, in your git directory do <tt>cd
 *   itests/capybara; mvn package</tt> then copy <tt>target/capybara-test-</tt><i>version
 *   </i><tt>.jar</tt> to <tt>$HIVE_HOME/lib</tt>/ and
 *   <tt>itests/capybara/src/scripts/capygen.sh</tt> to <tt>$HIVE_HOME/bin/ext</tt>.</li>
 *   <li>For each test you wish to run, write all of the SQL for that test into a file, one
 *   line for each SQL command.  Thus if you have 5 tests to run, you will have 5 separate
 *   files.</li>
 *   <li>Run <tt>analyze table </tt><i>yourtable</i><tt> compute statistics for columns;</tt> for
 *   each table referenced in the queries you wish to test.</li>
 *   <li>On a machine that has access to the metastore for your cluster run this class, passing
 *   in each of your input files and the name of the Java class for your test.  The command
 *   runs as a Hive service.  If you had two input files, <tt>test.sql</tt> and
 *   <tt>anothertest.sql</tt> and you wanted to create a class called TestMyApp the command line
 *   would be <tt>$HIVE_HOME/bin/hive --service capygen -i test.sql anothertest.sql -o TestMyApp
 *   </tt></li>
 *   <li>The resulting class will be in the package <tt>org.apache.hive.test.capybara
 *   .generated</tt>.  You can put it in the appropriate directory in your source code.  At this
 *   point doing <tt>cd itests/capybara; mvn test</tt> will run your test(s).  If you like you
 *   can change the package the test is in, though you will then also need to make sure and put
 *   it in a place with an appropriate pom file and infrastructure to run the tests.
 *   These tests should not be pushed back to Apache as they will have your queries in them.
 *   </li>
 * </ol>
 */
public class UserQueryGenerator {
  static final private Logger LOG = LoggerFactory.getLogger(UserQueryGenerator.class);

  private BufferedWriter writer;
  private int indent;

  public static void main(String[] args) {

    Options options = new Options();

    options.addOption(OptionBuilder
        .withLongOpt("help")
        .withDescription("You're looking at it")
        .create('h'));

    options.addOption(OptionBuilder
        .withLongOpt("input")
        .withDescription("Input files")
        .hasArgs()
        .isRequired()
        .create('i'));

    options.addOption(OptionBuilder
        .withLongOpt("output")
        .withDescription("Output class")
        .hasArg()
        .isRequired()
        .create('o'));

    try {
      CommandLine cli = new GnuParser().parse(options, args);

      if (cli.hasOption('h')) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("userquerygenerator", options);
      }

      UserQueryGenerator gen = new UserQueryGenerator();
      gen.run(cli.getOptionValues('i'), cli.getOptionValue('o'));
    } catch (ParseException|TException|IOException e) {
      System.err.println("Failed to run, " + e.getMessage());
      LOG.error("Failure", e);
    }
  }

  private UserQueryGenerator() {

  }

  private void run(String[] inputFiles, String outputClass) throws IOException, TException {
    Map<String, List<SQLStatement>> tests = readStatementsFromFile(inputFiles);
    Set<TableInfo> tables = determineTables(tests);
    getTableInfo(tables);
    writeCode(tests, tables, outputClass);
  }

  private Map<String, List<SQLStatement>> readStatementsFromFile(String[] filenames) throws IOException {
    Map<String, List<SQLStatement>> files = new HashMap<>();
    for (String filename : filenames) {
      List<SQLStatement> stmts = new ArrayList<>();
      BufferedReader reader = new BufferedReader(new FileReader(filename));
      String line;
      List<String> statement = new ArrayList<>();
      while ((line = reader.readLine()) != null) {
        LOG.info("Evaluating line: " + line);
        if (statement.size() == 0 && line.toLowerCase().matches(".*;\\s*")) {
          // It's a statement on one line
          stmts.add(new SQLStatement(Arrays.asList(line)));
          LOG.info("Found one line query <" + line + ">");
        } else if (statement.size() == 0 && line.toLowerCase().matches(".*\\S+.*")) {
          LOG.info("Starting new statement");
          statement.add(line);
        } else if (statement.size() > 0) {
          statement.add(line);
          LOG.info("Appending " + line + " to existing statement");
          if (line.matches(".*;\\s*")) {
            SQLStatement stmt = new SQLStatement(statement);
            stmts.add(stmt);
            LOG.info("Found multi-line query <" + stmt.toString() + ">");
            statement = new ArrayList<>();
          }
        }
      }
      reader.close();
      if (files.put(cleanFilename(filename), stmts) != null) {
        throw new IOException("Two files both map to " + cleanFilename(filename));
      };
    }
    return files;
  }

  private String cleanFilename(String original) {
    if (original.contains(System.getProperty("file.separator"))) {
      original = original.substring(original.lastIndexOf(System.getProperty("file.separator")) + 1);
    }
    return original.replace(".sql", "").replaceAll("[^a-zA-Z0-9_]", "_");
  }

  private Set<TableInfo> determineTables(Map<String, List<SQLStatement>> statements) {
    Set<TableInfo> tables = new HashSet<>();

    // Look for the table name at the head of the block.
    // Each of these should either have a table name or possibly a subquery.  Only worry about
    // the table names
    // TODO - doesn't handle from a, b
    // TODO - doesn't handle use db
    Pattern fromAndjoin = Pattern.compile("([A-Za-z0-9_\\.]+).*");
    Pattern insert = Pattern.compile("insert\\s+into\\s+([A-Za-z0-9_\\.]+).*");
    Pattern insertTable = Pattern.compile("insert\\s+into\\s+table\\s+([A-Za-z0-9_\\.]+).*");
    Pattern insertOverwrite = Pattern.compile("insert\\s+overwrite\\s+table\\s+([A-Za-z0-9_\\.]+).*");
    Pattern update = Pattern.compile("update\\s+([A-Za-z0-9_\\.]+).*");
    Pattern delete = Pattern.compile("delete\\s+from\\s+([A-Za-z0-9_\\.]+).*");
    for (List<SQLStatement> stmts : statements.values()) {
      for (SQLStatement stmt : stmts) {
        if (stmt.toString().startsWith("set")) continue;
        findTableName(fromAndjoin, stmt.toString().split("\\s*from\\s*"), stmt, tables, true);
        findTableName(fromAndjoin, stmt.toString().split("\\s*join\\s*"), stmt, tables, true);
        findTableName(insert, new String[]{"", stmt.toString()}, stmt, tables, false);
        findTableName(insertTable, new String[]{"", stmt.toString()}, stmt, tables, false);
        findTableName(insertOverwrite, new String[]{"", stmt.toString()}, stmt, tables, false);
        findTableName(update, new String[]{"", stmt.toString()}, stmt, tables, false);
        findTableName(delete, new String[]{"", stmt.toString()}, stmt, tables, false);
      }
    }
    return tables;
  }

  private void findTableName(Pattern pattern, String[] segments, SQLStatement stmt,
                             Set<TableInfo> tables, boolean needsPopulated) {
    // Skip the first segment since it will be the part before 'from' or 'join'
    for (int i = 1; i < segments.length; i++) {
      Matcher matcher = pattern.matcher(segments[i]);
      if (matcher.matches()) {
        String tableName = matcher.group(1);
        LOG.info("Found table " + tableName + ", needsPopulated is " + needsPopulated);
        TableInfo ti = new TableInfo(needsPopulated, tableName);
        if (!needsPopulated) stmt.targetTable = ti;
        tables.add(ti);
      }
    }
  }

  private void getTableInfo(Set<TableInfo> tables) throws TException {
    HiveConf conf = new HiveConf();
    IMetaStoreClient msClient = new HiveMetaStoreClient(conf);
    for (TableInfo table : tables) {
      String dbName, tableOnlyName;
      if (table.nameFromStatement.contains(".")) {
        String[] compoundName = table.nameFromStatement.split("\\.");
        dbName = compoundName[0];
        tableOnlyName = compoundName[1];
      } else {
        dbName = "default";
        tableOnlyName = table.nameFromStatement;
      }

      try {
        table.msTable = msClient.getTable(dbName, tableOnlyName);
      } catch (NoSuchObjectException e) {
        // This might be ok, because the table might be created as part of the test.
        LOG.warn("Failed to find table " + dbName + "." + tableOnlyName);
        continue;
      }

      List<String> colNames = Lists.transform(table.msTable.getSd().getCols(),
          new Function<FieldSchema, String>() {
            @Override
            public String apply(FieldSchema input) {
              return input.getName();
            }
          });

      if (table.msTable.getPartitionKeys() != null && table.msTable.getPartitionKeys().size() > 0) {
        List<String> partNames = msClient.listPartitionNames(dbName, tableOnlyName, (short)-1);
        LOG.info("Found partitions: " + StringUtils.join(partNames, ","));
        table.numParts = partNames.size();
        Map<String, List<ColumnStatisticsObj>> partStats = msClient.getPartitionColumnStatistics(dbName,
            tableOnlyName, partNames, colNames);
        table.colStats = combineColStats(partStats);
        // To get the sizes we need to fetch indivudal partitions and look at the aggregate stats
        // .  I don't want to fetch all of them at once for fear of blowing out the metastore
        // memory.  So we fetch them a thousand at a time.
        List<List<String>> partitionGroups = new ArrayList<>();
        int numPartsInGroup = 0;
        List<String> currentGroup = null;
        for (String partName : partNames) {
          if (numPartsInGroup % 1000 == 0) {
            if (currentGroup != null) {
              partitionGroups.add(currentGroup);
            }
            currentGroup = new ArrayList<>(1000);
          }
          currentGroup.add(partName);
          numPartsInGroup++;
        }
        // Add the last group
        partitionGroups.add(currentGroup);

        for (List<String> oneGroup : partitionGroups) {
          LOG.info("Fetching stats for partitions: " + StringUtils.join(oneGroup, ","));
          List<Partition> parts = msClient.getPartitionsByNames(dbName, tableOnlyName, oneGroup);
          LOG.info("Got " + parts.size() + " partitions");
          for (Partition part : parts) {
            table.dataSize += Long.valueOf(part.getParameters().get(StatsSetupConst.RAW_DATA_SIZE));
            table.rowCount += Long.valueOf(part.getParameters().get(StatsSetupConst.ROW_COUNT));
          }
        }
      } else {
        table.numParts = 0;
        table.colStats = msClient.getTableColumnStatistics(dbName, tableOnlyName, colNames);
        table.dataSize = Long.valueOf(table.msTable.getParameters().get(StatsSetupConst.RAW_DATA_SIZE));
        table.rowCount = Long.valueOf(table.msTable.getParameters().get(StatsSetupConst.ROW_COUNT));
      }
    }
  }

  private List<ColumnStatisticsObj> combineColStats(Map<String, List<ColumnStatisticsObj>> partStats) {
    Map<String, ColumnStatisticsObj> colToStatsMap = new HashMap<>();

    for (List<ColumnStatisticsObj> statsObjs : partStats.values()) {
      for (ColumnStatisticsObj latest : statsObjs) {
        ColumnStatisticsObj existing = colToStatsMap.get(latest.getColName());
        colToStatsMap.put(latest.getColName(), combineTwoStats(existing, latest));
      }
    }
    return new ArrayList<>(colToStatsMap.values());
  }

  private ColumnStatisticsObj combineTwoStats(ColumnStatisticsObj existing,
                                              ColumnStatisticsObj latest) {
    if (existing == null) return latest;

    ColumnStatisticsData newStatsData = new ColumnStatisticsData();
    if (existing.getStatsData().isSetLongStats()) {
      LongColumnStatsData existingStats = existing.getStatsData().getLongStats();
      LongColumnStatsData latestStats = latest.getStatsData().getLongStats();
      LongColumnStatsData newStats =
          new LongColumnStatsData(existingStats.getNumNulls() + latestStats.getNumNulls(),
          (long)(Math.max(existingStats.getNumDVs(), latestStats.getNumDVs()) * 1.2));
      newStats.setHighValue(Math.max(existingStats.getHighValue(), latestStats.getHighValue()));
      newStats.setLowValue(Math.max(existingStats.getLowValue(), latestStats.getLowValue()));
      newStatsData.setLongStats(newStats);
    } else if (existing.getStatsData().isSetDoubleStats()) {
      DoubleColumnStatsData existingStats = existing.getStatsData().getDoubleStats();
      DoubleColumnStatsData latestStats = latest.getStatsData().getDoubleStats();
      DoubleColumnStatsData newStats =
          new DoubleColumnStatsData(existingStats.getNumNulls() + latestStats.getNumNulls(),
              (long)(Math.max(existingStats.getNumDVs(), latestStats.getNumDVs()) * 1.2));
      newStats.setHighValue(Math.max(existingStats.getHighValue(), latestStats.getHighValue()));
      newStats.setLowValue(Math.max(existingStats.getLowValue(), latestStats.getLowValue()));
      newStatsData.setDoubleStats(newStats);
    } else if (existing.getStatsData().isSetDecimalStats()) {
      DecimalColumnStatsData existingStats = existing.getStatsData().getDecimalStats();
      DecimalColumnStatsData latestStats = latest.getStatsData().getDecimalStats();
      DecimalColumnStatsData newStats =
          new DecimalColumnStatsData(existingStats.getNumNulls() + latestStats.getNumNulls(),
              (long) (Math.max(existingStats.getNumDVs(), latestStats.getNumDVs()) * 1.2));
      newStats.setHighValue(existingStats.getHighValue().compareTo(latestStats.getHighValue()) > 1 ?
        existingStats.getHighValue() : latestStats.getHighValue());
      newStats.setLowValue(existingStats.getLowValue().compareTo(latestStats.getLowValue()) < 1 ?
          existingStats.getLowValue() : latestStats.getLowValue());
      newStatsData.setDecimalStats(newStats);
    } else if (existing.getStatsData().isSetDateStats()) {
      DateColumnStatsData existingStats = existing.getStatsData().getDateStats();
      DateColumnStatsData latestStats = latest.getStatsData().getDateStats();
      DateColumnStatsData newStats =
          new DateColumnStatsData(existingStats.getNumNulls() + latestStats.getNumNulls(),
              (long) (Math.max(existingStats.getNumDVs(), latestStats.getNumDVs()) * 1.2));
      newStats.setHighValue(existingStats.getHighValue().compareTo(latestStats.getHighValue()) > 1 ?
          existingStats.getHighValue() : latestStats.getHighValue());
      newStats.setLowValue(existingStats.getLowValue().compareTo(latestStats.getLowValue()) < 1 ?
          existingStats.getLowValue() : latestStats.getLowValue());
      newStatsData.setDateStats(newStats);
    } else if (existing.getStatsData().isSetBooleanStats()) {
      BooleanColumnStatsData existingStats = existing.getStatsData().getBooleanStats();
      BooleanColumnStatsData latestStats = latest.getStatsData().getBooleanStats();
      BooleanColumnStatsData newStats =
          new BooleanColumnStatsData( existingStats.getNumTrues() + latestStats.getNumTrues(),
              existingStats.getNumFalses() + latestStats.getNumFalses(),
              existingStats.getNumNulls() + latestStats.getNumNulls());
      newStatsData.setBooleanStats(newStats);
    } else if (existing.getStatsData().isSetStringStats()) {
      StringColumnStatsData existingStats = existing.getStatsData().getStringStats();
      StringColumnStatsData latestStats = latest.getStatsData().getStringStats();
      StringColumnStatsData newStats =
          new StringColumnStatsData(
              Math.max(existingStats.getMaxColLen(), latestStats.getMaxColLen()),
              // TODO improve this
              existingStats.getAvgColLen() + latestStats.getAvgColLen() / 2.0,
              existingStats.getNumNulls() + latestStats.getNumNulls(),
              (long) (Math.max(existingStats.getNumDVs(), latestStats.getNumDVs()) * 1.2));
      newStatsData.setStringStats(newStats);
    } else if (existing.getStatsData().isSetBinaryStats()) {
      BinaryColumnStatsData existingStats = existing.getStatsData().getBinaryStats();
      BinaryColumnStatsData latestStats = latest.getStatsData().getBinaryStats();
      BinaryColumnStatsData newStats =
          new BinaryColumnStatsData(
              Math.max(existingStats.getMaxColLen(), latestStats.getMaxColLen()),
              // TODO improve this
              existingStats.getAvgColLen() + latestStats.getAvgColLen() / 2.0,
              existingStats.getNumNulls() + latestStats.getNumNulls());
      newStatsData.setBinaryStats(newStats);
    }

    return new ColumnStatisticsObj(latest.getColName(), latest.getColType(), newStatsData);
  }

  private void writeCode(Map<String, List<SQLStatement>> statements, Set<TableInfo> tables,
                         String outputClass)
      throws IOException {
    writer = new BufferedWriter(new FileWriter(outputClass + ".java"));
    indent = 0;
    for (String line : prologue) writeALine(line);
    writeALine("public class " + outputClass + " extends IntegrationTest {");
    indent++;

    // Create set of tables so we can fetch them in the tests if we need them.
    writeALine("private Map<String, TestTable> targetTables = new HashMap<>();");

    // Write the methods that build the tables, marking each with @Before
    for (TableInfo info : tables) writeBefore(info);

    // Write the tests, naming them with the filenames
    for (Map.Entry<String, List<SQLStatement>> entry : statements.entrySet()) {
      writeTest(entry.getKey(), entry.getValue());
    }

    indent--;
    writeALine("}");
    writer.close();
  }

  private void writeBefore(TableInfo info) throws IOException {
    if (info.msTable == null) {
      // If we don't have info from the metastore on the table we can't build it.  We will assume
      // it will be created somewhere as part of the test.
      LOG.info("No metastore info for table " + info.nameFromStatement +
          ", not creating @Before method for it.");
      return;
    }
    writeALine("@Before");
    writeALine("public void createTable" + safeTableName(info) + "() throws Exception {");
    indent++;
    writeALine("TestTable tTable = TestTable.getBuilder(\"" + info.msTable.getTableName() + "\")");
    indent++; indent++;
    writeALine(".setDbName(\"" + info.msTable.getDbName() + "\")");
    for (FieldSchema fs : info.msTable.getSd().getCols()) {
      writeALine(".addCol(\"" + fs.getName() + "\", \"" + fs.getType() + "\")");
    }

    if (info.msTable.getPartitionKeys() != null && info.msTable.getPartitionKeysSize() > 0) {
      for (FieldSchema partCol : info.msTable.getPartitionKeys()) {
        writeALine(".addPartCol(\"" + partCol.getName() + "\", \"" + partCol.getType() + "\")");

      }
      writeALine(".setNumParts(" + info.numParts + ")");
    }
    writeALine(".build();");
    indent--; indent--;

    writeALine("Map<String, StatsDataGenerator.ColStats> colStats = new HashMap<>();");
    for (ColumnStatisticsObj cso : info.colStats) {
      writeALine("colStats.put(\"" + cso.getColName() + "\",");
      indent++;
      writeALine("new StatsDataGenerator.ColStats(\"" + cso.getColName() + "\", \"" +
          cso.getColType() + "\", ");
      indent++;
      if (cso.getColType().equalsIgnoreCase("bigint")) {
        LongColumnStatsData lcsd = cso.getStatsData().getLongStats();
        writeALine(lcsd.getLowValue() + "L, " + lcsd.getHighValue() + "L, 0, 0, 0, 0, " +
            lcsd.getNumDVs() + ", " + lcsd.getNumNulls() + "));");

      } else if (cso.getColType().toLowerCase().startsWith("int")) {
        LongColumnStatsData lcsd = cso.getStatsData().getLongStats();
        writeALine(lcsd.getLowValue() + ", " + lcsd.getHighValue() + ", 0, 0, 0, 0, " +
            lcsd.getNumDVs() + ", " + lcsd.getNumNulls() + "));");

      } else if (cso.getColType().equalsIgnoreCase("smallint")) {
        LongColumnStatsData lcsd = cso.getStatsData().getLongStats();
        writeALine("(short)" + lcsd.getLowValue() + ", (short)" + lcsd.getHighValue() +
            ", 0, 0, 0, 0, " + lcsd.getNumDVs() + ", " + lcsd.getNumNulls() + "));");

      } else if (cso.getColType().equalsIgnoreCase("tinyint")) {
        LongColumnStatsData lcsd = cso.getStatsData().getLongStats();
        writeALine("(byte)" + lcsd.getLowValue() + ", (byte)" + lcsd.getHighValue() +
            ", 0, 0, 0, 0, " + lcsd.getNumDVs() + ", " + lcsd.getNumNulls() + "));");
      } else if (cso.getColType().equalsIgnoreCase("float")) {
        DoubleColumnStatsData dcsd = cso.getStatsData().getDoubleStats();
        writeALine(dcsd.getLowValue() + "f, " + dcsd.getHighValue() + "f, 0, 0, 0, 0, " +
            dcsd.getNumDVs() + ", " + dcsd.getNumNulls() + "));");

      } else if (cso.getColType().equalsIgnoreCase("double")) {
        DoubleColumnStatsData dcsd = cso.getStatsData().getDoubleStats();
        writeALine(dcsd.getLowValue() + ", " + dcsd.getHighValue() + ", 0, 0, 0, 0, " +
            dcsd.getNumDVs() + ", " + dcsd.getNumNulls() + "));");
      } else if (cso.getColType().toLowerCase().startsWith("decimal")) {
        DecimalColumnStatsData dcsd = cso.getStatsData().getDecimalStats();
        Decimal low = dcsd.getLowValue();
        Decimal high = dcsd.getHighValue();
        writeALine("new BigDecimal(new BigInteger(Base64.decodeBase64(\"" +
            Base64.encodeBase64URLSafeString(low.getUnscaled()) + "\")), " + low.getScale() +
            "), new BigDecimal(new BigInteger(Base64.decodeBase64(\""
            + Base64.encodeBase64URLSafeString(high.getUnscaled()) + "\")), " + high.getScale() +
            "), 0, 0, 0, 0, " + dcsd.getNumDVs() + ", " + dcsd.getNumNulls() + "));");
      } else if (cso.getColType().equalsIgnoreCase("date")) {
        DateColumnStatsData dcsd = cso.getStatsData().getDateStats();
        writeALine("new Date(" + (dcsd.getLowValue().getDaysSinceEpoch() * 86400) + "), " +
            "new Date(" + (dcsd.getHighValue().getDaysSinceEpoch() * 86400) + "), 0, 0, 0, 0, " +
            dcsd.getNumDVs() + ", " + dcsd.getNumNulls() + "));");
      } else if (cso.getColType().equalsIgnoreCase("timestamp")) {
        LongColumnStatsData lcsd = cso.getStatsData().getLongStats();
        writeALine("new Timestamp(" + (lcsd.getLowValue()) + "), " +
            "new Timestamp(" + (lcsd.getHighValue()) + "), 0, 0, 0, 0, " + lcsd.getNumDVs() + ", "
            + lcsd.getNumNulls() + "));");
      } else if (cso.getColType().equalsIgnoreCase("string") ||
          cso.getColType().toLowerCase().startsWith("varchar") ||
          cso.getColType().toLowerCase().startsWith("char")) {
        StringColumnStatsData scsd = cso.getStatsData().getStringStats();
        writeALine(
            "null, null, " + scsd.getAvgColLen() + ", " + scsd.getMaxColLen() + ", 0, 0, " +
                scsd.getNumDVs() + ", " + scsd.getNumNulls() + "));");
      } else if (cso.getColType().equalsIgnoreCase("boolean")) {
        BooleanColumnStatsData bcsd = cso.getStatsData().getBooleanStats();
        writeALine("0, 0, 0, 0, " + bcsd.getNumFalses() + ", " + bcsd.getNumTrues() + ", 0, " +
            bcsd.getNumNulls() + "));");
      } else if (cso.getColType().equalsIgnoreCase("binary")) {
        BinaryColumnStatsData bcsd = cso.getStatsData().getBinaryStats();
        writeALine(
            "null, null, " + bcsd.getAvgColLen() + ", " + bcsd.getMaxColLen() + ", 0, 0, 0,"
                + bcsd.getNumNulls() + "));");
      } else {
        throw new RuntimeException("Unknown column type " + cso.getColType());
      }
      indent--;
      indent--;
    }
    writeALine("StatsDataGenerator.TableStats tableStats = new StatsDataGenerator.TableStats" +
        "(colStats, " + info.numParts + ", \"" + info.msTable.getDbName() + "\", \"" +
        info.msTable.getTableName() + "\", " + info.dataSize + ", " + info.rowCount + ");");
    writeALine("StatsDataGenerator gen = new StatsDataGenerator(tableStats, " +
        new Random().nextInt() + ");");
    if (info.needsPopulated) {
      writeALine("tTable.create();");
      writeALine("tTable.populate(gen);");
    } else {
      writeALine("tTable.createTargetTable();");
      writeALine("targetTables.put(\"" + info.nameFromStatement + "\", tTable);");
    }
    indent--;
    writeALine("}");
    writeALine("");
  }

  private String safeTableName(TableInfo info) {
    return info.msTable == null ?  info.nameFromStatement :
      info.msTable.getDbName() + "_" + info.msTable.getTableName();
  }

  private void writeTest(String name, List<SQLStatement> statements) throws IOException {
    writeALine("@Test");
    writeALine("public void " + name + "() throws Exception {");
    indent++;
    SQLStatement lastStmt = null;
    for (SQLStatement stmt : statements) {
      if (stmt.toString().startsWith("set")) {
        String[] setParts = stmt.toString().substring(3).split("=");
        writeALine("set(\"" + setParts[0].trim() + "\", \"" + setParts[1].trim() + "\");");
      } else {
        writeALine("runQuery(");
        stmt.writeOut();
        writeALine(");");
      }
      lastStmt = stmt;
    }
    // Only compare based on the last statement.
    if (lastStmt.toString().startsWith("select")) {
      if (lastStmt.toString().contains("order by")) {
        writeALine("compare();");
      } else {
        writeALine("sortAndCompare();");
      }
    } else if (lastStmt.toString().startsWith("insert") || lastStmt.toString().startsWith("update") ||
        lastStmt.toString().startsWith("delete")) {
      TableInfo targetTable = lastStmt.targetTable;
      if (targetTable.msTable == null) {
        // We didn't build the table before because we didn't have metastore info.  But by now
        // the table must exist, so we need to go build it.
        writeALine("String dbName, tableOnlyName;");
        if (targetTable.nameFromStatement.contains(".")) {
          String[] compoundName = targetTable.nameFromStatement.split("\\.");
          writeALine("dbName = \"" + compoundName[0] + "\";");
          writeALine("tableOnlyName = \"" + compoundName[1] + "\";");
        } else {
          writeALine("dbName = \"default\";");
          writeALine("tableOnlyName = \"" + targetTable.nameFromStatement + "\";");
        }

        writeALine("TestTable tTable = TestTable.fromHiveMetastore(dbName, tableOnlyName);");
      } else {
        writeALine("TestTable tTable = targetTables.get(\"" + targetTable.nameFromStatement + "\");");
      }
      writeALine("tableCompare(tTable);");
    } else {
      throw new IOException("Unclear how to do comparison for statement " + lastStmt.toString());
    }
    indent--;
    writeALine("}");
    writeALine("");
  }

  private void writeALine(String line) throws IOException {
    for (int i = 0; i < indent; i++) writer.write("  ");
    writer.write(line);
    writer.newLine();
  }

  private static class TableInfo {
    Table msTable;
    int numParts;
    long dataSize;
    long rowCount;
    List<ColumnStatisticsObj> colStats;
    final boolean needsPopulated;
    final String nameFromStatement;

    TableInfo(boolean needsPopulated, String nameFromStatement) {
      this.needsPopulated = needsPopulated;
      this.nameFromStatement = nameFromStatement;
    }

    @Override
    public boolean equals(Object other) {
      if (!(other instanceof TableInfo)) return false;
      return nameFromStatement.equals(((TableInfo)other).nameFromStatement);
    }

    @Override
    public int hashCode() {
      return nameFromStatement.hashCode();
    }
  }

  private class SQLStatement {
    final String[] stmt;
    TableInfo targetTable;
    private String asOneLine = null;

    SQLStatement(List<String> lines) {
      // Trim anything after a comment indicator
      stmt = new String[lines.size()];
      for (int i = 0; i < lines.size(); i++) {
        if (lines.get(i).contains("--")) {
          if (lines.get(i).indexOf("--") != 0) {
            stmt[i] = lines.get(i).substring(0, lines.get(i).indexOf("--")).trim();
          } else {
            stmt[i] = "";
          }
        } else {
          stmt[i] = lines.get(i).trim();
        }

        if (stmt[i].length() == 0) continue;

        stmt[i] = stmt[i].toLowerCase();

        // Trim any ending ;s off
        if (stmt[i].charAt(stmt[i].length() - 1) == ';') {
          stmt[i] = stmt[i].substring(0, stmt[i].length() - 1);
        }
        // Escape any quotes
        stmt[i] = stmt[i].replaceAll("\"", "\\\"");
      }
    }

    void writeOut() throws IOException {
      indent += 2;
      boolean first = true;
      for (String line : stmt) {
        String plus = "";
        if (first) first = false;
        else plus = "+ ";
        writeALine(plus + "\"" + line + "\"");
      }
      indent -= 2;
    }

    @Override
    public String toString() {
      if (asOneLine == null) asOneLine = StringUtils.join(stmt, " ");
      return asOneLine;
    }
  }

  private static final String[] prologue = {
      "package org.apache.hive.test.capybara.generated;",
      " ",
      "import org.apache.commons.codec.binary.Base64;",
      "import org.apache.hadoop.hive.conf.HiveConf;",
      "import org.apache.hive.test.capybara.IntegrationTest;",
      "import org.apache.hive.test.capybara.infra.StatsDataGenerator;",
      "import org.apache.hive.test.capybara.infra.TestTable;",
      "import org.junit.Assert;",
      "import org.junit.Before;",
      "import org.junit.Test;",
      "import java.math.BigDecimal;",
      "import java.math.BigInteger;",
      "import java.sql.Date;",
      "import java.sql.Timestamp;",
      "import java.util.ArrayList;",
      "import java.util.HashMap;",
      "import java.util.List;",
      "import java.util.Map;",
      " "
  };

  // Problems:
  // q2, q6 - grabbing the wrong table name
}
