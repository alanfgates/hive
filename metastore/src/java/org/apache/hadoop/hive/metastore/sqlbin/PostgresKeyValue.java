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
package org.apache.hadoop.hive.metastore.sqlbin;

import com.jolbox.bonecp.BoneCPConfig;
import com.jolbox.bonecp.BoneCPDataSource;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.commons.dbcp.ConnectionFactory;
import org.apache.commons.dbcp.DriverManagerConnectionFactory;
import org.apache.commons.dbcp.PoolableConnectionFactory;
import org.apache.commons.dbcp.PoolingDataSource;
import org.apache.commons.pool.ObjectPool;
import org.apache.commons.pool.impl.GenericObjectPool;
import org.apache.hadoop.hive.common.ObjectPair;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.SQLForeignKey;
import org.apache.hadoop.hive.metastore.api.SQLPrimaryKey;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.thrift.protocol.TCompactProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.AggrStats;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TSimpleJSONProtocol;
import org.apache.thrift.transport.TMemoryBuffer;

import javax.sql.DataSource;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;


/**
 * Class to manage storing object in and reading them from HBase.
 */
public class PostgresKeyValue {
  public final static List<PostgresTable> initialPostgresTables = new ArrayList<>();

  // Column names used throughout
  // General column name that catalog objects are stored in.
  final static String CATALOG_COL = "catalog_object";
  // General column for storing stats
  final static String STATS_COL = "stats";


  // Define the Aggregate stats table
  /*
  final static String STATS_HASH = "stats_md5";
  final static String AGGR_STATS_BLOOM_COL = "bloom_filter";
  final static PostgresTable AGGR_STATS_TABLE =
      new PostgresTable("MS_AGGR_STATS")
        .addColumn(STATS_HASH, "bytea", true)
        .addByteColumn(STATS_COL)
        .addByteColumn(AGGR_STATS_BLOOM_COL);
        */

  // Define the Database table
  final static PostgresTable DB_TABLE =
      new PostgresTable("MS_DBS")
        .addStringKey("db_name")
        .addByteColumn(CATALOG_COL)
        .addToInitialTableList();

  // Define the Function table
  /*
  final static PostgresTable FUNC_TABLE =
      new PostgresTable("MS_FUNCS")
          .addStringKey("db_name")
          .addStringKey("function_name")
          .addByteColumn(CATALOG_COL);

  final static PostgresTable GLOBAL_PRIVS_TABLE =
      new PostgresTable("MS_GLOBAL_PRIVS")
        .addByteColumn(CATALOG_COL);

  final static PostgresTable ROLE_TABLE =
      new PostgresTable("MS_ROLES")
        .addStringKey("role_name")
        .addByteColumn(CATALOG_COL);

  final static String DELEGATION_TOKEN_COL = "delegation_token";
  final static String MASTER_KEY_COL = "master_key";

  final static PostgresTable DELEGATION_TOKEN_TABLE =
      new PostgresTable("MS_DELEGATION_TOKENS")
        .addStringKey("token_id")
        .addByteColumn(DELEGATION_TOKEN_COL);

  final static PostgresTable MASTER_KEY_TABLE =
      new PostgresTable("MS_MASTER_KEY_TABLE")
        .addColumn("key_id", "bigint", true)
        .addByteColumn(MASTER_KEY_COL);

  final static String SEQUENCES_NEXT = "nextval";
  final static PostgresTable SEQUENCES_TABLE =
      new PostgresTable("MS_SEQUENCES")
        .addStringKey("sequence_name")
        .addColumn(SEQUENCES_NEXT, "bigint");
        */

  final static String PRIMARY_KEY_COL = "primary_key_obj";
  final static String FOREIGN_KEY_COL = "foreign_key_obj";
  final static PostgresTable TABLE_TABLE =
      new PostgresTable("MS_TBLS")
          .addStringKey("db_name")
          .addStringKey("table_name")
          .addByteColumn(CATALOG_COL)
          .addByteColumn(STATS_COL)
          .addByteColumn(PRIMARY_KEY_COL)
          .addByteColumn(FOREIGN_KEY_COL)
          .addToInitialTableList();

  /*
  final static PostgresTable INDEX_TABLE =
      new PostgresTable("MS_INDEX")
        .addStringKey("index_name")
        .addByteColumn(CATALOG_COL);

  final static PostgresTable USER_TO_ROLE_TABLE =
      new PostgresTable("MS_USER_TO_ROLE")
        .addStringKey("user_name")
        .addByteColumn(CATALOG_COL);

  final static PostgresTable FILE_METADATA_TABLE =
      new PostgresTable("MS_FILE_METADATA")
        .addColumn("file_ids", "bytea", true)
        .addByteColumn(CATALOG_COL)
        .addByteColumn(STATS_COL);
        */

  // False positives are very bad here because they cause us to invalidate entries we shouldn't.
  // Space used and # of hash functions grows in proportion to ln of num bits so a 10x increase
  // in accuracy doubles the required space and number of hash functions.
  //private final static double STATS_BF_ERROR_RATE = 0.001;

  static final private Logger LOG = LoggerFactory.getLogger(PostgresKeyValue.class.getName());

  // This is implemented by a ConcurrentHashMap so it can be simultaneously accessed by multiple
  // threads.  The key is expected to be the partition table name, that is
  // "parttable_<dbname>_<tablename>"
  private static Map<String, PostgresTable> partitionTables = new ConcurrentHashMap<>();

  private static DataSource connPool;
  private static boolean tablesCreated = false;

  private Configuration conf;
  private Connection currentConnection;

  private Map<String, Database> dbCache;
  private Map<ObjectPair<String, String>, Table> tableCache;
  private Map<List<String>, Partition> partCache;

  /**
   * Set the configuration for all HBaseReadWrite instances.
   * @param configuration Configuration object
   */
  public void setConf(Configuration configuration) {
    conf = configuration;
  }

  public Configuration getConf() {
    return conf;
  }

  public PostgresKeyValue() {
  }

  /**********************************************************************************************
   * Methods to connect to Postgres and make sure all of the appropriate tables exist.
   *********************************************************************************************/
  private void init() {
    if (partCache == null) {
      try {
        connectToPostgres();

        dbCache = new HashMap<>(5);
        tableCache = new HashMap<>(20);
        partCache = new HashMap<>(1000);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  private void connectToPostgres() throws IOException {
    if (connPool != null) return;

    synchronized (PostgresKeyValue.class) {
      LOG.info("Connecting to Postgres");
      String driverUrl = HiveConf.getVar(conf, HiveConf.ConfVars.METASTORECONNECTURLKEY);
      String user = HiveConf.getVar(conf, HiveConf.ConfVars.METASTORE_CONNECTION_USER_NAME);
      String passwd =
          ShimLoader.getHadoopShims().getPassword(conf, HiveConf.ConfVars.METASTOREPWD.varname);
      String connectionPooler =
          HiveConf.getVar(conf, HiveConf.ConfVars.METASTORE_CONNECTION_POOLING_TYPE).toLowerCase();

      if ("bonecp".equals(connectionPooler)) {
        BoneCPConfig config = new BoneCPConfig();
        config.setJdbcUrl(driverUrl);
        //if we are waiting for connection for 60s, something is really wrong
        //better raise an error than hang forever
        config.setConnectionTimeoutInMs(60000);
        config.setMaxConnectionsPerPartition(10);
        config.setPartitionCount(1);
        config.setUser(user);
        config.setPassword(passwd)
        ;
        connPool = new BoneCPDataSource(config);
      } else if ("dbcp".equals(connectionPooler)) {
        ObjectPool objectPool = new GenericObjectPool();
        ConnectionFactory connFactory = new DriverManagerConnectionFactory(driverUrl, user, passwd);
        // This doesn't get used, but it's still necessary, see
        // http://svn.apache.org/viewvc/commons/proper/dbcp/branches/DBCP_1_4_x_BRANCH/doc/ManualPoolingDataSourceExample.java?view=markup
        PoolableConnectionFactory poolConnFactory =
            new PoolableConnectionFactory(connFactory, objectPool, null, null, false, true);
        connPool = new PoolingDataSource(objectPool);
      } else if ("hikaricp".equals(connectionPooler)) {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(driverUrl);
        config.setUsername(user);
        config.setPassword(passwd);

        connPool = new HikariDataSource(config);
      } /*else if ("none".equals(connectionPooler)) {
      LOG.info("Choosing not to pool JDBC connections");
      connPool = new NoPoolConnectionPool(conf);
    } */ else {
        throw new RuntimeException("Unknown JDBC connection pooling " + connectionPooler);
      }
    }
  }

  // Synchronize this so not everyone's doing it at once.  This should only be called by begin()
  private void createTablesIfNotExist() throws SQLException {
    assert currentConnection != null;
    if (!tablesCreated) {
      synchronized (PostgresKeyValue.class) {
        if (!tablesCreated) { // check again, someone else might have done it while we waited.
          boolean commit = false;
          try {
            // First, figure out if they are already there
            DatabaseMetaData dmd = currentConnection.getMetaData();
            ResultSet rs =
                dmd.getTables(currentConnection.getCatalog(), currentConnection.getSchema(),
                    TABLE_TABLE.getName().toLowerCase(), null);
            if (rs.next()) {
              LOG.debug("Tables have already been created, not creating.");
              commit = true;
              return; // We've already created the tables.
            }
            LOG.info("Tables not found in catalog " + currentConnection.getCatalog() + ", schema " +
                currentConnection.getSchema() + ", creating them...");
            for (PostgresTable table : initialPostgresTables) {
              createTableInPostgres(table);
            }
            LOG.info("Done creating tables");
            tablesCreated = true;
            commit = true;
          } finally {
            if (commit) {
              commit();
              beginInternal(); // We need to open a new connection so the next call has one.
            }
            else rollback();
          }
        }
      }
    }
  }

  private void createTableInPostgres(PostgresTable table) throws SQLException {
    StringBuilder buf = new StringBuilder("create table ")
        .append(table.getName())
        .append(" (");
    boolean first = true;
    for (PostgresColumn col : table.getCols()) {
      if (first) first = false;
      else buf.append(", ");
      buf.append(col.getName())
          .append(' ')
          .append(col.getType());
    }
    if (table.hasPrimaryKey()) {
      buf.append(", primary key (");
      first = true;
      for (String pkCol : table.getPrimaryKeyCols()) {
        if (first) first = false;
        else buf.append(", ");
        buf.append(pkCol);
      }
      buf.append(')');
    }
    buf.append(')');
    try (Statement stmt = currentConnection.createStatement()) {
      if (LOG.isInfoEnabled()) {
        LOG.info("Creating table with statement <" + buf.toString() + ">");
      }
      stmt.execute(buf.toString());
    }

  }

  /**********************************************************************************************
   * Transaction related methods
   *********************************************************************************************/

  /**
   * Begin a transaction
   */
  public void begin() throws SQLException{
    assert currentConnection == null;
    // We can't connect to postgres in the constructor because we don't have the configuration
    // file yet at that point.  So instead always check here in the begin to see if we've connected.
    init();
    beginInternal();
    createTablesIfNotExist();
  }

  private void beginInternal() throws SQLException {
    int rc = 10;
    while (true) {
      try {
        currentConnection = connPool.getConnection();
        currentConnection.setAutoCommit(false);
        currentConnection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
        return;
      } catch (SQLException e){
        if (currentConnection != null) currentConnection.close();
        if ((--rc) <= 0) throw e;
        LOG.error("There is a problem with a connection from the pool, retrying(rc=" + rc + "): ", e);
      }
    }
  }

  /**
   * Commit a transaction
   */
  public void commit() throws SQLException {
    assert currentConnection != null;
    currentConnection.commit();
    currentConnection.close();
    currentConnection = null;
  }

  public void rollback() throws SQLException {
    if (currentConnection != null) {
      currentConnection.rollback();
      currentConnection.close();
      currentConnection = null;
    }
  }

  public void close() throws SQLException {
    if (currentConnection != null) rollback();
  }

  /**********************************************************************************************
   * Database related methods
   *********************************************************************************************/

  /**
   * Fetch a database object
   * @param name name of the database to fetch
   * @return the database object, or null if there is no such database
   * @throws SQLException
   */
  public Database getDb(String name) throws SQLException {
    Database db = dbCache.get(name);
    if (db == null) {
      byte[] serialized = getByKey(DB_TABLE, CATALOG_COL, name);
      if (serialized == null) return null;
      db = new Database();
      deserialize(db, serialized);
      dbCache.put(name, db);
    }
    return db;
  }

  /**
   * Get a list of databases.
   * @param regex Regular expression to use in searching for database names.  It is expected to
   *              be a Java regular expression.  If it is null then all databases will be returned.
   * @return list of databases matching the regular expression.
   * @throws SQLException
   */
  public List<Database> scanDatabases(String regex) throws SQLException {
    List<byte[]> results = scanOnKeyWithRegex(DB_TABLE, CATALOG_COL, regex);
    List<Database> dbs = new ArrayList<>();
    for (byte[] serialized : results) {
      Database db = new Database();
      deserialize(db, serialized);
      dbs.add(db);
    }
    return dbs;
  }

  /**
   * Store a database object
   * @param database database object to store
   * @throws SQLException
   */
  public void putDb(Database database) throws SQLException {
    byte[] serialized = serialize(database);
    storeOnKey(DB_TABLE, CATALOG_COL, serialized, database.getName());
    dbCache.put(database.getName(), database);
  }

  /**
   * Drop a database
   * @param name name of db to drop
   * @throws SQLException
   */
  public void deleteDb(String name) throws SQLException {
    deleteOnKey(DB_TABLE, name);
    dbCache.remove(name);
  }

  public int getDatabaseCount() throws SQLException {
    return (int)getCount(DB_TABLE);
  }

  /**********************************************************************************************
   * Partition related methods
   *********************************************************************************************/

  /**
   * Fetch one partition
   * @param dbName database table is in
   * @param tableName table partition is in
   * @param partVals list of values that specify the partition, given in the same order as the
   *                 columns they belong to
   * @return The partition objec,t or null if there is no such partition
   * @throws SQLException
   */
  public Partition getPartition(String dbName, String tableName, List<String> partVals)
      throws SQLException {
    List<String> partCacheKey = buildPartCacheKey(dbName, tableName, partVals);
    Partition part = partCache.get(partCacheKey);
    if (part == null) {
      Table hTable = getTable(dbName, tableName);
      PostgresTable pTable = findPartitionTable(dbName, tableName);
      byte[] serialized =
          getPartitionByKey(pTable, CATALOG_COL, translatePartVals(hTable, partVals));
      if (serialized == null) return null;
      part = new Partition();
      deserialize(part, serialized);
      partCache.put(partCacheKey, part);
    }
    return part;
  }

  /**
   * Add a partition.  This should only be called for new partitions.  For altering existing
   * partitions this should not be called as it will blindly increment the ref counter for the
   * storage descriptor.
   * @param partition partition object to add
   * @throws SQLException
   */
  public void putPartition(Partition partition) throws SQLException {
    /*
    byte[] hash = putStorageDescriptor(partition.getSd());
    byte[][] serialized = HBaseUtils.serializePartition(partition,
        HBaseUtils.getPartitionKeyTypes(getTable(partition.getDbName(), partition.getTableName()).getPartitionKeys()), hash);
    store(PART_TABLE, serialized[0], CATALOG_CF, CATALOG_COL, serialized[1]);
    partCache.put(partition.getDbName(), partition.getTableName(), partition);
    */
  }

  /**
   * Find all the partitions in a table.
   * @param dbName name of the database the table is in
   * @param tableName table name
   * @param maxPartitions max partitions to fetch.  If negative all partitions will be returned.
   * @return List of partitions that match the criteria.
   * @throws SQLException
   */
  public List<Partition> scanPartitionsInTable(String dbName, String tableName, int maxPartitions)
      throws SQLException {
    /*
    if (maxPartitions < 0) maxPartitions = Integer.MAX_VALUE;
    Collection<Partition> cached = partCache.getAllForTable(dbName, tableName);
    if (cached != null) {
      return maxPartitions < cached.size()
          ? new ArrayList<>(cached).subList(0, maxPartitions)
          : new ArrayList<>(cached);
    }
    byte[] keyPrefix = HBaseUtils.buildPartitionKey(dbName, tableName, new ArrayList<String>(),
        new ArrayList<String>(), false);
    List<Partition> parts = scanPartitionsWithFilter(dbName, tableName, keyPrefix,
        HBaseUtils.getEndPrefix(keyPrefix), -1, null);
    partCache.put(dbName, tableName, parts, true);
    return maxPartitions < parts.size() ? parts.subList(0, maxPartitions) : parts;
    */
    return null;
  }


  public int getPartitionCount() throws SQLException {
    /*
    Filter fil = new FirstKeyOnlyFilter();
    Iterator<Result> iter = scan(PART_TABLE, fil);
    return Iterators.size(iter);
    */
    return 0;
  }

  private byte[] getPartitionByKey(PostgresTable table, String colName, List<Object> partVals)
      throws SQLException {
    List<String> pkCols = table.getPrimaryKeyCols();
    assert pkCols.size() == 2 + partVals.size();
    StringBuilder buf = new StringBuilder("select ")
        .append(colName)
        .append(" from ")
        .append(table.getName())
        .append(" where ");
    for (int i = 0; i < partVals.size(); i++) {
      if (i != 0) buf.append(" and ");
      buf.append(pkCols.get(i))
          .append(" = ? ");
    }
    PreparedStatement stmt = currentConnection.prepareStatement(buf.toString());
    for (int i = 0; i < partVals.size(); i++) {
      stmt.setObject(i + 1, partVals.get(i));
    }
    ResultSet rs = stmt.executeQuery();
    if (rs.next()) {
      return rs.getBytes(1);
    } else {
      return null;
    }

  }

  // Figure out if we alrady have a table for this partition.  If so, return it.  Otherwise
  // create one.
  private PostgresTable findPartitionTable(String dbName, String tableName) throws SQLException {
    String partTableName = buildPartTableName(dbName, tableName);
    PostgresTable table = partitionTables.get(partTableName);
    if (table == null) {
      // Check to see if the table exists but we don't have it in the cache.  I don't put this
      // part in the synchronized section because it's ok if two threads do this at the same time,
      // if a bit wasteful.
      DatabaseMetaData dmd = currentConnection.getMetaData();
      ResultSet rs = dmd.getTables(currentConnection.getCatalog(), currentConnection.getSchema(),
          partTableName, null);
      if (rs.next()) {
        // So we know the table is there, we understand the mapping so we can build the
        // PostgresTable object.
        table = postgresTableFromPartition(dbName, tableName);
        // Put it in the map.  It's ok if we're in a race condition and someone else already has.
        partitionTables.put(partTableName, table);
      } else {
        // The table isn't there.  We need to lock because we don't want a race condition on
        // creating the table.
        synchronized (PostgresKeyValue.class) {
          // check again, someone may have built it while we waited for the lock.
          table = partitionTables.get(partTableName);
          if (table == null) {
            table = postgresTableFromPartition(dbName, tableName);
            createTableInPostgres(table);
          }
        }
      }
    }
    return table;
  }

  private PostgresTable postgresTableFromPartition(String dbName, String tableName) throws
      SQLException {
    PostgresTable pTable = new PostgresTable(buildPartTableName(dbName, tableName));
    Table hTable = getTable(dbName, tableName);
    for (FieldSchema fs : hTable.getPartitionKeys()) {
      pTable.addColumn(fs.getName(), hiveToPostgresType(fs.getType()), true);
    }
    pTable.addByteColumn(CATALOG_COL)
        .addByteColumn(STATS_COL);
    return pTable;
  }

  private List<Object> translatePartVals(Table hTable, List<String> partVals) {
    List<Object> translated = new ArrayList<>(partVals.size());
    List<FieldSchema> partCols = hTable.getPartitionKeys();
    assert partCols.size() == partVals.size();
    for (int i = 0; i < partVals.size(); i++) {
      if (partCols.get(i).getType().equals("string")) {
        translated.add(partVals.get(i));
      } else {
        throw new RuntimeException("Unknown translation " + partCols.get(i).getType());
      }
    }
    return translated;
  }

  private String hiveToPostgresType(String hiveType) {
    if (hiveType.equals("string")) {
      return "varchar(1024)";
    } else {
      throw new RuntimeException("Unknown translation " + hiveType);
    }
  }

  private String buildPartTableName(String dbName, String tableName) {
    return "parttable_" + dbName + "_" + tableName;
  }

  private List<String> buildPartCacheKey(String dbName, String tableName, List<String> partVals) {
    List<String> key = new ArrayList<>(2 + partVals.size());
    key.add(dbName);
    key.add(tableName);
    key.addAll(partVals);
    return key;

  }

  /**********************************************************************************************
   * Table related methods
   *********************************************************************************************/

  /**
   * Fetch a table object
   * @param dbName database the table is in
   * @param tableName table name
   * @return Table object, or null if no such table
   * @throws SQLException
   */
  public Table getTable(String dbName, String tableName) throws SQLException {
    ObjectPair<String, String> tableNameObj = new ObjectPair<>(dbName, tableName);
    Table table = tableCache.get(tableNameObj);
    if (table == null) {
      byte[] serialized = getByKey(TABLE_TABLE, CATALOG_COL, dbName, tableName);
      if (serialized == null) return null;
      table = new Table();
      deserialize(table, serialized);
      tableCache.put(tableNameObj, table);
    }
    return table;
  }

  /**
   * Get a list of tables.
   * @param dbName Database these tables are in
   * @param regex Regular expression to use in searching for table names.  It is expected to
   *              be a Java regular expression.  If it is null then all tables in the indicated
   *              database will be returned.
   * @return list of tables matching the regular expression.
   * @throws SQLException
   */
  public List<Table> scanTables(String dbName, String regex) throws SQLException {
    List<byte[]> results = scanOnKeyWithRegex(TABLE_TABLE, CATALOG_COL, regex, dbName);
    List<Table> tables = new ArrayList<>();
    for (byte[] serialized : results) {
      Table table = new Table();
      deserialize(table, serialized);
      tables.add(table);
    }
    return tables;
  }

  /**
   * Put a table object.
   * @param table table object
   * @throws SQLException
   */
  public void putTable(Table table) throws SQLException {
    storeOnKey(TABLE_TABLE, CATALOG_COL, serialize(table), table.getDbName(), table.getTableName());
    tableCache.put(new ObjectPair<>(table.getDbName(), table.getTableName()), table);
  }

  /**
   * Delete a table
   * @param dbName name of database table is in
   * @param tableName table to drop
   * @throws SQLException
   */
  public void deleteTable(String dbName, String tableName) throws SQLException {
    deleteOnKey(TABLE_TABLE, dbName, tableName);
    tableCache.remove(new ObjectPair<>(dbName, tableName));
  }

  /**
   * Print out a table.
   * @param name The name for the table.  This must include dbname.tablename
   * @return string containing the table
   * @throws SQLException
   * @throws TException
   */
  public String printTable(String name) throws IOException, TException {
    // TODO
    /*
    byte[] key = HBaseUtils.buildKey(name);
    @SuppressWarnings("deprecation")
    HTableInterface htab = conn.getHBaseTable(TABLE_TABLE);
    Get g = new Get(key);
    g.addColumn(CATALOG_CF, CATALOG_COL);
    g.addFamily(STATS_CF);
    Result result = htab.get(g);
    if (result.isEmpty()) return noSuch(name, "table");
    return printOneTable(result);
    */
    return null;
  }

  public int getTableCount() throws SQLException {
    return (int)getCount(TABLE_TABLE);
  }


  /**********************************************************************************************
   * Statistics related methods
   *********************************************************************************************/

  /**
   * Update statistics for one or more columns for a table or a partition.
   *
   * @param dbName database the table is in
   * @param tableName table to update statistics for
   * @param partVals partition values that define partition to update statistics for. If this is
   *          null, then these will be assumed to be table level statistics
   * @param stats Stats object with stats for one or more columns
   * @throws SQLException
   */
  public void updateStatistics(String dbName, String tableName, List<String> partVals,
      ColumnStatistics stats) throws SQLException {
    /*
    byte[] key = getStatisticsKey(dbName, tableName, partVals);
    String hbaseTable = getStatisticsTable(partVals);
    byte[][] colnames = new byte[stats.getStatsObjSize()][];
    byte[][] serialized = new byte[stats.getStatsObjSize()][];
    for (int i = 0; i < stats.getStatsObjSize(); i++) {
      ColumnStatisticsObj obj = stats.getStatsObj().get(i);
      serialized[i] = HBaseUtils.serializeStatsForOneColumn(stats, obj);
      String colname = obj.getColName();
      colnames[i] = HBaseUtils.buildKey(colname);
    }
    store(hbaseTable, key, STATS_CF, colnames, serialized);
    */
  }

  /**
   * Get statistics for a table
   *
   * @param dbName name of database table is in
   * @param tblName name of table
   * @param colNames list of column names to get statistics for
   * @return column statistics for indicated table
   * @throws IOException
   */
  public ColumnStatistics getTableStatistics(String dbName, String tblName, List<String> colNames)
      throws IOException {
    /*
    byte[] tabKey = HBaseUtils.buildKey(dbName, tblName);
    ColumnStatistics tableStats = new ColumnStatistics();
    ColumnStatisticsDesc statsDesc = new ColumnStatisticsDesc();
    statsDesc.setIsTblLevel(true);
    statsDesc.setDbName(dbName);
    statsDesc.setTableName(tblName);
    tableStats.setStatsDesc(statsDesc);
    byte[][] colKeys = new byte[colNames.size()][];
    for (int i = 0; i < colKeys.length; i++) {
      colKeys[i] = HBaseUtils.buildKey(colNames.get(i));
    }
    Result result = read(TABLE_TABLE, tabKey, STATS_CF, colKeys);
    for (int i = 0; i < colKeys.length; i++) {
      byte[] serializedColStats = result.getValue(STATS_CF, colKeys[i]);
      if (serializedColStats == null) {
        // There were no stats for this column, so skip it
        continue;
      }
      ColumnStatisticsObj obj =
          HBaseUtils.deserializeStatsForOneColumn(tableStats, serializedColStats);
      obj.setColName(colNames.get(i));
      tableStats.addToStatsObj(obj);
    }
    return tableStats;
    */
    return null;
  }

  /**
   * Get statistics for a set of partitions
   *
   * @param dbName name of database table is in
   * @param tblName table partitions are in
   * @param partNames names of the partitions, used only to set values inside the return stats
   *          objects
   * @param partVals partition values for each partition, needed because this class doesn't know how
   *          to translate from partName to partVals
   * @param colNames column names to fetch stats for. These columns will be fetched for all
   *          requested partitions
   * @return list of ColumnStats, one for each partition for which we found at least one column's
   * stats.
   * @throws IOException
   */
  public List<ColumnStatistics> getPartitionStatistics(String dbName, String tblName,
      List<String> partNames, List<List<String>> partVals, List<String> colNames)
      throws IOException {
    /*
    List<ColumnStatistics> statsList = new ArrayList<>(partNames.size());
    Map<List<String>, String> valToPartMap = new HashMap<>(partNames.size());
    List<Get> gets = new ArrayList<>(partNames.size() * colNames.size());
    assert partNames.size() == partVals.size();

    byte[][] colNameBytes = new byte[colNames.size()][];
    for (int i = 0; i < colNames.size(); i++) {
      colNameBytes[i] = HBaseUtils.buildKey(colNames.get(i));
    }

    for (int i = 0; i < partNames.size(); i++) {
      valToPartMap.put(partVals.get(i), partNames.get(i));
      byte[] partKey = HBaseUtils.buildPartitionKey(dbName, tblName,
          HBaseUtils.getPartitionKeyTypes(getTable(dbName, tblName).getPartitionKeys()),
          partVals.get(i));
      Get get = new Get(partKey);
      for (byte[] colName : colNameBytes) {
        get.addColumn(STATS_CF, colName);
      }
      gets.add(get);
    }

    HTableInterface htab = conn.getHBaseTable(PART_TABLE);
    Result[] results = htab.get(gets);
    for (int i = 0; i < results.length; i++) {
      ColumnStatistics colStats = null;
      for (int j = 0; j < colNameBytes.length; j++) {
        byte[] serializedColStats = results[i].getValue(STATS_CF, colNameBytes[j]);
        if (serializedColStats != null) {
          if (colStats == null) {
            // We initialize this late so that we don't create extras in the case of
            // partitions with no stats
            colStats = buildColStats(results[i].getRow(), false);
            statsList.add(colStats);
        }
          ColumnStatisticsObj cso =
              HBaseUtils.deserializeStatsForOneColumn(colStats, serializedColStats);
          cso.setColName(colNames.get(j));
          colStats.addToStatsObj(cso);
        }
      }
    }

    return statsList;
    */
    return null;
  }

  /**
   * Get a reference to the stats cache.
   * @return the stats cache.
   */
  /*
  public StatsCache getStatsCache() {
    //return statsCache;
    return null;
  }
  */

  /**
   * Get aggregated stats.  Only intended for use by
   * {@link org.apache.hadoop.hive.metastore.hbase.StatsCache}.  Others should not call directly
   * but should call StatsCache.get instead.
   * @param key The md5 hash associated with this partition set
   * @return stats if hbase has them, else null
   * @throws IOException
   */
  public AggrStats getAggregatedStats(byte[] key) throws IOException{
    /*
    byte[] serialized = read(AGGR_STATS_TABLE, key, CATALOG_CF, AGGR_STATS_STATS_COL);
    if (serialized == null) return null;
    return HBaseUtils.deserializeAggrStats(serialized);
    */
    return null;

  }

  /**
   * Put aggregated stats  Only intended for use by
   * {@link org.apache.hadoop.hive.metastore.hbase.StatsCache}.  Others should not call directly
   * but should call StatsCache.put instead.
   * @param key The md5 hash associated with this partition set
   * @param dbName Database these partitions are in
   * @param tableName Table these partitions are in
   * @param partNames Partition names
   * @param colName Column stats are for
   * @param stats Stats
   * @throws IOException
   */
  public void putAggregatedStats(byte[] key, String dbName, String tableName, List<String> partNames,
                          String colName, AggrStats stats) throws IOException {
    /*
    // Serialize the part names
    List<String> protoNames = new ArrayList<>(partNames.size() + 3);
    protoNames.add(dbName);
    protoNames.add(tableName);
    protoNames.add(colName);
    protoNames.addAll(partNames);
    // Build a bloom Filter for these partitions
    BloomFilter bloom = new BloomFilter(partNames.size(), STATS_BF_ERROR_RATE);
    for (String partName : partNames) {
      bloom.add(partName.getBytes(HBaseUtils.ENCODING));
    }
    byte[] serializedFilter = HBaseUtils.serializeBloomFilter(dbName, tableName, bloom);

    byte[] serializedStats = HBaseUtils.serializeAggrStats(stats);
    store(AGGR_STATS_TABLE, key, CATALOG_CF,
        new byte[][]{AGGR_STATS_BLOOM_COL, AGGR_STATS_STATS_COL},
        new byte[][]{serializedFilter, serializedStats});
    */
  }

  // TODO - We shouldn't remove an entry from the cache as soon as a single partition is deleted.
  // TODO - Instead we should keep track of how many partitions have been deleted and only remove
  // TODO - an entry once it passes a certain threshold, like 5%, of partitions have been removed.
  // TODO - That requires moving this from a filter to a co-processor.
  /**
   * Invalidate stats associated with the listed partitions.  This method is intended for use
   * only by {@link org.apache.hadoop.hive.metastore.hbase.StatsCache}.
   * @param filter serialized version of the filter to pass
   * @return List of md5 hash keys for the partition stat sets that were removed.
   * @throws IOException
   */
    /*
  public List<StatsCache.StatsCacheKey>
  invalidateAggregatedStats(HbaseMetastoreProto.AggrStatsInvalidatorFilter filter)
      throws IOException {
    Iterator<Result> results = scan(AGGR_STATS_TABLE, new AggrStatsInvalidatorFilter(filter));
    if (!results.hasNext()) return Collections.emptyList();
    List<Delete> deletes = new ArrayList<>();
    List<StatsCache.StatsCacheKey> keys = new ArrayList<>();
    while (results.hasNext()) {
      Result result = results.next();
      deletes.add(new Delete(result.getRow()));
      keys.add(new StatsCache.StatsCacheKey(result.getRow()));
    }
    HTableInterface htab = conn.getHBaseTable(AGGR_STATS_TABLE);
    htab.delete(deletes);
    return keys;
  }
    */

  /*
  private byte[] getStatisticsKey(String dbName, String tableName, List<String> partVals) throws IOException {
    return partVals == null ? HBaseUtils.buildKey(dbName, tableName) : HBaseUtils
        .buildPartitionKey(dbName, tableName,
            HBaseUtils.getPartitionKeyTypes(getTable(dbName, tableName).getPartitionKeys()),
            partVals);
  }

  private String getStatisticsTable(List<String> partVals) {
    return partVals == null ? TABLE_TABLE : PART_TABLE;
  }

  private ColumnStatistics buildColStats(byte[] key, boolean fromTable) throws IOException {
    // We initialize this late so that we don't create extras in the case of
    // partitions with no stats
    ColumnStatistics colStats = new ColumnStatistics();
    ColumnStatisticsDesc csd = new ColumnStatisticsDesc();

    // If this is a table key, parse it as one
    List<String> reconstructedKey;
    if (fromTable) {
      reconstructedKey = Arrays.asList(HBaseUtils.deserializeKey(key));
      csd.setIsTblLevel(true);
    } else {
      reconstructedKey = HBaseUtils.deserializePartitionKey(key, this);
      csd.setIsTblLevel(false);
    }
    csd.setDbName(reconstructedKey.get(0));
    csd.setTableName(reconstructedKey.get(1));
    if (!fromTable) {
      // Build the part name, for which we need the table
      Table table = getTable(reconstructedKey.get(0), reconstructedKey.get(1));
      if (table == null) {
        throw new RuntimeException("Unable to find table " + reconstructedKey.get(0) + "." +
            reconstructedKey.get(1) + " even though I have a partition for it!");
      }
      csd.setPartName(HBaseStore.buildExternalPartName(table, reconstructedKey.subList(2,
          reconstructedKey.size())));
    }
    colStats.setStatsDesc(csd);
    return colStats;
  }


  /**********************************************************************************************
   * Constraints (pk/fk) related methods
   *********************************************************************************************/

  /**
   * Fetch a primary key
   * @param dbName database the table is in
   * @param tableName table name
   * @return List of primary key objects, which together make up one key
   * @throws IOException if there's a read error
   */
  public List<SQLPrimaryKey> getPrimaryKey(String dbName, String tableName) throws IOException {
    /*
    byte[] key = HBaseUtils.buildKey(dbName, tableName);
    byte[] serialized = read(TABLE_TABLE, key, CATALOG_CF, PRIMARY_KEY_COL);
    if (serialized == null) return null;
    return HBaseUtils.deserializePrimaryKey(dbName, tableName, serialized);
    */
    return null;
  }

  /**
   * Fetch a the foreign keys for a table
   * @param dbName database the table is in
   * @param tableName table name
   * @return All of the foreign key columns thrown together in one list.  Have fun sorting them out.
   * @throws IOException if there's a read error
   */
  public List<SQLForeignKey> getForeignKeys(String dbName, String tableName) throws IOException {
    /*
    byte[] key = HBaseUtils.buildKey(dbName, tableName);
    byte[] serialized = read(TABLE_TABLE, key, CATALOG_CF, FOREIGN_KEY_COL);
    if (serialized == null) return null;
    return HBaseUtils.deserializeForeignKeys(dbName, tableName, serialized);
    */
    return null;
  }

  /**
   * Create a primary key on a table.
   * @param pk Primary key for this table
   * @throws IOException if unable to write the data to the store.
   */
  public void putPrimaryKey(List<SQLPrimaryKey> pk) throws IOException {
    /*
    byte[][] serialized = HBaseUtils.serializePrimaryKey(pk);
    store(TABLE_TABLE, serialized[0], CATALOG_CF, PRIMARY_KEY_COL, serialized[1]);
    */
  }

  /**
   * Create one or more foreign keys on a table.  Note that this will not add a foreign key, it
   * will overwrite whatever is there.  So if you wish to add a key to a table that may already
   * foreign keys you need to first use {@link #getForeignKeys(String, String)} to fetch the
   * existing keys, add to the list, and then call this.
   * @param fks Foreign key(s) for this table
   * @throws IOException if unable to write the data to the store.
   */
  public void putForeignKeys(List<SQLForeignKey> fks) throws IOException {
    /*
    byte[][] serialized = HBaseUtils.serializeForeignKeys(fks);
    store(TABLE_TABLE, serialized[0], CATALOG_CF, FOREIGN_KEY_COL, serialized[1]);
    */
  }

  /**
   * Drop the primary key from a table.
   * @param dbName database the table is in
   * @param tableName table name
   * @throws IOException if unable to delete from the store
   */
  public void deletePrimaryKey(String dbName, String tableName) throws IOException {
    /*
    byte[] key = HBaseUtils.buildKey(dbName, tableName);
    delete(TABLE_TABLE, key, CATALOG_CF, PRIMARY_KEY_COL);
    */
  }

  /**
   * Drop all foreign keys from a table.  Note that this will drop all keys blindly.  You should
   * only call this if you're sure you want to drop them all.  If you just want to drop one you
   * should instead all {@link #getForeignKeys(String, String)}, modify the list it returns, and
   * then call {@link #putForeignKeys(List)}.
   * @param dbName database the table is in
   * @param tableName table name
   * @throws IOException if unable to delete from the store
   */
  public void deleteForeignKeys(String dbName, String tableName) throws IOException {
    /*
    byte[] key = HBaseUtils.buildKey(dbName, tableName);
    delete(TABLE_TABLE, key, CATALOG_CF, FOREIGN_KEY_COL);
    */
  }

  /**********************************************************************************************
   * Cache methods
   *********************************************************************************************/

  /**
   * This should be called whenever a new query is started.
   */
  public void flushCatalogCache() {
    dbCache.clear();
    tableCache.clear();
    partCache.clear();
  }

  /**********************************************************************************************
   * General access methods
   *********************************************************************************************/

  /**
   * Read one binary column in one record using the key.
   * @param table table to read from
   * @param colName column name to read
   * @param keyVals 1 or more key values.  This must match the number of items in the primary key.
   * @return serialized version of the object, or null if there is no matching version.
   * @throws IOException wraps any SQLExceptions thrown by the database
   */
  private byte[] getByKey(PostgresTable table, String colName, String... keyVals)
      throws SQLException {
    List<String> pkCols = table.getPrimaryKeyCols();
    assert pkCols.size() == keyVals.length;
    try (Statement stmt = currentConnection.createStatement()) {
      StringBuilder buf = new StringBuilder("select ")
          .append(colName)
          .append(" from ")
          .append(table.getName())
          .append(" where ");
      for (int i = 0; i < keyVals.length; i++) {
        if (i != 0) buf.append(" and ");
        buf.append(pkCols.get(i))
            .append(" = '")
            .append(keyVals[i])
            .append('\'');
      }
      ResultSet rs = stmt.executeQuery(buf.toString());
      if (rs.next()) {
        return rs.getBytes(1);
      } else {
        return null;
      }
    }

  }

  /**
   * Scan a table, getting entries for one binary column.  This can can be done with a
   * regular expression and with key values. If keyVals
   * is provided it must be a proper prefix of the primary key of the table.  If regex is
   * provided, it will be used to filter the next key components after keyVals.
   * @param table table to read from
   * @param colName column to scan
   * @param regex regular expression to use on key elements after keyVals.  Can be null, in which
   *              case no regular expression will be applied.
   * @param keyVals proper subset of key, these values will be matched exactly.  Can be null if
   *                there are not key elements to match exactly.
   * @return list of serialized objects retrieved by the scan.  If no objects match, the list
   * will be empty.
   * @throws IOException wraps any SQLException
   */
  private List<byte[]> scanOnKeyWithRegex(PostgresTable table, String colName, String regex,
                                          String... keyVals) throws SQLException {
    try (Statement stmt = currentConnection.createStatement()) {
      List<byte[]> results = new ArrayList<>();
      StringBuilder buf = new StringBuilder("select ")
          .append(colName)
          .append(" from ")
          .append(table.getName());
      if (regex != null || keyVals != null) {
        buf.append("where ");
        boolean first = true;
        int keyColNum = 0;
        List<String> keyCols = table.getPrimaryKeyCols();
        if (keyVals != null) {
          for (String keyVal : keyVals) {
            if (first) first = false;
            else buf.append(" and ");
            buf.append(keyCols.get(keyColNum++))
                .append(" = '")
                .append(keyVal)
                .append("' ");
          }
        }
        if (regex != null) {
          if (first) first = false;
          else buf.append(" and ");
          buf.append(keyCols.get(keyColNum++))
              .append(" ~ '") // postgress regex operator
              .append(regex)
              .append('\'');
        }
      }
      ResultSet rs = stmt.executeQuery(buf.toString());
      while (rs.next()) {
        results.add(rs.getBytes(1));
      }
      return results;
    }
  }

  /**
   * Store a record with one column using the primary key.  This can be used to do both insert
   * and update.
   * @param table table to store record in
   * @param colName column name to store into
   * @param colVal value to set the column to
   * @param keyVals list of values for the primary key.  This must match the number of columns in
   *                the primary key.
   * @throws SQLException wraps any SQLExceptions that happen
   */
  private void storeOnKey(PostgresTable table, String colName, byte[] colVal, String... keyVals)
      throws SQLException {
    List<String> pkCols = table.getPrimaryKeyCols();
    assert pkCols.size() == keyVals.length;
    byte[] serialized = getByKey(table, colName, keyVals);
    if (serialized == null) insertOnKey(table, colName, colVal, keyVals);
    else updateOnKey(table, colName, colVal, keyVals);
  }

  // Do not call directly, use storeOnKey
  private void insertOnKey(PostgresTable table, String colName, byte[] colVal, String... keyVals)
      throws SQLException {
    List<String> pkCols = table.getPrimaryKeyCols();
    StringBuilder buf = new StringBuilder("insert into ")
        .append(table.getName())
        .append(" (");
    for (String pkCol : pkCols) {
      buf.append(pkCol)
          .append(" ?,");
    }
    buf.append(colName)
        .append(") values (");
    for (int i = 0; i < pkCols.size(); i++) buf.append("?,");
    buf.append("?)");
    try (PreparedStatement stmt = currentConnection.prepareStatement(buf.toString())) {
      for (int i = 0; i < keyVals.length; i++) stmt.setString(i + 1, keyVals[i]);
      stmt.setBytes(keyVals.length + 2, colVal);

      int rowsInserted = stmt.executeUpdate();
      if (rowsInserted != 1) {
        String msg = "Expected to insert a row for " + serializeKey(table, keyVals) + ", but did not";
        LOG.error(msg);
        throw new RuntimeException(msg);
      }
    }
  }

  private void updateOnKey(PostgresTable table, String colName, byte[] colVal, String... keyVals)
      throws SQLException {
    List<String> pkCols = table.getPrimaryKeyCols();
    StringBuilder buf = new StringBuilder("update table ")
        .append(table.getName())
        .append(" set ")
        .append(colName)
        .append(" = ?")
        .append(" where ");
    for (int i = 0; i < keyVals.length; i++) {
      if (i != 0) buf.append(" and ");
      buf.append(pkCols.get(i))
          .append(" = '")
          .append(keyVals[i])
          .append('\'');
    }
    try (PreparedStatement stmt = currentConnection.prepareStatement(buf.toString())) {
      stmt.setBytes(1, colVal);

      int rowsUpdated = stmt.executeUpdate();
      if (rowsUpdated != 1) {
        String msg = "Expected to update a row for " + serializeKey(table, keyVals) + ", but did not";
        LOG.error(msg);
        throw new RuntimeException(msg);
      }
    }
  }

  private void deleteOnKey(PostgresTable table, String... keyVals) throws SQLException {
    List<String> pkCols = table.getPrimaryKeyCols();
    assert pkCols.size() == keyVals.length;
    StringBuilder buf = new StringBuilder("delete from ")
        .append(table.getName())
        .append(" where ");
    for (int i = 0; i < keyVals.length; i++) {
      if (i != 0) buf.append(" and ");
      buf.append(pkCols.get(0))
          .append(" = '")
          .append(keyVals[i])
          .append('\'');
    }
    try (Statement stmt = currentConnection.createStatement()) {
      int rowsDeleted = stmt.executeUpdate(buf.toString());
      if (rowsDeleted != 1) {
        String msg = "Expected to delete a row for " + serializeKey(table, keyVals) + ", but did not";
        LOG.error(msg);
        throw new RuntimeException(msg);
      }
    }
  }

  private long getCount(PostgresTable table) throws SQLException {
    StringBuilder buf = new StringBuilder("select count(*) from ")
        .append(table.getName());
    try (Statement stmt = currentConnection.createStatement()) {
      ResultSet rs = stmt.executeQuery(buf.toString());
      rs.next();
      return rs.getLong(1);
    }
  }

  /**********************************************************************************************
   * Serialization methods
   *********************************************************************************************/
  private String serializeKey(PostgresTable table, String[] keyVals) {
    List<String> pkCols = table.getPrimaryKeyCols();
    StringBuilder buf = new StringBuilder("table: ")
        .append(table.getName())
        .append(" key: (");
    for (int i = 0; i < keyVals.length; i++) {
      if (i != 0) buf.append(", ");
      buf.append(pkCols.get(i))
          .append(" = ")
          .append(keyVals[i]);
    }
    return buf.toString();
  }

  private byte[] serialize(TBase obj) {
    try {
      TMemoryBuffer buf = new TMemoryBuffer(1000);
      TProtocol protocol = new TCompactProtocol(buf);
      obj.write(protocol);
      return buf.getArray();
    } catch (TException e) {
      throw new RuntimeException(e);
    }
  }

  private void deserialize(TBase obj, byte[] serialized) {
    try {
      TMemoryBuffer buf = new TMemoryBuffer(serialized.length);
      buf.read(serialized, 0, serialized.length);
      TProtocol protocol = new TCompactProtocol(buf);
      obj.read(protocol);
    } catch (TException e) {
      throw new RuntimeException(e);
    }
  }

  private String dumpThriftObject(TBase obj) throws TException, UnsupportedEncodingException {
    TMemoryBuffer buf = new TMemoryBuffer(1000);
    TProtocol protocol = new TSimpleJSONProtocol(buf);
    obj.write(protocol);
    return buf.toString("UTF-8");
  }

  /**********************************************************************************************
   * Postgres table representation
   *********************************************************************************************/
  static class PostgresColumn {
    private final String name;
    private final String type;

    PostgresColumn(String name) {
      this(name, "bytea");
    }

    PostgresColumn(String name, String type) {
      this.name = name;
      this.type = type;
    }

    String getName() {
      return name;
    }

    String getType() {
      return type;
    }
  }

  static class PostgresTable {
    private String name;
    private List<String> pkCols;
    private Map<String, PostgresColumn> colsByName;

    // TODO sort the table names so we get consistent order of creation

    PostgresTable(String name) {
      this.name = name;
      colsByName = new HashMap<>();
      pkCols = new ArrayList<>();
    }

    PostgresTable addColumn(String name, String type, boolean isPrimaryKey) {
      colsByName.put(name, new PostgresColumn(name, type));
      if (isPrimaryKey) pkCols.add(name);
      return this;
    }

    PostgresTable addColumn(String name, String type) {
      return addColumn(name, type, false);
    }

    PostgresTable addByteColumn(String name) {
      return addColumn(name, "bytea", false);
    }

    PostgresTable addStringKey(String name) {
      return addColumn(name, "varchar(255)", true);
    }

    String getName() {
      return name;
    }

    List<String> getPrimaryKeyCols() {
      return pkCols;
    }

    Collection<PostgresColumn> getCols() {
      return colsByName.values();
    }

    boolean hasPrimaryKey() {
      return !pkCols.isEmpty();
    }

    PostgresColumn getCol(String name) {
      return colsByName.get(name);
    }

    PostgresTable addToInitialTableList() {
      initialPostgresTables.add(this);
      return this;
    }

  }

}
