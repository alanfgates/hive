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
package org.apache.hadoop.hive.metastore.hbase;

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
import org.apache.hadoop.hbase.filter.Filter;
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
import org.apache.hadoop.hive.metastore.api.Function;
import org.apache.hadoop.hive.metastore.api.Index;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PrincipalPrivilegeSet;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.Role;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TSimpleJSONProtocol;
import org.apache.thrift.transport.TMemoryBuffer;

import javax.sql.DataSource;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;


/**
 * Class to manage storing object in and reading them from HBase.
 */
public class PostgresKeyValue implements MetadataStore {
  public final static List<PostgresTable> tables = new ArrayList<>();

  // Column names used throughout
  // General column name that catalog objects are stored in.
  final static String CATALOG_COL = "catalog_object";
  // General column for storing stats
  final static String STATS_COL = "stats";


  // Define the Aggregate stats table
  final static String STATS_HASH = "stats_md5";
  final static String AGGR_STATS_BLOOM_COL = "bloom_filter";
  final static PostgresTable AGGR_STATS_TABLE =
      new PostgresTable("MS_AGGR_STATS")
        .addColumn(STATS_HASH, "bytea", true)
        .addByteColumn(STATS_COL)
        .addByteColumn(AGGR_STATS_BLOOM_COL);

  // Define the Database table
  final static PostgresTable DB_TABLE =
      new PostgresTable("MS_DBS")
        .addStringKey("db_name")
        .addByteColumn(CATALOG_COL);

  // Define the Function table
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

  final static String PRIMARY_KEY_COL = "primary_key_obj";
  final static String FOREIGN_KEY_COL = "foreign_key_obj";
  final static PostgresTable TABLE_TABLE =
      new PostgresTable("MS_TBLS")
          .addStringKey("db_name")
          .addStringKey("table_name")
          .addByteColumn(CATALOG_COL)
          .addByteColumn(STATS_COL)
          .addByteColumn(PRIMARY_KEY_COL)
          .addByteColumn(FOREIGN_KEY_COL);

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

  // False positives are very bad here because they cause us to invalidate entries we shouldn't.
  // Space used and # of hash functions grows in proportion to ln of num bits so a 10x increase
  // in accuracy doubles the required space and number of hash functions.
  private final static double STATS_BF_ERROR_RATE = 0.001;

  static final private Logger LOG = LoggerFactory.getLogger(PostgresKeyValue.class.getName());

  // This is implemented by a ConcurrentHashMap so it can be simultaneously accessed by multiple
  // threads.  The key is expected to be the partition table name, that is
  // "parttable_<dbname>_<tablename>"
  private static Map<String, PostgresTable> partitionTables = new ConcurrentHashMap<>();

  private static DataSource connPool;
  private static boolean tablesCreated = false;

  private Configuration conf;
  private MessageDigest md;
  private enum TXN_STATE { OPEN, CLOSED };
  private TXN_STATE currentTxnState;
  private int txnDepth;
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

  public PostgresKeyValue() throws IOException {
    try {
      md = MessageDigest.getInstance("MD5");
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException(e);
    }

    currentTxnState = TXN_STATE.CLOSED;
    connectToPostgres();
    createTablesIfNotExist();

    dbCache = new HashMap<>(5);
    tableCache = new HashMap<>(20);
    partCache = new HashMap<>(1000);
  }

  private void connectToPostgres() throws IOException {
    if (connPool != null) return;

    synchronized (PostgresKeyValue.class) {
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

  // Synchronize this so not everyone's doing it at once.
  private void createTablesIfNotExist() throws IOException {
    synchronized (PostgresKeyValue.class) {
      if (!tablesCreated) {
        LOG.info("Tables not yet created, creating now...");
        try {
          // First, figure out if they are already there
          Connection conn = getDbConn();
          DatabaseMetaData dmd = conn.getMetaData();
          ResultSet rs =
              dmd.getTables(conn.getCatalog(), conn.getSchema(), TABLE_TABLE.getName(), null);
          if (rs.next()) return; // We've already created the tables.
          for (PostgresTable table : tables) {
            createTableInPostgres(table);
          }
          LOG.info("Done creating tables");
          tablesCreated = true;
        } catch (SQLException e) {
          throw new IOException(e);
        }
      }
    }
  }

  private void createTableInPostgres(PostgresTable table)
      throws SQLException {
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
    Statement stmt = currentConnection.createStatement();
    if (LOG.isInfoEnabled()) {
      LOG.info("Creating table with statement <" + buf.toString() + ">");
    }
    stmt.execute(buf.toString());

  }

  private Connection getDbConn() throws IOException {
    if (currentTxnState == TXN_STATE.OPEN) {
      assert currentConnection != null;
      return currentConnection;
    }

    int rc = 10;
    Connection dbConn = null;
    while (true) {
      try {
        dbConn = connPool.getConnection();
        dbConn.setAutoCommit(false);
        dbConn.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
        return dbConn;
      } catch (SQLException e){
        closeDbConn(dbConn);
        if ((--rc) <= 0) throw new IOException(e);
        LOG.error("There is a problem with a connection from the pool, retrying(rc=" + rc + "): ", e);
      }
    }
  }

  private void closeDbConn(Connection dbConn) {
    try {
      if (dbConn != null && !dbConn.isClosed()) {
        dbConn.close();
      }
    } catch (SQLException e) {
      LOG.warn("Failed to close db connection ", e);
    }
  }

  /**********************************************************************************************
   * Transaction related methods
   *********************************************************************************************/

  /**
   * Begin a transaction
   */
  public void begin() {
    try {
      if (currentTxnState == TXN_STATE.CLOSED) {
        assert currentConnection == null;
        currentConnection = getDbConn();
        txnDepth = 0;
        currentTxnState = TXN_STATE.OPEN;
      } else {
        txnDepth++;
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Commit a transaction
   */
  public void commit() {
    try {
      assert currentTxnState == TXN_STATE.OPEN;
      if (--txnDepth > 0) return;
      assert currentConnection != null;
      currentConnection.commit();
      currentTxnState = TXN_STATE.CLOSED;
      currentConnection = null;
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  public void rollback() {
    try {
      // If it's closed that's ok, it means someone else above us has already aborted it
      if (currentTxnState == TXN_STATE.OPEN) {
        currentConnection.rollback();
        currentTxnState = TXN_STATE.CLOSED;
        currentConnection = null;
      }
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  public void close() throws IOException {
  }

  /**********************************************************************************************
   * Database related methods
   *********************************************************************************************/

  /**
   * Fetch a database object
   * @param name name of the database to fetch
   * @return the database object, or null if there is no such database
   * @throws IOException
   */
  public Database getDb(String name) throws IOException {
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
   * @throws IOException
   */
  public List<Database> scanDatabases(String regex) throws IOException {
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
   * @throws IOException
   */
  public void putDb(Database database) throws IOException {
    byte[] serialized = serialize(database);
    storeOnKey(DB_TABLE, CATALOG_COL, serialized, database.getName());
    dbCache.put(database.getName(), database);
  }

  /**
   * Drop a database
   * @param name name of db to drop
   * @throws IOException
   */
  public void deleteDb(String name) throws IOException {
    deleteOnKey(DB_TABLE, name);
    dbCache.remove(name);
  }

  /**
   * Print out the database. Intended for use by {@link org.apache.hadoop.hive.metastore.hbase.HBaseSchemaTool}
   * @param name name of database to print
   * @return string printout of database
   */
  public String printDatabase(String name) throws IOException, TException {
    Database db = getDb(name);
    if (db == null) return noSuch(name, "database");
    else return dumpThriftObject(db);
  }

  /**
   * Print out databases.
   * @param regex regular to use to search for databases
   * @return databases as a string, one each
   * @throws IOException
   * @throws TException
   */
  public List<String> printDatabases(String regex) throws IOException, TException {
    List<Database> dbs = scanDatabases(regex);
    if (dbs.size() == 0) {
      return noMatch(regex, "database");
    } else {
      List<String> lines = new ArrayList<>();
      for (Database db : dbs) lines.add(dumpThriftObject(db));
      return lines;
    }
  }

  public int getDatabaseCount() throws IOException {
    return (int)getCount(DB_TABLE);
  }

  /**********************************************************************************************
   * Function related methods
   *********************************************************************************************/

  /**
   * Fetch a function object
   * @param dbName name of the database the function is in
   * @param functionName name of the function to fetch
   * @return the function object, or null if there is no such function
   * @throws IOException
   */
  public Function getFunction(String dbName, String functionName) throws IOException {
    // TODO
    /*
    byte[] key = HBaseUtils.buildKey(dbName, functionName);
    byte[] serialized = read(FUNC_TABLE, key, CATALOG_CF, CATALOG_COL);
    if (serialized == null) return null;
    return HBaseUtils.deserializeFunction(dbName, functionName, serialized);
    */
    return null;
  }

  /**
   * Get a list of functions.
   * @param dbName Name of the database to search in.
   * @param regex Regular expression to use in searching for function names.  It is expected to
   *              be a Java regular expression.  If it is null then all functions will be returned.
   * @return list of functions matching the regular expression.
   * @throws IOException
   */
  public List<Function> scanFunctions(String dbName, String regex) throws IOException {
    // TODO
    /*
    byte[] keyPrefix = null;
    if (dbName != null) {
      keyPrefix = HBaseUtils.buildKeyWithTrailingSeparator(dbName);
    }
    Filter filter = null;
    if (regex != null) {
      filter = new RowFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator(regex));
    }
    Iterator<Result> iter =
        scan(FUNC_TABLE, keyPrefix, HBaseUtils.getEndPrefix(keyPrefix), CATALOG_CF, CATALOG_COL, filter);
    List<Function> functions = new ArrayList<>();
    while (iter.hasNext()) {
      Result result = iter.next();
      functions.add(HBaseUtils.deserializeFunction(result.getRow(),
                                                   result.getValue(CATALOG_CF, CATALOG_COL)));
    }
    return functions;
    */
    return null;
  }

  /**
   * Store a function object
   * @param function function object to store
   * @throws IOException
   */
  public void putFunction(Function function) throws IOException {
    // TODO
    /*
    byte[][] serialized = HBaseUtils.serializeFunction(function);
    store(FUNC_TABLE, serialized[0], CATALOG_CF, CATALOG_COL, serialized[1]);
    */
  }

  /**
   * Drop a function
   * @param dbName name of database the function is in
   * @param functionName name of function to drop
   * @throws IOException
   */
  public void deleteFunction(String dbName, String functionName) throws IOException {
    // TODO
    /*
    byte[] key = HBaseUtils.buildKey(dbName, functionName);
    delete(FUNC_TABLE, key, null, null);
    */
  }

  /**
   * Print out a function
   * @param key key to get the function, must include dbname.
   * @return string of the function
   * @throws IOException
   * @throws TException
   */
  public String printFunction(String key) throws IOException, TException {
    // TODO
    /*
    byte[] k = HBaseUtils.buildKey(key);
    byte[] serialized = read(FUNC_TABLE, k, CATALOG_CF, CATALOG_COL);
    if (serialized == null) return noSuch(key, "function");
    Function func = HBaseUtils.deserializeFunction(k, serialized);
    return dumpThriftObject(func);
    */
    return null;
  }

  /**
   * Print out functions
   * @param regex regular expression to use in matching functions
   * @return list of strings, one function each
   * @throws IOException
   * @throws TException
   */
  public List<String> printFunctions(String regex) throws IOException, TException {
    // TODO
    /*
    Filter  filter = new RowFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator(regex));
    Iterator<Result> iter = scan(FUNC_TABLE, null, null, CATALOG_CF, CATALOG_COL, filter);
    List<String> lines = new ArrayList<>();
    while (iter.hasNext()) {
      Result result = iter.next();
      lines.add(dumpThriftObject(HBaseUtils.deserializeFunction(result.getRow(),
          result.getValue(CATALOG_CF, CATALOG_COL))));
    }
    if (lines.size() == 0) lines = noMatch(regex, "function");
    return lines;
    */
    return null;
  }

  /**********************************************************************************************
   * Global privilege related methods
   *********************************************************************************************/

  /**
   * Fetch the global privileges object
   * @return
   * @throws IOException
   */
  public PrincipalPrivilegeSet getGlobalPrivs() throws IOException {
    // TODO
    /*
    byte[] key = GLOBAL_PRIVS_KEY;
    byte[] serialized = read(GLOBAL_PRIVS_TABLE, key, CATALOG_CF, CATALOG_COL);
    if (serialized == null) return null;
    return HBaseUtils.deserializePrincipalPrivilegeSet(serialized);
    */
    return null;
  }

  /**
   * Store the global privileges object
   * @throws IOException
   */
  public void putGlobalPrivs(PrincipalPrivilegeSet privs) throws IOException {
    // TODO
    /*
    byte[] key = GLOBAL_PRIVS_KEY;
    byte[] serialized = HBaseUtils.serializePrincipalPrivilegeSet(privs);
    store(GLOBAL_PRIVS_TABLE, key, CATALOG_CF, CATALOG_COL, serialized);
    */
  }

  /**
   * Print out the global privileges.
   * @return string containing the global privileges
   * @throws IOException
   * @throws TException
   */
  public String printGlobalPrivs() throws IOException, TException {
    // TODO
    /*
    PrincipalPrivilegeSet pps = getGlobalPrivs();
    if (pps == null) return "No global privileges";
    else return dumpThriftObject(pps);

     */
    return null;
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
   * @throws IOException
   */
  public Partition getPartition(String dbName, String tableName, List<String> partVals)
      throws IOException {
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
   * Get a set of specific partitions.  This cannot be used to do a scan, each partition must be
   * completely specified.  This does not use the partition cache.
   * @param dbName database table is in
   * @param tableName table partitions are in
   * @param partValLists list of list of values, each list should uniquely identify one partition
   * @return a list of partition objects.
   * @throws IOException
   */
   public List<Partition> getPartitions(String dbName, String tableName, List<String> partTypes,
       List<List<String>> partValLists) throws IOException {
    /*
     List<Partition> parts = new ArrayList<>(partValLists.size());
     List<Get> gets = new ArrayList<>(partValLists.size());
     for (List<String> partVals : partValLists) {
       byte[] key = HBaseUtils.buildPartitionKey(dbName, tableName, partTypes, partVals);
       Get get = new Get(key);
       get.addColumn(CATALOG_CF, CATALOG_COL);
       gets.add(get);
     }
     HTableInterface htab = conn.getHBaseTable(PART_TABLE);
     Result[] results = htab.get(gets);
     for (int i = 0; i < results.length; i++) {
       HBaseUtils.StorageDescriptorParts sdParts =
           HBaseUtils.deserializePartition(dbName, tableName, partValLists.get(i),
               results[i].getValue(CATALOG_CF, CATALOG_COL));
       StorageDescriptor sd = getStorageDescriptor(sdParts.sdHash);
       HBaseUtils.assembleStorageDescriptor(sd, sdParts);
       parts.add(sdParts.containingPartition);
     }

     return parts;
    */
     return null;
  }

  /**
   * Add a partition.  This should only be called for new partitions.  For altering existing
   * partitions this should not be called as it will blindly increment the ref counter for the
   * storage descriptor.
   * @param partition partition object to add
   * @throws IOException
   */
  public void putPartition(Partition partition) throws IOException {
    /*
    byte[] hash = putStorageDescriptor(partition.getSd());
    byte[][] serialized = HBaseUtils.serializePartition(partition,
        HBaseUtils.getPartitionKeyTypes(getTable(partition.getDbName(), partition.getTableName()).getPartitionKeys()), hash);
    store(PART_TABLE, serialized[0], CATALOG_CF, CATALOG_COL, serialized[1]);
    partCache.put(partition.getDbName(), partition.getTableName(), partition);
    */
  }

  /**
   * Replace an existing partition.
   * @param oldPart partition to be replaced
   * @param newPart partitiion to replace it with
   * @throws IOException
   */
  public void replacePartition(Partition oldPart, Partition newPart, List<String> partTypes) throws IOException {
    /*
    byte[] hash;
    byte[] oldHash = HBaseUtils.hashStorageDescriptor(oldPart.getSd(), md);
    byte[] newHash = HBaseUtils.hashStorageDescriptor(newPart.getSd(), md);
    if (Arrays.equals(oldHash, newHash)) {
      hash = oldHash;
    } else {
      decrementStorageDescriptorRefCount(oldPart.getSd());
      hash = putStorageDescriptor(newPart.getSd());
    }
    byte[][] serialized = HBaseUtils.serializePartition(newPart,
        HBaseUtils.getPartitionKeyTypes(getTable(newPart.getDbName(), newPart.getTableName()).getPartitionKeys()), hash);
    store(PART_TABLE, serialized[0], CATALOG_CF, CATALOG_COL, serialized[1]);
    partCache.put(newPart.getDbName(), newPart.getTableName(), newPart);
    if (!oldPart.getTableName().equals(newPart.getTableName())) {
      deletePartition(oldPart.getDbName(), oldPart.getTableName(), partTypes, oldPart.getValues());
    }
    */
  }

  /**
   * Add a group of partitions.  This should only be used when all partitions are new.  It
   * blindly increments the ref count on the storage descriptor.
   * @param partitions list of partitions to add
   * @throws IOException
   */
  public void putPartitions(List<Partition> partitions) throws IOException {
    /*
    List<Put> puts = new ArrayList<>(partitions.size());
    for (Partition partition : partitions) {
      byte[] hash = putStorageDescriptor(partition.getSd());
      List<String> partTypes = HBaseUtils.getPartitionKeyTypes(
          getTable(partition.getDbName(), partition.getTableName()).getPartitionKeys());
      byte[][] serialized = HBaseUtils.serializePartition(partition, partTypes, hash);
      Put p = new Put(serialized[0]);
      p.add(CATALOG_CF, CATALOG_COL, serialized[1]);
      puts.add(p);
      partCache.put(partition.getDbName(), partition.getTableName(), partition);
    }
    HTableInterface htab = conn.getHBaseTable(PART_TABLE);
    htab.put(puts);
    conn.flush(htab);
    */
  }

  public void replacePartitions(List<Partition> oldParts, List<Partition> newParts, List<String> oldPartTypes) throws IOException {
    /*
    if (oldParts.size() != newParts.size()) {
      throw new RuntimeException("Number of old and new partitions must match.");
    }
    List<Put> puts = new ArrayList<>(newParts.size());
    for (int i = 0; i < newParts.size(); i++) {
      byte[] hash;
      byte[] oldHash = HBaseUtils.hashStorageDescriptor(oldParts.get(i).getSd(), md);
      byte[] newHash = HBaseUtils.hashStorageDescriptor(newParts.get(i).getSd(), md);
      if (Arrays.equals(oldHash, newHash)) {
        hash = oldHash;
      } else {
        decrementStorageDescriptorRefCount(oldParts.get(i).getSd());
        hash = putStorageDescriptor(newParts.get(i).getSd());
      }
      Partition newPart = newParts.get(i);
      byte[][] serialized = HBaseUtils.serializePartition(newPart,
          HBaseUtils.getPartitionKeyTypes(getTable(newPart.getDbName(), newPart.getTableName()).getPartitionKeys()), hash);
      Put p = new Put(serialized[0]);
      p.add(CATALOG_CF, CATALOG_COL, serialized[1]);
      puts.add(p);
      partCache.put(newParts.get(i).getDbName(), newParts.get(i).getTableName(), newParts.get(i));
      if (!newParts.get(i).getTableName().equals(oldParts.get(i).getTableName())) {
        // We need to remove the old record as well.
        deletePartition(oldParts.get(i).getDbName(), oldParts.get(i).getTableName(), oldPartTypes,
            oldParts.get(i).getValues(), false);
      }
    }
    HTableInterface htab = conn.getHBaseTable(PART_TABLE);
    htab.put(puts);
    conn.flush(htab);
    */
  }

  /**
   * Find all the partitions in a table.
   * @param dbName name of the database the table is in
   * @param tableName table name
   * @param maxPartitions max partitions to fetch.  If negative all partitions will be returned.
   * @return List of partitions that match the criteria.
   * @throws IOException
   */
  public List<Partition> scanPartitionsInTable(String dbName, String tableName, int maxPartitions)
      throws IOException {
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

  /**
   * Scan partitions based on partial key information.
   * @param dbName name of database, required
   * @param tableName name of table, required
   * @param partVals partial specification of values.  Any values that are unknown can instead be
   *                 a '*'.  For example, if a table had two partition columns date
   *                 and region (in that order), and partitions ('today', 'na'), ('today', 'eu'),
   *                 ('tomorrow', 'na'), ('tomorrow', 'eu') then passing ['today', '*'] would return
   *                 ('today', 'na') and ('today', 'eu') while passing ['*', 'eu'] would return
   *                 ('today', 'eu') and ('tomorrow', 'eu').  Also the list can terminate early,
   *                 which will be the equivalent of adding '*' for all non-included values.
   *                 I.e. ['today'] is the same as ['today', '*'].
   * @param maxPartitions Maximum number of entries to return.
   * @return list of partitions that match the specified information
   * @throws IOException
   * @throws org.apache.hadoop.hive.metastore.api.NoSuchObjectException if the table containing
   * the partitions can't be found.
   */
  public List<Partition> scanPartitions(String dbName, String tableName, List<String> partVals,
                                 int maxPartitions) throws IOException, NoSuchObjectException {

    /*
    PartitionScanInfo psi = scanPartitionsInternal(dbName, tableName, partVals, maxPartitions);
    List<Partition> parts = scanPartitionsWithFilter(dbName, tableName, psi.keyPrefix,
        psi.endKeyPrefix, maxPartitions, psi.filter);
    partCache.put(dbName, tableName, parts, false);
    return parts;
    */
    return null;
  }

  public List<Partition> scanPartitions(String dbName, String tableName, byte[] keyStart, byte[] keyEnd,
                                        Filter filter, int  maxPartitions)
      throws IOException, NoSuchObjectException {
    /*
    byte[] startRow = keyStart;
    byte[] endRow;
    if (keyEnd == null || keyEnd.length == 0) {
      // stop when current db+table entries are over
      endRow = HBaseUtils.getEndPrefix(startRow);
    } else {
      endRow = keyEnd;
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("Scanning partitions with start row <" + new String(startRow) + "> and end row <"
          + new String(endRow) + ">");
    }
    return scanPartitionsWithFilter(dbName, tableName, startRow, endRow, maxPartitions, filter);
    */
    return null;
  }

  /**
   * Delete a partition
   * @param dbName database name that table is in
   * @param tableName table partition is in
   * @param partVals partition values that define this partition, in the same order as the
   *                 partition columns they are values for
   * @throws IOException
   */
  public void deletePartition(String dbName, String tableName, List<String> partTypes,
      List<String> partVals) throws IOException {
    //deletePartition(dbName, tableName, partTypes, partVals, true);
  }

  /**
   * Print out a partition.
   * @param partKey The key for the partition.  This must include dbname.tablename._partkeys_
   *                where _partkeys_ is a dot separated list of partition values in the proper
   *                order.
   * @return string containing the partition
   * @throws IOException
   * @throws TException
   */
  public String printPartition(String partKey) throws IOException, TException {
    /*
    // First figure out the table and fetch it
    String[] partKeyParts = partKey.split(HBaseUtils.KEY_SEPARATOR_STR);
    if (partKeyParts.length < 3) return noSuch(partKey, "partition");
    Table table = getTable(partKeyParts[0], partKeyParts[1]);
    if (table == null) return noSuch(partKey, "partition");

    byte[] key = HBaseUtils.buildPartitionKey(partKeyParts[0], partKeyParts[1],
        HBaseUtils.getPartitionKeyTypes(table.getPartitionKeys()),
        Arrays.asList(Arrays.copyOfRange(partKeyParts, 2, partKeyParts.length)));
    @SuppressWarnings("deprecation")
    HTableInterface htab = conn.getHBaseTable(PART_TABLE);
    Get g = new Get(key);
    g.addColumn(CATALOG_CF, CATALOG_COL);
    g.addFamily(STATS_CF);
    Result result = htab.get(g);
    if (result.isEmpty()) return noSuch(partKey, "partition");
    return printOnePartition(result);
    */
    return null;
  }

  /**
   * Print partitions
   * @param partKey a partial partition key.  This must match the beginings of the partition key.
   *                It can be just dbname.tablename, or dbname.table.pval... where pval are the
   *                partition values in order.  They must be in the correct order and they must
   *                be literal values (no regular expressions)
   * @return partitions as strings
   * @throws IOException
   * @throws TException
   */
  public List<String> printPartitions(String partKey) throws IOException, TException {
    /*
    // First figure out the table and fetch it
    // Split on dot here rather than the standard separator because this will be passed in as a
    // regex, even though we aren't fully supporting regex's.
    String[] partKeyParts = partKey.split("\\.");
    if (partKeyParts.length < 2) return noMatch(partKey, "partition");
    List<String> partVals = partKeyParts.length == 2 ? Arrays.asList("*") :
        Arrays.asList(Arrays.copyOfRange(partKeyParts, 2, partKeyParts.length));
    PartitionScanInfo psi;
    try {
      psi =
          scanPartitionsInternal(partKeyParts[0], partKeyParts[1], partVals, -1);
    } catch (NoSuchObjectException e) {
      return noMatch(partKey, "partition");
    }

    @SuppressWarnings("deprecation")
    HTableInterface htab = conn.getHBaseTable(PART_TABLE);
    Scan scan = new Scan();
    scan.addColumn(CATALOG_CF, CATALOG_COL);
    scan.addFamily(STATS_CF);
    scan.setStartRow(psi.keyPrefix);
    scan.setStopRow(psi.endKeyPrefix);
    scan.setFilter(psi.filter);
    Iterator<Result> iter = htab.getScanner(scan).iterator();
    if (!iter.hasNext()) return noMatch(partKey, "partition");
    List<String> lines = new ArrayList<>();
    while (iter.hasNext()) {
      lines.add(printOnePartition(iter.next()));
    }
    return lines;
    */
    return null;
  }

  public int getPartitionCount() throws IOException {
    /*
    Filter fil = new FirstKeyOnlyFilter();
    Iterator<Result> iter = scan(PART_TABLE, fil);
    return Iterators.size(iter);
    */
    return 0;
  }

  /*
  private String printOnePartition(Result result) throws IOException, TException {
    byte[] key = result.getRow();
    HBaseUtils.StorageDescriptorParts sdParts =
        HBaseUtils.deserializePartition(key, result.getValue(CATALOG_CF, CATALOG_COL), this);
    StringBuilder builder = new StringBuilder();
    builder.append(dumpThriftObject(sdParts.containingPartition))
        .append(" sdHash: ")
        .append(Base64.encodeBase64URLSafeString(sdParts.sdHash))
        .append(" stats:");
    NavigableMap<byte[], byte[]> statsCols = result.getFamilyMap(STATS_CF);
    for (Map.Entry<byte[], byte[]> statsCol : statsCols.entrySet()) {
      builder.append(" column ")
          .append(new String(statsCol.getKey(), HBaseUtils.ENCODING))
          .append(": ");
      ColumnStatistics pcs = buildColStats(key, false);
      ColumnStatisticsObj cso = HBaseUtils.deserializeStatsForOneColumn(pcs, statsCol.getValue());
      builder.append(dumpThriftObject(cso));
    }
    return builder.toString();
  }

  private void deletePartition(String dbName, String tableName, List<String> partTypes,
        List<String> partVals, boolean decrementRefCnt) throws IOException {
    // Find the partition so I can get the storage descriptor and drop it
    partCache.remove(dbName, tableName, partVals);
    if (decrementRefCnt) {
      Partition p = getPartition(dbName, tableName, partVals, false);
      decrementStorageDescriptorRefCount(p.getSd());
    }
    byte[] key = HBaseUtils.buildPartitionKey(dbName, tableName, partTypes, partVals);
    delete(PART_TABLE, key, null, null);
  }

  private Partition getPartition(String dbName, String tableName, List<String> partVals,
                                 boolean populateCache) throws IOException {
    Partition cached = partCache.get(dbName, tableName, partVals);
    if (cached != null) return cached;
    byte[] key = HBaseUtils.buildPartitionKey(dbName, tableName,
        HBaseUtils.getPartitionKeyTypes(getTable(dbName, tableName).getPartitionKeys()), partVals);
    byte[] serialized = read(PART_TABLE, key, CATALOG_CF, CATALOG_COL);
    if (serialized == null) return null;
    HBaseUtils.StorageDescriptorParts sdParts =
        HBaseUtils.deserializePartition(dbName, tableName, partVals, serialized);
    StorageDescriptor sd = getStorageDescriptor(sdParts.sdHash);
    HBaseUtils.assembleStorageDescriptor(sd, sdParts);
    if (populateCache) partCache.put(dbName, tableName, sdParts.containingPartition);
    return sdParts.containingPartition;
  }


  private static class PartitionScanInfo {
    final String dbName;
    final String tableName;
    final byte[] keyPrefix;
    final byte[] endKeyPrefix;
    final int maxPartitions;
    final Filter filter;

    PartitionScanInfo(String d, String t, byte[] k, byte[] e, int m, Filter f) {
      dbName = d;
      tableName = t;
      keyPrefix = k;
      endKeyPrefix = e;
      maxPartitions = m;
      filter = f;
    }

    @Override
    public String toString() {
      return new StringBuilder("dbName:")
          .append(dbName)
          .append(" tableName:")
          .append(tableName)
          .append(" keyPrefix:")
          .append(Base64.encodeBase64URLSafeString(keyPrefix))
          .append(" endKeyPrefix:")
          .append(Base64.encodeBase64URLSafeString(endKeyPrefix))
          .append(" maxPartitions:")
          .append(maxPartitions)
          .append(" filter:")
          .append(filter.toString())
          .toString();
    }
  }

  private PartitionScanInfo scanPartitionsInternal(String dbName, String tableName,
                                                   List<String> partVals, int maxPartitions)
      throws IOException, NoSuchObjectException {
    // First, build as much of the key as we can so that we make the scan as tight as possible.
    List<String> keyElements = new ArrayList<>();
    keyElements.add(dbName);
    keyElements.add(tableName);

    int firstStar = -1;
    for (int i = 0; i < partVals.size(); i++) {
      if ("*".equals(partVals.get(i))) {
        firstStar = i;
        break;
      } else {
        // empty string equals to null partition,
        // means star
        if (partVals.get(i).equals("")) {
          break;
        } else {
          keyElements.add(partVals.get(i));
        }
      }
    }

    byte[] keyPrefix;
    // We need to fetch the table to determine if the user fully specified the partitions or
    // not, as it affects how we build the key.
    Table table = getTable(dbName, tableName);
    if (table == null) {
      throw new NoSuchObjectException("Unable to find table " + dbName + "." + tableName);
    }
    keyPrefix = HBaseUtils.buildPartitionKey(dbName, tableName,
        HBaseUtils.getPartitionKeyTypes(table.getPartitionKeys().subList(0, keyElements.size()-2)),
        keyElements.subList(2, keyElements.size()));

    // Now, build a filter out of the remaining keys
    List<PartitionKeyComparator.Range> ranges = new ArrayList<PartitionKeyComparator.Range>();
    List<Operator> ops = new ArrayList<Operator>();
    if (!(partVals.size() == table.getPartitionKeys().size() && firstStar == -1)) {

      for (int i = Math.max(0, firstStar);
           i < table.getPartitionKeys().size() && i < partVals.size(); i++) {

        if ("*".equals(partVals.get(i))) {
          PartitionKeyComparator.Operator op = new PartitionKeyComparator.Operator(
              PartitionKeyComparator.Operator.Type.LIKE,
              table.getPartitionKeys().get(i).getName(),
              ".*");
          ops.add(op);
        } else {
          PartitionKeyComparator.Range range = new PartitionKeyComparator.Range(
              table.getPartitionKeys().get(i).getName(),
              new PartitionKeyComparator.Mark(partVals.get(i), true),
              new PartitionKeyComparator.Mark(partVals.get(i), true));
          ranges.add(range);
        }
      }
    }

    Filter filter = null;
    if (!ranges.isEmpty() || !ops.isEmpty()) {
      filter = new RowFilter(CompareFilter.CompareOp.EQUAL, new PartitionKeyComparator(
          StringUtils.join(HBaseUtils.getPartitionNames(table.getPartitionKeys()), ","),
          StringUtils.join(HBaseUtils.getPartitionKeyTypes(table.getPartitionKeys()), ","),
          ranges, ops));
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("Scanning partitions with prefix <" + new String(keyPrefix) + "> and filter <" +
          filter + ">");
    }

    return new PartitionScanInfo(dbName, tableName, keyPrefix, HBaseUtils.getEndPrefix(keyPrefix),
        maxPartitions, filter);
  }

  private List<Partition> scanPartitionsWithFilter(String dbName, String tableName,
                                                   byte[] startRow, byte [] endRow, int maxResults,
                                                   Filter filter) throws IOException {
    Iterator<Result> iter =
        scan(PART_TABLE, startRow, endRow, CATALOG_CF, CATALOG_COL, filter);
    List<FieldSchema> tablePartitions = getTable(dbName, tableName).getPartitionKeys();
    List<Partition> parts = new ArrayList<>();
    int numToFetch = maxResults < 0 ? Integer.MAX_VALUE : maxResults;
    for (int i = 0; i < numToFetch && iter.hasNext(); i++) {
      Result result = iter.next();
      HBaseUtils.StorageDescriptorParts sdParts = HBaseUtils.deserializePartition(dbName, tableName,
          tablePartitions, result.getRow(), result.getValue(CATALOG_CF, CATALOG_COL), conf);
      StorageDescriptor sd = getStorageDescriptor(sdParts.sdHash);
      HBaseUtils.assembleStorageDescriptor(sd, sdParts);
      parts.add(sdParts.containingPartition);
    }
    return parts;
  }
  */

  private byte[] getPartitionByKey(PostgresTable table, String colName, List<Object> partVals)
      throws IOException {
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
    try (PreparedStatement stmt = currentConnection.prepareStatement(buf.toString())) {
      for (int i = 0; i < partVals.size(); i++) {
        stmt.setObject(i + 1, partVals.get(i));
      }
      ResultSet rs = stmt.executeQuery();
      if (rs.next()) {
        return rs.getBytes(1);
      } else {
        return null;
      }
    } catch (SQLException e) {
      throw new IOException(e);
    }

  }

  // Figure out if we alrady have a table for this partition.  If so, return it.  Otherwise
  // create one.
  private PostgresTable findPartitionTable(String dbName, String tableName) throws IOException {
    String partTableName = buildPartTableName(dbName, tableName);
    PostgresTable table = partitionTables.get(partTableName);
    if (table == null) {
      // Check to see if the table exists but we don't have it in the cache.  I don't put this
      // part in the synchronized section because it's ok if two threads do this at the same time,
      // if a bit wasteful.
      try {
        Connection conn = getDbConn();
        DatabaseMetaData dmd = conn.getMetaData();
        ResultSet rs = dmd.getTables(conn.getCatalog(), conn.getSchema(), partTableName, null);
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
      } catch (SQLException e) {
        throw new IOException(e);
      }
    }
    return table;
  }

  private PostgresTable postgresTableFromPartition(String dbName, String tableName) throws
      IOException {
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
   * Role related methods
   *********************************************************************************************/

  /**
   * Fetch the list of all roles for a user
   * @param userName name of the user
   * @return the list of all roles this user participates in
   * @throws IOException
   */
  public List<String> getUserRoles(String userName) throws IOException {
    /*
    byte[] key = HBaseUtils.buildKey(userName);
    byte[] serialized = read(USER_TO_ROLE_TABLE, key, CATALOG_CF, CATALOG_COL);
    if (serialized == null) return null;
    return HBaseUtils.deserializeRoleList(serialized);
    */
    return null;
  }

  /**
   * Find all roles directly participated in by a given principal.  This builds the role cache
   * because it assumes that subsequent calls may be made to find roles participated in indirectly.
   * @param name username or role name
   * @param type user or role
   * @return map of role name to grant info for all roles directly participated in.
   */
  public List<Role> getPrincipalDirectRoles(String name, PrincipalType type)
      throws IOException {
    /*
    buildRoleCache();

    Set<String> rolesFound = new HashSet<>();
    for (Map.Entry<String, HbaseMetastoreProto.RoleGrantInfoList> e : roleCache.entrySet()) {
      for (HbaseMetastoreProto.RoleGrantInfo giw : e.getValue().getGrantInfoList()) {
        if (HBaseUtils.convertPrincipalTypes(giw.getPrincipalType()) == type &&
            giw.getPrincipalName().equals(name)) {
          rolesFound.add(e.getKey());
          break;
        }
      }
    }
    List<Role> directRoles = new ArrayList<>(rolesFound.size());
    List<Get> gets = new ArrayList<>();
    HTableInterface htab = conn.getHBaseTable(ROLE_TABLE);
    for (String roleFound : rolesFound) {
      byte[] key = HBaseUtils.buildKey(roleFound);
      Get g = new Get(key);
      g.addColumn(CATALOG_CF, CATALOG_COL);
      gets.add(g);
    }

    Result[] results = htab.get(gets);
    for (int i = 0; i < results.length; i++) {
      byte[] serialized = results[i].getValue(CATALOG_CF, CATALOG_COL);
      if (serialized != null) {
        directRoles.add(HBaseUtils.deserializeRole(results[i].getRow(), serialized));
      }
    }

    return directRoles;
    */
    return null;
  }

  /**
   * Fetch all roles and users included directly in a given role.
   * @param roleName name of the principal
   * @return a list of all roles included in this role
   * @throws IOException
   */
  public HbaseMetastoreProto.RoleGrantInfoList getRolePrincipals(String roleName)
      throws IOException, NoSuchObjectException {
    /*
    HbaseMetastoreProto.RoleGrantInfoList rolePrincipals = roleCache.get(roleName);
    if (rolePrincipals != null) return rolePrincipals;
    byte[] key = HBaseUtils.buildKey(roleName);
    byte[] serialized = read(ROLE_TABLE, key, CATALOG_CF, ROLES_COL);
    if (serialized == null) return null;
    rolePrincipals = HbaseMetastoreProto.RoleGrantInfoList.parseFrom(serialized);
    roleCache.put(roleName, rolePrincipals);
    return rolePrincipals;
    */
    return null;
  }

  /**
   * Given a role, find all users who are either directly or indirectly participate in this role.
   * This is expensive, it should be used sparingly.  It scan the entire userToRole table and
   * does a linear search on each entry.
   * @param roleName name of the role
   * @return set of all users in the role
   * @throws IOException
   */
  public Set<String> findAllUsersInRole(String roleName) throws IOException {
    /*
    // Walk the userToRole table and collect every user that matches this role.
    Set<String> users = new HashSet<>();
    Iterator<Result> iter = scan(USER_TO_ROLE_TABLE, CATALOG_CF, CATALOG_COL);
    while (iter.hasNext()) {
      Result result = iter.next();
      List<String> roleList =
          HBaseUtils.deserializeRoleList(result.getValue(CATALOG_CF, CATALOG_COL));
      for (String rn : roleList) {
        if (rn.equals(roleName)) {
          users.add(new String(result.getRow(), HBaseUtils.ENCODING));
          break;
        }
      }
    }
    return users;
    */
    return null;
  }

  /**
   * Add a principal to a role.
   * @param roleName name of the role to add principal to
   * @param grantInfo grant information for this principal.
   * @throws java.io.IOException
   * @throws NoSuchObjectException
   *
   */
  public void addPrincipalToRole(String roleName, HbaseMetastoreProto.RoleGrantInfo grantInfo)
      throws IOException, NoSuchObjectException {
    /*
    HbaseMetastoreProto.RoleGrantInfoList proto = getRolePrincipals(roleName);
    List<HbaseMetastoreProto.RoleGrantInfo> rolePrincipals = new ArrayList<>();
    if (proto != null) {
      rolePrincipals.addAll(proto.getGrantInfoList());
    }

    rolePrincipals.add(grantInfo);
    proto = HbaseMetastoreProto.RoleGrantInfoList.newBuilder()
        .addAllGrantInfo(rolePrincipals)
        .build();
    byte[] key = HBaseUtils.buildKey(roleName);
    store(ROLE_TABLE, key, CATALOG_CF, ROLES_COL, proto.toByteArray());
    roleCache.put(roleName, proto);
    */
  }

  /**
   * Drop a principal from a role.
   * @param roleName Name of the role to drop the principal from
   * @param principalName name of the principal to drop from the role
   * @param type user or role
   * @param grantOnly if this is true, just remove the grant option, don't actually remove the
   *                  user from the role.
   * @throws NoSuchObjectException
   * @throws IOException
   */
  public void dropPrincipalFromRole(String roleName, String principalName, PrincipalType type,
                             boolean grantOnly)
      throws NoSuchObjectException, IOException {
    /*
    HbaseMetastoreProto.RoleGrantInfoList proto = getRolePrincipals(roleName);
    if (proto == null) return;
    List<HbaseMetastoreProto.RoleGrantInfo> rolePrincipals = new ArrayList<>();
    rolePrincipals.addAll(proto.getGrantInfoList());

    for (int i = 0; i < rolePrincipals.size(); i++) {
      if (HBaseUtils.convertPrincipalTypes(rolePrincipals.get(i).getPrincipalType()) == type &&
          rolePrincipals.get(i).getPrincipalName().equals(principalName)) {
        if (grantOnly) {
          rolePrincipals.set(i,
              HbaseMetastoreProto.RoleGrantInfo.newBuilder(rolePrincipals.get(i))
                  .setGrantOption(false)
                  .build());
        } else {
          rolePrincipals.remove(i);
        }
        break;
      }
    }
    byte[] key = HBaseUtils.buildKey(roleName);
    proto = HbaseMetastoreProto.RoleGrantInfoList.newBuilder()
        .addAllGrantInfo(rolePrincipals)
        .build();
    store(ROLE_TABLE, key, CATALOG_CF, ROLES_COL, proto.toByteArray());
    roleCache.put(roleName, proto);
    */
  }

  /**
   * Rebuild the row for a given user in the USER_TO_ROLE table.  This is expensive.  It
   * should be called as infrequently as possible.
   * @param userName name of the user
   * @throws IOException
   */
  public void buildRoleMapForUser(String userName) throws IOException, NoSuchObjectException {
    /*
    // This is mega ugly.  Hopefully we don't have to do this too often.
    // First, scan the role table and put it all in memory
    buildRoleCache();
    LOG.debug("Building role map for " + userName);

    // Second, find every role the user participates in directly.
    Set<String> rolesToAdd = new HashSet<>();
    Set<String> rolesToCheckNext = new HashSet<>();
    for (Map.Entry<String, HbaseMetastoreProto.RoleGrantInfoList> e : roleCache.entrySet()) {
      for (HbaseMetastoreProto.RoleGrantInfo grantInfo : e.getValue().getGrantInfoList()) {
        if (HBaseUtils.convertPrincipalTypes(grantInfo.getPrincipalType()) == PrincipalType.USER &&
            userName .equals(grantInfo.getPrincipalName())) {
          rolesToAdd.add(e.getKey());
          rolesToCheckNext.add(e.getKey());
          LOG.debug("Adding " + e.getKey() + " to list of roles user is in directly");
          break;
        }
      }
    }

    // Third, find every role the user participates in indirectly (that is, they have been
    // granted into role X and role Y has been granted into role X).
    while (rolesToCheckNext.size() > 0) {
      Set<String> tmpRolesToCheckNext = new HashSet<>();
      for (String roleName : rolesToCheckNext) {
        HbaseMetastoreProto.RoleGrantInfoList grantInfos = roleCache.get(roleName);
        if (grantInfos == null) continue;  // happens when a role contains no grants
        for (HbaseMetastoreProto.RoleGrantInfo grantInfo : grantInfos.getGrantInfoList()) {
          if (HBaseUtils.convertPrincipalTypes(grantInfo.getPrincipalType()) == PrincipalType.ROLE &&
              rolesToAdd.add(grantInfo.getPrincipalName())) {
            tmpRolesToCheckNext.add(grantInfo.getPrincipalName());
            LOG.debug("Adding " + grantInfo.getPrincipalName() +
                " to list of roles user is in indirectly");
          }
        }
      }
      rolesToCheckNext = tmpRolesToCheckNext;
    }

    byte[] key = HBaseUtils.buildKey(userName);
    byte[] serialized = HBaseUtils.serializeRoleList(new ArrayList<>(rolesToAdd));
    store(USER_TO_ROLE_TABLE, key, CATALOG_CF, CATALOG_COL, serialized);
    */
  }

  /**
   * Remove all of the grants for a role.  This is not cheap.
   * @param roleName Role to remove from all other roles and grants
   * @throws IOException
   */
  public void removeRoleGrants(String roleName) throws IOException {
    /*
    buildRoleCache();

    List<Put> puts = new ArrayList<>();
    // First, walk the role table and remove any references to this role
    for (Map.Entry<String, HbaseMetastoreProto.RoleGrantInfoList> e : roleCache.entrySet()) {
      boolean madeAChange = false;
      List<HbaseMetastoreProto.RoleGrantInfo> rgil = new ArrayList<>();
      rgil.addAll(e.getValue().getGrantInfoList());
      for (int i = 0; i < rgil.size(); i++) {
        if (HBaseUtils.convertPrincipalTypes(rgil.get(i).getPrincipalType()) == PrincipalType.ROLE &&
            rgil.get(i).getPrincipalName().equals(roleName)) {
          rgil.remove(i);
          madeAChange = true;
          break;
        }
      }
      if (madeAChange) {
        Put put = new Put(HBaseUtils.buildKey(e.getKey()));
        HbaseMetastoreProto.RoleGrantInfoList proto =
            HbaseMetastoreProto.RoleGrantInfoList.newBuilder()
            .addAllGrantInfo(rgil)
            .build();
        put.add(CATALOG_CF, ROLES_COL, proto.toByteArray());
        puts.add(put);
        roleCache.put(e.getKey(), proto);
      }
    }

    if (puts.size() > 0) {
      HTableInterface htab = conn.getHBaseTable(ROLE_TABLE);
      htab.put(puts);
      conn.flush(htab);
    }

    // Remove any global privileges held by this role
    PrincipalPrivilegeSet global = getGlobalPrivs();
    if (global != null &&
        global.getRolePrivileges() != null &&
        global.getRolePrivileges().remove(roleName) != null) {
      putGlobalPrivs(global);
    }

    // Now, walk the db table
    puts.clear();
    List<Database> dbs = scanDatabases(null);
    if (dbs == null) dbs = new ArrayList<>(); // rare, but can happen
    for (Database db : dbs) {
      if (db.getPrivileges() != null &&
          db.getPrivileges().getRolePrivileges() != null &&
          db.getPrivileges().getRolePrivileges().remove(roleName) != null) {
        byte[][] serialized = HBaseUtils.serializeDatabase(db);
        Put put = new Put(serialized[0]);
        put.add(CATALOG_CF, CATALOG_COL, serialized[1]);
        puts.add(put);
      }
    }

    if (puts.size() > 0) {
      HTableInterface htab = conn.getHBaseTable(DB_TABLE);
      htab.put(puts);
      conn.flush(htab);
    }

    // Finally, walk the table table
    puts.clear();
    for (Database db : dbs) {
      List<Table> tables = scanTables(db.getName(), null);
      if (tables != null) {
        for (Table table : tables) {
          if (table.getPrivileges() != null &&
              table.getPrivileges().getRolePrivileges() != null &&
              table.getPrivileges().getRolePrivileges().remove(roleName) != null) {
            byte[][] serialized = HBaseUtils.serializeTable(table,
                HBaseUtils.hashStorageDescriptor(table.getSd(), md));
            Put put = new Put(serialized[0]);
            put.add(CATALOG_CF, CATALOG_COL, serialized[1]);
            puts.add(put);
          }
        }
      }
    }

    if (puts.size() > 0) {
      HTableInterface htab = conn.getHBaseTable(TABLE_TABLE);
      htab.put(puts);
      conn.flush(htab);
    }
    */
  }

  /**
   * Fetch a role
   * @param roleName name of the role
   * @return role object, or null if no such role
   * @throws IOException
   */
  public Role getRole(String roleName) throws IOException {
    /*
    byte[] key = HBaseUtils.buildKey(roleName);
    byte[] serialized = read(ROLE_TABLE, key, CATALOG_CF, CATALOG_COL);
    if (serialized == null) return null;
    return HBaseUtils.deserializeRole(roleName, serialized);
    */
    return null;
  }

  /**
   * Get a list of roles.
   * @return list of all known roles.
   * @throws IOException
   */
  public List<Role> scanRoles() throws IOException {
    //return scanRoles(null);
    return null;
  }

  /**
   * Add a new role
   * @param role role object
   * @throws IOException
   */
  public void putRole(Role role) throws IOException {
    /*
    byte[][] serialized = HBaseUtils.serializeRole(role);
    store(ROLE_TABLE, serialized[0], CATALOG_CF, CATALOG_COL, serialized[1]);
    */
  }

  /**
   * Drop a role
   * @param roleName name of role to drop
   * @throws IOException
   */
  public void deleteRole(String roleName) throws IOException {
    /*
    byte[] key = HBaseUtils.buildKey(roleName);
    delete(ROLE_TABLE, key, null, null);
    roleCache.remove(roleName);
    */
  }

  public String printRolesForUser(String userName) throws IOException {
    /*
    List<String> roles = getUserRoles(userName);
    if (roles == null || roles.size() == 0) return noSuch(userName, "user");
    return org.apache.commons.lang.StringUtils.join(roles, ',');
    */
    return null;
  }

  public List<String> printRolesForUsers(String regex) throws IOException {
    /*
    Filter  filter = new RowFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator(regex));
    Iterator<Result> iter = scan(USER_TO_ROLE_TABLE, null, null, CATALOG_CF, CATALOG_COL, filter);
    List<String> lines = new ArrayList<>();
    while (iter.hasNext()) {
      Result result = iter.next();
      lines.add(new String(result.getRow(), HBaseUtils.ENCODING) + ": " +
          org.apache.commons.lang.StringUtils.join(
            HBaseUtils.deserializeRoleList(result.getValue(CATALOG_CF, CATALOG_COL)), ','));
    }
    if (lines.size() == 0) lines = noMatch(regex, "user");
    return lines;
    */
    return null;
  }

  /**
   * Print out a role
   * @param name name of role to print
   * @return string printout of role
   */
  public String printRole(String name) throws IOException, TException {
    /*
    Role role = getRole(name);
    if (role == null) return noSuch(name, "role");
    else return dumpThriftObject(role);
    */
    return null;
  }

  /**
   * Print out roles.
   * @param regex regular to use to search for roles
   * @return string printout of roles
   * @throws IOException
   * @throws TException
   */
  public List<String> printRoles(String regex) throws IOException, TException {
    /*
    List<Role> roles = scanRoles(regex);
    if (roles.size() == 0) {
      return noMatch(regex, "role");
    } else {
      List<String> lines = new ArrayList<>();
      for (Role role : roles) lines.add(dumpThriftObject(role));
      return lines;
    }
    */
    return null;
  }

  /*
  private List<Role> scanRoles(String regex) throws IOException {
    Filter filter = null;
    if (regex != null) {
      filter = new RowFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator(regex));
    }
    Iterator<Result> iter = scan(ROLE_TABLE, null, null, CATALOG_CF, CATALOG_COL, filter);
    List<Role> roles = new ArrayList<>();
    while (iter.hasNext()) {
      Result result = iter.next();
      roles.add(HBaseUtils.deserializeRole(result.getRow(),
          result.getValue(CATALOG_CF, CATALOG_COL)));
    }
    return roles;
  }

  private void buildRoleCache() throws IOException {
    if (!entireRoleTableInCache) {
      Iterator<Result> roles = scan(ROLE_TABLE, CATALOG_CF, ROLES_COL);
      while (roles.hasNext()) {
        Result res = roles.next();
        String roleName = new String(res.getRow(), HBaseUtils.ENCODING);
        HbaseMetastoreProto.RoleGrantInfoList grantInfos =
            HbaseMetastoreProto.RoleGrantInfoList.parseFrom(res.getValue(CATALOG_CF, ROLES_COL));
        roleCache.put(roleName, grantInfos);
      }
      entireRoleTableInCache = true;
    }
  }

  /**********************************************************************************************
   * Table related methods
   *********************************************************************************************/

  /**
   * Fetch a table object
   * @param dbName database the table is in
   * @param tableName table name
   * @return Table object, or null if no such table
   * @throws IOException
   */
  public Table getTable(String dbName, String tableName) throws IOException {
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
   * Fetch a list of table objects.
   * @param dbName Database that all fetched tables are in
   * @param tableNames list of table names
   * @return list of tables, in the same order as the provided names.
   * @throws IOException
   */
  public List<Table> getTables(String dbName, List<String> tableNames) throws IOException {
    // TODO
    // I could implement getTable in terms of this method.  But it is such a core function
    // that I don't want to slow it down for the much less common fetching of multiple tables.
    /*
    List<Table> results = new ArrayList<>(tableNames.size());
    ObjectPair<String, String>[] hashKeys = new ObjectPair[tableNames.size()];
    boolean atLeastOneMissing = false;
    for (int i = 0; i < tableNames.size(); i++) {
      hashKeys[i] = new ObjectPair<>(dbName, tableNames.get(i));
      // The result may be null, but we still want to add it so that we have a slot in the list
      // for it.
      results.add(tableCache.get(hashKeys[i]));
      if (results.get(i) == null) atLeastOneMissing = true;
    }
    if (!atLeastOneMissing) return results;

    // Now build a single get that will fetch the remaining tables
    List<Get> gets = new ArrayList<>();
    HTableInterface htab = conn.getHBaseTable(TABLE_TABLE);
    for (int i = 0; i < tableNames.size(); i++) {
      if (results.get(i) != null) continue;
      byte[] key = HBaseUtils.buildKey(dbName, tableNames.get(i));
      Get g = new Get(key);
      g.addColumn(CATALOG_CF, CATALOG_COL);
      gets.add(g);
    }
    Result[] res = htab.get(gets);
    for (int i = 0, nextGet = 0; i < tableNames.size(); i++) {
      if (results.get(i) != null) continue;
      byte[] serialized = res[nextGet++].getValue(CATALOG_CF, CATALOG_COL);
      if (serialized != null) {
        HBaseUtils.StorageDescriptorParts sdParts =
            HBaseUtils.deserializeTable(dbName, tableNames.get(i), serialized);
        StorageDescriptor sd = getStorageDescriptor(sdParts.sdHash);
        HBaseUtils.assembleStorageDescriptor(sd, sdParts);
        tableCache.put(hashKeys[i], sdParts.containingTable);
        results.set(i, sdParts.containingTable);
      }
    }
    return results;
    */
    return null;
  }

  /**
   * Get a list of tables.
   * @param dbName Database these tables are in
   * @param regex Regular expression to use in searching for table names.  It is expected to
   *              be a Java regular expression.  If it is null then all tables in the indicated
   *              database will be returned.
   * @return list of tables matching the regular expression.
   * @throws IOException
   */
  public List<Table> scanTables(String dbName, String regex) throws IOException {
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
   * Put a table object.  This should only be called when the table is new (create table) as it
   * will blindly add/increment the storage descriptor.  If you are altering an existing table
   * call {@link #replaceTable} instead.
   * @param table table object
   * @throws IOException
   */
  public void putTable(Table table) throws IOException {
    storeOnKey(TABLE_TABLE, CATALOG_COL, serialize(table), table.getDbName(), table.getTableName());
    tableCache.put(new ObjectPair<>(table.getDbName(), table.getTableName()), table);
  }

  /**
   * Replace an existing table.  This will also compare the storage descriptors and see if the
   * reference count needs to be adjusted
   * @param oldTable old version of the table
   * @param newTable new version of the table
   */
  public void replaceTable(Table oldTable, Table newTable) throws IOException {
    // TODO
    /*
    byte[] hash;
    byte[] oldHash = HBaseUtils.hashStorageDescriptor(oldTable.getSd(), md);
    byte[] newHash = HBaseUtils.hashStorageDescriptor(newTable.getSd(), md);
    if (Arrays.equals(oldHash, newHash)) {
      hash = oldHash;
    } else {
      decrementStorageDescriptorRefCount(oldTable.getSd());
      hash = putStorageDescriptor(newTable.getSd());
    }
    byte[][] serialized = HBaseUtils.serializeTable(newTable, hash);
    store(TABLE_TABLE, serialized[0], CATALOG_CF, CATALOG_COL, serialized[1]);
    tableCache.put(new ObjectPair<>(newTable.getDbName(), newTable.getTableName()), newTable);
    if (!oldTable.getTableName().equals(newTable.getTableName())) {
      deleteTable(oldTable.getDbName(), oldTable.getTableName());
    }
    */
  }

  /**
   * Delete a table
   * @param dbName name of database table is in
   * @param tableName table to drop
   * @throws IOException
   */
  public void deleteTable(String dbName, String tableName) throws IOException {
    deleteOnKey(TABLE_TABLE, dbName, tableName);
    tableCache.remove(new ObjectPair<>(dbName, tableName));
  }

  /**
   * Print out a table.
   * @param name The name for the table.  This must include dbname.tablename
   * @return string containing the table
   * @throws IOException
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

  /**
   * Print tables
   * @param regex to use to find the tables.  Remember that dbname is in each
   *              table name.
   * @return tables as strings
   * @throws IOException
   * @throws TException
   */
  public List<String> printTables(String regex) throws IOException, TException {
    // TODO
    /*
    Filter  filter = new RowFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator(regex));
    @SuppressWarnings("deprecation")
    HTableInterface htab = conn.getHBaseTable(TABLE_TABLE);
    Scan scan = new Scan();
    scan.addColumn(CATALOG_CF, CATALOG_COL);
    scan.addFamily(STATS_CF);
    scan.setFilter(filter);
    Iterator<Result> iter = htab.getScanner(scan).iterator();
    if (!iter.hasNext()) return noMatch(regex, "table");
    List<String> lines = new ArrayList<>();
    while (iter.hasNext()) {
      lines.add(printOneTable(iter.next()));
    }
    return lines;
    */
    return null;
  }

  public int getTableCount() throws IOException {
    return (int)getCount(TABLE_TABLE);
  }

  /**********************************************************************************************
   * Index related methods
   *********************************************************************************************/

  /**
   * Put an index object.  This should only be called when the index is new (create index) as it
   * will blindly add/increment the storage descriptor.  If you are altering an existing index
   * call {@link #replaceIndex} instead.
   * @param index index object
   * @throws IOException
   */
  public void putIndex(Index index) throws IOException {
    /*
    byte[] hash = putStorageDescriptor(index.getSd());
    byte[][] serialized = HBaseUtils.serializeIndex(index, hash);
    store(INDEX_TABLE, serialized[0], CATALOG_CF, CATALOG_COL, serialized[1]);
    */
  }

  /**
   * Fetch an index object
   * @param dbName database the table is in
   * @param origTableName original table name
   * @param indexName index name
   * @return Index object, or null if no such table
   * @throws IOException
   */
  public Index getIndex(String dbName, String origTableName, String indexName) throws IOException {
    /*
    byte[] key = HBaseUtils.buildKey(dbName, origTableName, indexName);
    byte[] serialized = read(INDEX_TABLE, key, CATALOG_CF, CATALOG_COL);
    if (serialized == null) return null;
    HBaseUtils.StorageDescriptorParts sdParts =
        HBaseUtils.deserializeIndex(dbName, origTableName, indexName, serialized);
    StorageDescriptor sd = getStorageDescriptor(sdParts.sdHash);
    HBaseUtils.assembleStorageDescriptor(sd, sdParts);
    return sdParts.containingIndex;
    */
    return null;
  }

  /**
   * Delete a table
   * @param dbName name of database table is in
   * @param origTableName table the index is built on
   * @param indexName index name
   * @throws IOException
   */
  public void deleteIndex(String dbName, String origTableName, String indexName) throws IOException {
    deleteIndex(dbName, origTableName, indexName, true);
  }

  public void deleteIndex(String dbName, String origTableName, String indexName, boolean decrementRefCnt)
      throws IOException {
    /*
    // Find the index so I can get the storage descriptor and drop it
    if (decrementRefCnt) {
      Index index = getIndex(dbName, origTableName, indexName);
      decrementStorageDescriptorRefCount(index.getSd());
    }
    byte[] key = HBaseUtils.buildKey(dbName, origTableName, indexName);
    delete(INDEX_TABLE, key, null, null);
    */
  }

  /**
   * Get a list of tables.
   * @param dbName Database these tables are in
   * @param origTableName original table name
   * @param maxResults max indexes to fetch.  If negative all indexes will be returned.
   * @return list of indexes of the table
   * @throws IOException
   */
  public List<Index> scanIndexes(String dbName, String origTableName, int maxResults) throws IOException {
    // There's no way to know whether all the tables we are looking for are
    // in the cache, so we would need to scan one way or another.  Thus there's no value in hitting
    // the cache for this function.
    /*
    byte[] keyPrefix = null;
    if (dbName != null) {
      keyPrefix = HBaseUtils.buildKeyWithTrailingSeparator(dbName, origTableName);
    }
    Iterator<Result> iter = scan(INDEX_TABLE, keyPrefix, HBaseUtils.getEndPrefix(keyPrefix),
        CATALOG_CF, CATALOG_COL, null);
    List<Index> indexes = new ArrayList<>();
    int numToFetch = maxResults < 0 ? Integer.MAX_VALUE : maxResults;
    for (int i = 0; i < numToFetch && iter.hasNext(); i++) {
      Result result = iter.next();
      HBaseUtils.StorageDescriptorParts sdParts = HBaseUtils.deserializeIndex(result.getRow(),
          result.getValue(CATALOG_CF, CATALOG_COL));
      StorageDescriptor sd = getStorageDescriptor(sdParts.sdHash);
      HBaseUtils.assembleStorageDescriptor(sd, sdParts);
      indexes.add(sdParts.containingIndex);
    }
    return indexes;
    */
    return null;
  }

  /**
   * Replace an existing index.  This will also compare the storage descriptors and see if the
   * reference count needs to be adjusted
   * @param oldIndex old version of the index
   * @param newIndex new version of the index
   */
  public void replaceIndex(Index oldIndex, Index newIndex) throws IOException {
    /*
    byte[] hash;
    byte[] oldHash = HBaseUtils.hashStorageDescriptor(oldIndex.getSd(), md);
    byte[] newHash = HBaseUtils.hashStorageDescriptor(newIndex.getSd(), md);
    if (Arrays.equals(oldHash, newHash)) {
      hash = oldHash;
    } else {
      decrementStorageDescriptorRefCount(oldIndex.getSd());
      hash = putStorageDescriptor(newIndex.getSd());
    }
    byte[][] serialized = HBaseUtils.serializeIndex(newIndex, hash);
    store(INDEX_TABLE, serialized[0], CATALOG_CF, CATALOG_COL, serialized[1]);
    if (!(oldIndex.getDbName().equals(newIndex.getDbName()) &&
        oldIndex.getOrigTableName().equals(newIndex.getOrigTableName()) &&
        oldIndex.getIndexName().equals(newIndex.getIndexName()))) {
      deleteIndex(oldIndex.getDbName(), oldIndex.getOrigTableName(), oldIndex.getIndexName(), false);
    }
    */
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
   * @throws IOException
   */
  public void updateStatistics(String dbName, String tableName, List<String> partVals,
      ColumnStatistics stats) throws IOException {
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
  public StatsCache getStatsCache() {
    //return statsCache;
    return null;
  }

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
  public List<StatsCache.StatsCacheKey>
  invalidateAggregatedStats(HbaseMetastoreProto.AggrStatsInvalidatorFilter filter)
      throws IOException {
    /*
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
    */
    return null;
  }

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
   * File metadata related methods
   *********************************************************************************************/

  /**
   * @param fileIds file ID list.
   * @return Serialized file metadata.
   */
  public ByteBuffer[] getFileMetadata(List<Long> fileIds) throws IOException {
    /*
    ByteBuffer[] result = new ByteBuffer[fileIds.size()];
    getFileMetadata(fileIds, result);
    return result;
    */
    return null;
  }

  /**
   * @param fileIds file ID list.
   * @return Serialized file metadata.
   */
  @Override
  public void getFileMetadata(List<Long> fileIds, ByteBuffer[] result) throws IOException {
    /*
    byte[][] keys = new byte[fileIds.size()][];
    for (int i = 0; i < fileIds.size(); ++i) {
      keys[i] = HBaseUtils.makeLongKey(fileIds.get(i));
    }
    multiRead(FILE_METADATA_TABLE, CATALOG_CF, CATALOG_COL, keys, result);
    */
  }

  /**
   * @param fileIds file ID list.
   * @param metadataBuffers Serialized file metadatas, one per file ID.
   * @param addedCols The column names for additional columns created by file-format-specific
   *                  metadata handler, to be stored in the cache.
   * @param addedVals The values for addedCols; one value per file ID per added column.
   */
  @Override
  public void storeFileMetadata(List<Long> fileIds, List<ByteBuffer> metadataBuffers,
      ByteBuffer[] addedCols, ByteBuffer[][] addedVals)
      throws IOException, InterruptedException {
    /*
    byte[][] keys = new byte[fileIds.size()][];
    for (int i = 0; i < fileIds.size(); ++i) {
      keys[i] = HBaseUtils.makeLongKey(fileIds.get(i));
    }
    // HBase APIs are weird. To supply bytebuffer value, you have to also have bytebuffer
    // column name, but not column family. So there. Perhaps we should add these to constants too.
    ByteBuffer colNameBuf = ByteBuffer.wrap(CATALOG_COL);
    @SuppressWarnings("deprecation")
    HTableInterface htab = conn.getHBaseTable(FILE_METADATA_TABLE);
    List<Row> actions = new ArrayList<>(keys.length);
    for (int keyIx = 0; keyIx < keys.length; ++keyIx) {
      ByteBuffer value = (metadataBuffers != null) ? metadataBuffers.get(keyIx) : null;
      ByteBuffer[] av = addedVals == null ? null : addedVals[keyIx];
      if (value == null) {
        actions.add(new Delete(keys[keyIx]));
        assert av == null;
      } else {
        Put p = new Put(keys[keyIx]);
        p.addColumn(CATALOG_CF, colNameBuf, HConstants.LATEST_TIMESTAMP, value);
        if (av != null) {
          assert av.length == addedCols.length;
          for (int colIx = 0; colIx < addedCols.length; ++colIx) {
            p.addColumn(STATS_CF, addedCols[colIx], HConstants.LATEST_TIMESTAMP, av[colIx]);
          }
        }
        actions.add(p);
      }
    }
    Object[] results = new Object[keys.length];
    htab.batch(actions, results);
    // TODO: should we check results array? we don't care about partial results
    conn.flush(htab);
    */
  }

  @Override
  public void storeFileMetadata(long fileId, ByteBuffer metadata,
      ByteBuffer[] addedCols, ByteBuffer[] addedVals) throws IOException, InterruptedException {
    /*
    @SuppressWarnings("deprecation")
    HTableInterface htab = conn.getHBaseTable(FILE_METADATA_TABLE);
    Put p = new Put(HBaseUtils.makeLongKey(fileId));
    p.addColumn(CATALOG_CF, ByteBuffer.wrap(CATALOG_COL), HConstants.LATEST_TIMESTAMP, metadata);
    assert (addedCols == null && addedVals == null) || (addedCols.length == addedVals.length);
    if (addedCols != null) {
      for (int i = 0; i < addedCols.length; ++i) {
        p.addColumn(STATS_CF, addedCols[i], HConstants.LATEST_TIMESTAMP, addedVals[i]);
      }
    }
    htab.put(p);
    conn.flush(htab);
    */
  }

  /**********************************************************************************************
   * Security related methods
   *********************************************************************************************/

  /**
   * Fetch a delegation token
   * @param tokId identifier of the token to fetch
   * @return the delegation token, or null if there is no such delegation token
   * @throws IOException
   */
  public String getDelegationToken(String tokId) throws IOException {
    /*
    byte[] key = HBaseUtils.buildKey(tokId);
    byte[] serialized = read(SECURITY_TABLE, key, CATALOG_CF, DELEGATION_TOKEN_COL);
    if (serialized == null) return null;
    return HBaseUtils.deserializeDelegationToken(serialized);
    */
    return null;
  }

  /**
   * Get all delegation token ids
   * @return list of all delegation token identifiers
   * @throws IOException
   */
  public List<String> scanDelegationTokenIdentifiers() throws IOException {
    /*
    Iterator<Result> iter = scan(SECURITY_TABLE, CATALOG_CF, DELEGATION_TOKEN_COL);
    List<String> ids = new ArrayList<>();
    while (iter.hasNext()) {
      Result result = iter.next();
      byte[] serialized = result.getValue(CATALOG_CF, DELEGATION_TOKEN_COL);
      if (serialized != null) {
        // Don't deserialize the value, as what we're after is the key.  We just had to check the
        // value wasn't null in order to check this is a record with a delegation token and not a
        // master key.
        ids.add(new String(result.getRow(), HBaseUtils.ENCODING));

      }
    }
    return ids;
    */
    return null;
  }

  /**
   * Store a delegation token
   * @param tokId token id
   * @param token delegation token to store
   * @throws IOException
   */
  public void putDelegationToken(String tokId, String token) throws IOException {
    /*
    byte[][] serialized = HBaseUtils.serializeDelegationToken(tokId, token);
    store(SECURITY_TABLE, serialized[0], CATALOG_CF, DELEGATION_TOKEN_COL, serialized[1]);
    */
  }

  /**
   * Delete a delegation token
   * @param tokId identifier of token to drop
   * @throws IOException
   */
  public void deleteDelegationToken(String tokId) throws IOException {
    /*
    byte[] key = HBaseUtils.buildKey(tokId);
    delete(SECURITY_TABLE, key, CATALOG_CF, DELEGATION_TOKEN_COL);
    */
  }

  /**
   * Fetch a master key
   * @param seqNo sequence number of the master key
   * @return the master key, or null if there is no such master key
   * @throws IOException
   */
  public String getMasterKey(Integer seqNo) throws IOException {
    /*
    byte[] key = HBaseUtils.buildKey(seqNo.toString());
    byte[] serialized = read(SECURITY_TABLE, key, CATALOG_CF, MASTER_KEY_COL);
    if (serialized == null) return null;
    return HBaseUtils.deserializeMasterKey(serialized);
    */
    return null;
  }

  /**
   * Get all master keys
   * @return list of all master keys
   * @throws IOException
   */
  public List<String> scanMasterKeys() throws IOException {
    /*
    Iterator<Result> iter = scan(SECURITY_TABLE, CATALOG_CF, MASTER_KEY_COL);
    List<String> keys = new ArrayList<>();
    while (iter.hasNext()) {
      Result result = iter.next();
      byte[] serialized = result.getValue(CATALOG_CF, MASTER_KEY_COL);
      if (serialized != null) {
        keys.add(HBaseUtils.deserializeMasterKey(serialized));

      }
    }
    return keys;
    */
    return null;
  }

  /**
   * Store a master key
   * @param seqNo sequence number
   * @param key master key to store
   * @throws IOException
   */
  public void putMasterKey(Integer seqNo, String key) throws IOException {
    /*
    byte[][] serialized = HBaseUtils.serializeMasterKey(seqNo, key);
    store(SECURITY_TABLE, serialized[0], CATALOG_CF, MASTER_KEY_COL, serialized[1]);
    */
  }

  /**
   * Delete a master key
   * @param seqNo sequence number of master key to delete
   * @throws IOException
   */
  public void deleteMasterKey(Integer seqNo) throws IOException {
    /*
    byte[] key = HBaseUtils.buildKey(seqNo.toString());
    delete(SECURITY_TABLE, key, CATALOG_CF, MASTER_KEY_COL);
    */
  }

  /**
   * One method to print all rows in the security table.  It's not expected to be large.
   * @return each row as one string
   * @throws IOException
   */
  public List<String> printSecurity() throws IOException {
    /*
    HTableInterface htab = conn.getHBaseTable(SECURITY_TABLE);
    Scan scan = new Scan();
    scan.addColumn(CATALOG_CF, MASTER_KEY_COL);
    scan.addColumn(CATALOG_CF, DELEGATION_TOKEN_COL);
    Iterator<Result> iter = htab.getScanner(scan).iterator();
    if (!iter.hasNext()) return Arrays.asList("No security related entries");
    List<String> lines = new ArrayList<>();
    while (iter.hasNext()) {
      Result result = iter.next();
      byte[] val =  result.getValue(CATALOG_CF, MASTER_KEY_COL);
      if (val != null) {
        int seqNo = Integer.parseInt(new String(result.getRow(), HBaseUtils.ENCODING));
        lines.add("Master key " + seqNo + ": " + HBaseUtils.deserializeMasterKey(val));
      } else {
        val = result.getValue(CATALOG_CF, DELEGATION_TOKEN_COL);
        if (val == null) throw new RuntimeException("Huh?  No master key, no delegation token!");
        lines.add("Delegation token " + new String(result.getRow(), HBaseUtils.ENCODING) + ": " +
          HBaseUtils.deserializeDelegationToken(val));
      }
    }
    return lines;
    */
    return null;
  }

  /**********************************************************************************************
   * Sequence methods
   *********************************************************************************************/

  public long peekAtSequence(byte[] sequence) throws IOException {
    /*
    byte[] serialized = read(SEQUENCES_TABLE, sequence, CATALOG_CF, CATALOG_COL);
    return serialized == null ? 0 : Long.parseLong(new String(serialized, HBaseUtils.ENCODING));
    */
    return 0;
  }

  public long getNextSequence(byte[] sequence) throws IOException {
    /*
    byte[] serialized = read(SEQUENCES_TABLE, sequence, CATALOG_CF, CATALOG_COL);
    long val = 0;
    if (serialized != null) {
      val = Long.parseLong(new String(serialized, HBaseUtils.ENCODING));
    }
    byte[] incrSerialized = new Long(val + 1).toString().getBytes(HBaseUtils.ENCODING);
    store(SEQUENCES_TABLE, sequence, CATALOG_CF, CATALOG_COL, incrSerialized);
    return val;
    */
    return 0;
  }

  /**
   * One method to print all entries in the sequence table.  It's not expected to be large.
   * @return each sequence as one string
   * @throws IOException
   */
  public List<String> printSequences() throws IOException {
    /*
    HTableInterface htab = conn.getHBaseTable(SEQUENCES_TABLE);
    Iterator<Result> iter =
        scan(SEQUENCES_TABLE, CATALOG_CF, CATALOG_COL, null);
    List<String> sequences = new ArrayList<>();
    if (!iter.hasNext()) return Arrays.asList("No sequences");
    while (iter.hasNext()) {
      Result result = iter.next();
      sequences.add(new StringBuilder(new String(result.getRow(), HBaseUtils.ENCODING))
          .append(": ")
          .append(new String(result.getValue(CATALOG_CF, CATALOG_COL), HBaseUtils.ENCODING))
          .toString());
    }
    return sequences;
    */
    return null;
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
      throws IOException {
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
    } catch (SQLException e) {
      throw new IOException(e);
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
                                          String... keyVals) throws IOException {
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
    } catch (SQLException e) {
      throw new IOException(e);
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
   * @throws IOException wraps any SQLExceptions that happen
   */
  private void storeOnKey(PostgresTable table, String colName, byte[] colVal, String... keyVals)
      throws IOException {
    List<String> pkCols = table.getPrimaryKeyCols();
    assert pkCols.size() == keyVals.length;
    byte[] serialized = getByKey(table, colName, keyVals);
    if (serialized == null) insertOnKey(table, colName, colVal, keyVals);
    else updateOnKey(table, colName, colVal, keyVals);
  }

  // Do not call directly, use storeOnKey
  private void insertOnKey(PostgresTable table, String colName, byte[] colVal, String... keyVals)
      throws IOException {
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
        throw new IOException(msg);
      }
    } catch (SQLException e) {
      throw new IOException(e);
    }
  }

  private void updateOnKey(PostgresTable table, String colName, byte[] colVal, String... keyVals)
      throws IOException {
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
        throw new IOException(msg);
      }
    } catch (SQLException e) {
      throw new IOException(e);
    }
  }

  private void deleteOnKey(PostgresTable table, String... keyVals) throws IOException {
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
        throw new IOException(msg);
      }
    } catch (SQLException e) {
      throw new IOException(e);
    }
  }

  private long getCount(PostgresTable table) throws IOException {
    StringBuilder buf = new StringBuilder("select count(*) from ")
        .append(table.getName());
    try (Statement stmt = currentConnection.createStatement()) {
      ResultSet rs = stmt.executeQuery(buf.toString());
      rs.next();
      return rs.getLong(1);
    } catch (SQLException e) {
      throw new IOException(e);
    }

  }

  /**********************************************************************************************
   * Printing methods
   *********************************************************************************************/
  private String noSuch(String name, String type) {
    return "No such " + type + ": " + name.replaceAll(HBaseUtils.KEY_SEPARATOR_STR, ".");
  }

  private List<String> noMatch(String regex, String type) {
    return Arrays.asList("No matching " + type + ": " + regex);
  }

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

  private byte[] serialize(TBase obj) throws IOException {
    try {
      TMemoryBuffer buf = new TMemoryBuffer(1000);
      TProtocol protocol = new TCompactProtocol(buf);
      obj.write(protocol);
      return buf.getArray();
    } catch (TException e) {
      throw new IOException(e);
    }
  }

  private void deserialize(TBase obj, byte[] serialized) throws IOException {
    try {
      TMemoryBuffer buf = new TMemoryBuffer(serialized.length);
      buf.read(serialized, 0, serialized.length);
      TProtocol protocol = new TCompactProtocol(buf);
      obj.read(protocol);
    } catch (TException e) {
      throw new IOException(e);
    }
  }

  private String dumpThriftObject(TBase obj) throws TException, UnsupportedEncodingException {
    TMemoryBuffer buf = new TMemoryBuffer(1000);
    TProtocol protocol = new TSimpleJSONProtocol(buf);
    obj.write(protocol);
    return buf.toString("UTF-8");
  }

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
      return addColumn(name, "varchar(255)", false);
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

  }

  /**********************************************************************************************
   * Testing methods and classes
   *********************************************************************************************/
  /*
  @VisibleForTesting
  int countStorageDescriptor() throws IOException {
    ResultScanner scanner = conn.getHBaseTable(SD_TABLE).getScanner(new Scan());
    int cnt = 0;
    Result r;
    do {
      r = scanner.next();
      if (r != null) {
        LOG.debug("Saw record with hash " + Base64.encodeBase64String(r.getRow()));
        cnt++;
      }
    } while (r != null);

    return cnt;
  }

  /**
   * Use this for unit testing only, so that a mock connection object can be passed in.
   * @param connection Mock connection objecct
   */
  /*
  @VisibleForTesting
  static void setTestConnection(HBaseConnection connection) {
    testConn = connection;
  }


  // For testing without the cache
  private static class BogusObjectCache<K, V> extends ObjectCache<K, V> {
    static Counter bogus = new Counter("bogus");

   BogusObjectCache() {
      super(1, bogus, bogus, bogus);
    }

    @Override
    V get(K key) {
      return null;
    }
  }

  private static class BogusPartitionCache extends PartitionCache {
    static Counter bogus = new Counter("bogus");

    BogusPartitionCache() {
      super(1, bogus, bogus, bogus);
    }

    @Override
    Collection<Partition> getAllForTable(String dbName, String tableName) {
      return null;
    }

    @Override
    Partition get(String dbName, String tableName, List<String> partVals) {
      return null;
    }
  }
  */
}
