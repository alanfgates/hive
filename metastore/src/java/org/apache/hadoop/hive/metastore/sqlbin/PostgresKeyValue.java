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

import com.google.common.annotations.VisibleForTesting;
import com.jolbox.bonecp.BoneCPConfig;
import com.jolbox.bonecp.BoneCPDataSource;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.commons.dbcp.ConnectionFactory;
import org.apache.commons.dbcp.DriverManagerConnectionFactory;
import org.apache.commons.dbcp.PoolableConnectionFactory;
import org.apache.commons.dbcp.PoolingDataSource;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.pool.ObjectPool;
import org.apache.commons.pool.impl.GenericObjectPool;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.common.ObjectPair;
import org.apache.hadoop.hive.metastore.RetryingHMSHandler;
import org.apache.hadoop.hive.metastore.api.AggrStats;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Function;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PrincipalPrivilegeSet;
import org.apache.hadoop.hive.metastore.api.Role;
import org.apache.hadoop.hive.metastore.api.SQLForeignKey;
import org.apache.hadoop.hive.metastore.api.SQLPrimaryKey;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.hbase.HBaseStore;
import org.apache.hadoop.hive.metastore.hbase.HbaseMetastoreProto;
import org.apache.hadoop.hive.metastore.hbase.MetadataStore;
import org.apache.hadoop.hive.metastore.parser.ExpressionTree;
import org.apache.hadoop.hive.metastore.txn.TxnHandler;
import org.apache.hadoop.hive.ql.log.PerfLogger;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.thrift.protocol.TCompactProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TMemoryBuffer;

import javax.sql.DataSource;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.sql.Array;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;


/**
 * Class to manage storing object in and reading them from HBase.
 *
 * Currently all classes are serialized directly as thrift objects and stored as is.  This has
 * the advantage that the code doesn't need to spend any time translating object to relational.
 * Most objects are stored in an object specific table (i.e., one for databases, one for tables,
 * etc.).  Partitions are stored in a table specific to their Hive table (ie, there's a 1-1
 * mapping between partitioned Hive tables and Postgres tables).  This allows to use the
 * partition keys as a primary key.  It also keeps from generating one enormous partition table.
 *
 * Currently the implementation is Postgres specific.  However, 90% of this should work
 * regardless of the database.  If the inner classes for PostgresTable and PostgresColumn are
 * abstracted out, with an implementation for each database type, I believe the rest of this could
 * be shared.
 *
 * A couple of thoughts on possible improvements:
 *
 * 1) Right now the code doesn't pull the storage descriptors out of partitions.  This will badly
 * bloat both the database and memory.  If we do pull them out there's no need to reference count
 * in the db, we can just run a background process every hour or so that does a join on the keys
 * and finds any abandoned storage descriptors.
 *
 * 2) The column stats are kept as one big serialized blob.  Since there is a partition table per
 * Hive table we could instead create columns for every stat.  I'm not sure how well this would
 * work since it would create a table with roughly 5x columns of the number of Hive columns.  And
 * I don't know if the NDV calculations can be done in the database.  But the up side of this
 * would be most of the stats aggregation could be done by a single SQL query.  It's not clear
 * this is worth it.
 */
public class PostgresKeyValue implements MetadataStore {
  private final static List<PostgresTable> initialPostgresTables = new ArrayList<>();

  // Column names used throughout
  // General column name that catalog objects are stored in.
  final static String CATALOG_COL = "catalog_object";
  // General column for storing stats
  final static String STATS_COL = "stats";
  final static String DB_NAME_COL = "db_name";
  final static String TABLE_NAME_COL = "table_name";
  final static String UUID_MSB_COL = "part_uuid_msb";
  final static String UUID_LSB_COL = "part_uuid_lsb";
  // Default length for varchar columns
  final static int VARCHAR_LENGTH = 255;

  // These two are used together a lot, so let's store them together and avoid all the object creation.
  private final static List<String> CAT_AND_STATS_COLS = Arrays.asList(CATALOG_COL, STATS_COL);
  private final static List<String> UUID_COLS = Arrays.asList(UUID_MSB_COL, UUID_LSB_COL);

  // If this is set in the config, the cache will be set such that it always misses, thus ensuring
  // we're really reading from and writing to postgres.
  @VisibleForTesting
  static final String CACHE_OFF = "hive.metastore.postgres.nocache";
  static final String TEST_POSTGRES_JDBC = "hive.test.postgres.jdbc";
  static final String TEST_POSTGRES_USER = "hive.test.postgres.user";
  static final String TEST_POSTGRES_PASSWD = "hive.test.postgres.password";
  // WARNING - setting the following will likely make subsequent test runs fail, but it's
  // useful for debugging at times.
  static final String KEEP_TABLES_AFTER_TEST = "hive.test.postgres.keeptables";


  // Define the Database table
  @VisibleForTesting
  final static PostgresTable DB_TABLE =
      new PostgresTable("MS_DBS")
        .addStringKey(DB_NAME_COL)
        .addByteColumn(CATALOG_COL)
        .addToInitialTableList();

  // Define the Function table
  final static PostgresTable FUNC_TABLE =
      new PostgresTable("MS_FUNCS")
          .addStringKey(DB_NAME_COL)
          .addStringKey("function_name")
          .addByteColumn(CATALOG_COL)
          .addToInitialTableList();

  // Define the global privileges table.  Giving this a key is shear laziness since it will
  // always have only one record.  But it makes it easier to write the code because there are
  // already a bunch of methods to store and retrieve on a key.
  final static String GLOBAL_PRIVS_KEY = "globalprivs";
  final static PostgresTable GLOBAL_PRIVS_TABLE =
      new PostgresTable("MS_GLOBAL_PRIVS")
          .addStringKey(GLOBAL_PRIVS_KEY)
          .addByteColumn(CATALOG_COL)
          .addToInitialTableList();

  final static String R_NAME_COL = "role_name";
  final static String R_PP_COL = "participating_principals";
  final static PostgresTable ROLE_TABLE =
      new PostgresTable("MS_ROLES")
          .addStringKey(R_NAME_COL)
          .addByteColumn(CATALOG_COL)
          .addByteColumn(R_PP_COL)
          .addToInitialTableList();

  final static String U2R_USER_NAME_COLUMN = "user_name";
  final static String U2R_ROLE_LIST_COLUMN = "roles_user_is_in";
  final static PostgresTable USER_TO_ROLE_TABLE =
      new PostgresTable("MS_USER_TO_ROLE")
          .addStringKey(U2R_USER_NAME_COLUMN)
          .addColumn(U2R_ROLE_LIST_COLUMN, "varchar(255) array")
          .addToInitialTableList();

  /*
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

  @VisibleForTesting
  final static String PRIMARY_KEY_COL = "primary_key_obj";
  final static String FOREIGN_KEY_COL = "foreign_key_obj";
  final static PostgresTable TABLE_TABLE =
      new PostgresTable("MS_TBLS")
          .addStringKey(DB_NAME_COL)
          .addStringKey(TABLE_NAME_COL)
          .addLongColumn(UUID_LSB_COL)
          .addLongColumn(UUID_MSB_COL)
          .addByteColumn(CATALOG_COL)
          .addByteColumn(STATS_COL)
          .addByteColumn(PRIMARY_KEY_COL)
          .addByteColumn(FOREIGN_KEY_COL)
          .addToInitialTableList();

  // The UUID columns are changed each time a partition is added or dropped.  This gives us a way
  // of knowing when we should update the entries in the partition cache.

  /*
  final static PostgresTable INDEX_TABLE =
      new PostgresTable("MS_INDEX")
        .addStringKey("index_name")
        .addByteColumn(CATALOG_COL);

  final static PostgresTable FILE_METADATA_TABLE =
      new PostgresTable("MS_FILE_METADATA")
        .addColumn("file_ids", "bytea", true)
        .addByteColumn(CATALOG_COL)
        .addByteColumn(STATS_COL);
        */

  static final private Logger LOG = LoggerFactory.getLogger(PostgresKeyValue.class.getName());

  // Hive has a default partition name it uses in dynamic partitioning when it doesn't yet know
  // the name of the partition.  Since it is a string it plays poorly with non-string types.  We
  // translate it to the following values for each type
  static final private Long DEFAULT_PARTITION_LONG = Long.MIN_VALUE + 1;

  // This is implemented by a ConcurrentHashMap so it can be simultaneously accessed by multiple
  // threads.  The key is expected to be the partition table name, that is
  // "parttable_<dbname>_<tablename>"
  private static Map<String, PostgresTable> partitionTables = new ConcurrentHashMap<>();

  private static DataSource connPool;
  private static boolean tablesCreated = false;
  private static PostgresPartitionCache partCache;

  // Maps between the types, built just once to avoid duplicating code everywhere.  Don't access
  // these directly, use the get methods for each one, as they may be null the first time.
  //private static Map<Integer, String> postgresToHiveTypes;
  private static Map<String, String> hiveToPostgresTypes;

  private Configuration conf;
  // A HiveConf built from our conf.  A few calls we make need a HiveConf, and we don't want to
  // depend on the caller
  private static HiveConf hiveConf;
  private Connection currentConnection;

  private Map<String, Database> dbCache;
  private Map<ObjectPair<String, String>, Table> tableCache;
  private Map<ObjectPair<String, String>, SeparableColumnStatistics> tableStatsCache;
  private int clearCnt;

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
    if (dbCache == null) {
      try {
        staticInit(conf);

        if (conf.getBoolean(CACHE_OFF, false)) {
          LOG.info("Turning off caching, I hope you know what you're doing!");
          dbCache = new MissingCache<>();
          tableCache = new MissingCache<>();
          tableStatsCache = new MissingCache<>();
        } else {
          // TODO make the sizes configurable
          // TODO bound the caches
          dbCache = new HashMap<>(5);
          tableCache = new HashMap<>(20);
          tableStatsCache = new HashMap<>(20);
        }
        clearCnt = 0;
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  // Do shared configuration such as connecting to postgres, building the part cache, etc.
  private static synchronized void staticInit(Configuration conf) throws IOException {
    if (connPool == null) {
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
        config.setPartitionCount(5);
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
      } else if ("none".equals(connectionPooler)) {
        LOG.info("Choosing not to pool JDBC connections");
        connPool = new TxnHandler.NoPoolConnectionPool((HiveConf)conf);
      } else {
        throw new RuntimeException("Unknown JDBC connection pooling " + connectionPooler);
      }
    }
    if (partCache == null) {
      partCache = new PostgresPartitionCache(conf);
    }

  }

  // Synchronize this so not everyone's doing it at once.  This should only be called by begin()
  private void createTablesIfNotExist() throws SQLException {
    assert currentConnection != null;
    if (!tablesCreated) {
      // TODO - this isn't sufficient.  If two metastore instances are started on separate machines
      // at the same time they'll be in a race condition.  I need to figure out how to lock
      // between these.  Maybe a file in HDFS?
      synchronized (PostgresKeyValue.class) {
        if (!tablesCreated) { // check again, someone else might have done it while we waited.
          boolean commit = false;
          // First, figure out if they are already there.  Just select from one of the tables
          // and see if it returns.  Some older versions of Postgres' JDBC driver don't support
          // DatabaseMetaData.getTables.
          try (Statement stmt = currentConnection.createStatement()) {
            stmt.executeQuery("select 1 from " + TABLE_TABLE.getName() + " where table_name = " +
                "'there_better_never_be_a_table_named_this'");
            // If we're still here, that means the query didn't error out, so do nothing...
            LOG.debug("Tables have already been created, not creating.");
            commit = true;
          } catch (SQLException e) {
            LOG.info("Caught SQL exception", e);
            currentConnection.rollback(); // Rollback the failed query transaction so we can
            // start another
            LOG.info("Tables not found in catalog " + currentConnection.getCatalog() +
                ", creating them...");
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

  /**
   * Drop a postgres table.  This is only intended for use by tests so they can have a clean slate.
   * You should not be dropping any tables in postgres in non-testing cases.
   * @param tableName name of table to drop.
   * @throws SQLException
   */
  @VisibleForTesting
  void dropPostgresTable(String tableName) throws SQLException {
    StringBuilder buf = new StringBuilder("drop table ")
        .append(tableName);
    try (Statement stmt = currentConnection.createStatement()) {
      LOG.info("Dropping table with statement <" + buf.toString() + ">");
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
    if (currentConnection == null) {
      // We can't connect to postgres in the constructor because we don't have the configuration
      // file yet at that point.  So instead always check here in the begin to see if we've connected.
      init();
      beginInternal();
      createTablesIfNotExist();
    }
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
      byte[] serialized = getBinaryColumnByKey(DB_TABLE, CATALOG_COL, name);
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
    try {
      List<byte[]> results = scanOnKeyWithRegex(DB_TABLE, CATALOG_COL, regex);
      List<Database> dbs = new ArrayList<>();
      for (byte[] serialized : results) {
        Database db = new Database();
        deserialize(db, serialized);
        dbs.add(db);
      }
      return dbs;
    } catch (SQLException e) {
      LOG.error("Unable to scan databases", e);
      throw new IOException(e);
    }
  }

  /**
   * Create a new database object
   * @param database database object to store
   * @throws SQLException
   */
  public void insertDb(Database database) throws SQLException {
    byte[] serialized = serialize(database);
    insertOnKey(DB_TABLE, CATALOG_COL, serialized, database.getName());
    dbCache.put(database.getName(), database);
  }

  /**
   * Update an existing database object
   * @param database database object to update
   * @throws SQLException if something goes wrong
   */
  public void updateDb(Database database) throws SQLException {
    byte[] serialized = serialize(database);
    updateOnKey(DB_TABLE, CATALOG_COL, serialized, database.getName());
    dbCache.put(database.getName(), database);
  }

  @Override
  public void writeBackChangedDatabases(List<Database> dbs) throws IOException {
    try {
      for (Database db : dbs) {
        updateOnKey(DB_TABLE, CATALOG_COL, serialize(db), db.getName());
      }
    } catch (SQLException e) {
      LOG.error("Unable to write databases", e);
      throw new IOException(e);
    }
  }

  /**
   * Drop a database
   * @param name name of db to drop
   * @throws SQLException
   */
  public boolean deleteDb(String name) throws SQLException {
    dbCache.remove(name);
    return deleteOnKey(DB_TABLE, name);
  }

  public int getDatabaseCount() throws SQLException {
    return (int)getCount(DB_TABLE);
  }

  /**********************************************************************************************
   * File metadata related methods
   *********************************************************************************************/
  @Override
  public void getFileMetadata(List<Long> fileIds, ByteBuffer[] result) throws IOException {

  }

  @Override
  public void storeFileMetadata(List<Long> fileIds, List<ByteBuffer> metadataBuffers,
                                ByteBuffer[] addedCols, ByteBuffer[][] addedVals) throws IOException, InterruptedException {

  }

  @Override
  public void storeFileMetadata(long fileId, ByteBuffer metadata, ByteBuffer[] addedCols, ByteBuffer[] addedVals) throws IOException, InterruptedException {

  }

  /**********************************************************************************************
   * Function related methods
   *********************************************************************************************/

  /**
   * Create a function object
   * @param function function object to store
   * @throws SQLException
   */
  void insertFunction(Function function) throws SQLException {
    byte[] serialized = serialize(function);
    insertOnKey(FUNC_TABLE, CATALOG_COL, serialized, function.getDbName(),
        function.getFunctionName());
  }

  /**
   * Update a function object
   * @param function function object to store
   * @throws SQLException
   */
  void updateFunction(Function function) throws SQLException {
    byte[] serialized = serialize(function);
    updateOnKey(FUNC_TABLE, CATALOG_COL, serialized, function.getDbName(),
        function.getFunctionName());
  }

  /**
   * Get a function
   * @param dbName database the function lives in
   * @param funcName function name
   * @return function object, or null if no such function
   * @throws SQLException
   */
  Function getFunction(String dbName, String funcName) throws SQLException {
    byte[] serialized = getBinaryColumnByKey(FUNC_TABLE, CATALOG_COL, dbName, funcName);
    if (serialized == null) return null;
    Function func = new Function();
    deserialize(func, serialized);
    return func;
  }

  /**
   * Get a list of functions.
   * @param dbName Name of the database to search in.  Null means to search all.
   * @param regex Regular expression to use in searching for function names.  It is expected to
   *              be a Java regular expression.  If it is null then all functions will be returned.
   * @return list of functions matching the regular expression.
   * @throws SQLException
   */
  public List<Function> scanFunctions(String dbName, String regex) throws SQLException {
    String[] key = dbName == null ? null : new String[]{dbName};
    List<byte[]> results = scanOnKeyWithRegex(FUNC_TABLE, CATALOG_COL, regex, key);
    List<Function> funcs = new ArrayList<>();
    for (byte[] serialized : results) {
      Function func = new Function();
      deserialize(func, serialized);
      funcs.add(func);
    }
    return funcs;
  }

  /**
   *
   * @param dbName database the function is in
   * @param funcName function name
   * @return true if the function was dropped
   * @throws SQLException
   */
  public boolean deleteFunction(String dbName, String funcName) throws SQLException {
    return deleteOnKey(FUNC_TABLE, dbName, funcName);
  }

  /**********************************************************************************************
   * Global Privilege related methods
   *********************************************************************************************/

  /**
   * Fetch the global privileges object
   * @return
   * @throws IOException
   */
  public PrincipalPrivilegeSet getGlobalPrivs() throws IOException {
    try {
      byte[] serialized = getBinaryColumnByKey(GLOBAL_PRIVS_TABLE, CATALOG_COL, GLOBAL_PRIVS_KEY);
      if (serialized == null) return null;
      PrincipalPrivilegeSet globalPrivs = new PrincipalPrivilegeSet();
      deserialize(globalPrivs, serialized);
      return globalPrivs;
    } catch (SQLException e) {
      LOG.error("Failed to fetch global privileges", e);
      throw new IOException(e);
    }
  }

  /**
   * Store the global privileges object
   * @throws IOException
   */
  public void putGlobalPrivs(PrincipalPrivilegeSet privs) throws IOException {
    try {
      byte[] serialized = serialize(privs);
      storeOnKey(GLOBAL_PRIVS_TABLE, CATALOG_COL, serialized, GLOBAL_PRIVS_KEY);
    } catch (SQLException e) {
      LOG.error("Failed to store global privileges", e);
      throw new IOException(e);
    }
  }

  /**********************************************************************************************
   * Partition related methods
   * The whole way that partitions are stored requires some explanation.  Each partition is stored
   * in its own table, with the primary key of the table being the partition columns of the
   * Hive table.  This allows us to correctly map the data types and be very efficient in our
   * filtering of partitions.  Statistics are stored in the same table, and are pre-fetched along
   * with the partition on the assumption that when asking for a partition the user will often
   * also soon ask for the statistics.
   *********************************************************************************************/

  /**
   * Fetch one partition.  Fetch the partition statistics at the same time on the guess that we
   * will probably want them later.
   * @param dbName database table is in
   * @param tableName table partition is in
   * @param partVals list of values that specify the partition, given in the same order as the
   *                 columns they belong to
   * @return The partition object, or null if there is no such partition
   * @throws SQLException
   */
  public Partition getPartition(String dbName, String tableName, List<String> partVals)
      throws SQLException {
    List<String> partCacheKey = buildPartCacheKey(dbName, tableName, partVals);
    LOG.debug("Partition not found in cache, going to Postgres");
    Table hTable = getTable(dbName, tableName);
    PostgresTable pTable = findPartitionTable(dbName, tableName, false);
    byte[] serialized = getPartitionByKey(pTable, CATALOG_COL, translatePartVals(hTable, partVals));
    if (serialized == null) return null;
    Partition part = new Partition();
    deserialize(part, serialized);
    return part;
  }

  /**
   * Add a partition.
   * @param partition partition object to add
   * @throws SQLException
   */
  public void putPartition(Partition partition) throws SQLException {
    // We need to figure out which table we're going to write this too.
    PostgresTable pTable = findPartitionTable(partition.getDbName(), partition.getTableName(),
        false);
    // We will need the Hive table to properly translate the partition values.
    Table hTable = getTable(partition.getDbName(), partition.getTableName());
    List<Object> translated = translatePartVals(hTable, partition.getValues());
    byte[] serialized = serialize(partition);
    // We need to determine if this partition is already in store.  If so, we need to update
    // instead of insert.
    if (partitionExists(pTable, translated)) {
      updatePartitionOnKey(pTable, CATALOG_COL, serialized, translated);
    } else {
      insertPartitionOnKey(pTable, CATALOG_COL, serialized, partition, translated);
    }
    List<String> partCacheKey =
        buildPartCacheKey(partition.getDbName(), partition.getTableName(), partition.getValues());
    updateUUID(hTable.getDbName(), hTable.getTableName());
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
    PerfLogger perfLogger = PerfLogger.getPerfLogger((HiveConf)conf, false);
    perfLogger.PerfLogBegin(RetryingHMSHandler.class.getName(), "scanPartitionsInTable");
    List<Partition> parts = partCache.getAllpartitions(this, dbName, tableName, maxPartitions);
    perfLogger.PerfLogEnd(RetryingHMSHandler.class.getName(), "scanPartitionsInTable");
    return parts;
  }

  /**
   * Get the partition names of all partitions in a table.  The partitions and stats will be
   * cached but not deserialized.
   * @param dbName database table is in
   * @param tableName table name
   * @return list of names of all partitions in the cluster
   * @throws SQLException
   */
  public List<String> scanPartitionNames(String dbName, String tableName)
      throws SQLException {
    PerfLogger perfLogger = PerfLogger.getPerfLogger((HiveConf)conf, false);
    perfLogger.PerfLogBegin(RetryingHMSHandler.class.getName(), "scanPartitionNames");
    List<String> partNames = partCache.getPartitionNames(this, dbName, tableName);
    perfLogger.PerfLogEnd(RetryingHMSHandler.class.getName(), "scanPartitionNames");
    return partNames;
  }

  Map<String, PostgresPartitionCache.CacheValueElement>
  cachePartitions(String dbName, String tableName) throws SQLException {
    PerfLogger perfLogger = PerfLogger.getPerfLogger((HiveConf)conf, false);
    perfLogger.PerfLogBegin(RetryingHMSHandler.class.getName(), "cachePartitions");
    Map<String, PostgresPartitionCache.CacheValueElement> partMap = new HashMap<>();
    PostgresTable pTable = findPartitionTable(dbName, tableName, false);
    StringBuilder buf = new StringBuilder("select ");
    for (String key : pTable.getPrimaryKeyCols()) {
      buf.append(key)
          .append(", ");
    }
    buf.append(CATALOG_COL)
        .append(", ")
        .append(STATS_COL)
        .append(" from ")
        .append(pTable.getName());
    try (Statement stmt = currentConnection.createStatement()) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Going to execute query " + buf.toString());
      }
      ResultSet rs = stmt.executeQuery(buf.toString());
      List<PostgresColumn> pkCols = pTable.getPKColsAsPostgresColumns();
      while (rs.next()) {
        int pos = 1;
        List<Object> pVals = new ArrayList<>(pkCols.size());
        for (; pos <= pkCols.size(); pos++) {
          pVals.add(pkCols.get(pos - 1).getFromResultSet(rs, pos));
        }
        MaybeSerialized<Partition> part = new MaybeSerialized<>(Partition.class, rs.getBytes(pos++));
        List<String> untranslatedPartVals = untranslatePartVals(pVals);
        String partName = FileUtils.makePartName(pTable.getPrimaryKeyCols(), untranslatedPartVals);
        byte[] serializedStats = rs.getBytes(pos);
        SeparableColumnStatistics stats = null;
        if (serializedStats != null) {
          stats = new SeparableColumnStatistics(serializedStats);
        }
        partMap.put(partName, new PostgresPartitionCache.CacheValueElement(part, stats));
      }
    }
    perfLogger.PerfLogEnd(RetryingHMSHandler.class.getName(), "cachePartitions");
    return partMap;
  }

  /**
   * Get a list of partitions.
   * @param dbName database table is in
   * @param tableName table partitions are in
   * @param partNames names of partitions to be fetched
   * @return list of partitions
   */
  public List<Partition> fetchPartitionsByName(String dbName, String tableName,
                                               List<String> partNames) throws SQLException {
    PerfLogger perfLogger = PerfLogger.getPerfLogger((HiveConf)conf, false);
    perfLogger.PerfLogBegin(RetryingHMSHandler.class.getName(), "fetchPartitionsFromCache");
    List<Partition> result = partCache.fetchPartitions(this, dbName, tableName, partNames);
    perfLogger.PerfLogEnd(RetryingHMSHandler.class.getName(), "fetchPartitionsFromCache");
    return result;
  }

  /**
   * Pass a partition filter down to Postgres and use that to determine the partitions to fetch.
   * @param dbName database the table is in
   * @param tableName table name
   * @param exprTree expression tree, unparsed already
   * @param result list of partitions matching the expression, or perhaps more if we returned true
   * @return if true, then there are partitions in the result that are
   */
  public boolean scanPartitionsByExpr(String dbName, String tableName, ExpressionTree exprTree,
                                      int maxPartitions, List<Partition> result)
      throws SQLException, MetaException {
    // For now make this always fail, since Hive never calls it.  We may want in the future to
    // allow it to work in the case where we haven't already cached the partitions.  In that case
    // we have to figure out how to properly cache the stats
    throw new UnsupportedOperationException();
    /*
    PostgresTable pTable = findPartitionTable(dbName, tableName, false);
    Table hTable = getTable(dbName, tableName);
    PartitionFilterBuilder visitor = new PartitionFilterBuilder(pTable, hTable);
    exprTree.accept(visitor);
    if (visitor.generatedFilter.hasError()) {
      LOG.warn("Failed to push down SQL filter to metastore");
      result.addAll(scanPartitionsInTable(dbName, tableName, maxPartitions));
      return true;
    }

    // Otherwise, let's use this filter to run the query.
    int maxPartitionsInternal = maxPartitions > 0 ? maxPartitions : Integer.MAX_VALUE;
    StringBuilder buf = new StringBuilder("select ")
        .append(CATALOG_COL)
        .append(", ")
        .append(STATS_COL)
        .append(" from ")
        .append(pTable.getName())
        .append(" where ")
        .append(visitor.generatedFilter.getFilter());
    if (LOG.isDebugEnabled()) {
      LOG.debug("Going to prepare statement " + buf.toString());
    }
    try (PreparedStatement stmt = currentConnection.prepareStatement(buf.toString())) {
      assert visitor.pVals.size() == visitor.pTypes.size();
      for (int i = 0; i < visitor.pVals.size(); i++) {
        switch (visitor.pTypes.get(i)) {
          case Types.VARCHAR:
            stmt.setString(i + 1, (String)visitor.pVals.get(i));
            break;

          default:
            throw new RuntimeException("Unsupported type " + visitor.pTypes.get(i));
        }
      }
      ResultSet rs = stmt.executeQuery();
      while (rs.next() && result.size() <= maxPartitionsInternal) {
        Partition part = new Partition();
        deserialize(part, rs.getBytes(1));
        result.add(part);
        byte[] serializedStats = rs.getBytes(2);
        if (serializedStats != null) {
          SeparableColumnStatistics stats = new SeparableColumnStatistics(serializedStats);
          partStatsCache.put(buildPartCacheKey(dbName, tableName, part.getValues()), stats);
        }
      }
      return false;
    }
    */
  }

  public boolean dropPartition(String dbName, String tableName, List<String> partVals)
      throws SQLException {
    int rowsDeleted = dropPartitions(dbName, tableName, Collections.singletonList(partVals));
    if (rowsDeleted == 0) {
      LOG.debug("No partition " + HBaseStore.partNameForErrorMsg(dbName, tableName, partVals) +
          " found to drop");
      return false;
    } else if (rowsDeleted == 1) {
      updateUUID(dbName, tableName);
      return true;
    } else {
      String msg = "Dropping single partition " +
          HBaseStore.partNameForErrorMsg(dbName, tableName, partVals) + " resulted in dropping " +
          "more than one row from the table.  Way bad!";
      LOG.error(msg);
      throw new RuntimeException(msg);
    }
  }

  public int dropPartitions(String dbName, String tableName, List<List<String>> partVals)
      throws SQLException {
    List<List<Object>> allTranslated = new ArrayList<>();
    // We need to figure out which table we're going to write this too.
    PostgresTable pTable = findPartitionTable(dbName, tableName, false);
    // We will need the Hive table to properly translate the partition values.
    Table hTable = getTable(dbName, tableName);

    StringBuilder buf = new StringBuilder("delete from ")
        .append(pTable.getName())
        .append(" where ");
    boolean first = true;
    for (List<String> pVals : partVals) {
      if (first) first = false;
      else buf.append(" or ");
      buf.append('(');
      List<Object> translated = translatePartVals(hTable, pVals);
      allTranslated.add(translated);
      assert translated.size() == pTable.getPrimaryKeyCols().size();
      for (int i = 0; i < translated.size(); i++) {
        if (i != 0) buf.append(" and ");
        buf.append(pTable.getPrimaryKeyCols().get(i))
            .append(" = ?");
      }
      buf.append(')');
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Going to prepare statement " + buf.toString());
    }
    try (PreparedStatement stmt = currentConnection.prepareStatement(buf.toString())) {
      int offset = 1;
      for (List<Object> translated : allTranslated) {
        fillInPartitionKeyWithSpecificTypes(pTable.getPrimaryKeyCols(), stmt, translated, offset);
        offset += translated.size();
      }
      int rc = stmt.executeUpdate();
      updateUUID(dbName, tableName);
      return rc;
    }
  }

  private void insertPartitionOnKey(PostgresTable pTable, String colName, byte[] serialized,
                                    Partition part, List<Object> translatedKeys) throws SQLException {
    List<String> pkCols = pTable.getPrimaryKeyCols();
    StringBuilder buf = new StringBuilder("insert into ")
        .append(pTable.getName())
        .append(" (");
    for (String pkCol : pkCols) {
      buf.append(pkCol)
          .append(", ");
    }
    buf.append(colName)
        .append(") values (");
    for (int i = 0; i < pkCols.size(); i++) buf.append("?,");
    buf.append("?)");
    if (LOG.isDebugEnabled()) {
      LOG.debug("Going to prepare statement " + buf.toString());
    }
    try (PreparedStatement stmt = currentConnection.prepareStatement(buf.toString())) {
      fillInPartitionKeyWithSpecificTypes(pkCols, stmt, translatedKeys, 1);
      stmt.setBytes(part.getValuesSize() + 1, serialized);

      int rowsInserted = stmt.executeUpdate();
      if (rowsInserted != 1) {
        String msg = "Expected to insert a row into " + pTable.getName() + " with key " +
            StringUtils.join(part.getValues(), ':') + ", but did not";
        LOG.error(msg);
        throw new RuntimeException(msg);
      }
    }
  }

  private void updatePartitionOnKey(PostgresTable pTable, String colName, byte[] serialized,
                                    List<Object> translatedKeys) throws SQLException {
    List<String> pkCols = pTable.getPrimaryKeyCols();
    StringBuilder buf = new StringBuilder("update ")
        .append(pTable.getName())
        .append(" set ")
        .append(colName)
        .append(" = ?")
        .append(" where ");
    boolean first = true;
    for (String pkCol : pkCols) {
      if (first) first = false;
      else buf.append(" and ");
      buf.append(pkCol)
          .append(" = ? ");
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Going to prepare statement " + buf.toString());
    }
    try (PreparedStatement stmt = currentConnection.prepareStatement(buf.toString())) {
      stmt.setBytes(1, serialized);
      fillInPartitionKeyWithSpecificTypes(pkCols, stmt, translatedKeys, 2);

      int rowsUpdated = stmt.executeUpdate();
      if (rowsUpdated != 1) {
        String msg = "Expected to update a row in " + pTable.getName() + " with key " +
            StringUtils.join(untranslatePartVals(translatedKeys), ':') + ", but did not";
        LOG.error(msg);
        throw new RuntimeException(msg);
      }
    }
  }

  private void fillInPartitionKeyWithSpecificTypes(List<String> pkCols, PreparedStatement stmt,
                                                   List<Object> translatedKeys, int colOffset)
      throws SQLException {
    for (int i = 0; i < pkCols.size(); i++) {
      stmt.setObject(i + colOffset, translatedKeys.get(i));
    }
  }

  private boolean partitionExists(PostgresTable table, List<Object> partVals) throws SQLException {
    List<String> pkCols = table.getPrimaryKeyCols();
    assert pkCols.size() == partVals.size();
    StringBuilder buf = new StringBuilder("select 1 from ")
        .append(table.getName())
        .append(" where ");
    for (int i = 0; i < partVals.size(); i++) {
      if (i != 0) buf.append(" and ");
      buf.append(pkCols.get(i))
          .append(" = ? ");
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Going to prepare statement " + buf.toString());
    }
    PreparedStatement stmt = currentConnection.prepareStatement(buf.toString());
    for (int i = 0; i < partVals.size(); i++) {
      stmt.setObject(i + 1, partVals.get(i));
    }
    ResultSet rs = stmt.executeQuery();
    return rs.next();
  }

  private byte[] getPartitionByKey(PostgresTable table, String colName, List<Object> partVals)
      throws SQLException {
    byte[][] result = getPartitionByKey(table, Collections.singletonList(colName), partVals);
    return result == null ? null : result[0];
  }

  private byte[][] getPartitionByKey(PostgresTable table, List<String> colNames,
                                     List<Object> partVals) throws SQLException {
    List<String> pkCols = table.getPrimaryKeyCols();
    assert pkCols.size() == partVals.size();
    StringBuilder buf = new StringBuilder("select ");
    boolean first = true;
    for (String colName : colNames) {
      if (first) first = false;
      else buf.append(", ");
      buf.append(colName);
    }
    buf.append(" from ")
        .append(table.getName())
        .append(" where ");
    for (int i = 0; i < partVals.size(); i++) {
      if (i != 0) buf.append(" and ");
      buf.append(pkCols.get(i))
          .append(" = ? ");
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Going to prepare statement " + buf.toString());
    }
    PreparedStatement stmt = currentConnection.prepareStatement(buf.toString());
    for (int i = 0; i < partVals.size(); i++) {
      stmt.setObject(i + 1, partVals.get(i));
    }
    ResultSet rs = stmt.executeQuery();
    if (rs.next()) {
      byte[][] result = new byte[colNames.size()][];
      for (int i = 0; i < colNames.size(); i++) {
        result[i] = rs.getBytes(i + 1);
      }
      return result;
    } else {
      return null;
    }
  }

  private PostgresTable findPartitionTable(String dbName, String tableName,
                                           boolean buildIfNotExists) throws SQLException {
    String partTableName = buildPartTableName(dbName, tableName);
    PostgresTable table = partitionTables.get(partTableName);
    if (table == null) {
      LOG.debug("Couldn't find table in memory, looking to see if it's in the db " + tableName);
      // Check to see if the table exists but we don't have it in the cache.  I don't put this
      // part in the synchronized section because it's ok if two threads do this at the same time,
      // if a bit wasteful.
      // Try to select from the table and see if it
      DatabaseMetaData dmd = currentConnection.getMetaData();
      ResultSet rs = dmd.getTables(currentConnection.getCatalog(), null, partTableName, null);
      if (rs.next()) {
        // So we know the table is there, we understand the mapping so we can build the
        // PostgresTable object.
        table = postgresTableFromPartition(dbName, tableName);
      } else if (buildIfNotExists) {
        LOG.info("Unable to find table " + tableName + " in Postgres, creating it");
        // The table isn't there.  We need to lock because we don't want a race condition on
        // creating the table.
        // TODO - this isn't sufficient.  If two Hive instances on different machines (metastore
        // or task in the case of MoveFile) try to access the partition at the same time this
        // won't prevent a race condition.  I suspect the right answer is to have a lock table
        // in postgres.  For now I've minimized the likelyhood of this by calling this method
        // from create table when appropriate.  But this won't always solve the problem.
        synchronized (PostgresKeyValue.class) {
          // check again, someone may have built it while we waited for the lock.
          table = partitionTables.get(partTableName);
          if (table == null) {
            table = postgresTableFromPartition(dbName, tableName);
            createTableInPostgres(table);
          }
        }
      }
      // Put it in the map, but only if we were supposed to build it.  It's ok if we're in a race
      // condition and someone else already has.
      if (buildIfNotExists) partitionTables.put(partTableName, table);
    }
    return table;
  }

  public PostgresTable postgresTableFromPartition(String dbName, String tableName) throws
      SQLException {
    PostgresTable pTable = new PostgresTable(buildPartTableName(dbName, tableName));
    Table hTable = getTable(dbName, tableName);
    for (FieldSchema fs : hTable.getPartitionKeys()) {
      pTable.addColumn(fs, true);
    }
    pTable.addByteColumn(CATALOG_COL)
        .addByteColumn(STATS_COL);
    return pTable;
  }

  // TODO I'm doing a lot of double translation, see if I can't re-arrange things to avoid that
  private List<Object> translatePartVals(Table hTable, List<String> partVals) {
    List<Object> translated = new ArrayList<>(partVals.size());
    List<FieldSchema> partCols = hTable.getPartitionKeys();
    assert partCols.size() == partVals.size();
    for (int i = 0; i < partVals.size(); i++) {
      String hiveType = partCols.get(i).getType();
      if (hiveType.equals(serdeConstants.STRING_TYPE_NAME) ||
          hiveType.equals(serdeConstants.VARCHAR_TYPE_NAME) ||
          hiveType.equals(serdeConstants.CHAR_TYPE_NAME)) {
        translated.add(partVals.get(i));
      } else if (hiveType.equals(serdeConstants.BIGINT_TYPE_NAME)) {
        // Special case, if it's the default partition value translate that to a predefined default
        if (partVals.get(i).equals(HiveConf.getVar(conf, HiveConf.ConfVars.DEFAULTPARTITIONNAME))) {
          translated.add(DEFAULT_PARTITION_LONG);
        } else {
          translated.add(Long.parseLong(partVals.get(i)));
        }
      } else {
        throw new RuntimeException("Haven't yet implemented part vals for " +
            partCols.get(i).getType());
      }
    }
    return translated;
  }

  private List<String> untranslatePartVals(List<Object> partVals) {
    List<String> untranslated = new ArrayList<>(partVals.size());
    for (Object partVal : partVals) untranslated.add(partVal.toString());
    return untranslated;
  }

  @VisibleForTesting
  static String buildPartTableName(String dbName, String tableName) {
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
   * Get a role.
   * @param roleName name of the role to get
   * @return the role, or null if there is no such role.
   * @throws IOException
   */
  @Override
  public Role getRole(String roleName) throws IOException {
    try {
      byte[] serialized = getBinaryColumnByKey(ROLE_TABLE, CATALOG_COL, roleName);
      if (serialized == null) return null;
      Role role = new Role();
      deserialize(role, serialized);
      return role;
    } catch (SQLException e) {
      LOG.error("Caught exception trying to fetch role", e);
      throw new IOException(e);
    }
  }

  @Override
  public List<Role> getRoles(Collection<String> roleNames) throws IOException {
    List<Role> roles = new ArrayList<>(roleNames.size());
    try (Statement stmt = currentConnection.createStatement()) {
      StringBuilder buf = new StringBuilder("select ")
          .append(CATALOG_COL)
          .append(" from ")
          .append(ROLE_TABLE.getName())
          .append(" where ")
          .append(R_NAME_COL)
          .append(" in (");
      boolean first = true;
      for (String roleName : roleNames) {
        if (first) first = false;
        else buf.append(", ");
        buf.append('\'')
            .append(roleName)
            .append('\'');
      }
      buf.append(')');
      if (LOG.isDebugEnabled()) {
        LOG.debug("Going to execute query " + buf.toString());
      }
      ResultSet rs = stmt.executeQuery(buf.toString());
      while (rs.next()) {
        byte[] serialized = rs.getBytes(1);
        if (serialized != null) {
          Role role = new Role();
          deserialize(role, serialized);
          roles.add(role);
        }
      }
      return roles;
    } catch (SQLException e) {
      LOG.error("Unable to fetch roles", e);
      throw new IOException(e);
    }
  }

  public List<Role> scanRoles() throws SQLException {
    List<byte[]> serializeds = scanOnKeyWithRegex(ROLE_TABLE, CATALOG_COL, null);
    if (serializeds.size() == 0) return Collections.emptyList();
    List<Role> roles = new ArrayList<>(serializeds.size());
    for (byte[] serialized : serializeds) {
      Role role = new Role();
      deserialize(role, serialized);
      roles.add(role);
    }
    return roles;
  }

  @Override
  public void putRole(Role role) throws IOException {
    try {
      byte[] serialized = serialize(role);
      storeOnKey(ROLE_TABLE, CATALOG_COL, serialized, role.getRoleName());
    } catch (SQLException e) {
      LOG.error("Caught exception trying to store role", e);
      throw new IOException(e);
    }
  }

  @Override
  public boolean deleteRole(String roleName) throws IOException {
    try {
      return deleteOnKey(ROLE_TABLE, roleName);
    } catch (SQLException e) {
      LOG.error("Unable to delete role", e);
      throw new IOException(e);
    }
  }

  @Override
  public Set<String> findAllUsersInRole(String roleName) throws IOException {
    Set<String> users = new HashSet<>();

    try (Statement stmt = currentConnection.createStatement()) {
      StringBuilder buf = new StringBuilder("select ")
          .append(U2R_USER_NAME_COLUMN)
          .append(", ")
          .append(U2R_ROLE_LIST_COLUMN)
          .append(" from ")
          .append(USER_TO_ROLE_TABLE.getName());
      if (LOG.isDebugEnabled()) {
        LOG.debug("Going to run query " + buf.toString());
      }
      ResultSet rs = stmt.executeQuery(buf.toString());
      while (rs.next()) {
        Array array = rs.getArray(2);
        if (!rs.wasNull()) {
          String[] roles = (String[])array.getArray();
          for (String role : roles) {
            if (role.equals(roleName)) {
              users.add(rs.getString(1));
              break;
            }
          }
        }
      }
      return users;
    } catch (SQLException e) {
      LOG.error("Unable to fetch all roles for user", e);
      throw new IOException(e);
    }
  }

  @Override
  public void writeBackChangedRoles(Map<String, HbaseMetastoreProto.RoleGrantInfoList> changedRoles)
      throws IOException {
    try {
      for (Map.Entry<String, HbaseMetastoreProto.RoleGrantInfoList> e : changedRoles.entrySet()) {
        updateOnKey(ROLE_TABLE, R_PP_COL, e.getValue().toByteArray(), e.getKey());
      }
    } catch (SQLException e) {
      LOG.error("Unable to write back changed roles", e);
      throw new IOException(e);
    }
  }

  @Override
  public void storeUserToRollMapping(String userName, Set<String> roles) throws IOException {
    try {
      if (recordExists(USER_TO_ROLE_TABLE, userName)) {
        StringBuilder buf = new StringBuilder("update ")
            .append(USER_TO_ROLE_TABLE.getName())
            .append(" set ")
            .append(U2R_ROLE_LIST_COLUMN)
            .append(" = ?")
            .append(" where ")
            .append(U2R_USER_NAME_COLUMN)
            .append(" = ?");
        if (LOG.isDebugEnabled()) {
          LOG.debug("Going to prepare statement " + buf.toString());
        }
        try (PreparedStatement stmt = currentConnection.prepareStatement(buf.toString())) {
          stmt.setArray(1, currentConnection.createArrayOf("varchar", roles.toArray(new String[roles.size()])));
          stmt.setString(2, userName);

          int rowsUpdated = stmt.executeUpdate();
          if (rowsUpdated != 1) {
            String msg = "Expected to update a row for " + USER_TO_ROLE_TABLE + " " + userName +
                ", but did not";
            LOG.error(msg);
            throw new RuntimeException(msg);
          }
        }
      } else {
        StringBuilder buf = new StringBuilder("insert into ")
            .append(USER_TO_ROLE_TABLE.getName())
            .append(" (")
            .append(U2R_USER_NAME_COLUMN)
            .append(", ")
            .append(U2R_ROLE_LIST_COLUMN)
            .append(") values (?, ?)");
        if (LOG.isDebugEnabled()) {
          LOG.debug("Going to prepare statement " + buf.toString());
        }
        try (PreparedStatement stmt = currentConnection.prepareStatement(buf.toString())) {
          stmt.setString(1, userName);
          stmt.setArray(2, currentConnection.createArrayOf("varchar", roles.toArray(new String[roles.size()])));

          int rowsInserted = stmt.executeUpdate();
          if (rowsInserted != 1) {
            String msg = "Expected to insert a row for " + USER_TO_ROLE_TABLE + " " +  userName +
                ", but did not";
            LOG.error(msg);
            throw new RuntimeException(msg);
          }
        }
      }
    } catch (SQLException e) {
      LOG.error("Unable to store user to roll mapping", e);
      throw new IOException(e);
    }
  }

  @Override
  public void populateRoleCache(Map<String, HbaseMetastoreProto.RoleGrantInfoList> cache) throws IOException {
    StringBuilder buf = new StringBuilder("select ")
        .append(R_NAME_COL)
        .append(", ")
        .append(R_PP_COL)
        .append(" from ")
        .append(ROLE_TABLE.getName());
    try (Statement stmt = currentConnection.createStatement()) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Going to execute query " + buf.toString());
      }
      ResultSet rs = stmt.executeQuery(buf.toString());
      while (rs.next()) {
        byte[] serializedRgil = rs.getBytes(2);
        if (serializedRgil != null) {
          cache.put(rs.getString(1), HbaseMetastoreProto.RoleGrantInfoList.parseFrom(serializedRgil));
        }
      }
    } catch (SQLException e) {
      LOG.error("Unable to scan user to role table", e);
      throw new IOException(e);
    }
  }

  @Override
  public HbaseMetastoreProto.RoleGrantInfoList getRolePrincipals(String roleName) throws IOException {
    try {
      byte[] serialized = getBinaryColumnByKey(ROLE_TABLE, R_PP_COL, R_NAME_COL);
      if (serialized == null) return null;
      return HbaseMetastoreProto.RoleGrantInfoList.parseFrom(serialized);
    } catch (SQLException e) {
      LOG.error("Unable to get role principals", e);
      throw new IOException(e);
    }
  }

  @Override
  public List<String> getUserRoles(String userName) throws IOException {
    try (Statement stmt = currentConnection.createStatement()) {
      StringBuilder buf = new StringBuilder("select ")
          .append(U2R_ROLE_LIST_COLUMN)
          .append(" from ")
          .append(USER_TO_ROLE_TABLE.getName())
          .append(" where ")
          .append(U2R_USER_NAME_COLUMN)
          .append(" = '")
          .append(userName)
          .append('\'');
      if (LOG.isDebugEnabled()) {
        LOG.debug("Going to run query " + buf.toString());
      }
      ResultSet rs = stmt.executeQuery(buf.toString());
      if (rs.next()) {
        Array array = rs.getArray(1);
        if (!rs.wasNull()) {
          return Arrays.asList((String[])array.getArray());
        }
      }
      return null;
    } catch (SQLException e) {
      LOG.error("Unable to get user roles", e);
      throw new IOException(e);
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
   * @throws SQLException
   */
  public Table getTable(String dbName, String tableName) throws SQLException {
    ObjectPair<String, String> cacheKey = new ObjectPair<>(dbName, tableName);
    Table table = tableCache.get(cacheKey);
    if (table == null) {
      // Prefetch the statis object as well, because the odds are we'll need it.
      byte[][] serialized = getBinaryColumnsByKey(TABLE_TABLE, CAT_AND_STATS_COLS, dbName,
          tableName);
      if (serialized == null) return null;
      table = new Table();
      deserialize(table, serialized[0]);
      tableCache.put(cacheKey, table);
      if (serialized[1] != null) {
        SeparableColumnStatistics tableStats = new SeparableColumnStatistics(serialized[1]);
        tableStatsCache.put(cacheKey, tableStats);
      }
    }
    return table;
  }

  /**
   * Get a list of tables.  This method does not populate the table cache or the stats cache because
   * it assumes that this is being used to do show tables or something, not a query where we may
   * need to reference the tables again soon.
   * @param dbName Database these tables are in
   * @param regex Regular expression to use in searching for table names.  It is expected to
   *              be a Java regular expression.  If it is null then all tables in the indicated
   *              database will be returned.
   * @return list of tables matching the regular expression.
   * @throws IOException
   */
  public List<Table> scanTables(String dbName, String regex) throws IOException {
    try {
      List<byte[]> results = scanOnKeyWithRegex(TABLE_TABLE, CATALOG_COL, regex, dbName);
      List<Table> tables = new ArrayList<>();
      for (byte[] serialized : results) {
        Table table = new Table();
        deserialize(table, serialized);
        tables.add(table);
      }
      return tables;
    } catch (SQLException e) {
      LOG.error("Unable to scan tables", e);
      throw new IOException(e);
    }
  }

  /**
   * Put a table object.
   * @param table table object
   * @throws SQLException
   */
  public void insertTable(Table table) throws SQLException {
    insertOnKey(TABLE_TABLE, CATALOG_COL, serialize(table), table.getDbName(), table.getTableName());
    tableCache.put(new ObjectPair<>(table.getDbName(), table.getTableName()), table);
    if (table.getPartitionKeysSize() > 0) {
      // This is a work around for a race condition in creating the partition table.  It isn't
      // foolproof, but it helps.
      findPartitionTable(table.getDbName(), table.getTableName(), true);
    }
  }

  public void updateTable(Table table) throws SQLException {
    updateOnKey(TABLE_TABLE, CATALOG_COL, serialize(table), table.getDbName(), table.getTableName());
    tableCache.put(new ObjectPair<>(table.getDbName(), table.getTableName()), table);
  }

  @Override
  public void writeBackChangedTables(List<Table> tables) throws IOException {
    try {
      for (Table table : tables) {
        updateOnKey(TABLE_TABLE, CATALOG_COL, serialize(table), table.getDbName(), table.getTableName());
      }
    } catch (SQLException e) {
      LOG.error("Unable to write tables", e);
      throw new IOException(e);
    }
  }

  /**
   * Delete a table
   * @param dbName name of database table is in
   * @param tableName table to drop
   * @throws SQLException
   */
  public boolean deleteTable(String dbName, String tableName) throws SQLException {
    // If we've built a special partition table for this table drop it first.
    PostgresTable partitionTable = findPartitionTable(dbName, tableName, false);
    if (partitionTable != null) {
      dropPostgresTable(partitionTable.getName());
      partitionTables.remove(partitionTable.getName());
    }
    boolean rc = deleteOnKey(TABLE_TABLE, dbName, tableName);
    ObjectPair<String, String> cacheKey = new ObjectPair<>(dbName, tableName);
    tableCache.remove(cacheKey);
    tableStatsCache.remove(cacheKey);
    return rc;
  }

  UUID getCurrentUUID(String dbName, String tableName) throws SQLException {
    try (Statement stmt = currentConnection.createStatement()) {
      StringBuilder buf = new StringBuilder("select ")
          .append(UUID_MSB_COL)
          .append(", ")
          .append(UUID_LSB_COL)
          .append(" from ")
          .append(TABLE_TABLE.getName())
          .append(" where ")
          .append(DB_NAME_COL)
          .append(" = '")
          .append(dbName)
          .append("' and ")
          .append(TABLE_NAME_COL)
          .append(" = '")
          .append(tableName)
          .append('\'');
      if (LOG.isDebugEnabled()) {
        LOG.debug("Going to run query " + buf.toString());
      }
      ResultSet rs = stmt.executeQuery(buf.toString());
      return rs.next() ? new UUID(rs.getLong(1), rs.getLong(2)) : null;
    }
  }

  void updateUUID(String dbName, String tableName) throws SQLException {
    UUID newUUID = UUID.randomUUID();
    try (Statement stmt = currentConnection.createStatement()) {
      StringBuilder buf = new StringBuilder("update ")
          .append(TABLE_TABLE.getName())
          .append(" set ")
          .append(UUID_MSB_COL)
          .append(" = ")
          .append(Long.toString(newUUID.getMostSignificantBits()))
          .append(", ")
          .append(UUID_LSB_COL)
          .append(" = ")
          .append(Long.toString(newUUID.getLeastSignificantBits()))
          .append(" where ")
          .append(DB_NAME_COL)
          .append(" = '")
          .append(dbName)
          .append("' and ")
          .append(TABLE_NAME_COL)
          .append(" = '")
          .append(tableName)
          .append('\'');
      if (LOG.isDebugEnabled()) {
        LOG.debug("Going to run statement " + buf.toString());
      }
      int rc = stmt.executeUpdate(buf.toString());
      if (rc != 1) throw new RuntimeException("Expected to update 1 record, but updated " + rc);
    }
  }

  /**********************************************************************************************
   * Statistics related methods
   *********************************************************************************************/

  /**
   * Update statistics for one or more columns for a table.
   *
   * @param dbName database the table is in
   * @param tableName table to update statistics for
   * @param stats Stats object with stats for one or more columns
   * @throws SQLException
   */
  public void updateTableStatistics(String dbName, String tableName, ColumnStatistics stats)
      throws SQLException, IOException {
    // It's possible that this is for a different set of columns.  So we need to union any existing
    // stats for different columns
    ObjectPair<String, String> cacheKey = new ObjectPair<>(dbName, tableName);
    SeparableColumnStatistics currentStats = getTableStatisticsInternal(dbName, tableName, null);
    SeparableColumnStatistics unionedStats =
        (currentStats == null) ? new SeparableColumnStatistics(stats) : currentStats.union(stats);
    updateOnKey(TABLE_TABLE, STATS_COL, unionedStats.serialize(), dbName, tableName);
    tableStatsCache.put(cacheKey, unionedStats);
  }

  /**
   * Get statistics for a table.
   *
   * @param dbName name of database table is in
   * @param tblName name of table
   * @param colNames names of the columns to fetch.  If null is passed, all available stats will
   *                 be returned.
   * @return column statistics for indicated table
   * @throws SQLException if something goes wrong
   */
  public ColumnStatistics getTableStatistics(String dbName, String tblName, List<String> colNames)
      throws SQLException, IOException {
    SeparableColumnStatistics separable = getTableStatisticsInternal(dbName, tblName, colNames);
    return (separable == null) ? null : separable.asColumnStatistics();
  }

  private SeparableColumnStatistics getTableStatisticsInternal(String dbName, String tblName,
                                                               List<String> colNames)
      throws SQLException, IOException {
    ObjectPair<String, String> cacheKey = new ObjectPair<>(dbName, tblName);
    SeparableColumnStatistics stats = tableStatsCache.get(cacheKey);
    if (stats == null) {
      byte[] serialized = getBinaryColumnByKey(TABLE_TABLE, STATS_COL, dbName, tblName);
      if (serialized != null) {
        LOG.warn("Table stats were in database but not in cache, not sure why they weren't prefetched");
        stats = new SeparableColumnStatistics(serialized);
        tableStatsCache.put(cacheKey, stats);
      }
    }

    // If the caller has asked for only some columns, return just the columns requested
    if (stats != null && colNames != null) return stats.trim(colNames);
    else return stats;
  }

  /**
   * Update statistics for one or more columns for a partition.
   * @param dbName database the table is in
   * @param tableName table the partitions are in
   * @param partVals values for the partition, should be a complete match that specifies exactly
   *                 one partition.
   * @param stats column stats for this partition, this may include a number of columns.
   * @throws SQLException if something goes wrong
   */
  public void updatePartitionStatistics(String dbName, String tableName, List<String> partVals,
                                        ColumnStatistics stats) throws SQLException, IOException {
    PostgresTable pTable = findPartitionTable(dbName, tableName, false);
    Table hTable = getTable(dbName, tableName);
    List<Object> translatedPartVals = translatePartVals(hTable, partVals);
    SeparableColumnStatistics currentStats =
        getSinglePartitionStatistics(dbName, tableName, pTable, partVals, translatedPartVals, null);
    SeparableColumnStatistics unionedStats =
        (currentStats == null) ? new SeparableColumnStatistics(stats) : currentStats.union(stats);
    updatePartitionOnKey(pTable, STATS_COL, unionedStats.serialize(), translatedPartVals);
    updateUUID(dbName, tableName);
  }

  /**
   * Get statistics for a set of partitions.  This depends heavily on the partition statistics
   * already being pre-fetched in the various getPartitions calls.  It will work even if that
   * hasn't happened, but it will be very slow.
   *
   * @param dbName name of database table is in
   * @param tblName table partitions are in
   * @param partValLists partition values for each partition, needed because this class doesn't know how
   *          to translate from partName to partVals
   * @param colNames names of the columns to fetch.  If null is passed, all available stats will
   *                 be returned.
   * @return list of ColumnStats, one for each partition for which we found at least one column's
   * stats.
   * @throws SQLException
   */
  public List<ColumnStatistics> getPartitionStatistics(String dbName, String tblName,
                                                       List<String> partNames,
                                                       List<List<String>> partValLists,
                                                       List<String> colNames) throws SQLException, IOException {
    List<SeparableColumnStatistics> separables =
        getPartitionStatisticsInternal(dbName, tblName, partNames, partValLists, colNames);
    List<ColumnStatistics> css = new ArrayList<>(separables.size());
    for (SeparableColumnStatistics separable : separables) css.add(separable.asColumnStatistics());
    return css;
  }

  private List<SeparableColumnStatistics>
  getPartitionStatisticsInternal(String dbName, String tblName,
                                 List<String> partNames, List<List<String>> partValLists,
                                 List<String> colNames) throws SQLException, IOException {
    PerfLogger perfLogger = PerfLogger.getPerfLogger((HiveConf)conf, false);
    perfLogger.PerfLogBegin(RetryingHMSHandler.class.getName(), "getPartitionStatistics");
    List<SeparableColumnStatistics> statsList = new ArrayList<>(partValLists.size());
    PostgresTable pTable = findPartitionTable(dbName, tblName, false);
    if (pTable == null) {
      // This means we couldn't find the partition table, which is obviously a problem.
      String msg = "Attempt to get statistics for table " +
          PostgresStore.tableNameForErrorMsg(dbName, tblName) +
          " because partition table does not exist for this table.";
      LOG.error(msg);
      throw new SQLException(msg);
    }
    for (int i = 0; i < partValLists.size(); i++) {
      SeparableColumnStatistics separable =
          getSinglePartitionStatistics(dbName, tblName, pTable, partValLists.get(i), null,
              partNames.get(i));
      if (separable != null) {
        if (colNames != null) separable = separable.trim(colNames);
        statsList.add(separable);
      }
    }
    perfLogger.PerfLogEnd(RetryingHMSHandler.class.getName(), "getPartitionStatistics");
    return statsList;
  }

  /**
   * Get stats for a single partition.  This will pull from the cache if it can.
   * @param dbName
   * @param tableName
   * @param pTable
   * @param partVals
   * @param translatedPartVals these can be null, in which case they'll be generated here.  They
   *                           are passed in here in case the caller has already calculated them.
   * @param partName name of the partition.  This can be null in which case it will be generated.
   *
   *
   * @return
   * @throws SQLException
   */
  private SeparableColumnStatistics getSinglePartitionStatistics(String dbName, String tableName,
                                                                 PostgresTable pTable,
                                                                 List<String> partVals,
                                                                 List<Object> translatedPartVals,
                                                                 String partName)
      throws SQLException {
    if (partName == null) partName = FileUtils.makePartName(pTable.getPrimaryKeyCols(), partVals);
    SeparableColumnStatistics stats =
        partCache.getSinglePartitionStats(dbName, tableName, partName);
    if (stats == null) {
      if (translatedPartVals == null) {
        Table hTable = getTable(dbName, tableName);
        translatedPartVals = translatePartVals(hTable, partVals);
      }
      byte[] serialized = getPartitionByKey(pTable, STATS_COL, translatedPartVals);
      if (serialized != null) {
        LOG.warn("Partition stats in database but not in cache, not sure why they weren't prefetched");
        stats = new SeparableColumnStatistics(serialized);
      }
    }
    return stats;
  }

  /**
   * Get aggregated statistics for partitions.  If this is in the cache it will be returned from
   * there.  If not, it will aggregate the stats, cache the result, and return.
   * @param dbName database the table is in
   * @param tableName table name
   * @param partNames names of partitions to aggregate
   * @param colNames column names to aggregate
   * @return aggregated stats
   * @throws SQLException
   */
  public AggrStats getAggregatedStats(String dbName, String tableName, List<String> partNames,
                                      List<String> colNames)
      throws SQLException, IOException {
    LOG.info("Aggregating stats for " + colNames.size() + " columns for table " + tableName +
        " for " + partNames.size() + " partitions");
    PerfLogger perfLogger = PerfLogger.getPerfLogger((HiveConf)conf, false);
    perfLogger.PerfLogBegin(RetryingHMSHandler.class.getName(), "getAggregatedStats");
    AggrStats aggrStats = partCache.getAggregatedStats(this, dbName, tableName, partNames, colNames);
    // - look to see if we already have it in our local cache
    /*
    byte[] cacheKey = getAggrStatsKey(dbName, tableName, partNames, colNames);
    AggrStats aggrStats = aggrStatsCache.get(cacheKey);
    if (aggrStats == null) {
      LOG.debug("Aggregated stats not found in the cache, aggregating stats");
      // Ok, no dice.  Fetch the requested stats and aggregate them ourselves
      List<SeparableColumnStatistics> partStats =
          getPartitionStatisticsInternal(dbName, tableName, partNames, partValList, colNames);
      if (partStats != null && partStats.size() > 0) {
        // What we have now is a set of partition column statistics, each of which have entries
        // for (some) of the columns we're interested in.  (We are guaranteed that none of them
        // have columns we aren't interested in.)  We need to pivot this so that we have
        // ColumnStatistics instances of each column, which we can then feed to an aggregator for
        // that column.
        try {
          aggrStats = new AggrStats();
          int numBitVectors = HiveStatsUtils.getNumBitVectorsForNDVEstimation(conf);
          boolean useDensityFunctionForNDVEstimation =
              HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVE_METASTORE_STATS_NDV_DENSITY_FUNCTION);

          for (String colName : colNames) {
            List<ColumnStatistics> colStatsForCol = new ArrayList<>(partStats.size());
            List<String> partNamesList = new ArrayList<>(partStats.size());
            for (SeparableColumnStatistics separable : partStats) {
              ColumnStatisticsObj cso = separable.getStatsForCol(colName);
              if (cso != null) {
                ColumnStatistics cs = new ColumnStatistics();
                partNamesList.add(separable.getStatsDesc().getPartName());
                cs.setStatsDesc(separable.getStatsDesc());
                cs.addToStatsObj(cso);
                colStatsForCol.add(cs);
              }
            }
            ColumnStatsAggregator aggregator = ColumnStatsAggregatorFactory.getColumnStatsAggregator(
                colStatsForCol.get(0).getStatsObj().get(0).getStatsData().getSetField(),
                numBitVectors,
                useDensityFunctionForNDVEstimation);
            ColumnStatisticsObj cso = aggregator.aggregate(colName, partNamesList, colStatsForCol);
            aggrStats.setPartsFound(Math.max(aggrStats.getPartsFound(), colStatsForCol.size()));
            aggrStats.addToColStats(cso);
          }
          aggrStatsCache.put(cacheKey, aggrStats);
        } catch (Exception e) {
          throw new SQLException(e);
        }
      }
    }
    */
    perfLogger.PerfLogEnd(RetryingHMSHandler.class.getName(), "getAggregatedStats");
    return aggrStats;
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
    if (clearCnt++ % 100 == 0) {
      // TODO we should do something more sophisticated than this, but for now it allows us to keep
      // the stats a little longer before we clear the cache.
      tableStatsCache.clear();
    }
  }

  /**********************************************************************************************
   * General access methods
   *********************************************************************************************/

  private boolean recordExists(PostgresTable table, String... keyVals) throws SQLException {
    List<String> pkCols = table.getPrimaryKeyCols();
    assert pkCols.size() == keyVals.length;
    try (Statement stmt = currentConnection.createStatement()) {
      StringBuilder buf = new StringBuilder("select 1 from ")
          .append(table.getName())
          .append(" where ");
      for (int i = 0; i < keyVals.length; i++) {
        if (i != 0) buf.append(" and ");
        buf.append(pkCols.get(i))
            .append(" = '")
            .append(keyVals[i])
            .append('\'');
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug("Going to run query " + buf.toString());
      }
      ResultSet rs = stmt.executeQuery(buf.toString());
      return rs.next();
    }
  }

  /**
   * Read one binary column in one record using the key.
   * @param table table to read from
   * @param colName column name to read
   * @param keyVals 1 or more key values.  This must match the number of items in the primary key.
   * @return serialized version of the object, or null if there is no matching version.
   * @throws SQLException any SQLExceptions thrown by the database
   */
  private byte[] getBinaryColumnByKey(PostgresTable table, String colName, String... keyVals)
      throws SQLException {
    byte[][] cols = getBinaryColumnsByKey(table, Collections.singletonList(colName), keyVals);
    return cols == null ? null : cols[0];
  }

  private byte[][] getBinaryColumnsByKey(PostgresTable table, List<String> colNames,
                                       String... keyVals) throws SQLException {
    List<String> pkCols = table.getPrimaryKeyCols();
    assert pkCols.size() == keyVals.length;
    try (Statement stmt = currentConnection.createStatement()) {
      StringBuilder buf = new StringBuilder("select ");
      boolean first = true;
      for (String colName : colNames) {
        if (first) first = false;
        else buf.append(", ");
        buf.append(colName);
      }
      buf.append(" from ")
          .append(table.getName())
          .append(" where ");
      for (int i = 0; i < keyVals.length; i++) {
        if (i != 0) buf.append(" and ");
        buf.append(pkCols.get(i))
            .append(" = '")
            .append(keyVals[i])
            .append('\'');
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug("Going to run query " + buf.toString());
      }
      ResultSet rs = stmt.executeQuery(buf.toString());
      if (rs.next()) {
        byte[][] result = new byte[(colNames.size())][];
        for (int i = 0; i < colNames.size(); i++) {
          result[i] = rs.getBytes(i + 1);
        }
        return result;
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
   * @throws SQLException wraps any SQLException
   */
  private List<byte[]> scanOnKeyWithRegex(PostgresTable table, String colName, String regex,
                                          String... keyVals) throws SQLException {
    try (Statement stmt = currentConnection.createStatement()) {
      List<byte[]> results = new ArrayList<>();
      StringBuilder buf = new StringBuilder("select ")
          .append(colName)
          .append(" from ")
          .append(table.getName());
      if (regex != null || (keyVals != null && keyVals.length > 0)) {
        buf.append(" where ");
        boolean first = true;
        int keyColNum = 0;
        List<String> keyCols = table.getPrimaryKeyCols();
        if (keyVals != null && keyVals.length > 0) {
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
      if (LOG.isDebugEnabled()) {
        LOG.debug("Going to run query " + buf.toString());
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
   * and update.  Whenever possible insert/update should be used.
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
    if (recordExists(table, keyVals)) updateOnKey(table, colName, colVal, keyVals);
    else insertOnKey(table, colName, colVal, keyVals);
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
        .append(", ");
    }
    buf.append(colName)
        .append(") values (");
    for (int i = 0; i < pkCols.size(); i++) buf.append("?,");
    buf.append("?)");
    if (LOG.isDebugEnabled()) {
      LOG.debug("Going to prepare statement " + buf.toString());
    }
    try (PreparedStatement stmt = currentConnection.prepareStatement(buf.toString())) {
      for (int i = 0; i < keyVals.length; i++) stmt.setString(i + 1, keyVals[i]);
      stmt.setBytes(keyVals.length + 1, colVal);

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
    StringBuilder buf = new StringBuilder("update ")
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
    if (LOG.isDebugEnabled()) {
      LOG.debug("Going to prepare statement " + buf.toString());
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

  private boolean deleteOnKey(PostgresTable table, String... keyVals) throws SQLException {
    List<String> pkCols = table.getPrimaryKeyCols();
    assert pkCols.size() == keyVals.length;
    StringBuilder buf = new StringBuilder("delete from ")
        .append(table.getName())
        .append(" where ");
    for (int i = 0; i < keyVals.length; i++) {
      if (i != 0) buf.append(" and ");
      buf.append(pkCols.get(i))
          .append(" = '")
          .append(keyVals[i])
          .append('\'');
    }
    try (Statement stmt = currentConnection.createStatement()) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Going to run statement " + buf.toString());
      }
      int rowsDeleted = stmt.executeUpdate(buf.toString());
      if (rowsDeleted == 0) {
        LOG.info("Unable to find table to drop matching " + serializeKey(table, keyVals));
        return false;
      } else if (rowsDeleted == 1) {
        return true;
      } else {
        String msg = "Deleted " + rowsDeleted + " for " + serializeKey(table, keyVals) +
            ", way bad!";
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

  static byte[] serialize(TBase obj) {
    try {
      TMemoryBuffer buf = new TMemoryBuffer(1000);
      TProtocol protocol = new TCompactProtocol(buf);
      obj.write(protocol);
      byte[] serialized = new byte[buf.length()];
      buf.read(serialized, 0, buf.length());
      return serialized;

    } catch (TException e) {
      throw new RuntimeException(e);
    }
  }

  static void deserialize(TBase obj, byte[] serialized) {
    try {
      TMemoryBuffer buf = new TMemoryBuffer(serialized.length);
      buf.write(serialized, 0, serialized.length);
      TProtocol protocol = new TCompactProtocol(buf);
      obj.read(protocol);
    } catch (TException e) {
      throw new RuntimeException(e);
    }
  }

  /**********************************************************************************************
   * Postgres table representation
   *********************************************************************************************/
  /*
  private static Map<Integer, String> getPostgresToHiveTypeMap() {
    if (postgresToHiveTypes == null) {
      synchronized (PostgresKeyValue.class) {
        if (postgresToHiveTypes == null) {
          postgresToHiveTypes = new HashMap<>();
          postgresToHiveTypes.put(Types.ARRAY, serdeConstants.LIST_TYPE_NAME);
          postgresToHiveTypes.put(Types.BIGINT, serdeConstants.BIGINT_TYPE_NAME);
          postgresToHiveTypes.put(Types.BINARY, serdeConstants.BINARY_TYPE_NAME);
          postgresToHiveTypes.put(Types.BOOLEAN, serdeConstants.BOOLEAN_TYPE_NAME);
          postgresToHiveTypes.put(Types.CHAR, serdeConstants.CHAR_TYPE_NAME);
          postgresToHiveTypes.put(Types.DATE, serdeConstants.DATE_TYPE_NAME);
          postgresToHiveTypes.put(Types.DECIMAL, serdeConstants.DECIMAL_TYPE_NAME);
          postgresToHiveTypes.put(Types.DOUBLE, serdeConstants.DOUBLE_TYPE_NAME);
          postgresToHiveTypes.put(Types.FLOAT, serdeConstants.FLOAT_TYPE_NAME);
          postgresToHiveTypes.put(Types.INTEGER, serdeConstants.INT_TYPE_NAME);
          postgresToHiveTypes.put(Types.SMALLINT, serdeConstants.SMALLINT_TYPE_NAME);
          postgresToHiveTypes.put(Types.TIMESTAMP, serdeConstants.TIMESTAMP_TYPE_NAME);
          postgresToHiveTypes.put(Types.TINYINT, serdeConstants.TINYINT_TYPE_NAME);
          postgresToHiveTypes.put(Types.VARCHAR, serdeConstants.VARCHAR_TYPE_NAME);
        }
      }
    }
    return postgresToHiveTypes;
  }
  */

  private static String hiveToPostgresType(String hiveType) {
    if (hiveToPostgresTypes == null) {
      synchronized (PostgresKeyValue.class) {
        if (hiveToPostgresTypes == null) {
          hiveToPostgresTypes = new HashMap<>();
          hiveToPostgresTypes.put(serdeConstants.BIGINT_TYPE_NAME, "bigint");
          hiveToPostgresTypes.put(serdeConstants.BINARY_TYPE_NAME, "bytea");
          hiveToPostgresTypes.put(serdeConstants.BOOLEAN_TYPE_NAME, "boolean");
          hiveToPostgresTypes.put(serdeConstants.CHAR_TYPE_NAME, "char(255)"); // TODO not sure what to do on length
          hiveToPostgresTypes.put(serdeConstants.DATE_TYPE_NAME, "date");
          hiveToPostgresTypes.put(serdeConstants.DECIMAL_TYPE_NAME, "decimal(10,2)"); // TODO not sure what to do on length
          hiveToPostgresTypes.put(serdeConstants.DOUBLE_TYPE_NAME, "double");
          hiveToPostgresTypes.put(serdeConstants.FLOAT_TYPE_NAME, "float");
          hiveToPostgresTypes.put(serdeConstants.INT_TYPE_NAME, "integer");
          hiveToPostgresTypes.put(serdeConstants.SMALLINT_TYPE_NAME, "smallint");
          hiveToPostgresTypes.put(serdeConstants.TIMESTAMP_TYPE_NAME, "timestamp");
          hiveToPostgresTypes.put(serdeConstants.TINYINT_TYPE_NAME, "tinyint");
          hiveToPostgresTypes.put(serdeConstants.VARCHAR_TYPE_NAME, "varchar(255)"); // TODO not sure what to do on length
          hiveToPostgresTypes.put(serdeConstants.STRING_TYPE_NAME, "varchar(255)"); // TODO not sure what to do on length
        }
      }
    }
    String pType = hiveToPostgresTypes.get(hiveType);
    if (pType == null) throw new RuntimeException("Unknown translation " + hiveType);
    else return pType;
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

    Object getFromResultSet(ResultSet rs, int pos) throws SQLException {
      if (type.startsWith("varchar") || type.startsWith("char")) {
        return rs.getString(pos);
      } else if (type.equals("bigint")) {
        return rs.getLong(pos);
      } else {
        throw new RuntimeException("Unhandled type " + type);
      }
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

    PostgresTable addColumn(FieldSchema fs, boolean isPrimaryKey) {
      return addColumn(fs.getName(), hiveToPostgresType(fs.getType()), isPrimaryKey);
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

    PostgresTable addLongColumn(String name) {
      return addColumn(name, "bigint", false);
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

    List<PostgresColumn> getPKColsAsPostgresColumns() {
      List<PostgresColumn> cols = new ArrayList<>();
      for (String pkCol : pkCols) {
        cols.add(colsByName.get(pkCol));
      }
      return cols;
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

  private static class MissingCache<K, V> extends HashMap<K, V> {
    // Always return null, thus assuring the cache always misses.
    @Override
    public V get(Object key) {
      return null;
    }
  }

  // A visitor for the expression tree that generators a Postgres where clause
  private static class PartitionFilterBuilder extends ExpressionTree.TreeVisitor {
    private final PostgresTable pTable;
    private final Table hTable;
    private final ExpressionTree.FilterBuilder generatedFilter;
    private final List<Object> pVals;
    private final List<Integer> pTypes; // needed for prepareStatement to know how to insert values

    public PartitionFilterBuilder(PostgresTable pTable, Table hTable) {
      this.pTable = pTable;
      this.hTable = hTable;
      this.generatedFilter = new ExpressionTree.FilterBuilder(false);
      this.pVals = new ArrayList<>();
      pTypes = new ArrayList<>();

    }

    @Override
    protected void beginTreeNode(ExpressionTree.TreeNode node) throws MetaException {
      generatedFilter.append(" ( ");
    }

    @Override
    protected void midTreeNode(ExpressionTree.TreeNode node) throws MetaException {
      // This looks strange, as it seems to assume that anything here that isn't an and is an or,
      // which does not seem obvious.  I took this from MetaStoreDirectSql.PartitionFilterGenerator
      generatedFilter.append(node.getAndOr() == ExpressionTree.LogicalOperator.AND ? " and " :
          " or ");
    }

    @Override
    protected void endTreeNode(ExpressionTree.TreeNode node) throws MetaException {
      generatedFilter.append(" ) ");
    }

    @Override
    protected void visit(ExpressionTree.LeafNode node) throws MetaException {
      // Figure out which partition column we're dealing with here
      int partColIndex = node.getPartColIndexForFilter(hTable, generatedFilter);
      if (generatedFilter.hasError()) return;

      // Get value and figure out type based on column types
      String colType = hTable.getPartitionKeys().get(partColIndex).getType();
      if (colType.equals(serdeConstants.STRING_TYPE_NAME)) {
        pTypes.add(Types.VARCHAR);
      } else {
        // TODO, expand this
        throw new RuntimeException("Unsupported type " + colType);
      }
      pVals.add(node.value);

      generatedFilter.append(hTable.getPartitionKeys().get(partColIndex).getName())
          .append(" ")
          .append(node.operator.getSqlOp())
          .append(" ? ");
    }

    @Override
    protected boolean shouldStop() {
      return generatedFilter.hasError();
    }
  }

  /*****************************************************************************************
   * Methods for test classes.  These are provided so that tests can connect to Postgres.
   *****************************************************************************************/
  /**
   * Connect to the store.  This must be called before PostgresStore is instantiated.  This
   * methods assumes the systems property defined by {@link #TEST_POSTGRES_JDBC} has been set.
   * Optionally the user can also set {@link #TEST_POSTGRES_USER} and {@link #TEST_POSTGRES_PASSWD}
   * @param conf configuration file.  This will be populated with a set of values appropriate
   *             for testing.
   * @param tablesToDrop A list of tables that should be dropped at the end of testing.  This list
   *                     will be populated by this method and should be passed to
   *                     {@link #cleanupAfterTest(PostgresStore, List)}.
   * @return
   */
  static PostgresStore connectForTest(HiveConf conf, List<String> tablesToDrop) {
    String jdbc = System.getProperty(TEST_POSTGRES_JDBC);
    if (jdbc != null) {
      for (PostgresTable pTable : initialPostgresTables) {
        tablesToDrop.add(pTable.getName());
      }
      conf.setVar(HiveConf.ConfVars.METASTORE_RAW_STORE_IMPL,
          PostgresStore.class.getCanonicalName());
      conf.setVar(HiveConf.ConfVars.METASTORECONNECTURLKEY, jdbc);
      String user = System.getProperty(TEST_POSTGRES_USER);
      if (user == null) user = "hive";
      conf.setVar(HiveConf.ConfVars.METASTORE_CONNECTION_USER_NAME, user);
      String passwd = System.getProperty(TEST_POSTGRES_PASSWD);
      if (passwd == null) passwd = "";
      conf.setVar(HiveConf.ConfVars.METASTOREPWD, passwd);
      conf.set(PostgresKeyValue.CACHE_OFF, "true");

      PostgresStore store = new PostgresStore();
      store.setConf(conf);
      return store;
    } else {
      return null;
    }
  }

  static void cleanupAfterTest(PostgresStore store, List<String> tablesToDrop) throws SQLException {
    String jdbc = System.getProperty(TEST_POSTGRES_JDBC);
    String keepThem = System.getProperty(KEEP_TABLES_AFTER_TEST);
    if (jdbc != null && keepThem == null) {
      PostgresKeyValue psql = store.connectionForTest();
      try {
        psql.begin();
        for (String table : tablesToDrop) {
          try {
            psql.dropPostgresTable(table);
          } catch (SQLException e) {
            LOG.error("Error dropping table, your next test run will likely fail", e);
            // Ignore it, as it likely just means we haven't created the tables previously
          }
        }
      } finally {
        psql.commit();
      }
    }
  }
}
