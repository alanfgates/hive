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
package org.apache.hive.test.capybara;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.QueryPlan;
import org.apache.hive.hcatalog.streaming.HiveEndPoint;
import org.apache.hive.test.capybara.annotations.AcidOn;
import org.apache.hive.test.capybara.annotations.MetadataOnly;
import org.apache.hive.test.capybara.annotations.SqlStdAuthOn;
import org.apache.hive.test.capybara.annotations.VectorOn;
import org.apache.hive.test.capybara.data.DataSet;
import org.apache.hive.test.capybara.data.FetchResult;
import org.apache.hive.test.capybara.data.ResultCode;
import org.apache.hive.test.capybara.iface.ClusterManager;
import org.apache.hive.test.capybara.iface.DataStore;
import org.apache.hive.test.capybara.iface.TestTable;
import org.apache.hive.test.capybara.infra.CapyEndPoint;
import org.apache.hive.test.capybara.infra.IntegrationRunner;
import org.apache.hive.test.capybara.infra.TestConf;
import org.apache.hive.test.capybara.infra.TestManager;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.annotation.Annotation;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

/**
 * <p>A super class for all integration tests.  This class provides methods for running queries,
 * generating expected results, comparing against expected results, etc.</p>
 *
 * <p>The general expected flow is {@link #runQuery(String)} followed by {@link #compare}.  In most
 * cases the system can auto-generate the expected results.  If auto-generation won't work you can
 * instead use {@link #runHive} to run Hive and then get
 * specifically tailored results by using {@link #runBenchmark} (for example, in
 * the case where a feature is not supported by the Benchmark but can be produced by a different
 * but semantically equivalent query).  This is not generally required for syntax differences as
 * the test framework can convert Hive SQL to ANSI SQL.</p>
 *
 * <p>If your query requires any setting not handled in the general annotations you can set that
 * using {@link #setConfVar}.</p>
 *
 * <p>{@link #runQuery(String)}expects the query to return successfully.  For testing negative
 * queries use {@link #runQuery(String, org.apache.hive.test.capybara.data.ResultCode, Throwable)} which allows
 * you to specify an expected result (success or failure) and potentially an expected exception.</p>
 *
 * <p>You can explain a query using {@link #explain}.  This will return an
 * {@link org.apache.hive.test.capybara.Explain} object.</p>
 *
 * <p>Users of this class should never directly create a
 * {@link org.apache.hadoop.hive.conf.HiveConf} object.  Special care has to be taken when
 * creating these configuration objects as this class plays games with what is happening locally
 * versus on the cluster, and these don't always play well with what the junit infrastructure is
 * doing via maven.  If you need a conf object call {@link #getConf}.
 * </p>
 *
 * <p>This class depends on being run by
 * {@link org.apache.hive.test.capybara.infra.IntegrationRunner} to work properly.  This is
 * achieved with the @RunWith annotation.  Don't override that.</p>
 */
@RunWith(IntegrationRunner.class)
public abstract class IntegrationTest {
  static final private Logger LOG = LoggerFactory.getLogger(IntegrationTest.class);

  private static TestManager testManager;
  private static ClusterManager testCluster;
  private static ClusterManager benchCluster;

  // This is the configuration used for a particular test.  This will be re-created in the
  // @Before method.
  private DataStore testStore;
  private DataStore benchStore;
  private FetchResult testResults;
  private FetchResult benchmarkResults;
  private Map<String, List<Annotation>> allAnnotations;
  private String lastQuery; // Used to keep track of the last query sent to runQuery/runHive
  private boolean metadataOnly;

  @Rule public TestName name = new TestName();

  @BeforeClass
  public static void initClass() throws IOException {
    LOG.trace("Entering initClass");
    testManager = TestManager.getTestManager();
    // Start any necessary mini-clusters
    testCluster = testManager.getTestClusterManager();
    testCluster.setup(TestConf.TEST_CLUSTER);
    benchCluster = testManager.getBenchmarkClusterManager();
    benchCluster.setup(TestConf.BENCH_CLUSTER);
    LOG.trace("Leaving initClass");
  }

  @AfterClass
  public static void teardownClass() throws IOException {
    LOG.trace("Entering teardownClass");
    // tear down any miniclusters we started
    testCluster.tearDown();
    benchCluster.tearDown();
    LOG.trace("Leaving teardownClass");
  }

  @Before
  public void initTest() throws SQLException, IOException {
    LOG.trace("Entering initTest");

    // Give the cluster manager a chance to do any setup it needs to do.
    testCluster.beforeTest();
    benchCluster.beforeTest();

    // This has to be done after we create a new config so that Hive get's the right
    // config object.
    testStore = testCluster.getStore();
    benchStore = benchCluster.getStore();

    // Add necessary configs
    setConfVarForOneTest(HiveConf.ConfVars.HIVEDEFAULTFILEFORMAT.varname,
        testManager.getTestConf().getFileFormat());

    // Handle any annotations that set values in the config file
    List<Annotation> annotations = allAnnotations.get(name.getMethodName());
    if (annotations != null) {
      for (Annotation annotation : annotations) {
        // Find the values method and invoke it, then split the values to get what we should set.
        if (annotation.annotationType().equals(AcidOn.class)) {
          AcidOn ao = (AcidOn) annotation;
          handleValueAnnotation(ao.values());
        } else if (annotation.annotationType().equals(SqlStdAuthOn.class)) {
          SqlStdAuthOn ssao = (SqlStdAuthOn)annotation;
          handleValueAnnotation(ssao.values());
        } else if (annotation.annotationType().equals(VectorOn.class)) {
          VectorOn vo = (VectorOn)annotation;
          handleValueAnnotation(vo.values());
        } else if (annotation.annotationType().equals(MetadataOnly.class)) {
          metadataOnly = true;
        }
      }
    }

    LOG.trace("Leaving initTest");
  }

  private void handleValueAnnotation(String[] values) throws SQLException, IOException {
    Assert.assertEquals(0, values.length % 2);
    for (int i = 0; i < values.length; i += 2) {
      setConfVar(testCluster, values[i], values[i + 1]);
    }

  }

  @After
  public void teardownTest() throws Exception {
    LOG.trace("Entering teardownTest");
    // Give the benchmark a chance to cleanup if it needs to
    testCluster.afterTest();
    benchCluster.afterTest();
    LOG.trace("Leaving teardownTest");
  }

  /**
   * Get a copy of the configuration object that is active for this test.  Note that settings
   * values in this will not affect the tests.  To do that call
   * {@link #setConfVarForOneTest(String, String)}
   * @return conf
   */
  protected HiveConf getConf() {
    return testCluster.getHiveConf();
  }

  /**
   * Run a SQL query.  This will be run against the configuration provided in the
  * conf file.  This will return an error if it does not succeed (that is, if the rc is not 0).
   * @param sql SQL string to execute
   * @throws SQLException
   * @throws java.io.IOException
   */
  protected void runQuery(String sql) throws SQLException, IOException {
    runQuery(sql, ResultCode.SUCCESS, null, false);
  }

  /**
   * Run a SQL query.  This will be run against the configuration provided in the conf file.
   * @param sql SQL string to execute
   * @param expectedResult expected result from running Hive.
   * @param expectedException exception that it is expected the JDBC connection will throw.  If
   *                          this query is running in the CLI this value will be ignored.
   * @throws SQLException
   * @throws java.io.IOException
   */
  protected void runQuery(String sql, final ResultCode expectedResult,
                          Throwable expectedException) throws SQLException, IOException {
    runQuery(sql, expectedResult, expectedException, false);
  }


  private void runQuery(String sql, final ResultCode expectedResult,
                        Throwable expectedException, boolean hiveOnly)
      throws SQLException, IOException {
    lastQuery = sql;
    try {
      StoreRunner testRunner = new StoreRunner(testStore, sql, expectedException);
      testRunner.start();
      if (!hiveOnly) {
        // While Hive runs in another thread, run the benchmark.
        StoreRunner benchRunner = new StoreRunner(benchStore, sql, null);
        benchRunner.run();
        benchmarkResults = benchRunner.results;
      }
      testRunner.join();
      testResults = testRunner.results;
      if (expectedException != null && !testRunner.sawExpectedException) {
        Assert.fail("Expected exception " + expectedException.getClass().getSimpleName() + " but "
          + (testRunner.stashedException == null ? "got no exception " : " got " +
            testRunner.stashedException.getClass().getSimpleName() + " instead."));
      }
      // If an exception was thrown and we didn't expect it, go ahead and throw it on now.
      if (testRunner.stashedException != null) {
        if (SQLException.class.equals(testRunner.stashedException)) {
          throw (SQLException)testRunner.stashedException;
        } else if (IOException.class.equals(testRunner.stashedException)) {
          throw (IOException)testRunner.stashedException;
        } else {
          throw new RuntimeException(testRunner.stashedException);
        }
      }
      if (expectedResult != ResultCode.ANY) {
        Assert.assertEquals("Unexpected fetch result", expectedResult, testRunner.results.rc);
      }
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  private static class StoreRunner extends Thread {
    Throwable stashedException;
    boolean sawExpectedException = false;
    final String sql;
    final Throwable expectedException;
    final DataStore store;
    FetchResult results;


    StoreRunner(DataStore store, String s, Throwable ee) {
      this.store = store;
      sql = s;
      expectedException = ee;
    }

    @Override
    public void run() {
      try {
        results = store.executeSql(sql);
      } catch (Throwable e) {
        if (e.equals(expectedException)) sawExpectedException = true;
        else stashedException = e;
      }
    }
  }

  /**
   * Run a query only against Hive.  Generally when you call this you need to call
   * {@link #runBenchmark} yourself with different syntax.
   * @param sql query to run
   * @throws SQLException
   * @throws IOException
   */
  protected void runHive(String sql) throws SQLException, IOException {
    runQuery(sql, ResultCode.SUCCESS, null, true);
  }

  /**
   * Fetch the query plan for the last query that was run.  When running on the cluster this will
   * return null, so your tests should handle that.  (This is because it depends on being in the
   * same memory space as the Hive client to work.)
   * @return Explain object that contains the QueryPlan, or null when on the cluster.
   * @throws IOException
   * @throws SQLException
   */
  protected Explain explain() throws IOException, SQLException {
    if (lastQuery != null) return explain(lastQuery);
    throw new RuntimeException("You must run a query before you explain it.");
  }

  /**
   * Fetch the query plan for a query.  When running on the cluster this return null, so your
   * tests should handle that.  This is because it depends on being in the same memory space as
   * the Hive client to work.  Also on the cluster the client may not share the same
   * configuration as the cluster and thus may not produce appropriate explain results.
   * @param sql SQL string of the query
   * @return Explain object that contains the QueryPlan.  null will be returned if running on the
   * cluster or the query has not been run.
   * @throws SQLException
   * @throws IOException
   */
  protected Explain explain(String sql) throws IOException, SQLException {
    LOG.debug("Going to explain query <" + sql + ">");
    QueryPlan plan = testStore.explain(sql);
    // plan may be null if we're on the cluster.
    return plan == null ? null : new Explain(plan);
  }

 /**
   * Get results of the query.  It is recommended that whenever possible you avoid using this and
   * instead use {@link #compare}.  For cases where comparing against a source of truth just
   * won't work you can get back the {@link java.sql.ResultSet} and do the comparison yourself.
   * @return resulting data set
   */
  protected DataSet getResults() {
    return testResults.data;
  }

   /**
   * Generate a benchmark for this data using the included SQL.  This should only be used when
   * the SQL has to be different than that passed to {@link #runQuery}.
   * @param sql SQL to execute
   * @param expectedResult expected result from running this query
   */
  protected void runBenchmark(String sql, ResultCode expectedResult)
      throws  SQLException, IOException {
    StoreRunner runner = new StoreRunner(benchStore, sql, null);
    benchmarkResults = runner.results;
    if (expectedResult != ResultCode.ANY) {
      Assert.assertEquals(expectedResult, benchmarkResults.rc);
    }
  }

  /**
   * Sort and compare the results of running the query against Hive and the benchmark.  If
   * runQuery has not been invoked with a select query, this will result in an error.  This
   * expects the query to have returned results.  If you expect the query to return no results
   * call {@link #assertEmpty} instead.
   */
  protected void sortAndCompare() throws SQLException, IOException {
    compare(true);
  }

  /**
   * Compare the results of running the query against Hive and the benchmark without sorting.  If
   * runQuery has not been invoked with a select query, this will result in an error.  This
   * expects the query to have returned results.  If you expect the query to return no results
   * call {@link #assertEmpty} instead.
   */
  protected void compare() throws SQLException, IOException {
    compare(false);
  }

  /**
   * Compare the results of an insert query that creates a table in Hive and the benchmark.  Note
   * that this is not fast for small data sets because it has to go look at the data and decide
   * whether to do the comparison locally or in the cluster.  So if you can run the query as a
   * select instead, or maybe even do the insert and then do a select yourself to compare on, it
   * will be faster for small data sets.  For large data sets (eg ETL queries) this will be much
   * faster because it will do the comparison in the cluster.
   * @param table table that data was inserted into.
   * @throws IOException
   * @throws SQLException
   */
  protected void tableCompare(TestTable table) throws IOException, SQLException {
    testManager.getTableComparator().compare(testCluster, benchCluster, table);

  }

  /**
   * Check that the results of a query are empty.  This does not check whether the query ran
   * successfully, as that is controlled by how you call runQuery.
   */
  protected void assertEmpty() {
    Assert.assertTrue("Expected results of query to be empty", resultIsEmpty(testResults));
    Assert.assertTrue("Expected results of benchmark to be empty", resultIsEmpty(benchmarkResults));
  }

  private boolean resultIsEmpty(FetchResult result) {
    return result.data == null || result.data.isEmpty();
  }

  private void compare(boolean sort) throws SQLException, IOException {
    Assert.assertNotNull("Expected results of query to be non-empty", testResults.data);
    Assert.assertNotNull("Expected results of benchmark to be non-empty", benchmarkResults.data);
    if (testResults.data.getSchema() == null) {
      // When Hive's working from the command line it doesn't know the schema of its output.  To
      // solve this cheat and grab the schema from the benchmark, so we know how to interpret the
      // output.
      testResults.data.setSchema(benchmarkResults.data.getSchema());
    }
    testManager.getResultComparator(sort).compare(testResults.data, benchmarkResults.data);
  }

 /**
   * Set a value in the configuration for this test.  This value will not persist across tests.
   * To set a value across all tests use the test configuration file.
   * @param var variable to set
   * @param val value to set it to
   */
  protected void setConfVarForOneTest(String var, String val) {
    setConfVar(testCluster, var, val);
    setConfVar(benchCluster, var, val);
  }

  /**
   * Get a configuration value that was set just for this test.
   * @param var
   * @return
   */
  protected String getSingleTestConfVar(String var) {
    return testCluster.getConfVars().get(var);
  }

  /**
   * Set a value in the configuration for this test.  This value will not persist across tests.
   * To set a value across all tests use the test configuration file.
   * @param var variable to set
   * @param val value to set it to
   */
  protected void setConfVarForOneTest(String var, int val) {
    setConfVarForOneTest(var, Integer.toString(val));
  }

  /**
   * Get a configuration value that was set just for this test.
   * @param var
   * @return
   */
  protected Integer getSingleTestConfInt(String var) {
    String str = testCluster.getConfVars().get(var);
    if (str == null) return null;
    else return Integer.valueOf(str);
  }

  /**
   * Set a value in the configuration for this test.  This value will not persist across tests.
   * To set a value across all tests use the test configuration file.
   * @param var variable to set
   * @param val value to set it to
   */
  protected void setConfVarForOneTest(String var, boolean val) {
    setConfVarForOneTest(var, Boolean.toString(val));
  }

  /**
   * Get a configuration value that was set just for this test.
   * @param var
   * @return
   */
  protected Boolean getSingleTestConfBool(String var) {
    String str = testCluster.getConfVars().get(var);
    if (str == null) return null;
    else return Boolean.valueOf(str);
  }

  /**
   * Set a value in the configuration for this test.  This value will not persist across tests.
   * To set a value across all tests use the test configuration file.
   * @param var variable to set
   * @param val value to set it to
   */
  protected void setConfVarForOneTest(String var, double val) {
    setConfVarForOneTest(var, Double.toString(val));
  }

  /**
   * Get a configuration value that was set just for this test.
   * @param var
   * @return
   */
  protected Double getSingleTestConfDouble(String var) {
    String str = testCluster.getConfVars().get(var);
    if (str == null) return null;
    else return Double.valueOf(str);
  }

  private void setConfVar(ClusterManager cluster, String var, String val) {
    cluster.setConfVar(var, val);
  }

  /**
   * Get a HiveEndPoint for streaming data too.  To test Hive streaming you must call this rather
   * than construct HiveEndPoint directly.  This call gives a subclass of HiveEndPoint that
   * splits the stream and passes it to both Hive and the benchmark.  When you call
   * @param testTable table to stream data to
   * @param partVals partition values for this end point.
   * @return a HiveEndPoint
   */
  protected HiveEndPoint getHiveEndPoint(TestTable testTable, List<String> partVals) {
    return new CapyEndPoint(testStore.getStreamingEndPoint(testTable, partVals),
        benchStore.getStreamingEndPoint(testTable, partVals), testTable,
        testCluster.getHiveConf(), partVals);
  }

  protected IntegrationTest() {
  }

  public void phantomTest() {
    // This does nothing.  It is here as a place holder for the case where all tests are excluded
    // because they've all been weeded out by the annotations.
    LOG.debug("in phantomTest");
  }

  /**
   * For use by {@link org.apache.hive.test.capybara.infra.IntegrationRunner} only.  This sets
   * the list so that we can determine values to set up for the test.
   * @param annotations annotations for this test
   */
  public void setAnnotations(Map<String, List<Annotation>> annotations) {
    allAnnotations = annotations;
  }
}
