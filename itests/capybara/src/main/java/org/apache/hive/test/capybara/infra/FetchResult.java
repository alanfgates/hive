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

/**
 * Result of fetching data.  This includes the
 * {@link org.apache.hive.test.capybara.infra.DataSet} and info on the result.  If the result
 * was a failure the data set will be null.
 */
public class FetchResult {

  /**
   * Result from Hive.  This is done as an enum since values differ based on whether
   * Hive is being accessed by command line or JDBC.  ANY indicates that it's ok whether
   * something passes or fails.
   */
  public enum ResultCode { SUCCESS, RETRIABLE_FAILURE, NON_RETRIABLE_FAILURE, ANY };

  final public DataSet data;
  final public ResultCode rc;

  /**
   * Create a FetchResult with SUCCESS as the ResultCode and a DataSet.
   * @param d DataSet for this FetchResult.
   */
  public FetchResult(DataSet d) {
    data = d;
    rc = ResultCode.SUCCESS;
  }

  /**
   * Create a FetchResult with no data.
   * @param r ResultCode for this FetchResult.
   */
  public FetchResult(ResultCode r) {
    data = null;
    rc = r;
  }

  /**
   * Determine whether this query returned results.  Select queries will in general return
   * results, while DML and DDL will not.
   * @return true if this query has results.
   */
  public boolean hasResults() {
    return data != null;
  }
}
