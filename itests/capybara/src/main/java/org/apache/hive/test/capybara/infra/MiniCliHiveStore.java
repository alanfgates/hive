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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.ql.CommandNeedRetryException;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * An implementation of HiveStore that works with mini clusters and the cli.
 */
class MiniCliHiveStore extends MiniHiveStoreBase {

  static final private Logger LOG = LoggerFactory.getLogger(MiniCliHiveStore.class.getName());


  MiniCliHiveStore(ClusterManager clusterManager) {
    super(clusterManager);
  }

  @Override
  public FetchResult fetchData(String sql) throws SQLException, IOException {
    try {
      LOG.debug("Going to send to Hive: " + sql);
      CommandProcessorResponse rsp = getDriver().run(sql);
      if (rsp.getResponseCode() != 0) {
        LOG.info("Response code from query <" + sql + "> is " + rsp.getResponseCode() + ", error" +
            " message <" + rsp.getErrorMessage() + "> SQLState <" + rsp.getSQLState() + ">");
        return new FetchResult(FetchResult.ResultCode.NON_RETRIABLE_FAILURE);
      }
      List<String> rows = new ArrayList<>();
      if (getDriver().getResults(rows)) {
        return new FetchResult(new StringDataSet(rows, "\t", "NULL"));
      } else {
        return new FetchResult(FetchResult.ResultCode.SUCCESS);
      }
    } catch (CommandNeedRetryException e) {
      LOG.info("Query <" + sql + "> failed with retriable error");
      return new FetchResult(FetchResult.ResultCode.RETRIABLE_FAILURE);
    }
  }

  @Override
  protected String connectionURL() {
    return null;
  }

  @Override
  protected Properties connectionProperties() {
    return null;
  }
}

