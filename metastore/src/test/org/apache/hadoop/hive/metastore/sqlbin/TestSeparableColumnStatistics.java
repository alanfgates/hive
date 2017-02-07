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

import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsDesc;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.LongColumnStatsData;
import org.apache.hadoop.hive.metastore.api.StringColumnStatsData;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

public class TestSeparableColumnStatistics {

  @Test
  public void serializeDeserialize() throws IOException, SQLException {
    String dbName = "default";
    String tableName = "T";
    String partName = "P";
    String col1 = "a";
    String col2 = "b";

    ColumnStatisticsData csd_a = ColumnStatisticsData.stringStats(
        new StringColumnStatsData(317, 87.2, 57, 98732)
    );

    LongColumnStatsData ldata = new LongColumnStatsData(97, 102920);
    ldata.setLowValue(0);
    ldata.setHighValue(14123123);

    ColumnStatisticsData csd_b = ColumnStatisticsData.longStats(ldata);

    ColumnStatisticsDesc statsDec = new ColumnStatisticsDesc(false, dbName, tableName);
    statsDec.setPartName(partName);
    List<ColumnStatisticsObj> statsObjs = Arrays.asList(
        new ColumnStatisticsObj(col1, "varchar(32)", csd_a),
        new ColumnStatisticsObj(col2, "int", csd_b)
    );
    ColumnStatistics cs = new ColumnStatistics(statsDec, statsObjs);
    SeparableColumnStatistics separable = new SeparableColumnStatistics(cs);

    Assert.assertFalse(separable.isSerialized(col1));
    Assert.assertFalse(separable.isSerialized(col2));

    byte[] buf = separable.serialize();

    separable = new SeparableColumnStatistics(buf);

    Assert.assertEquals(dbName, separable.getStatsDesc().getDbName());

    Assert.assertTrue(separable.isSerialized(col1));
    Assert.assertTrue(separable.isSerialized(col2));

    ColumnStatisticsObj cso = separable.getStatsForCol(col1);
    Assert.assertEquals(col1, cso.getColName());
    Assert.assertEquals(317, cso.getStatsData().getStringStats().getMaxColLen());

    Assert.assertFalse(separable.isSerialized(col1));
    Assert.assertTrue(separable.isSerialized(col2));

  }
}
