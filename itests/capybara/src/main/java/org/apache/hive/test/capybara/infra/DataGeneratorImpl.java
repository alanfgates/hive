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

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hive.test.capybara.DataGenerator;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Base class for DataGenerators with commonly used methods.
 */
abstract class DataGeneratorImpl extends DataGenerator {

  protected abstract ColGenerator getGenerator(FieldSchema col, TestTable.PrimaryKey pk,
                                      TestTable.ForeignKey fk);

  protected List<DataSet.Row> determinePartVals(TestTable table, int scale, double[] pctNulls) {
    // There are three options for partition values.  The user could have specified the values
    // they want, or they could have just specified how many partitions they want or they could
    // have let us decide based one scale.
    List<FieldSchema> partCols = table.getPartCols();
    List<FieldSchema> cols = table.getCols();
    List<DataSet.Row> partVals = table.getPartVals();
    int numPartitions = table.getNumParts();

    // If the user did not preset the partVals, it will be null
    if (partVals == null) {
      partVals = new ArrayList<>();
      DataSet.RowBuilder partBuilder = new DataSet.RowBuilder(partCols, cols.size());
      // If the user did not specify a number of partitions, set it based on scale.
      if (numPartitions == 0) numPartitions = (int) Math.log10(scale) + 1;
      RandomDataGenerator.ColGenerator[] partGenerators = new RandomDataGenerator.ColGenerator[partCols.size()];
      for (int i = 0; i < partGenerators.length; i++) {
        partGenerators[i] = getGenerator(partCols.get(i), null, null);
      }
      Set<DataSet.Row> usedPartVals = new HashSet<>();
      for (int i = 0; i < numPartitions; i++) {
        DataSet.Row partVal = partBuilder.build();
        for (int j = 0; j < partGenerators.length; j++) {
          partVal.get(j).set(partGenerators[j].generate(pctNulls[j]));
        }
        if (!usedPartVals.add(partVal)) {
          // We've seen this partition value before, so try again.
          i--;
        } else {
          partVals.add(partVal);
        }
      }
    }
    return partVals;
  }

  interface ColGenerator {
    Object generate(double pctNull);
  }
}
