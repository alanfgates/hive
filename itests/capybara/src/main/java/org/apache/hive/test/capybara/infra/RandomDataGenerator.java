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

import org.apache.hive.test.capybara.data.DataSet;
import org.apache.hive.test.capybara.data.Row;
import org.apache.hive.test.capybara.data.RowBuilder;
import org.apache.hive.test.capybara.iface.TestTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hive.test.capybara.iface.DataGenerator;

import java.io.IOException;
import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A data generator that creates random data.  If you want the data generated to be the same each
 * time (that is, you're passing in a constant seed value) you should instantiate a new version
 * of RandomDataGenerator for each table.  Otherwise tables after the first one will inherit the
 * first one's rand, which won't generate the same data as if you generated that table first.
 */
public class RandomDataGenerator extends DataGeneratorImpl implements Serializable {
  static final private Logger LOG = LoggerFactory.getLogger(RandomDataGenerator.class);

  protected Random rand;
  protected long seed;

 /**
   * Constructor for default number of partitions (log10(scale)) and null values (1% of values
   * per column)
   * @param seed Seed to use for generating random numbers.  By setting this constant you can
   *             make sure the same data is generated each time.
   */
  public RandomDataGenerator(long seed) {
    this.seed = seed;
    rand = new Random(seed);
  }

  @Override
  protected DataGenerator copy(int copyNum) {
    return new RandomDataGenerator(seed + copyNum);
  }

  @Override
  public DataSet generateData(TestTable table, int scale, double[] pctNulls) throws IOException {
    // Decide whether we're going to generate locally or on the cluster.  Remember that scale is
    // in K, while other values are in b.
    if (scale * 1024 > TestManager.getTestManager().getTestConf().getClusterGenThreshold() &&
        TestManager.getTestManager().getTestClusterManager().remote()) {
      return new ClusterDataGenerator(this).generateData(table, scale, pctNulls);
    }

    LOG.debug("Generating data locally");

    // We'll handle partition columns and regular columns separately, as we want to control the
    // number of partitions.
    List<FieldSchema> cols = table.getCols();
    ColGenerator[] generators = new ColGenerator[cols.size()];
    for (int i = 0; i < generators.length; i++) {
      TestTable.PrimaryKey pk = null;
      if (table.getPrimaryKey() != null && table.getPrimaryKey().colNum == i) {
        pk = table.getPrimaryKey();
      }
      TestTable.ForeignKey fk = null;
      if (table.getForeignKeys() != null) {
        for (TestTable.ForeignKey f : table.getForeignKeys()) {
          if (f.colNumInSrc == i) {
            fk = f;
            break;
          }
        }
      }
      generators[i] = getGenerator(cols.get(i), pk, fk);
    }

    if (pctNulls == null) {
      pctNulls = new double[cols.size()];
      for (int i = 0; i < pctNulls.length; i++) pctNulls[i] = 0.01;
    } else {
      if (pctNulls.length != cols.size()) {
        throw new RuntimeException("Your null array must be the same size as the number of " +
            "columns in the table, excluding partition columns");
      }
    }

    List<FieldSchema> partCols = table.getPartCols();
    GeneratedDataSet rows;
    RowBuilder colBuilder = new RowBuilder(cols);
    rows = new GeneratedDataSet(table.getCombinedSchema());
    if (partCols == null || partCols.size() == 0) {
      generateData(rows, colBuilder, generators, null, scale, pctNulls);
    } else {
      List<Row> partVals = determinePartVals(table, scale, pctNulls);

      for (Row partVal : partVals) {
        // Divide scale by number of partitions so we still get the right amount of data.
        generateData(rows, colBuilder, generators, partVal, scale / partVals.size() + 1, pctNulls);
      }
    }
    return rows;
  }

  private void generateData(GeneratedDataSet rows, RowBuilder builder,
                            ColGenerator[] generators, Row partVals, int scale,
                            double[] pctNulls) throws IOException {
    long generatedSize = 0;
    while (generatedSize < scale * 1024) {
      Row row = builder.build();
      for (int i = 0; i < generators.length; i++) {
        row.get(i).set(generators[i].generate(pctNulls[i]));
      }
      if (partVals != null) row.append(partVals);
      generatedSize += row.lengthInBytes();
      rows.addRow(row);
    }
  }

  @Override
  protected ColGenerator getGenerator(FieldSchema col, TestTable.PrimaryKey pk,
                                      TestTable.ForeignKey fk) {

    String colType = col.getType();
    if (fk != null) {
      return new ForeignKeyGenerator(fk);
    }

    if (pk != null && pk.isSequence()) {
      if (!colType.equalsIgnoreCase("bigint")) {
        throw new RuntimeException("You tried to create a sequence with something other than a " +
            "bigint.  That can't end well.");
      }
      return new SequenceGenerator();
    }

    ColGenerator cg;
    if (colType.equalsIgnoreCase("bigint")) {
      cg = getLongGenerator(col);
    } else if (colType.substring(0, 3).equalsIgnoreCase("int")) {
      cg = getIntGenerator(col);
    } else if (colType.equalsIgnoreCase("smallint")) {
      cg = getShortGenerator(col);
    } else if (colType.equalsIgnoreCase("tinyint")) {
      cg = getByteGenerator(col);
    } else if (colType.equalsIgnoreCase("float")) {
      cg = getFloatGenerator(col);
    } else if (colType.equalsIgnoreCase("double")) {
      cg = getDoubleGenerator(col);
    } else if (colType.toLowerCase().startsWith("decimal")) {
      int precision, scale;
      if (colType.equalsIgnoreCase("decimal")) {
        // They didn't give a precision and scale, so pick the default
        precision = 10;
        scale = 2;
      } else {
        if (colType.contains(",")) {
          Pattern regex = Pattern.compile(".*\\(([0-9]+),([0-9]+)\\).*");
          Matcher matcher = regex.matcher(colType);
          if (!matcher.matches()) {
            throw new RuntimeException("Expected type to match decimal([0-9]+,[0-9]+) but it " +
                "doesn't appear to: " + colType);
          }
          precision = Integer.valueOf(matcher.group(1));
          scale = Integer.valueOf(matcher.group(2));
        } else {
          precision = getPrecision(colType);
          scale = 2;
        }
      }
      cg = getBigDecimalGenerator(col, precision, scale);
    } else if (colType.equalsIgnoreCase("date")) {
      cg = getDateGenerator(col);
    } else if (colType.equalsIgnoreCase("timestamp")) {
      cg = getTimestampGenerator(col);
    } else if (colType.toLowerCase().startsWith("varchar") ||
        colType.toLowerCase().startsWith("char")) {
      cg = getStringGenerator(col, getPrecision(colType));
    } else if (colType.equalsIgnoreCase("string")) {
      cg = getStringGenerator(col, 20);
    } else if (colType.equalsIgnoreCase("boolean")) {
      cg = getBoolGenerator(col);
    } else if (colType.equalsIgnoreCase("binary")) {
      cg = getBinaryGenerator(col, 100);
    } else {
      throw new RuntimeException("How'd we get here?  Unknown type");
    }

    if (pk != null) return new PrimaryKeyGenerator(cg);
    else return cg;
  }

  protected int getPrecision(String colType) {
    Pattern regex = Pattern.compile(".*\\(([0-9]+)\\).*");
    Matcher matcher = regex.matcher(colType);
    if (!matcher.matches()) {
      throw new RuntimeException("Expected type to match datatype([0-9]+) but it " +
          "doesn't appear to: " + colType);
    }
    return Integer.valueOf(matcher.group(1));
  }

  /**
   * If you override this class you MUST remember to updated generatedSize as you add values,
   * otherwise the generator will go into an infinite loop.
   * @return column value generator for long (bigint) values.
   */
  protected ColGenerator getLongGenerator(FieldSchema col) {
    return new ColGenerator() {
      @Override
      public Object generate(double pctNull) {
        if (rand.nextDouble() < pctNull) return null;
        return rand.nextLong();
      }
    };
  }

  protected ColGenerator getIntGenerator(FieldSchema col) {
   return new ColGenerator() {
      @Override
      public Object generate(double pctNull) {
        if (rand.nextDouble() < pctNull) return null;
        return rand.nextInt();
      }
    };
  }

  protected ColGenerator getShortGenerator(FieldSchema col) {
    return new ColGenerator() {
      @Override
      public Object generate(double pctNull) {
        if (rand.nextDouble() < pctNull) return null;
        return (short) rand.nextInt(Short.MAX_VALUE);
      }
    };
  }

  protected ColGenerator getByteGenerator(FieldSchema col) {
    return new ColGenerator() {
      @Override
      public Object generate(double pctNull) {
        if (rand.nextDouble() < pctNull) return null;
        return (byte) rand.nextInt(Byte.MAX_VALUE);
      }
    };
  }

  protected ColGenerator getFloatGenerator(FieldSchema col) {
    return new ColGenerator() {
      @Override
      public Object generate(double pctNull) {
        if (rand.nextDouble() < pctNull) return null;
        return rand.nextFloat() + rand.nextInt();
      }
    };
  }

  protected ColGenerator getDoubleGenerator(FieldSchema col) {
    return new ColGenerator() {
      @Override
      public Object generate(double pctNull) {
        if (rand.nextDouble() < pctNull) return null;
        return rand.nextDouble() + rand.nextLong();
      }
    };
  }

  protected ColGenerator getBigDecimalGenerator(FieldSchema col, final int precision,
                                                final int scale) {
    return new ColGenerator() {
      @Override
      public Object generate(double pctNull) {
        if (rand.nextDouble() < pctNull) return null;
        BigInteger bi = new BigInteger(precision * 3, rand);
        return new BigDecimal(bi, scale);
      }
    };

  }

  protected ColGenerator getDateGenerator(FieldSchema col) {
    return new ColGenerator() {
      @Override
      public Object generate(double pctNull) {
        if (rand.nextDouble() < pctNull) return null;
        // Limit the range of dates, otherwise the databases get bent out of shape when dates in
        // the year 20,000AD show up.
        return new Date(rand.nextLong() % (Integer.MAX_VALUE * 1000L));
      }
    };
  }

  protected ColGenerator getTimestampGenerator(FieldSchema col) {
    return new ColGenerator() {
      @Override
      public Object generate(double pctNull) {
        if (rand.nextDouble() < pctNull) return null;
        // Limit the range of timestamps, otherwise the databases get bent out of shape when
        // timestamps in the year 20,000AD show up.
        return new Timestamp(rand.nextLong() % (Integer.MAX_VALUE * 1000L));
      }
    };
  }

  protected ColGenerator getStringGenerator(FieldSchema col, final int maxLength) {

    return new ColGenerator() {
      @Override
      public Object generate(double pctNull) {
        if (rand.nextDouble() < pctNull) return null;
        int length = rand.nextInt(maxLength) + 1;
        StringBuilder builder = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
          // Generate alpha-characters only for now.  Hive uses POSIX sorting for strings and
          // other databases use other ones, so including symbols, numbers, etc. goes all wrong
          // on us in comparisons otherwise.
          char nextChar = (char)(rand.nextInt('z' - 'a') + 'a');
          if (nextChar == '\\') nextChar = '/';
          builder.append(nextChar);
        }
        return builder.toString();
      }
    };
  }

  protected ColGenerator getBoolGenerator(FieldSchema col) {
    return new ColGenerator() {
      @Override
      public Object generate(double pctNull) {
        if (rand.nextDouble() < pctNull) return null;
        return rand.nextBoolean();
      }
    };
  }

  protected ColGenerator getBinaryGenerator(FieldSchema col, final int maxLength) {

    return new ColGenerator() {
      @Override
      public Object generate(double pctNull) {
        if (rand.nextDouble() < pctNull) return null;
        int length = rand.nextInt(maxLength) + 1;
        byte[] bytes = new byte[length];
        rand.nextBytes(bytes);
        return bytes;
      }
    };
  }

  /**
   * A class to generate foreign keys.  Given a primary key in another table, this class will
   * generate foreign keys that are guaranteed to match a row in the target table's primary key.
   * The primary key is assumed to be single columned for now.  Pctnull is ignored by this generator.
   */
  protected class ForeignKeyGenerator implements ColGenerator {
    private final Map<Integer, Comparable> vals;

    ForeignKeyGenerator(TestTable.ForeignKey fk) {
      vals = new HashMap<>();
      int nextVal = 0;
      for (Row row : fk.targetTable) {
        vals.put(nextVal++, row.get(fk.colNumInTarget).get());
      }
    }

    @Override
    public Object generate(double pctNull) {
      return vals.get(rand.nextInt(vals.size()));
    }
  }

  /**
   * Generate a bigint sequence.  Pctnull is ignored by this generator.
   */
  protected class SequenceGenerator implements ColGenerator {
    long nextSequence = 1;

    @Override
    public Object generate(double pctNull) {
      return nextSequence++;
    }
  }

  /**
   * Generate a primary key for a table.  The values are guaranteed to be unique.  You can get a
   * sequence primary key by passing a SequenceGenerator as the wrapped ColGenerator.  Primary
   * keys are currently restricted to a single column.   Pctnull is ignored by this generator.
   */
  protected class PrimaryKeyGenerator implements ColGenerator {
    private final ColGenerator wrapped;
    private Set<Object> usedVals;

    PrimaryKeyGenerator(ColGenerator wrapped) {
      this.wrapped = wrapped;
      usedVals = new HashSet<>();
    }

    @Override
    public Object generate(double pctNull) {
      Object val;
      do {
        val = wrapped.generate(0.0);
      } while (!usedVals.add(val));
      return val;
    }
  }

}
