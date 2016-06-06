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
import org.apache.hive.test.capybara.iface.TestTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hive.test.capybara.iface.DataGenerator;

import java.io.IOException;
import java.io.Serializable;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;

/**
 * A data generator that generates data based on the stats produced by Hive.  This works by
 * provided {@link org.apache.hive.test.capybara.infra.DataGeneratorImpl.ColGenerator}s that take
 * the statistics gathered from Hive into account.
 */
public class StatsDataGenerator extends RandomDataGenerator implements Serializable {
  static final private Logger LOG = LoggerFactory.getLogger(StatsDataGenerator.class);

  private TableStats tableStats;

  public StatsDataGenerator(TableStats tableStats, long seed) {
    super(seed);
    this.tableStats = tableStats;
  }

  @Override
  protected DataGenerator copy(int copyNum) {
    return new StatsDataGenerator(tableStats, seed + copyNum);
  }

  @Override
  public DataSet generateData(TestTable table, int scale, double[] pctNulls) throws IOException {
    tableStats.scale(scale);
    return super.generateData(table, scale, pctNulls);
  }

  public static class ColStats implements Serializable {
    String colName;
    String dataType;
    Object min;
    Object max;
    long numNulls;
    long distinctCount;
    double avgColLen;
    long maxColLen;
    long numTrues;
    long numFalses;
    double pctNull;
    double pctTrue;
    double pctFalse;

    public ColStats(String colName, String dataType, Object min, Object max, double avgColLen,
                    long maxColLen, long numFalses, long numTrues, long distinctCount,
                    long numNulls) {
      this.colName = colName;
      this.dataType = dataType;
      this.min = min;
      this.max = max;
      this.avgColLen = avgColLen;
      this.maxColLen = maxColLen;
      this.numFalses = numFalses;
      this.numTrues = numTrues;
      this.distinctCount = distinctCount;
      this.numNulls = numNulls;
    }

    /**
     * Scale up or down the counts for this column
     * @param multiplier multiplier to use in the scaling
     * @param numRows total number of rows in the original
     */
    void scale(double multiplier, long numRows, long origNumRows) {

      // When the scale is small relative to the original data set we can get a very small
      // multiplier which ends up in everything else being rounded down to zero.  Prevent that.
      if (distinctCount != 0) {
        distinctCount = Math.max(10L, (long)(distinctCount * multiplier));
      }
      if (numTrues != 0) {
        pctTrue = numTrues / (double)origNumRows;
      }
      if (numFalses != 0) {
        pctFalse = numFalses / (double)origNumRows;
      }
      pctNull = (double)numNulls / (double)origNumRows;
    }

  }

  public static class TableStats implements Serializable {
    String dbName;
    String tableName;
    long totalSize;
    long numRows;
    int numPartitions;
    Map<String, ColStats> colStats;

    public TableStats(Map<String, ColStats> colStats, int numPartitions, String dbName,
                      String tableName, long totalSize, long numRows) {
      this.colStats = colStats;
      this.numPartitions = numPartitions;
      this.dbName = dbName;
      this.tableName = tableName;
      this.totalSize = totalSize;
      this.numRows = numRows;
    }

    /**
     * Modify values based on the scale.
     * @param scale Scale used for this test
     */
    TableStats scale(int scale) {
      double multiplier = (scale * 1024)/ (double)totalSize ;

      // When the scale is small relative to the original data set we can get a very small
      // multiplier which ends up in everything else being rounded down to zero.  Prevent that.
      if (numPartitions != 0) {
        numPartitions = Math.max(2, (int)(numPartitions * multiplier));
      }
      long origNumRows = numRows;
      numRows = Math.max(100L, (long)(numRows * multiplier));

      for (Map.Entry<String, ColStats> entry : colStats.entrySet()) {
        entry.getValue().scale(multiplier, numRows, origNumRows);
      }
      return this;
    }
  }

  @Override
  protected ColGenerator getLongGenerator(FieldSchema col) {
    final ColStats colStats = tableStats.colStats.get(col.getName());
    if (colStats == null) {
      LOG.warn("Unable to find column stats entry for " + col.getName() + ", using basic random " +
          "generation for that column.");
      return super.getLongGenerator(col);
    }
    final Long max = (Long)colStats.max;
    final Long min = (Long)colStats.min;
    final Map<Integer, Long> distinctVals = new HashMap<>();

    return new ColGenerator() {
      private long range = 0;

      @Override
      public Object generate(double pctNull) {
        // Always check whether we should return null before checking for distinct values
        if (rand.nextDouble() < colStats.pctNull) return null;

        if (distinctVals.size() >= colStats.distinctCount) {
          return distinctVals.get(rand.nextInt(distinctVals.size()));
        }
        if (range == 0) {
          range = max - min;
          if (range < 2) range = 2;
        }
        Long generated = Math.abs(rand.nextLong() % range) + min;
        distinctVals.put(distinctVals.size(), generated);
        return generated;
      }
    };
  }

  @Override
  protected ColGenerator getIntGenerator(FieldSchema col) {
    final ColStats colStats = tableStats.colStats.get(col.getName());
    if (colStats == null) {
      LOG.warn("Unable to find column stats entry for " + col.getName() + ", using basic random " +
          "generation for that column.");
      return super.getIntGenerator(col);
    }
    final Integer max = (Integer)colStats.max;
    final Integer min = (Integer)colStats.min;
    final Map<Integer, Integer> distinctVals = new HashMap<>();

    return new ColGenerator() {
      private int range = 0;

      @Override
      public Object generate(double pctNull) {
        // Always check whether we should return null before checking for distinct values
        if (rand.nextDouble() < colStats.pctNull) return null;

        if (distinctVals.size() >= colStats.distinctCount) {
          return distinctVals.get(rand.nextInt(distinctVals.size()));
        }
        if (range == 0) {
          range = max - min;
          if (range < 2) range = 2;
        }
        Integer generated = Math.abs(rand.nextInt() % range) + min;
        distinctVals.put(distinctVals.size(), generated);
        return generated;
      }
    };
  }

  @Override
  protected ColGenerator getShortGenerator(FieldSchema col) {
    final ColStats colStats = tableStats.colStats.get(col.getName());
    if (colStats == null) {
      LOG.warn("Unable to find column stats entry for " + col.getName() + ", using basic random " +
          "generation for that column.");
      return super.getShortGenerator(col);
    }
    final Short max = (Short)colStats.max;
    final Short min = (Short)colStats.min;
    final Map<Integer, Short> distinctVals = new HashMap<>();

    return new ColGenerator() {
      private short range = 0;

      @Override
      public Object generate(double pctNull) {
        // Always check whether we should return null before checking for distinct values
        if (rand.nextDouble() < colStats.pctNull) return null;

        if (distinctVals.size() >= colStats.distinctCount) {
          return distinctVals.get(rand.nextInt(distinctVals.size()));
        }
        if (range == 0) {
          range = (short)(max - min); // short - short = int?  seriously?
          if (range < 2) range = 2;
        }
        Short generated = (short)(rand.nextInt(range) + min);
        distinctVals.put(distinctVals.size(), generated);
        return generated;
      }
    };
  }

  @Override
  protected ColGenerator getByteGenerator(FieldSchema col) {
    final ColStats colStats = tableStats.colStats.get(col.getName());
    if (colStats == null) {
      LOG.warn("Unable to find column stats entry for " + col.getName() + ", using basic random " +
          "generation for that column.");
      return super.getByteGenerator(col);
    }
    final Byte max = (Byte)colStats.max;
    final Byte min = (Byte)colStats.min;
    final Map<Integer, Byte> distinctVals = new HashMap<>();

    return new ColGenerator() {
      private byte range = 0;

      @Override
      public Object generate(double pctNull) {
        // Always check whether we should return null before checking for distinct values
        if (rand.nextDouble() < colStats.pctNull) return null;

        if (distinctVals.size() >= colStats.distinctCount) {
          return distinctVals.get(rand.nextInt(distinctVals.size()));
        }
        if (range == 0) {
          range = (byte)(max - min);
          if (range < 2) range = 2;
        }
        Byte generated = (byte)(rand.nextInt(range) + min);
        distinctVals.put(distinctVals.size(), generated);
        return generated;
      }
    };
  }

  @Override
  protected ColGenerator getFloatGenerator(FieldSchema col) {
    final ColStats colStats = tableStats.colStats.get(col.getName());
    if (colStats == null) {
      LOG.warn("Unable to find column stats entry for " + col.getName() + ", using basic random " +
          "generation for that column.");
      return super.getFloatGenerator(col);
    }
    final Float max = (Float)colStats.max;
    final Float min = (Float)colStats.min;
    final Map<Integer, Float> distinctVals = new HashMap<>();

    return new ColGenerator() {
      @Override
      public Object generate(double pctNull) {
        // Always check whether we should return null before checking for distinct values
        if (rand.nextDouble() < colStats.pctNull) return null;

        if (distinctVals.size() >= colStats.distinctCount) {
          return distinctVals.get(rand.nextInt(distinctVals.size()));
        }
        float range = max - min;
        float tmp;
        if (range > 2) {
          tmp = Math.abs(rand.nextLong() % range) + rand.nextFloat() + min;
        } else {
          tmp = rand.nextFloat() + min;
        }
        // Take the min here because I could have a range of 0.5 and then have generated 0.75.
        Float generated = Math.min(max, tmp);
        distinctVals.put(distinctVals.size(), generated);
        return generated;
      }
    };
  }

  @Override
  protected ColGenerator getDoubleGenerator(FieldSchema col) {
    final ColStats colStats = tableStats.colStats.get(col.getName());
    if (colStats == null) {
      LOG.warn("Unable to find column stats entry for " + col.getName() + ", using basic random " +
          "generation for that column.");
      return super.getDoubleGenerator(col);
    }
    final Double max = (Double)colStats.max;
    final Double min = (Double)colStats.min;
    final Map<Integer, Double> distinctVals = new HashMap<>();

    return new ColGenerator() {
      @Override
      public Object generate(double pctNull) {
        // Always check whether we should return null before checking for distinct values
        if (rand.nextDouble() < colStats.pctNull) return null;

        if (distinctVals.size() >= colStats.distinctCount) {
          return distinctVals.get(rand.nextInt(distinctVals.size()));
        }
        double range = max - min;
        double tmp;
        if (range > 2) {
          tmp = Math.abs(rand.nextLong() % range) + rand.nextDouble() + min;
        } else {
          tmp = rand.nextDouble() + min;
        }
        // Take the min here because I could have a range of 0.5 and then have generated 0.75.
        Double generated = Math.min(max, tmp);
        distinctVals.put(distinctVals.size(), generated);
        return generated;
      }
    };
  }

  @Override
  protected ColGenerator getBigDecimalGenerator(FieldSchema col, final int precision,
                                                final int scale) {
    final ColStats colStats = tableStats.colStats.get(col.getName());
    if (colStats == null) {
      LOG.warn("Unable to find column stats entry for " + col.getName() + ", using basic random " +
          "generation for that column.");
      return super.getBigDecimalGenerator(col, precision, scale);
    }
    final BigDecimal max = (BigDecimal)colStats.max;
    final BigDecimal min = (BigDecimal)colStats.min;
    final Map<Integer, BigDecimal> distinctVals = new HashMap<>();

    return new ColGenerator() {
      @Override
      public Object generate(double pctNull) {
        // Always check whether we should return null before checking for distinct values
        if (rand.nextDouble() < colStats.pctNull) return null;

        if (distinctVals.size() >= colStats.distinctCount) {
          return distinctVals.get(rand.nextInt(distinctVals.size()));
        }
        double range = max.doubleValue() - min.doubleValue();
        double tmp;
        if (range > 2) {
          tmp = Math.abs(rand.nextLong() % range) + rand.nextDouble() + min.doubleValue();
        } else {
          tmp = rand.nextDouble() + min.doubleValue();
        }
        tmp = Math.min(max.doubleValue(), tmp);
        BigDecimal generated = new BigDecimal(tmp).setScale(scale, BigDecimal.ROUND_DOWN);
        distinctVals.put(distinctVals.size(), generated);
        return generated;
      }
    };
  }

  @Override
  protected ColGenerator getDateGenerator(FieldSchema col) {
    final ColStats colStats = tableStats.colStats.get(col.getName());
    if (colStats == null) {
      LOG.warn("Unable to find column stats entry for " + col.getName() + ", using basic random " +
          "generation for that column.");
      return super.getDateGenerator(col);
    }
    final Date max = (Date)colStats.max;
    final Date min = (Date)colStats.min;
    final Map<Integer, Date> distinctVals = new HashMap<>();

    return new ColGenerator() {
      private long range = 0;

      @Override
      public Object generate(double pctNull) {
        // Always check whether we should return null before checking for distinct values
        if (rand.nextDouble() < colStats.pctNull) return null;

        if (distinctVals.size() >= colStats.distinctCount) {
          return distinctVals.get(rand.nextInt(distinctVals.size()));
        }
        if (range == 0) {
          range = max.getTime() - min.getTime();
          if (range < 2) range = 2;
        }
        Date generated = new Date(Math.abs(rand.nextLong() % range) + min.getTime());
        distinctVals.put(distinctVals.size(), generated);
        return generated;
      }
    };
  }

  @Override
  protected ColGenerator getTimestampGenerator(FieldSchema col) {
    final ColStats colStats = tableStats.colStats.get(col.getName());
    if (colStats == null) {
      LOG.warn("Unable to find column stats entry for " + col.getName() + ", using basic random " +
          "generation for that column.");
      return super.getTimestampGenerator(col);
    }
    final Timestamp max = (Timestamp)colStats.max;
    final Timestamp min = (Timestamp)colStats.min;
    final Map<Integer, Timestamp> distinctVals = new HashMap<>();

    return new ColGenerator() {
      private long range = 0;

      @Override
      public Object generate(double pctNull) {
        // Always check whether we should return null before checking for distinct values
        if (rand.nextDouble() < colStats.pctNull) return null;

        if (distinctVals.size() >= colStats.distinctCount) {
          return distinctVals.get(rand.nextInt(distinctVals.size()));
        }
        if (range == 0) {
          range = max.getTime() - min.getTime();
          if (range < 2) range = 2;
        }
        Timestamp generated = new Timestamp(Math.abs(rand.nextLong() % range) + min.getTime());
        distinctVals.put(distinctVals.size(), generated);
        return generated;
      }
    };
  }

  @Override
  protected ColGenerator getStringGenerator(FieldSchema col, final int maxLength) {
    final ColStats colStats = tableStats.colStats.get(col.getName());
    if (colStats == null) {
      LOG.warn("Unable to find column stats entry for " + col.getName() + ", using basic random " +
          "generation for that column.");
      return super.getStringGenerator(col, maxLength);
    }
    final Map<Integer, String> distinctVals = new HashMap<>();

    return new ColGenerator() {
      long totalLen = 0L;

      @Override
      public Object generate(double pctNull) {
        if (rand.nextDouble() < colStats.pctNull) return null;
        if (distinctVals.size() >= colStats.distinctCount) {
          return distinctVals.get(rand.nextInt(distinctVals.size()));
        }

        // If the average length is trending too low, produce a larger string.  If it's trending
        // too high, produce a smaller string.  If it's within 10% of the requested average then
        // just take what we get.
        int length;
        if (colStats.maxColLen <= colStats.avgColLen + 1) {
          length = (int)colStats.avgColLen;
        } else if (distinctVals.size() > 0 &&
            totalLen / distinctVals.size() > colStats.avgColLen * 1.1) {
          if (colStats.avgColLen < 2.0) length = 1;
          else length = rand.nextInt((int)colStats.avgColLen) + 1;
        } else if (distinctVals.size() > 0 &&
            totalLen / distinctVals.size() < colStats.avgColLen * 0.9) {
          length = rand.nextInt((int)colStats.maxColLen - (int)colStats.avgColLen) +
              (int)colStats.avgColLen;
        } else {
          length = rand.nextInt((int)colStats.maxColLen) + 1;
        }

        totalLen += length;
        StringBuilder builder = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
          builder.append((char)(rand.nextInt('z' - 'a') + 'a'));
        }
        String generated = builder.toString();
        distinctVals.put(distinctVals.size(), generated);
        return generated;
      }
    };
  }

  @Override
  protected ColGenerator getBinaryGenerator(FieldSchema col, final int maxLength) {
    final ColStats colStats = tableStats.colStats.get(col.getName());
    if (colStats == null) {
      LOG.warn("Unable to find column stats entry for " + col.getName() + ", using basic random " +
          "generation for that column.");
      return super.getBinaryGenerator(col, maxLength);
    }

    return new ColGenerator() {
      long totalLen = 0L;
      long numGenerated = 0L;

      @Override
      public Object generate(double pctNull) {
        if (rand.nextDouble() < colStats.pctNull) return null;

        // If the average length is trending too low, produce a larger string.  If it's trending
        // too high, produce a smaller string.  If it's within 10% of the requested average then
        // just take what we get.
        int length;
        if (colStats.maxColLen <= colStats.avgColLen + 1) {
          length = (int)colStats.avgColLen;
        } else if (numGenerated > 0 && totalLen / numGenerated > colStats.avgColLen * 1.1) {
          if (colStats.avgColLen < 2.0) length = 1;
          else length = rand.nextInt((int)colStats.avgColLen) + 1;
        } else if (numGenerated > 0 && totalLen / numGenerated < colStats.avgColLen * 0.9) {
          length = rand.nextInt((int)colStats.maxColLen - (int)colStats.avgColLen) +
              (int)colStats.avgColLen;
        } else {
          length = rand.nextInt((int)colStats.maxColLen) + 1;
        }

        totalLen += length;
        numGenerated++;
        byte[] bytes = new byte[length];
        rand.nextBytes(bytes);
        return bytes;
      }
    };
  }

  @Override
  protected ColGenerator getBoolGenerator(FieldSchema col) {
    final ColStats colStats = tableStats.colStats.get(col.getName());
    if (colStats == null) {
      LOG.warn("Unable to find column stats entry for " + col.getName() + ", using basic random " +
          "generation for that column.");
      return super.getBoolGenerator(col);
    }

    return new ColGenerator() {

      @Override
      public Object generate(double pctNull) {
        double d = rand.nextDouble();
        if (d < colStats.pctTrue) return true;
        else if (d < colStats.pctTrue + colStats.pctFalse) return false;
        else return null;
      }
    };
  }
}
