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

import org.junit.Assert;
import org.junit.Test;

public class TestDataSet {

  @Test
  public void fuzzyFloat() {
    DataSet.FloatColumn one = new DataSet.FloatColumn(0);
    one.set(new Float(1.0f));
    Assert.assertEquals(one, one);

    DataSet.FloatColumn two = new DataSet.FloatColumn(0);
    two.set(new Float(2.0f));
    Assert.assertNotEquals(one, two);

    DataSet.FloatColumn pointOne = new DataSet.FloatColumn(0);
    pointOne.set(new Float(0.1f));
    Assert.assertNotEquals(one, pointOne);

    DataSet.FloatColumn quintillion = new DataSet.FloatColumn(0);
    quintillion.set(new Float(1000000000000000000.0f));
    Assert.assertEquals(quintillion, quintillion);

    DataSet.FloatColumn quintillionOne = new DataSet.FloatColumn(0);
    quintillionOne.set(new Float(1000000000000000001.0f));
    Assert.assertEquals(quintillion, quintillionOne);

    DataSet.FloatColumn fiveQuintillion = new DataSet.FloatColumn(0);
    fiveQuintillion.set(new Float(5000000000000000000.0f));
    Assert.assertNotEquals(quintillion, fiveQuintillion);

    DataSet.FloatColumn verySmall = new DataSet.FloatColumn(0);
    verySmall.set(new Float(0.0000000000000001f));
    Assert.assertEquals(verySmall, verySmall);

    DataSet.FloatColumn justOverOne = new DataSet.FloatColumn(0);
    justOverOne.set(new Float(1.0000000000000001f));
    Assert.assertEquals(one, justOverOne);
  }

  @Test
  public void fuzzyDouble() {
    DataSet.DoubleColumn one = new DataSet.DoubleColumn(0);
    one.set(new Double(1.0));
    Assert.assertEquals(one, one);

    DataSet.DoubleColumn two = new DataSet.DoubleColumn(0);
    two.set(new Double(2.0));
    Assert.assertNotEquals(one, two);

    DataSet.DoubleColumn pointOne = new DataSet.DoubleColumn(0);
    pointOne.set(new Double(0.1));
    Assert.assertNotEquals(one, pointOne);

    DataSet.DoubleColumn quintillion = new DataSet.DoubleColumn(0);
    quintillion.set(new Double(1000000000000000000.0));
    Assert.assertEquals(quintillion, quintillion);

    DataSet.DoubleColumn quintillionOne = new DataSet.DoubleColumn(0);
    quintillionOne.set(new Double(1000000000000000001.0));
    Assert.assertEquals(quintillion, quintillionOne);

    DataSet.DoubleColumn fiveQuintillion = new DataSet.DoubleColumn(0);
    fiveQuintillion.set(new Double(5000000000000000000.0));
    Assert.assertNotEquals(quintillion, fiveQuintillion);

    DataSet.DoubleColumn verySmall = new DataSet.DoubleColumn(0);
    verySmall.set(new Double(0.0000000000000001));
    Assert.assertEquals(verySmall, verySmall);

    DataSet.DoubleColumn justOverOne = new DataSet.DoubleColumn(0);
    justOverOne.set(new Double(1.0000000000000001));
    Assert.assertEquals(one, justOverOne);
  }
}
