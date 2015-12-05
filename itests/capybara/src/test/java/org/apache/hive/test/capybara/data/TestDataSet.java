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
package org.apache.hive.test.capybara.data;

import org.junit.Assert;
import org.junit.Test;

public class TestDataSet {

  @Test
  public void fuzzyFloat() {
    FloatColumn one = new FloatColumn(0);
    one.set(new Float(1.0f));
    Assert.assertEquals(one, one);

    FloatColumn two = new FloatColumn(0);
    two.set(new Float(2.0f));
    Assert.assertNotEquals(one, two);

    FloatColumn pointOne = new FloatColumn(0);
    pointOne.set(new Float(0.1f));
    Assert.assertNotEquals(one, pointOne);

    FloatColumn quintillion = new FloatColumn(0);
    quintillion.set(new Float(1000000000000000000.0f));
    Assert.assertEquals(quintillion, quintillion);

    FloatColumn quintillionOne = new FloatColumn(0);
    quintillionOne.set(new Float(1000000000000000001.0f));
    Assert.assertEquals(quintillion, quintillionOne);

    FloatColumn fiveQuintillion = new FloatColumn(0);
    fiveQuintillion.set(new Float(5000000000000000000.0f));
    Assert.assertNotEquals(quintillion, fiveQuintillion);

    FloatColumn verySmall = new FloatColumn(0);
    verySmall.set(new Float(0.0000000000000001f));
    Assert.assertEquals(verySmall, verySmall);

    FloatColumn justOverOne = new FloatColumn(0);
    justOverOne.set(new Float(1.0000000000000001f));
    Assert.assertEquals(one, justOverOne);
  }

  @Test
  public void fuzzyDouble() {
    DoubleColumn one = new DoubleColumn(0);
    one.set(new Double(1.0));
    Assert.assertEquals(one, one);

    DoubleColumn two = new DoubleColumn(0);
    two.set(new Double(2.0));
    Assert.assertNotEquals(one, two);

    DoubleColumn pointOne = new DoubleColumn(0);
    pointOne.set(new Double(0.1));
    Assert.assertNotEquals(one, pointOne);

    DoubleColumn quintillion = new DoubleColumn(0);
    quintillion.set(new Double(1000000000000000000.0));
    Assert.assertEquals(quintillion, quintillion);

    DoubleColumn quintillionOne = new DoubleColumn(0);
    quintillionOne.set(new Double(1000000000000000001.0));
    Assert.assertEquals(quintillion, quintillionOne);

    DoubleColumn fiveQuintillion = new DoubleColumn(0);
    fiveQuintillion.set(new Double(5000000000000000000.0));
    Assert.assertNotEquals(quintillion, fiveQuintillion);

    DoubleColumn verySmall = new DoubleColumn(0);
    verySmall.set(new Double(0.0000000000000001));
    Assert.assertEquals(verySmall, verySmall);

    DoubleColumn justOverOne = new DoubleColumn(0);
    justOverOne.set(new Double(1.0000000000000001));
    Assert.assertEquals(one, justOverOne);
  }
}
