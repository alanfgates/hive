/*
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
package org.apache.hadoop.hive.ql.udf.generic;

import org.apache.hadoop.hive.common.ObjectPair;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BooleanObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaConstantStringObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject;
import static org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredJavaObject;
import static org.apache.hadoop.hive.ql.udf.generic.GenericUDFJsonValue.WhatToReturn;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestGenericUDFJsonValue {

  private List<String> json;

  @Before
  public void buildJson() {
    json = Arrays.asList(
        "{" +
            "\"name\" : \"chris\"," +
            "\"age\"  : 21," +
            "\"gpa\"  : 3.24," +
            "\"honors\" : false," +
            "\"sports\" : [ \"baseball\", \"soccer\" ]" +
        "}",
        "{" +
            "\"name\" : \"tracy\"," +
            "\"age\"  : 20," +
            "\"gpa\"  : 3.94," +
            "\"honors\" : true," +
            "\"sports\" : [ \"basketball\" ]" +
        "}",
        "{" +
            "\"name\" : null," +
            "\"age\"  : null," +
            "\"gpa\"  : null," +
            "\"honors\" : null," +
            "\"sports\" : null" +
        "}",
        "{" +
            "\"boollist\"  : [ true, false ]," +
            "\"longlist\"  : [ 3, 26 ]," +
            "\"doublelist\"  : [ 3.52, 2.86 ]," +
            "\"multilist\"  : [ \"string\", 1, 2.3, false, null ]," +
            "\"longstring\" : \"42\"," +
            "\"doublestring\" : \"3.1415\"," +
            "\"boolstring\" :\"true\"," +
            "\"subobj\"     : { \"str\" : \"strval\" }" +
        "}"
    );
  }

  @Test
  public void simpleString() throws HiveException {
    ObjectPair<ObjectInspector, Object[]> results = test(json, "$.name");
    Assert.assertEquals(4, results.getSecond().length);
    Assert.assertTrue(results.getFirst() instanceof StringObjectInspector);
    StringObjectInspector soi = (StringObjectInspector)results.getFirst();
    Assert.assertEquals("chris", soi.getPrimitiveJavaObject(results.getSecond()[0]));
    Assert.assertEquals("tracy", soi.getPrimitiveJavaObject(results.getSecond()[1]));
    Assert.assertNull(soi.getPrimitiveJavaObject(results.getSecond()[2]));
    Assert.assertNull(soi.getPrimitiveJavaObject(results.getSecond()[3]));
  }

  @Test
  public void simpleLong() throws HiveException {
    ObjectPair<ObjectInspector, Object[]> results = test(json, "$.age", wrapInList(1L));
    Assert.assertEquals(4, results.getSecond().length);
    Assert.assertTrue(results.getFirst() instanceof LongObjectInspector);
    LongObjectInspector loi = (LongObjectInspector)results.getFirst();
    Assert.assertEquals(21L, loi.getPrimitiveJavaObject(results.getSecond()[0]));
    Assert.assertEquals(20L, loi.getPrimitiveJavaObject(results.getSecond()[1]));
    Assert.assertNull(loi.getPrimitiveJavaObject(results.getSecond()[2]));
    Assert.assertNull(loi.getPrimitiveJavaObject(results.getSecond()[3]));
  }

  @Test
  public void simpleInt() throws HiveException {
    ObjectPair<ObjectInspector, Object[]> results = test(json, "$.age", wrapInList(1));
    Assert.assertEquals(4, results.getSecond().length);
    Assert.assertTrue(results.getFirst() instanceof IntObjectInspector);
    IntObjectInspector ioi = (IntObjectInspector)results.getFirst();
    Assert.assertEquals(21, ioi.getPrimitiveJavaObject(results.getSecond()[0]));
    Assert.assertEquals(20, ioi.getPrimitiveJavaObject(results.getSecond()[1]));
    Assert.assertNull(ioi.getPrimitiveJavaObject(results.getSecond()[2]));
    Assert.assertNull(ioi.getPrimitiveJavaObject(results.getSecond()[3]));
  }

  @Test
  public void simpleDouble() throws HiveException {
    ObjectPair<ObjectInspector, Object[]> results = test(json, "$.gpa", wrapInList(1.0));
    Assert.assertEquals(4, results.getSecond().length);
    Assert.assertTrue(results.getFirst() instanceof DoubleObjectInspector);
    DoubleObjectInspector doi = (DoubleObjectInspector)results.getFirst();
    Assert.assertEquals(3.24, doi.getPrimitiveJavaObject(results.getSecond()[0]));
    Assert.assertEquals(3.94, doi.getPrimitiveJavaObject(results.getSecond()[1]));
    Assert.assertNull(doi.getPrimitiveJavaObject(results.getSecond()[2]));
    Assert.assertNull(doi.getPrimitiveJavaObject(results.getSecond()[3]));
  }

  @Test
  public void simpleBoolean() throws HiveException {
    ObjectPair<ObjectInspector, Object[]> results = test(json, "$.honors", wrapInList(true));
    Assert.assertEquals(4, results.getSecond().length);
    Assert.assertTrue(results.getFirst() instanceof BooleanObjectInspector);
    BooleanObjectInspector boi = (BooleanObjectInspector)results.getFirst();
    Assert.assertEquals(false, boi.getPrimitiveJavaObject(results.getSecond()[0]));
    Assert.assertEquals(true, boi.getPrimitiveJavaObject(results.getSecond()[1]));
    Assert.assertNull(boi.getPrimitiveJavaObject(results.getSecond()[2]));
    Assert.assertNull(boi.getPrimitiveJavaObject(results.getSecond()[3]));
  }

  @Test
  public void simpleListStr() throws HiveException {
    ObjectPair<ObjectInspector, Object[]> results = test(json, "$.sports",
        wrapInList(Collections.singletonList("a")));
    Assert.assertEquals(4, results.getSecond().length);
    Assert.assertTrue(results.getFirst() instanceof ListObjectInspector);
    ListObjectInspector loi = (ListObjectInspector)results.getFirst();
    Assert.assertTrue(loi.getListElementObjectInspector() instanceof StringObjectInspector);
    Assert.assertEquals(2, loi.getListLength(results.getSecond()[0]));
    StringObjectInspector soi = (StringObjectInspector)loi.getListElementObjectInspector();
    Assert.assertEquals("baseball", soi.getPrimitiveJavaObject(loi.getListElement(results.getSecond()[0], 0)));
    Assert.assertEquals("soccer", soi.getPrimitiveJavaObject(loi.getListElement(results.getSecond()[0], 1)));
    Assert.assertEquals(1, loi.getListLength(results.getSecond()[1]));
    Assert.assertEquals("basketball", soi.getPrimitiveJavaObject(loi.getListElement(results.getSecond()[1], 0)));
    Assert.assertNull(loi.getList(results.getSecond()[2]));
    Assert.assertNull(loi.getList(results.getSecond()[3]));
  }

  @Test
  public void simpleListLong() throws HiveException {
    ObjectPair<ObjectInspector, Object[]> results = test(json, "$.longlist",
        wrapInList(Collections.singletonList(1L)));
    Assert.assertEquals(4, results.getSecond().length);
    Assert.assertTrue(results.getFirst() instanceof ListObjectInspector);
    ListObjectInspector loi = (ListObjectInspector)results.getFirst();
    Assert.assertEquals(2, loi.getListLength(results.getSecond()[3]));
    Assert.assertTrue(loi.getListElementObjectInspector() instanceof LongObjectInspector);
    LongObjectInspector lloi = (LongObjectInspector)loi.getListElementObjectInspector();
    Assert.assertEquals(3L, lloi.getPrimitiveJavaObject(loi.getListElement(results.getSecond()[3], 0)));
    Assert.assertEquals(26L, lloi.getPrimitiveJavaObject(loi.getListElement(results.getSecond()[3], 1)));
  }

  @Test
  public void simpleListDouble() throws HiveException {
    ObjectPair<ObjectInspector, Object[]> results = test(json, "$.doublelist",
        wrapInList(Collections.singletonList(1.0)));
    Assert.assertEquals(4, results.getSecond().length);
    Assert.assertTrue(results.getFirst() instanceof ListObjectInspector);
    ListObjectInspector loi = (ListObjectInspector)results.getFirst();
    Assert.assertTrue(loi.getListElementObjectInspector() instanceof DoubleObjectInspector);
    DoubleObjectInspector doi = (DoubleObjectInspector)loi.getListElementObjectInspector();
    Assert.assertEquals(2, loi.getListLength(results.getSecond()[3]));
    Assert.assertEquals(3.52, doi.getPrimitiveJavaObject(loi.getListElement(results.getSecond()[3], 0)));
    Assert.assertEquals(2.86, doi.getPrimitiveJavaObject(loi.getListElement(results.getSecond()[3], 1)));
  }

  @Test
  public void simpleListBool() throws HiveException {
    ObjectPair<ObjectInspector, Object[]> results = test(json, "$.boollist",
        wrapInList(Collections.singletonList(true)));
    Assert.assertEquals(4, results.getSecond().length);
    Assert.assertTrue(results.getFirst() instanceof ListObjectInspector);
    ListObjectInspector loi = (ListObjectInspector)results.getFirst();
    Assert.assertTrue(loi.getListElementObjectInspector() instanceof BooleanObjectInspector);
    BooleanObjectInspector boi = (BooleanObjectInspector)loi.getListElementObjectInspector();
    Assert.assertEquals(2, loi.getListLength(results.getSecond()[3]));
    Assert.assertEquals(true, boi.getPrimitiveJavaObject(loi.getListElement(results.getSecond()[3], 0)));
    Assert.assertEquals(false, boi.getPrimitiveJavaObject(loi.getListElement(results.getSecond()[3], 1)));
  }

  @Test
  public void multitypeList() throws HiveException {
    ObjectPair<ObjectInspector, Object[]> results = test(json, "$.multilist",
        wrapInList(Collections.singletonList("a")));
    Assert.assertEquals(4, results.getSecond().length);
    Assert.assertTrue(results.getFirst() instanceof ListObjectInspector);
    ListObjectInspector loi = (ListObjectInspector)results.getFirst();
    Assert.assertEquals(5, loi.getListLength(results.getSecond()[3]));
    Assert.assertTrue(loi.getListElementObjectInspector() instanceof StringObjectInspector);
    StringObjectInspector soi = (StringObjectInspector)loi.getListElementObjectInspector();
    Assert.assertEquals("string", soi.getPrimitiveJavaObject(loi.getListElement(results.getSecond()[3], 0)));
    Assert.assertEquals("1", soi.getPrimitiveJavaObject(loi.getListElement(results.getSecond()[3], 1)));
    Assert.assertEquals("2.3", soi.getPrimitiveJavaObject(loi.getListElement(results.getSecond()[3], 2)));
    Assert.assertEquals("FALSE", soi.getPrimitiveJavaObject(loi.getListElement(results.getSecond()[3], 3)));

    results = test(json, "$.multilist", wrapInList(Collections.singletonList(1L)));
    loi = (ListObjectInspector)results.getFirst();
    Assert.assertTrue(loi.getListElementObjectInspector() instanceof LongObjectInspector);
    LongObjectInspector lloi = (LongObjectInspector)loi.getListElementObjectInspector();
    Assert.assertNull(loi.getListElement(results.getSecond()[3], 0));
    Assert.assertEquals(1L, lloi.getPrimitiveJavaObject(loi.getListElement(results.getSecond()[3], 1)));
    Assert.assertEquals(2L, lloi.getPrimitiveJavaObject(loi.getListElement(results.getSecond()[3], 2)));
    Assert.assertEquals(0L, lloi.getPrimitiveJavaObject(loi.getListElement(results.getSecond()[3], 3)));

    results = test(json, "$.multilist", wrapInList(Collections.singletonList(1.0)));
    loi = (ListObjectInspector)results.getFirst();
    Assert.assertNull(loi.getListElement(results.getSecond()[3], 0));
    Assert.assertTrue(loi.getListElementObjectInspector() instanceof DoubleObjectInspector);
    DoubleObjectInspector doi = (DoubleObjectInspector)loi.getListElementObjectInspector();
    Assert.assertEquals(1.0, doi.getPrimitiveJavaObject(loi.getListElement(results.getSecond()[3], 1)));
    Assert.assertEquals(2.3, doi.getPrimitiveJavaObject(loi.getListElement(results.getSecond()[3], 2)));
    Assert.assertEquals(0.0, doi.getPrimitiveJavaObject(loi.getListElement(results.getSecond()[3], 3)));

    results = test(json, "$.multilist", wrapInList(Collections.singletonList(true)));
    loi = (ListObjectInspector)results.getFirst();
    Assert.assertTrue(loi.getListElementObjectInspector() instanceof BooleanObjectInspector);
    BooleanObjectInspector boi = (BooleanObjectInspector)loi.getListElementObjectInspector();
    Assert.assertEquals(true, boi.getPrimitiveJavaObject(loi.getListElement(results.getSecond()[3], 0)));
    Assert.assertEquals(true, boi.getPrimitiveJavaObject(loi.getListElement(results.getSecond()[3], 1)));
    Assert.assertEquals(true, boi.getPrimitiveJavaObject(loi.getListElement(results.getSecond()[3], 2)));
    Assert.assertEquals(false, boi.getPrimitiveJavaObject(loi.getListElement(results.getSecond()[3], 3)));

  }

  @Test
  public void longAsString() throws HiveException {
    ObjectPair<ObjectInspector, Object[]> results = test(json, "$.age");
    Assert.assertEquals(4, results.getSecond().length);
    PrimitiveObjectInspector poi = (PrimitiveObjectInspector)results.getFirst();
    Assert.assertEquals("21", poi.getPrimitiveJavaObject(results.getSecond()[0]));
    Assert.assertEquals("20", poi.getPrimitiveJavaObject(results.getSecond()[1]));
    Assert.assertNull(poi.getPrimitiveJavaObject(results.getSecond()[2]));
    Assert.assertNull(poi.getPrimitiveJavaObject(results.getSecond()[3]));
  }

  @Test
  public void doubleAsString() throws HiveException {
    ObjectPair<ObjectInspector, Object[]> results = test(json, "$.gpa");
    Assert.assertEquals(4, results.getSecond().length);
    PrimitiveObjectInspector poi = (PrimitiveObjectInspector)results.getFirst();
    Assert.assertEquals("3.24", poi.getPrimitiveJavaObject(results.getSecond()[0]));
    Assert.assertEquals("3.94", poi.getPrimitiveJavaObject(results.getSecond()[1]));
    Assert.assertNull(poi.getPrimitiveJavaObject(results.getSecond()[2]));
    Assert.assertNull(poi.getPrimitiveJavaObject(results.getSecond()[3]));
  }

  @Test
  public void boolAsString() throws HiveException {
    ObjectPair<ObjectInspector, Object[]> results = test(json, "$.honors");
    Assert.assertEquals(4, results.getSecond().length);
    PrimitiveObjectInspector poi = (PrimitiveObjectInspector)results.getFirst();
    Assert.assertEquals("FALSE", poi.getPrimitiveJavaObject(results.getSecond()[0]));
    Assert.assertEquals("TRUE", poi.getPrimitiveJavaObject(results.getSecond()[1]));
    Assert.assertNull(poi.getPrimitiveJavaObject(results.getSecond()[2]));
    Assert.assertNull(poi.getPrimitiveJavaObject(results.getSecond()[3]));
  }

  @Test
  public void stringAsLong() throws HiveException {
    ObjectPair<ObjectInspector, Object[]> results = test(json, "$.name", wrapInList(1L));
    Assert.assertEquals(4, results.getSecond().length);
    PrimitiveObjectInspector poi = (PrimitiveObjectInspector) results.getFirst();
    Assert.assertNull(poi.getPrimitiveJavaObject(results.getSecond()[0]));
    results = test(json, "$.longstring", wrapInList(1L));
    Assert.assertEquals(4, results.getSecond().length);
    poi = (PrimitiveObjectInspector) results.getFirst();
    Assert.assertEquals(42L, poi.getPrimitiveJavaObject(results.getSecond()[3]));
  }

  @Test
  public void doubleAsLong() throws HiveException {
    ObjectPair<ObjectInspector, Object[]> results = test(json, "$.gpa", wrapInList(1L));
    Assert.assertEquals(4, results.getSecond().length);
    PrimitiveObjectInspector poi = (PrimitiveObjectInspector) results.getFirst();
    Assert.assertEquals(3L, poi.getPrimitiveJavaObject(results.getSecond()[0]));
    Assert.assertEquals(3L, poi.getPrimitiveJavaObject(results.getSecond()[1]));
    Assert.assertNull(poi.getPrimitiveJavaObject(results.getSecond()[2]));
    Assert.assertNull(poi.getPrimitiveJavaObject(results.getSecond()[3]));
  }

  @Test
  public void booleanAsLong() throws HiveException {
    ObjectPair<ObjectInspector, Object[]> results = test(json, "$.honors", wrapInList(1L));
    Assert.assertEquals(4, results.getSecond().length);
    PrimitiveObjectInspector poi = (PrimitiveObjectInspector) results.getFirst();
    // Hive allows booleans to be cast to longs.  This is sick and wrong, but ok
    Assert.assertEquals(0L, poi.getPrimitiveJavaObject(results.getSecond()[0]));
    Assert.assertEquals(1L, poi.getPrimitiveJavaObject(results.getSecond()[1]));
    Assert.assertNull(poi.getPrimitiveJavaObject(results.getSecond()[2]));
    Assert.assertNull(poi.getPrimitiveJavaObject(results.getSecond()[3]));
  }

  @Test
  public void stringAsDouble() throws HiveException {
    ObjectPair<ObjectInspector, Object[]> results = test(json, "$.name", wrapInList(1.0));
    Assert.assertEquals(4, results.getSecond().length);
    PrimitiveObjectInspector poi = (PrimitiveObjectInspector) results.getFirst();
    Assert.assertNull(poi.getPrimitiveJavaObject(results.getSecond()[0]));
    results = test(json, "$.doublestring", wrapInList(1.0));
    Assert.assertEquals(4, results.getSecond().length);
    poi = (PrimitiveObjectInspector) results.getFirst();
    Assert.assertEquals(3.1415, poi.getPrimitiveJavaObject(results.getSecond()[3]));
  }

  @Test
  public void longAsDouble() throws HiveException {
    ObjectPair<ObjectInspector, Object[]> results = test(json, "$.age", wrapInList(1.0));
    Assert.assertEquals(4, results.getSecond().length);
    PrimitiveObjectInspector poi = (PrimitiveObjectInspector) results.getFirst();
    Assert.assertEquals(21.0, poi.getPrimitiveJavaObject(results.getSecond()[0]));
    Assert.assertEquals(20.0, poi.getPrimitiveJavaObject(results.getSecond()[1]));
    Assert.assertNull(poi.getPrimitiveJavaObject(results.getSecond()[2]));
    Assert.assertNull(poi.getPrimitiveJavaObject(results.getSecond()[3]));
  }

  @Test
  public void booleanAsDouble() throws HiveException {
    ObjectPair<ObjectInspector, Object[]> results = test(json, "$.honors", wrapInList(1.0));
    Assert.assertEquals(4, results.getSecond().length);
    PrimitiveObjectInspector poi = (PrimitiveObjectInspector) results.getFirst();
    // Hive allows booleans to be cast to doubles.  This is sick and wrong, but ok
    Assert.assertEquals(0.0, poi.getPrimitiveJavaObject(results.getSecond()[0]));
    Assert.assertEquals(1.0, poi.getPrimitiveJavaObject(results.getSecond()[1]));
    Assert.assertNull(poi.getPrimitiveJavaObject(results.getSecond()[2]));
    Assert.assertNull(poi.getPrimitiveJavaObject(results.getSecond()[3]));
  }

  @Test
  public void stringAsBoolean() throws HiveException {
    ObjectPair<ObjectInspector, Object[]> results = test(json, "$.name", wrapInList(true));
    Assert.assertEquals(4, results.getSecond().length);
    PrimitiveObjectInspector poi = (PrimitiveObjectInspector) results.getFirst();
    Assert.assertEquals(true, poi.getPrimitiveJavaObject(results.getSecond()[0]));
    Assert.assertEquals(true, poi.getPrimitiveJavaObject(results.getSecond()[1]));
    Assert.assertNull(poi.getPrimitiveJavaObject(results.getSecond()[2]));
    Assert.assertNull(poi.getPrimitiveJavaObject(results.getSecond()[3]));
    results = test(json, "$.boolstring", wrapInList(true));
    Assert.assertEquals(4, results.getSecond().length);
    poi = (PrimitiveObjectInspector) results.getFirst();
    Assert.assertEquals(true, poi.getPrimitiveJavaObject(results.getSecond()[3]));
  }

  @Test
  public void longAsBoolean() throws HiveException {
    ObjectPair<ObjectInspector, Object[]> results = test(json, "$.age", wrapInList(true));
    Assert.assertEquals(4, results.getSecond().length);
    PrimitiveObjectInspector poi = (PrimitiveObjectInspector) results.getFirst();
    Assert.assertEquals(true, poi.getPrimitiveJavaObject(results.getSecond()[0]));
    Assert.assertEquals(true, poi.getPrimitiveJavaObject(results.getSecond()[1]));
    Assert.assertNull(poi.getPrimitiveJavaObject(results.getSecond()[2]));
    Assert.assertNull(poi.getPrimitiveJavaObject(results.getSecond()[3]));
  }

  @Test
  public void doubleAsBoolean() throws HiveException {
    ObjectPair<ObjectInspector, Object[]> results = test(json, "$.gpa", wrapInList(true));
    Assert.assertEquals(4, results.getSecond().length);
    PrimitiveObjectInspector poi = (PrimitiveObjectInspector) results.getFirst();
    Assert.assertEquals(true, poi.getPrimitiveJavaObject(results.getSecond()[0]));
    Assert.assertEquals(true, poi.getPrimitiveJavaObject(results.getSecond()[1]));
    Assert.assertNull(poi.getPrimitiveJavaObject(results.getSecond()[2]));
    Assert.assertNull(poi.getPrimitiveJavaObject(results.getSecond()[3]));
  }

  @Test
  public void unsupportedReturnType() throws HiveException {
    try {
      test(json, "$.gpa", wrapInList(3.0f));
    } catch (UDFArgumentTypeException e) {
      Assert.assertEquals("jsonvalue can return string, int, long, double, boolean, array of one of these, or struct with these", e.getMessage());
    }
  }

  @Test
  public void stringStaticDefaultOnEmpty() throws HiveException {
    ObjectPair<ObjectInspector, Object[]> results = test(json, "$.name",
        wrapInList("fred"), WhatToReturn.DEFAULT);
    Assert.assertEquals(4, results.getSecond().length);
    Assert.assertTrue(results.getFirst() instanceof StringObjectInspector);
    StringObjectInspector soi = (StringObjectInspector)results.getFirst();
    Assert.assertEquals("chris", soi.getPrimitiveJavaObject(results.getSecond()[0]));
    Assert.assertEquals("tracy", soi.getPrimitiveJavaObject(results.getSecond()[1]));
    Assert.assertNull(soi.getPrimitiveJavaObject(results.getSecond()[2]));
    Assert.assertEquals("fred", soi.getPrimitiveJavaObject(results.getSecond()[3]));
  }

  @Test
  public void longStaticDefaultOnEmpty() throws HiveException {
    ObjectPair<ObjectInspector, Object[]> results = test(json, "$.age",
        wrapInList(87L), WhatToReturn.DEFAULT);
    Assert.assertEquals(4, results.getSecond().length);
    Assert.assertTrue(results.getFirst() instanceof LongObjectInspector);
    LongObjectInspector loi = (LongObjectInspector)results.getFirst();
    Assert.assertEquals(21L, loi.getPrimitiveJavaObject(results.getSecond()[0]));
    Assert.assertEquals(20L, loi.getPrimitiveJavaObject(results.getSecond()[1]));
    Assert.assertNull(loi.getPrimitiveJavaObject(results.getSecond()[2]));
    Assert.assertEquals(87L, loi.getPrimitiveJavaObject(results.getSecond()[3]));
  }

  @Test
  public void intStaticDefaultOnEmpty() throws HiveException {
    ObjectPair<ObjectInspector, Object[]> results = test(json, "$.age",
        wrapInList(93), WhatToReturn.DEFAULT);
    Assert.assertEquals(4, results.getSecond().length);
    Assert.assertTrue(results.getFirst() instanceof IntObjectInspector);
    IntObjectInspector ioi = (IntObjectInspector)results.getFirst();
    Assert.assertEquals(21, ioi.getPrimitiveJavaObject(results.getSecond()[0]));
    Assert.assertEquals(20, ioi.getPrimitiveJavaObject(results.getSecond()[1]));
    Assert.assertNull(ioi.getPrimitiveJavaObject(results.getSecond()[2]));
    Assert.assertEquals(93, ioi.getPrimitiveJavaObject(results.getSecond()[3]));
  }

  @Test
  public void doubleStaticDefaultOnEmpty() throws HiveException {
    ObjectPair<ObjectInspector, Object[]> results = test(json, "$.gpa",
        wrapInList(2.5), WhatToReturn.DEFAULT);
    Assert.assertEquals(4, results.getSecond().length);
    Assert.assertTrue(results.getFirst() instanceof DoubleObjectInspector);
    DoubleObjectInspector doi = (DoubleObjectInspector)results.getFirst();
    Assert.assertEquals(3.24, doi.getPrimitiveJavaObject(results.getSecond()[0]));
    Assert.assertEquals(3.94, doi.getPrimitiveJavaObject(results.getSecond()[1]));
    Assert.assertNull(doi.getPrimitiveJavaObject(results.getSecond()[2]));
    Assert.assertEquals(2.5, doi.getPrimitiveJavaObject(results.getSecond()[3]));
  }

  @Test
  public void boolStaticDefaultOnEmpty() throws HiveException {
    ObjectPair<ObjectInspector, Object[]> results = test(json, "$.honors",
        wrapInList(true), WhatToReturn.DEFAULT);
    Assert.assertEquals(4, results.getSecond().length);
    Assert.assertTrue(results.getFirst() instanceof BooleanObjectInspector);
    BooleanObjectInspector boi = (BooleanObjectInspector)results.getFirst();
    Assert.assertEquals(false, boi.getPrimitiveJavaObject(results.getSecond()[0]));
    Assert.assertEquals(true, boi.getPrimitiveJavaObject(results.getSecond()[1]));
    Assert.assertNull(boi.getPrimitiveJavaObject(results.getSecond()[2]));
    Assert.assertEquals(true, boi.getPrimitiveJavaObject(results.getSecond()[3]));
  }

  @Test
  public void stringDynamicDefaultOnEmpty() throws HiveException {
    ObjectPair<ObjectInspector, Object[]> results = test(json, "$.name",
        Arrays.asList("one", "two", "three", "four"), WhatToReturn.DEFAULT);
    Assert.assertEquals(4, results.getSecond().length);
    Assert.assertTrue(results.getFirst() instanceof StringObjectInspector);
    StringObjectInspector soi = (StringObjectInspector)results.getFirst();
    Assert.assertEquals("chris", soi.getPrimitiveJavaObject(results.getSecond()[0]));
    Assert.assertEquals("tracy", soi.getPrimitiveJavaObject(results.getSecond()[1]));
    Assert.assertNull(soi.getPrimitiveJavaObject(results.getSecond()[2]));
    Assert.assertEquals("four", soi.getPrimitiveJavaObject(results.getSecond()[3]));
  }

  @Test
  public void longDynamicDefaultOnEmpty() throws HiveException {
    ObjectPair<ObjectInspector, Object[]> results = test(json, "$.age",
        Arrays.asList(1L, 2L, 3L, 4L), WhatToReturn.DEFAULT);
    Assert.assertEquals(4, results.getSecond().length);
    Assert.assertTrue(results.getFirst() instanceof LongObjectInspector);
    LongObjectInspector loi = (LongObjectInspector)results.getFirst();
    Assert.assertEquals(21L, loi.getPrimitiveJavaObject(results.getSecond()[0]));
    Assert.assertEquals(20L, loi.getPrimitiveJavaObject(results.getSecond()[1]));
    Assert.assertNull(loi.getPrimitiveJavaObject(results.getSecond()[2]));
    Assert.assertEquals(4L, loi.getPrimitiveJavaObject(results.getSecond()[3]));
  }

  @Test
  public void doubleDynamicDefaultOnEmpty() throws HiveException {
    ObjectPair<ObjectInspector, Object[]> results = test(json, "$.gpa",
        Arrays.asList(1.0, 2.0, 3.0, 4.0), WhatToReturn.DEFAULT);
    Assert.assertEquals(4, results.getSecond().length);
    Assert.assertTrue(results.getFirst() instanceof DoubleObjectInspector);
    DoubleObjectInspector doi = (DoubleObjectInspector)results.getFirst();
    Assert.assertEquals(3.24, doi.getPrimitiveJavaObject(results.getSecond()[0]));
    Assert.assertEquals(3.94, doi.getPrimitiveJavaObject(results.getSecond()[1]));
    Assert.assertNull(doi.getPrimitiveJavaObject(results.getSecond()[2]));
    Assert.assertEquals(4.0, doi.getPrimitiveJavaObject(results.getSecond()[3]));
  }

  @Test
  public void booleanDynamicDefaultOnEmpty() throws HiveException {
    ObjectPair<ObjectInspector, Object[]> results = test(json, "$.honors",
        Arrays.asList(false, false, false, true), WhatToReturn.DEFAULT);
    Assert.assertEquals(4, results.getSecond().length);
    Assert.assertTrue(results.getFirst() instanceof BooleanObjectInspector);
    BooleanObjectInspector boi = (BooleanObjectInspector)results.getFirst();
    Assert.assertEquals(false, boi.getPrimitiveJavaObject(results.getSecond()[0]));
    Assert.assertEquals(true, boi.getPrimitiveJavaObject(results.getSecond()[1]));
    Assert.assertNull(boi.getPrimitiveJavaObject(results.getSecond()[2]));
    Assert.assertEquals(true, boi.getPrimitiveJavaObject(results.getSecond()[3]));
  }

  @Test
  public void defaultOnError() throws HiveException {
    ObjectPair<ObjectInspector, Object[]> results = test(json, "$.subobj?(@.str == 5)",
        wrapInList("fred"), WhatToReturn.NULL, WhatToReturn.DEFAULT);
    Assert.assertEquals(4, results.getSecond().length);
    StringObjectInspector soi = (StringObjectInspector)results.getFirst();
    // The error will only occur on the final entry because it is the only one with 'subobj' key.  The filter
    // won't be evaluated on all the other records.
    Assert.assertEquals("fred", soi.getPrimitiveJavaObject(results.getSecond()[3]));
  }

  @Test
  public void nullOnError() throws HiveException {
    ObjectPair<ObjectInspector, Object[]> results = test(json, "$.subobj?(@.str == 5)");
    Assert.assertEquals(4, results.getSecond().length);
    StringObjectInspector soi = (StringObjectInspector)results.getFirst();
    // The error will only occur on the final entry because it is the only one with 'subobj' key.  The filter
    // won't be evaluated on all the other records.
    Assert.assertNull(soi.getPrimitiveJavaObject(results.getSecond()[3]));
  }

  @Test
  public void badSyntax() throws HiveException {
    try {
      test(json, "$.nosuchfunc()");
      Assert.fail();
    } catch (UDFArgumentException e) {
      Assert.assertTrue(e.getMessage().startsWith("Failed to parse JSON path exception: '$.nosuchfunc()'"));
    }
  }

  @Test
  public void errorOnEmpty() {
    try {
      test(json, "$.nosuch", wrapInList("a"), WhatToReturn.ERROR);
      Assert.fail();
    } catch (HiveException e) {
      Assert.assertTrue(e.getMessage().contains("Result of path expression is empty"));
    }
  }

  @Test
  public void errorOnError() {
    try {
      test(json, "$.subobj?(@.str == 5)", wrapInList("a"), WhatToReturn.NULL, WhatToReturn.ERROR);
      Assert.fail();
    } catch (HiveException e) {
      Assert.assertTrue(e.getMessage().contains("produced a semantic error: Cannot compare a string to a long at \"@.str == 5\""));
    }
  }

  @Test
  public void passing() throws HiveException {
    List<Map<String, Object>> passingVals = Arrays.asList(
        Collections.singletonMap("index", 1),
        Collections.singletonMap("index", 0),
        Collections.singletonMap("index", 0),
        Collections.singletonMap("index", 2)
    );

    ObjectPair<ObjectInspector, Object[]> results = test(json, "$.sports[$index]", wrapInList("a"),
        WhatToReturn.NULL, WhatToReturn.NULL, passingVals);
    Assert.assertEquals(4, results.getSecond().length);
    StringObjectInspector soi = (StringObjectInspector)results.getFirst();
    Assert.assertEquals("soccer", soi.getPrimitiveJavaObject(results.getSecond()[0]));
    Assert.assertEquals("basketball", soi.getPrimitiveJavaObject(results.getSecond()[1]));
    Assert.assertNull(soi.getPrimitiveJavaObject(results.getSecond()[2]));
    Assert.assertNull(soi.getPrimitiveJavaObject(results.getSecond()[3]));
  }

  private ObjectPair<ObjectInspector, Object[]> test(List<String> jsonValues, String pathExpr) throws HiveException {
    return test(jsonValues, pathExpr, null);
  }

  private ObjectPair<ObjectInspector, Object[]> test(List<String> jsonValues, String pathExpr, List<Object> defaultVals)
      throws HiveException {
    return test(jsonValues, pathExpr, defaultVals, null);
  }

  private ObjectPair<ObjectInspector, Object[]> test(List<String> jsonValues, String pathExpr, List<Object> defaultVals,
                                                     WhatToReturn onEmpty) throws HiveException {
    return test(jsonValues, pathExpr, defaultVals, onEmpty, null);
  }

  private ObjectPair<ObjectInspector, Object[]> test(List<String> jsonValues, String pathExpr, List<Object> defaultVals,
                                                     WhatToReturn onEmpty, WhatToReturn onError) throws HiveException {
    return test(jsonValues, pathExpr, defaultVals, onEmpty, onError, null);
  }

  /**
   * Run a test.
   * @param jsonValues list of values to pass to json_value()
   * @param pathExpr path expression
   * @param defaultVals list of default values.  Can be null if there are no default values.  Can contain a single
   *                    element if you want the default to be constant.  Otherwise it should have the same number of
   *                    values as jsonValues.
   * @param onEmpty action on empty, can be null
   * @param onError action on error, can be null
   * @param passing list of maps to pass in as passing values, can be null
   * @return object inspector to read results with and array of results
   * @throws HiveException if thrown by json_value
   */
  private ObjectPair<ObjectInspector, Object[]> test(List<String> jsonValues, String pathExpr, List<Object> defaultVals,
                                                     WhatToReturn onEmpty, WhatToReturn onError,
                                                     List<Map<String, Object>> passing) throws HiveException {
    GenericUDFJsonValue udf = new GenericUDFJsonValue();
    // Figure out our default values, if there are any.  Note that this also fills out the contents of defaultVal if
    // one 1 constant value was passed there
    ObjectInspector returnOI = objectInspectorFromDefaultVals(defaultVals, jsonValues.size(), defaultVals == null || defaultVals.size() == 1);
    // Figure out our passing object inspectors, if we need them
    Map<String, ObjectInspector> passingOIs = null;
    if (passing != null) {
      passingOIs = new HashMap<>(passing.get(0).size());
      Map<String, List<Object>> inverted = new HashMap<>();
      for (Map<String, Object> p : passing) {
        for (Map.Entry<String, Object> e : p.entrySet()) {
          List<Object> list = inverted.computeIfAbsent(e.getKey(), s -> new ArrayList<>());
          list.add(e.getValue());
        }
      }
      for (Map.Entry<String, List<Object>> inv : inverted.entrySet()) {
        passingOIs.put(inv.getKey(), objectInspectorFromDefaultVals(inv.getValue(), inv.getValue().size(), false));
      }
    }
    ObjectInspector resultObjectInspector = udf.initialize(buildInitArgs(pathExpr, returnOI, onEmpty, onError, passingOIs));
    Object[] results = new Object[jsonValues.size()];
    for (int i = 0; i < jsonValues.size(); i++) {
      Object o = udf.evaluate(buildExecArgs(jsonValues.get(i), pathExpr,
          defaultVals == null ? null : defaultVals.get(i), onEmpty, onError, passing == null ? null : passing.get(i)));
      results[i] = ObjectInspectorUtils.copyToStandardObject(o, resultObjectInspector);
    }
    return new ObjectPair<>(resultObjectInspector, results);
  }

  private ObjectInspector[] buildInitArgs(String pathExpr, ObjectInspector returnOI, WhatToReturn onEmpty,
                                          WhatToReturn onError, Map<String, ObjectInspector> passing) {
    List<ObjectInspector> initArgs = new ArrayList<>();
    initArgs.add(PrimitiveObjectInspectorFactory.writableStringObjectInspector);
    initArgs.add(new JavaConstantStringObjectInspector(pathExpr));
    // Nesting here is important because we want it to come out with the right number of args
    if (returnOI != null) {
      initArgs.add(returnOI);
      if (onEmpty != null) {
        initArgs.add(new JavaConstantStringObjectInspector(onEmpty.name()));
        if (onError != null) {
          initArgs.add(new JavaConstantStringObjectInspector(onError.name()));
          if (passing != null) {
            for (Map.Entry<String, ObjectInspector> entry : passing.entrySet()) {
              initArgs.add(new JavaConstantStringObjectInspector(entry.getKey()));
              initArgs.add(entry.getValue());
            }
          }
        }
      }
    }
    return initArgs.toArray(new ObjectInspector[0]);
  }


  private DeferredObject[] buildExecArgs(String jsonValue, String pathExpr, Object defaultVal, WhatToReturn onEmpty,
                                         WhatToReturn onError, Map<String, Object> passing) {
    List<DeferredObject> execArgs = new ArrayList<>();
    execArgs.add(wrapInDeferred(jsonValue));
    execArgs.add(wrapInDeferred(pathExpr));
    if (defaultVal != null) {
      execArgs.add(wrapInDeferred(defaultVal));
      if (onEmpty != null) {
        execArgs.add(wrapInDeferred(onEmpty.name()));
        if (onError != null) {
          execArgs.add(wrapInDeferred(onError.name()));
          if (passing != null) {
            for (Map.Entry<String, Object> entry : passing.entrySet()) {
              execArgs.add(wrapInDeferred(entry.getKey()));
              execArgs.add(wrapInDeferred(entry.getValue()));
            }
          }
        }
      }
    }
    return execArgs.toArray(new DeferredObject[0]);
  }

  private DeferredObject wrapInDeferred(Object obj) {
    if (obj == null) return null;
    else if (obj instanceof List) return new DeferredJavaObject(obj);
    else if (obj instanceof String) return new DeferredJavaObject(new Text((String)obj));
    else if (obj instanceof Long) return new DeferredJavaObject(new LongWritable((Long)obj));
    else if (obj instanceof Integer) return new DeferredJavaObject(new IntWritable((Integer) obj));
    else if (obj instanceof Double) return new DeferredJavaObject(new DoubleWritable((Double)obj));
    else if (obj instanceof Float) return new DeferredJavaObject(new FloatWritable((Float)obj));
    else if (obj instanceof Boolean) return new DeferredJavaObject(new BooleanWritable((Boolean)obj));
    else throw new RuntimeException("what?");
  }

  private ObjectInspector objectInspectorFromDefaultVals(List<Object> defaultVals, int numRecords, boolean isConstant) {
    if (defaultVals == null || defaultVals.isEmpty()) return PrimitiveObjectInspectorFactory.writableStringObjectInspector;

    Object first = defaultVals.get(0);
    if (isConstant) {
      while (defaultVals.size() < numRecords) defaultVals.add(defaultVals.get(0));
    }
    return objToObjectInspector(first, isConstant);
  }

  private ObjectInspector objToObjectInspector(Object obj, boolean isConstant) {
    if (obj instanceof List) {
      List list = (List)obj;
      if (isConstant) {
        return ObjectInspectorFactory.getStandardConstantListObjectInspector(objToObjectInspector(
            list.get(0), isConstant), list);
      } else {
        return ObjectInspectorFactory.getStandardListObjectInspector(objToObjectInspector(list.get(0), isConstant));
      }
    } else if (obj instanceof Map) {
      Map<String, Object> map = (Map<String, Object>)obj;
      List<String> fields = new ArrayList<>();
      List<ObjectInspector> ois = new ArrayList<>();
      List<Object> values = new ArrayList<>();
      for (Map.Entry<String, Object> e : map.entrySet()) {
        fields.add(e.getKey());
        ois.add(objToObjectInspector(e.getValue(), isConstant));
        values.add(e.getValue());
      }
      if (isConstant) {
        return ObjectInspectorFactory.getStandardConstantStructObjectInspector(fields, ois, values);
      } else {
        return ObjectInspectorFactory.getStandardStructObjectInspector(fields, ois);
      }
    } else if (obj instanceof String) {
      return isConstant ? PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(TypeInfoFactory.stringTypeInfo, new Text((String)obj)) :
          PrimitiveObjectInspectorFactory.writableStringObjectInspector;
    } else if (obj instanceof Long) {
      return isConstant ? PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(TypeInfoFactory.longTypeInfo, new LongWritable((Long)obj)) :
          PrimitiveObjectInspectorFactory.writableLongObjectInspector;
    } else if (obj instanceof Integer) {
      return isConstant ? PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(TypeInfoFactory.intTypeInfo, new IntWritable((Integer)obj)) :
          PrimitiveObjectInspectorFactory.writableIntObjectInspector;
    } else if (obj instanceof Float) {
      return isConstant ? PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(TypeInfoFactory.floatTypeInfo, new FloatWritable((Float)obj)) :
          PrimitiveObjectInspectorFactory.writableFloatObjectInspector;
    } else if (obj instanceof Double) {
      return isConstant ? PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(TypeInfoFactory.doubleTypeInfo, new DoubleWritable((Double)obj)) :
          PrimitiveObjectInspectorFactory.writableDoubleObjectInspector;
    } else if (obj instanceof Boolean) {
      return isConstant ? PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(TypeInfoFactory.booleanTypeInfo, new BooleanWritable((Boolean)obj)) :
          PrimitiveObjectInspectorFactory.writableBooleanObjectInspector;
    } else {
      throw new RuntimeException("programming error");
    }
  }

  private <T> List<T> wrapInList(T element) {
    List<T> list = new ArrayList<>();
    list.add(element);
    return list;
  }
}
