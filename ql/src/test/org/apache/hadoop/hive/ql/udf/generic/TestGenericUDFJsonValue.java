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
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BooleanObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaConstantBooleanObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaConstantDoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaConstantIntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaConstantLongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaConstantStringObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject;
import static org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredJavaObject;

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
    ObjectPair<ObjectInspector, Object[]> results = test(json, "$.age", serdeConstants.BIGINT_TYPE_NAME);
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
    ObjectPair<ObjectInspector, Object[]> results = test(json, "$.age", serdeConstants.INT_TYPE_NAME);
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
    ObjectPair<ObjectInspector, Object[]> results = test(json, "$.gpa", serdeConstants.DOUBLE_TYPE_NAME);
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
    ObjectPair<ObjectInspector, Object[]> results = test(json, "$.honors", serdeConstants.BOOLEAN_TYPE_NAME);
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
    ObjectPair<ObjectInspector, Object[]> results = test(json, "$.sports", serdeConstants.LIST_TYPE_NAME + "<" + serdeConstants.STRING_TYPE_NAME + ">");
    Assert.assertEquals(4, results.getSecond().length);
    Assert.assertTrue(results.getFirst() instanceof ListObjectInspector);
    ListObjectInspector loi = (ListObjectInspector)results.getFirst();
    Assert.assertTrue(loi.getListElementObjectInspector() instanceof StringObjectInspector);
    Assert.assertEquals(2, loi.getListLength(results.getSecond()[0]));
    Assert.assertEquals("baseball", loi.getListElement(results.getSecond()[0], 0));
    Assert.assertEquals("soccer", loi.getListElement(results.getSecond()[0], 1));
    Assert.assertEquals(1, loi.getListLength(results.getSecond()[1]));
    Assert.assertEquals("basketball", loi.getListElement(results.getSecond()[1], 0));
    Assert.assertNull(loi.getList(results.getSecond()[2]));
    Assert.assertNull(loi.getList(results.getSecond()[3]));
  }

  @Test
  public void simpleListLong() throws HiveException {
    ObjectPair<ObjectInspector, Object[]> results = test(json, "$.longlist", serdeConstants.LIST_TYPE_NAME + "<" + serdeConstants.BIGINT_TYPE_NAME + ">");
    Assert.assertEquals(4, results.getSecond().length);
    Assert.assertTrue(results.getFirst() instanceof ListObjectInspector);
    ListObjectInspector loi = (ListObjectInspector)results.getFirst();
    Assert.assertTrue(loi.getListElementObjectInspector() instanceof LongObjectInspector);
    Assert.assertEquals(2, loi.getListLength(results.getSecond()[3]));
    Assert.assertEquals(3L, loi.getListElement(results.getSecond()[3], 0));
    Assert.assertEquals(26L, loi.getListElement(results.getSecond()[3], 1));
  }

  @Test
  public void simpleListDouble() throws HiveException {
    ObjectPair<ObjectInspector, Object[]> results = test(json, "$.doublelist", serdeConstants.LIST_TYPE_NAME + "<" + serdeConstants.DOUBLE_TYPE_NAME + ">");
    Assert.assertEquals(4, results.getSecond().length);
    Assert.assertTrue(results.getFirst() instanceof ListObjectInspector);
    ListObjectInspector loi = (ListObjectInspector)results.getFirst();
    Assert.assertTrue(loi.getListElementObjectInspector() instanceof DoubleObjectInspector);
    Assert.assertEquals(2, loi.getListLength(results.getSecond()[3]));
    Assert.assertEquals(3.52, loi.getListElement(results.getSecond()[3], 0));
    Assert.assertEquals(2.86, loi.getListElement(results.getSecond()[3], 1));
  }

  @Test
  public void simpleListBool() throws HiveException {
    ObjectPair<ObjectInspector, Object[]> results = test(json, "$.boollist", serdeConstants.LIST_TYPE_NAME + "<" + serdeConstants.BOOLEAN_TYPE_NAME + ">");
    Assert.assertEquals(4, results.getSecond().length);
    Assert.assertTrue(results.getFirst() instanceof ListObjectInspector);
    ListObjectInspector loi = (ListObjectInspector)results.getFirst();
    Assert.assertTrue(loi.getListElementObjectInspector() instanceof BooleanObjectInspector);
    Assert.assertEquals(2, loi.getListLength(results.getSecond()[3]));
    Assert.assertEquals(true, loi.getListElement(results.getSecond()[3], 0));
    Assert.assertEquals(false, loi.getListElement(results.getSecond()[3], 1));
  }

  @Test
  public void multitypeList() throws HiveException {
    ObjectPair<ObjectInspector, Object[]> results = test(json, "$.multilist", serdeConstants.LIST_TYPE_NAME + "<" + serdeConstants.STRING_TYPE_NAME + ">");
    Assert.assertEquals(4, results.getSecond().length);
    Assert.assertTrue(results.getFirst() instanceof ListObjectInspector);
    ListObjectInspector loi = (ListObjectInspector)results.getFirst();
    Assert.assertEquals(5, loi.getListLength(results.getSecond()[3]));
    Assert.assertEquals("string", loi.getListElement(results.getSecond()[3], 0));
    Assert.assertEquals("1", loi.getListElement(results.getSecond()[3], 1));
    Assert.assertEquals("2.3", loi.getListElement(results.getSecond()[3], 2));
    Assert.assertEquals("false", loi.getListElement(results.getSecond()[3], 3));

    results = test(json, "$.multilist", serdeConstants.LIST_TYPE_NAME + "<" + serdeConstants.BIGINT_TYPE_NAME + ">");
    loi = (ListObjectInspector)results.getFirst();
    Assert.assertNull(loi.getListElement(results.getSecond()[3], 0));
    Assert.assertEquals(1L, loi.getListElement(results.getSecond()[3], 1));
    Assert.assertEquals(2L, loi.getListElement(results.getSecond()[3], 2));
    Assert.assertNull(loi.getListElement(results.getSecond()[3], 3));

    results = test(json, "$.multilist", serdeConstants.LIST_TYPE_NAME + "<" + serdeConstants.DOUBLE_TYPE_NAME + ">");
    loi = (ListObjectInspector)results.getFirst();
    Assert.assertNull(loi.getListElement(results.getSecond()[3], 0));
    Assert.assertEquals(1.0, loi.getListElement(results.getSecond()[3], 1));
    Assert.assertEquals(2.3, loi.getListElement(results.getSecond()[3], 2));
    Assert.assertNull(loi.getListElement(results.getSecond()[3], 3));

    results = test(json, "$.multilist", serdeConstants.LIST_TYPE_NAME + "<" + serdeConstants.BOOLEAN_TYPE_NAME + ">");
    loi = (ListObjectInspector)results.getFirst();
    Assert.assertNull(loi.getListElement(results.getSecond()[3], 0));
    Assert.assertNull(loi.getListElement(results.getSecond()[3], 1));
    Assert.assertNull(loi.getListElement(results.getSecond()[3], 2));
    Assert.assertEquals(false, loi.getListElement(results.getSecond()[3], 3));

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
    Assert.assertEquals("false", poi.getPrimitiveJavaObject(results.getSecond()[0]));
    Assert.assertEquals("true", poi.getPrimitiveJavaObject(results.getSecond()[1]));
    Assert.assertNull(poi.getPrimitiveJavaObject(results.getSecond()[2]));
    Assert.assertNull(poi.getPrimitiveJavaObject(results.getSecond()[3]));
  }

  @Test
  public void stringAsLong() throws HiveException {
    ObjectPair<ObjectInspector, Object[]> results = test(json, "$.name", serdeConstants.BIGINT_TYPE_NAME);
    Assert.assertEquals(4, results.getSecond().length);
    PrimitiveObjectInspector poi = (PrimitiveObjectInspector) results.getFirst();
    Assert.assertNull(poi.getPrimitiveJavaObject(results.getSecond()[0]));
    results = test(json, "$.longstring", serdeConstants.BIGINT_TYPE_NAME);
    Assert.assertEquals(4, results.getSecond().length);
    poi = (PrimitiveObjectInspector) results.getFirst();
    Assert.assertEquals(42L, poi.getPrimitiveJavaObject(results.getSecond()[3]));
  }

  @Test
  public void doubleAsLong() throws HiveException {
    ObjectPair<ObjectInspector, Object[]> results = test(json, "$.gpa", serdeConstants.BIGINT_TYPE_NAME);
    Assert.assertEquals(4, results.getSecond().length);
    PrimitiveObjectInspector poi = (PrimitiveObjectInspector) results.getFirst();
    Assert.assertEquals(3L, poi.getPrimitiveJavaObject(results.getSecond()[0]));
    Assert.assertEquals(3L, poi.getPrimitiveJavaObject(results.getSecond()[1]));
    Assert.assertNull(poi.getPrimitiveJavaObject(results.getSecond()[2]));
    Assert.assertNull(poi.getPrimitiveJavaObject(results.getSecond()[3]));
  }

  @Test
  public void booleanAsLong() throws HiveException {
    ObjectPair<ObjectInspector, Object[]> results = test(json, "$.honors", serdeConstants.BIGINT_TYPE_NAME);
    Assert.assertEquals(4, results.getSecond().length);
    PrimitiveObjectInspector poi = (PrimitiveObjectInspector) results.getFirst();
    Assert.assertNull(poi.getPrimitiveJavaObject(results.getSecond()[0]));
    Assert.assertNull(poi.getPrimitiveJavaObject(results.getSecond()[1]));
    Assert.assertNull(poi.getPrimitiveJavaObject(results.getSecond()[2]));
    Assert.assertNull(poi.getPrimitiveJavaObject(results.getSecond()[3]));
  }

  @Test
  public void stringAsDouble() throws HiveException {
    ObjectPair<ObjectInspector, Object[]> results = test(json, "$.name", serdeConstants.DOUBLE_TYPE_NAME);
    Assert.assertEquals(4, results.getSecond().length);
    PrimitiveObjectInspector poi = (PrimitiveObjectInspector) results.getFirst();
    Assert.assertNull(poi.getPrimitiveJavaObject(results.getSecond()[0]));
    results = test(json, "$.doublestring", serdeConstants.DOUBLE_TYPE_NAME);
    Assert.assertEquals(4, results.getSecond().length);
    poi = (PrimitiveObjectInspector) results.getFirst();
    Assert.assertEquals(3.1415, poi.getPrimitiveJavaObject(results.getSecond()[3]));
  }

  @Test
  public void longAsDouble() throws HiveException {
    ObjectPair<ObjectInspector, Object[]> results = test(json, "$.age", serdeConstants.DOUBLE_TYPE_NAME);
    Assert.assertEquals(4, results.getSecond().length);
    PrimitiveObjectInspector poi = (PrimitiveObjectInspector) results.getFirst();
    Assert.assertEquals(21.0, poi.getPrimitiveJavaObject(results.getSecond()[0]));
    Assert.assertEquals(20.0, poi.getPrimitiveJavaObject(results.getSecond()[1]));
    Assert.assertNull(poi.getPrimitiveJavaObject(results.getSecond()[2]));
    Assert.assertNull(poi.getPrimitiveJavaObject(results.getSecond()[3]));
  }

  @Test
  public void booleanAsDouble() throws HiveException {
    ObjectPair<ObjectInspector, Object[]> results = test(json, "$.honors", serdeConstants.DOUBLE_TYPE_NAME);
    Assert.assertEquals(4, results.getSecond().length);
    PrimitiveObjectInspector poi = (PrimitiveObjectInspector) results.getFirst();
    Assert.assertNull(poi.getPrimitiveJavaObject(results.getSecond()[0]));
    Assert.assertNull(poi.getPrimitiveJavaObject(results.getSecond()[1]));
    Assert.assertNull(poi.getPrimitiveJavaObject(results.getSecond()[2]));
    Assert.assertNull(poi.getPrimitiveJavaObject(results.getSecond()[3]));
  }

  @Test
  public void stringAsBoolean() throws HiveException {
    ObjectPair<ObjectInspector, Object[]> results = test(json, "$.name", serdeConstants.BOOLEAN_TYPE_NAME);
    Assert.assertEquals(4, results.getSecond().length);
    PrimitiveObjectInspector poi = (PrimitiveObjectInspector) results.getFirst();
    Assert.assertNull(poi.getPrimitiveJavaObject(results.getSecond()[0]));
    Assert.assertNull(poi.getPrimitiveJavaObject(results.getSecond()[1]));
    Assert.assertNull(poi.getPrimitiveJavaObject(results.getSecond()[2]));
    Assert.assertNull(poi.getPrimitiveJavaObject(results.getSecond()[3]));
    results = test(json, "$.boolstring", serdeConstants.BOOLEAN_TYPE_NAME);
    Assert.assertEquals(4, results.getSecond().length);
    poi = (PrimitiveObjectInspector) results.getFirst();
    Assert.assertEquals(true, poi.getPrimitiveJavaObject(results.getSecond()[3]));
  }

  @Test
  public void longAsBoolean() throws HiveException {
    ObjectPair<ObjectInspector, Object[]> results = test(json, "$.age", serdeConstants.BOOLEAN_TYPE_NAME);
    Assert.assertEquals(4, results.getSecond().length);
    PrimitiveObjectInspector poi = (PrimitiveObjectInspector) results.getFirst();
    Assert.assertNull(poi.getPrimitiveJavaObject(results.getSecond()[0]));
    Assert.assertNull(poi.getPrimitiveJavaObject(results.getSecond()[1]));
    Assert.assertNull(poi.getPrimitiveJavaObject(results.getSecond()[2]));
    Assert.assertNull(poi.getPrimitiveJavaObject(results.getSecond()[3]));
  }

  @Test
  public void doubleAsBoolean() throws HiveException {
    ObjectPair<ObjectInspector, Object[]> results = test(json, "$.gpa", serdeConstants.BOOLEAN_TYPE_NAME);
    Assert.assertEquals(4, results.getSecond().length);
    PrimitiveObjectInspector poi = (PrimitiveObjectInspector) results.getFirst();
    Assert.assertNull(poi.getPrimitiveJavaObject(results.getSecond()[0]));
    Assert.assertNull(poi.getPrimitiveJavaObject(results.getSecond()[1]));
    Assert.assertNull(poi.getPrimitiveJavaObject(results.getSecond()[2]));
    Assert.assertNull(poi.getPrimitiveJavaObject(results.getSecond()[3]));
  }

  @Test
  public void unsupportedReturnType() throws HiveException {
    try {
      test(json, "$.gpa", serdeConstants.CHAR_TYPE_NAME);
    } catch (UDFArgumentTypeException e) {
      Assert.assertEquals("jsonvalue can return primitive type of list of primitive types", e.getMessage());
    }
  }

  @Test
  public void stringStaticDefaultOnEmpty() throws HiveException {
    ObjectPair<ObjectInspector, Object[]> results = test(json, "$.name", serdeConstants.STRING_TYPE_NAME,
        new JavaConstantStringObjectInspector("fred"));
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
    ObjectPair<ObjectInspector, Object[]> results = test(json, "$.age", serdeConstants.BIGINT_TYPE_NAME,
        new JavaConstantLongObjectInspector(87L));
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
    ObjectPair<ObjectInspector, Object[]> results = test(json, "$.age", serdeConstants.INT_TYPE_NAME,
        new JavaConstantIntObjectInspector(93));
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
    ObjectPair<ObjectInspector, Object[]> results = test(json, "$.gpa", serdeConstants.DOUBLE_TYPE_NAME,
        new JavaConstantDoubleObjectInspector(2.5));
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
    ObjectPair<ObjectInspector, Object[]> results = test(json, "$.honors", serdeConstants.BOOLEAN_TYPE_NAME,
        new JavaConstantBooleanObjectInspector(true));
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
    ObjectPair<ObjectInspector, Object[]> results = test(json, "$.name", serdeConstants.STRING_TYPE_NAME,
        PrimitiveObjectInspectorFactory.writableStringObjectInspector, Arrays.asList("one", "two", "three", "four"));
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
    ObjectPair<ObjectInspector, Object[]> results = test(json, "$.age", serdeConstants.BIGINT_TYPE_NAME,
        PrimitiveObjectInspectorFactory.writableLongObjectInspector, Arrays.asList(1L, 2L, 3L, 4L));
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
    ObjectPair<ObjectInspector, Object[]> results = test(json, "$.gpa", serdeConstants.DOUBLE_TYPE_NAME,
        PrimitiveObjectInspectorFactory.writableDoubleObjectInspector, Arrays.asList(1.0, 2.0, 3.0, 4.0));
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
    ObjectPair<ObjectInspector, Object[]> results = test(json, "$.honors", serdeConstants.BOOLEAN_TYPE_NAME,
        PrimitiveObjectInspectorFactory.writableBooleanObjectInspector, Arrays.asList(false, false, false, true));
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
    ObjectPair<ObjectInspector, Object[]> results = test(json, "$.subobj?(@.str == 5)", serdeConstants.STRING_TYPE_NAME,
        new JavaConstantStringObjectInspector("fred"));
    Assert.assertEquals(4, results.getSecond().length);
    StringObjectInspector soi = (StringObjectInspector)results.getFirst();
    Assert.assertEquals("fred", soi.getPrimitiveJavaObject(results.getSecond()[0]));
    Assert.assertEquals("fred", soi.getPrimitiveJavaObject(results.getSecond()[1]));
    Assert.assertEquals("fred", soi.getPrimitiveJavaObject(results.getSecond()[2]));
    Assert.assertEquals("fred", soi.getPrimitiveJavaObject(results.getSecond()[3]));
  }

  @Test
  public void nullOnError() throws HiveException {
    ObjectPair<ObjectInspector, Object[]> results = test(json, "$.subobj?(@.str == 5)");
    Assert.assertEquals(4, results.getSecond().length);
    StringObjectInspector soi = (StringObjectInspector)results.getFirst();
    Assert.assertNull(soi.getPrimitiveJavaObject(results.getSecond()[0]));
    Assert.assertNull(soi.getPrimitiveJavaObject(results.getSecond()[1]));
    Assert.assertNull(soi.getPrimitiveJavaObject(results.getSecond()[2]));
    Assert.assertNull(soi.getPrimitiveJavaObject(results.getSecond()[3]));
  }

  @Test
  public void badSyntax() throws HiveException {
    try {
      test(json, "$.nosuchfunc()");
    } catch (UDFArgumentException e) {
      Assert.assertTrue(e.getMessage().startsWith("Failed to parse JSON path exception: '$.nosuchfunc()'"));
    }
  }

  @Test
  public void errorOnError() throws HiveException {
    test(json, "$.subojb?(@.str == 5)", serdeConstants.STRING_TYPE_NAME, true);
  }

  @Test
  public void passing() throws HiveException {
    Map<String, ObjectInspector> passingOIs = Collections.singletonMap("index", PrimitiveObjectInspectorFactory.writableIntObjectInspector);
    List<Map<String, Object>> passingVals = Arrays.asList(
        Collections.singletonMap("index", 1),
        Collections.singletonMap("index", 0),
        Collections.singletonMap("index", 0),
        Collections.singletonMap("index", 2)
    );

    ObjectPair<ObjectInspector, Object[]> results = test(json, "$.sports[$index]", serdeConstants.STRING_TYPE_NAME,
        passingOIs, passingVals);
    Assert.assertEquals(4, results.getSecond().length);
    StringObjectInspector soi = (StringObjectInspector)results.getFirst();
    Assert.assertEquals("soccer", soi.getPrimitiveJavaObject(results.getSecond()[0]));
    Assert.assertEquals("basketball", soi.getPrimitiveJavaObject(results.getSecond()[1]));
    Assert.assertNull(soi.getPrimitiveJavaObject(results.getSecond()[2]));
    // Side affect of the way we call test with a default value
    Assert.assertEquals("true", soi.getPrimitiveJavaObject(results.getSecond()[3]));
  }

  private ObjectPair<ObjectInspector, Object[]> test(List<String> jsonValues, String pathExpr) throws HiveException {
    GenericUDFJsonValue udf = new GenericUDFJsonValue();
    ObjectInspector resultObjectInspector = udf.initialize(buildInitArgs(pathExpr, null, null, null, null));
    Object[] results = new Object[jsonValues.size()];
    for (int i = 0; i < jsonValues.size(); i++) {
      results[i] = udf.evaluate(buildExecArgs(jsonValues.get(i), pathExpr, null, null, null, null));
    }
    return new ObjectPair<>(resultObjectInspector, results);
  }

  private ObjectPair<ObjectInspector, Object[]> test(List<String> jsonValues, String pathExpr, String returnType)
      throws HiveException {
    GenericUDFJsonValue udf = new GenericUDFJsonValue();
    ObjectInspector resultObjectInspector = udf.initialize(buildInitArgs(pathExpr, returnType, null, null, null));
    Object[] results = new Object[jsonValues.size()];
    for (int i = 0; i < jsonValues.size(); i++) {
      results[i] = udf.evaluate(buildExecArgs(jsonValues.get(i), pathExpr, returnType, null, null, null));
    }
    return new ObjectPair<>(resultObjectInspector, results);
  }

  // Test constant default value
  private ObjectPair<ObjectInspector, Object[]> test(List<String> jsonValues, String pathExpr, String returnType,
                                                     ObjectInspector defaultValOI) throws HiveException {
    GenericUDFJsonValue udf = new GenericUDFJsonValue();
    ObjectInspector resultObjectInspector = udf.initialize(buildInitArgs(pathExpr, returnType, defaultValOI, null, null));
    Object[] results = new Object[jsonValues.size()];
    for (int i = 0; i < jsonValues.size(); i++) {
      results[i] = udf.evaluate(buildExecArgs(jsonValues.get(i), pathExpr, returnType, "dummy", null, null));
    }
    return new ObjectPair<>(resultObjectInspector, results);
  }

  // Test dynamic default value
  private ObjectPair<ObjectInspector, Object[]> test(List<String> jsonValues, String pathExpr, String returnType,
                                                     ObjectInspector defaultValOI, List<Object> defaultVal) throws HiveException {
    GenericUDFJsonValue udf = new GenericUDFJsonValue();
    ObjectInspector resultObjectInspector = udf.initialize(buildInitArgs(pathExpr, returnType, defaultValOI, null, null));
    Object[] results = new Object[jsonValues.size()];
    for (int i = 0; i < jsonValues.size(); i++) {
      results[i] = udf.evaluate(buildExecArgs(jsonValues.get(i), pathExpr, returnType, defaultVal.get(i), null, null));
    }
    return new ObjectPair<>(resultObjectInspector, results);
  }

  private ObjectPair<ObjectInspector, Object[]> test(List<String> jsonValues, String pathExpr, String returnType,
                                                     Boolean errorOnError) throws HiveException {
    GenericUDFJsonValue udf = new GenericUDFJsonValue();
    ObjectInspector resultObjectInspector = udf.initialize(buildInitArgs(pathExpr, returnType, new JavaConstantBooleanObjectInspector(true), errorOnError, null));
    Object[] results = new Object[jsonValues.size()];
    for (int i = 0; i < jsonValues.size(); i++) {
      results[i] = udf.evaluate(buildExecArgs(jsonValues.get(i), pathExpr, returnType, true, errorOnError, null));
    }
    return new ObjectPair<>(resultObjectInspector, results);
  }

  private ObjectPair<ObjectInspector, Object[]> test(List<String> jsonValues, String pathExpr, String returnType,
                                                     Map<String, ObjectInspector> passingOIs,
                                                     List<Map<String, Object>> passing) throws HiveException {
    GenericUDFJsonValue udf = new GenericUDFJsonValue();
    ObjectInspector resultObjectInspector = udf.initialize(buildInitArgs(pathExpr, returnType, new JavaConstantBooleanObjectInspector(true), false, passingOIs));
    Object[] results = new Object[jsonValues.size()];
    for (int i = 0; i < jsonValues.size(); i++) {
      results[i] = udf.evaluate(buildExecArgs(jsonValues.get(i), pathExpr, returnType, false, false, passing.get(i)));
    }
    return new ObjectPair<>(resultObjectInspector, results);
  }

  private ObjectInspector[] buildInitArgs(String pathExpr, String returnType, ObjectInspector defaultValOI,
                                          Boolean errorOnError, Map<String, ObjectInspector> passing) {
    List<ObjectInspector> initArgs = new ArrayList<>();
    initArgs.add(PrimitiveObjectInspectorFactory.writableStringObjectInspector);
    initArgs.add(new JavaConstantStringObjectInspector(pathExpr));
    // Nesting here is important because we want it to come out with the right number of args
    if (returnType != null) {
      initArgs.add(new JavaConstantStringObjectInspector(returnType));
      if (defaultValOI != null) {
        initArgs.add(defaultValOI);
        if (errorOnError != null) {
          initArgs.add(new JavaConstantBooleanObjectInspector(errorOnError));
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

  private DeferredObject[] buildExecArgs(String jsonValue, String pathExpr, String returnType, Object defaultVal,
                                         Boolean errorOnError, Map<String, Object> passing) {
    List<DeferredObject> execArgs = new ArrayList<>();
    execArgs.add(wrapInDeferred(jsonValue));
    execArgs.add(wrapInDeferred(pathExpr));
    if (returnType != null) {
      execArgs.add(wrapInDeferred(returnType));
      if (defaultVal != null) {
        execArgs.add(wrapInDeferred(defaultVal));
        if (errorOnError != null) {
          execArgs.add(wrapInDeferred(errorOnError));
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
    else if (obj instanceof Boolean) return new DeferredJavaObject(new BooleanWritable((Boolean)obj));
    else throw new RuntimeException("what?");
  }
}
