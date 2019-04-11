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
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BooleanObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.hadoop.hive.ql.udf.generic.GenericUDFJsonValue.WhatToReturn;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestGenericUDFJsonValue extends BaseTestGenericUDFJson {

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
  public void scalarInList() throws HiveException {
    ObjectPair<ObjectInspector, Object[]> results = test(json, "$.sports[0]");
    PrimitiveObjectInspector poi = (PrimitiveObjectInspector)results.getFirst();
    Assert.assertEquals("baseball", poi.getPrimitiveJavaObject(results.getSecond()[0]));
    Assert.assertEquals("basketball", poi.getPrimitiveJavaObject(results.getSecond()[1]));

    results = test(json, "$.longlist[1]", wrapInList(1L));
    Assert.assertTrue(results.getFirst() instanceof LongObjectInspector);
    poi = (PrimitiveObjectInspector)results.getFirst();
    Assert.assertEquals(26L, poi.getPrimitiveJavaObject(results.getSecond()[3]));

    results = test(json, "$.doublelist[1]", wrapInList(1.0));
    Assert.assertTrue(results.getFirst() instanceof DoubleObjectInspector);
    poi = (PrimitiveObjectInspector)results.getFirst();
    Assert.assertEquals(2.86, poi.getPrimitiveJavaObject(results.getSecond()[3]));

    results = test(json, "$.boollist[1]", wrapInList(true));
    Assert.assertTrue(results.getFirst() instanceof BooleanObjectInspector);
    poi = (PrimitiveObjectInspector)results.getFirst();
    Assert.assertEquals(false, poi.getPrimitiveJavaObject(results.getSecond()[3]));
  }

  @Test
  public void simpleObj() throws HiveException {
    Map<String, Object> defaultVal = new HashMap<>();
    defaultVal.put("str", "a");
    defaultVal.put("long", 1L);
    defaultVal.put("decimal", 1.0);
    defaultVal.put("bool", true);
    ObjectPair<ObjectInspector, Object[]> results = test(json, "$.subobj", wrapInList(defaultVal));

    // First three return values should be null, last one should be a struct
    for (int i = 0; i < 3; i++) Assert.assertNull(results.getSecond()[i]);
    Assert.assertNotNull(results.getSecond()[3]);
    Object struct = results.getSecond()[3];

    // We should have a StructObjectInspector
    Assert.assertTrue(results.getFirst() instanceof StructObjectInspector);
    StructObjectInspector soi = (StructObjectInspector)results.getFirst();

    // Look for the string
    StructField sf = soi.getStructFieldRef("str");
    Assert.assertNotNull(sf);
    Assert.assertEquals("str", sf.getFieldName());
    Assert.assertTrue(sf.getFieldObjectInspector() instanceof StringObjectInspector);
    Assert.assertEquals("strval", ((StringObjectInspector)sf.getFieldObjectInspector()).getPrimitiveJavaObject(soi.getStructFieldData(struct, sf)));

    // Look for the long
    sf = soi.getStructFieldRef("long");
    Assert.assertNotNull(sf);
    Assert.assertEquals("long", sf.getFieldName());
    Assert.assertTrue(sf.getFieldObjectInspector() instanceof LongObjectInspector);
    Assert.assertEquals(-1L, ((LongObjectInspector)sf.getFieldObjectInspector()).getPrimitiveJavaObject(soi.getStructFieldData(struct, sf)));

    // Look for the double
    sf = soi.getStructFieldRef("decimal");
    Assert.assertNotNull(sf);
    Assert.assertEquals("decimal", sf.getFieldName());
    Assert.assertTrue(sf.getFieldObjectInspector() instanceof DoubleObjectInspector);
    Assert.assertEquals(-2.2, ((DoubleObjectInspector)sf.getFieldObjectInspector()).getPrimitiveJavaObject(soi.getStructFieldData(struct, sf)));

    // Look for the boolean
    sf = soi.getStructFieldRef("bool");
    Assert.assertNotNull(sf);
    Assert.assertEquals("bool", sf.getFieldName());
    Assert.assertTrue(sf.getFieldObjectInspector() instanceof BooleanObjectInspector);
    Assert.assertEquals(true, ((BooleanObjectInspector)sf.getFieldObjectInspector()).getPrimitiveJavaObject(soi.getStructFieldData(struct, sf)));
  }

  @Test
  public void scalarInObj() throws HiveException {
    ObjectPair<ObjectInspector, Object[]> results = test(json, "$.subobj.str");
    PrimitiveObjectInspector poi = (PrimitiveObjectInspector)results.getFirst();
    Assert.assertEquals("strval", poi.getPrimitiveJavaObject(results.getSecond()[3]));

    results = test(json, "$.subobj.long", wrapInList(1L));
    poi = (PrimitiveObjectInspector)results.getFirst();
    Assert.assertEquals(-1L, poi.getPrimitiveJavaObject(results.getSecond()[3]));

    results = test(json, "$.subobj.decimal", wrapInList(1.0));
    poi = (PrimitiveObjectInspector)results.getFirst();
    Assert.assertEquals(-2.2, poi.getPrimitiveJavaObject(results.getSecond()[3]));

    results = test(json, "$.subobj.bool", wrapInList(true));
    poi = (PrimitiveObjectInspector)results.getFirst();
    Assert.assertEquals(true, poi.getPrimitiveJavaObject(results.getSecond()[3]));
  }

  @Test
  public void nested() throws HiveException {
    ObjectPair<ObjectInspector, Object[]> results = test(json, "$.nested.nestedlist[0].anothernest[2]", wrapInList(1));
    PrimitiveObjectInspector poi = (PrimitiveObjectInspector)results.getFirst();
    Assert.assertEquals(1000, poi.getPrimitiveJavaObject(results.getSecond()[3]));

    Map<String, Object> defaultVal = new HashMap<>();
    defaultVal.put("anothernest", Arrays.asList(10L, 100L, 1000L));
    results = test(json, "$.nested.nestedlist[0]", wrapInList(defaultVal));
    StructObjectInspector soi = (StructObjectInspector)results.getFirst();

    StructField sf = soi.getStructFieldRef("anothernest");
    Assert.assertNotNull(sf);
    Assert.assertEquals("anothernest", sf.getFieldName());
    Assert.assertTrue(sf.getFieldObjectInspector() instanceof ListObjectInspector);
    ListObjectInspector loi = (ListObjectInspector)sf.getFieldObjectInspector();
    List list = ((ListObjectInspector) sf.getFieldObjectInspector()).getList(soi.getStructFieldData(results.getSecond()[3], sf));
    Assert.assertEquals(3, loi.getListLength(list));
    Assert.assertTrue(loi.getListElementObjectInspector() instanceof LongObjectInspector);
    LongObjectInspector lloi = (LongObjectInspector)loi.getListElementObjectInspector();
    Assert.assertEquals(10L, lloi.getPrimitiveJavaObject(loi.getListElement(list, 0)));
    Assert.assertEquals(100L, lloi.getPrimitiveJavaObject(loi.getListElement(list, 1)));
    Assert.assertEquals(1000L, lloi.getPrimitiveJavaObject(loi.getListElement(list, 2)));
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
  @Override
  protected GenericUDFJsonValue getUdf() {
    return new GenericUDFJsonValue();
  }
}
