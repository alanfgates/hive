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
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaConstantBooleanObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaConstantStringObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject;
import static org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredJavaObject;

import java.util.ArrayList;
import java.util.Arrays;
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
            "\"clubs\"  : null," +
            "\"sports\" : [ \"baseball\", \"soccer\" ]" +
        "}",
        "{" +
            "\"name\" : \"tracy\"," +
            "\"age\"  : 20," +
            "\"gpa\"  : 3.94," +
            "\"honors\" : true," +
            "\"clubs\"  : [ \"robotics\", \"chess\" ]" +
        "}",
        "{" +
            "\"name\" : \"kim\"," +
            "\"age\"  : 29," +
            "\"gpa\"  : null," +
            "\"honors\" : null," +
            "\"clubs\"  : [ \"hiking\" ]," +
            "\"sports\" : [ \"football\" ]" +
        "}",
        "{" +
            "\"name\" : null," +
            "\"age\"  : null," +
            "\"gpa\"  : 3.01," +
            "\"honors\" : false," +
            "\"clubs\"  : [ \"hang gliding\" ]," +
            "\"sports\" : [ \"basketball\" ]" +
        "}",
        "{" +
            "\"name\" : \"madison\"," +
            "\"age\"  : 19," +
            "\"clubs\"  : [ \"swimming\" ]," +
            "\"sports\" : [ \"volleyball\" ]" +
        "}",
        "{" +
            "\"gpa\"  : 1.01," +
            "\"honors\" : false," +
            "\"clubs\"  : [ \"astronomy\" ]," +
            "\"sports\" : [ \"tennis\" ]" +
        "}"
    );
  }

  @Test
  public void simpleString() throws HiveException {
    ObjectPair<ObjectInspector, Object[]> results = test(json, "$.name");
    Assert.assertEquals(6, results.getSecond().length);
    Assert.assertTrue(results.getFirst() instanceof StringObjectInspector);
    StringObjectInspector soi = (StringObjectInspector)results.getFirst();
    Assert.assertEquals(String.class, soi.getJavaPrimitiveClass());
    Assert.assertEquals("chris", soi.getPrimitiveJavaObject(results.getSecond()[0]));
    Assert.assertEquals("tracy", soi.getPrimitiveJavaObject(results.getSecond()[1]));
    Assert.assertEquals("kim", soi.getPrimitiveJavaObject(results.getSecond()[2]));
    Assert.assertNull(soi.getPrimitiveJavaObject(results.getSecond()[3]));
    Assert.assertEquals("madison", soi.getPrimitiveJavaObject(results.getSecond()[4]));
    Assert.assertNull(soi.getPrimitiveJavaObject(results.getSecond()[5]));
  }

  @Test
  public void simpleLong() throws HiveException {
    ObjectPair<ObjectInspector, Object[]> results = test(json, "$.age", serdeConstants.BIGINT_TYPE_NAME);
    Assert.assertEquals(6, results.getSecond().length);
    Assert.assertTrue(results.getFirst() instanceof LongObjectInspector);
    LongObjectInspector loi = (LongObjectInspector)results.getFirst();
    Assert.assertEquals(Long.class, loi.getJavaPrimitiveClass());
    Assert.assertEquals(21L, loi.getPrimitiveJavaObject(results.getSecond()[0]));
    Assert.assertEquals(20L, loi.getPrimitiveJavaObject(results.getSecond()[1]));
    Assert.assertEquals(29L, loi.getPrimitiveJavaObject(results.getSecond()[2]));
    Assert.assertNull(loi.getPrimitiveJavaObject(results.getSecond()[3]));
    Assert.assertEquals(19L, loi.getPrimitiveJavaObject(results.getSecond()[4]));
    Assert.assertNull(loi.getPrimitiveJavaObject(results.getSecond()[5]));
  }

  // TODO test returning types from inptus of other types
  // TODO test empty error and default
  // TODO test error error and default
  // TODO test passing

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

  private ObjectPair<ObjectInspector, Object[]> test(List<String> jsonValues, String pathExpr, String returnType,
                                                     ObjectInspector defaultValOI, Object defaultVal) throws HiveException {
    GenericUDFJsonValue udf = new GenericUDFJsonValue();
    ObjectInspector resultObjectInspector = udf.initialize(buildInitArgs(pathExpr, returnType, defaultValOI, null, null));
    Object[] results = new Object[jsonValues.size()];
    for (int i = 0; i < jsonValues.size(); i++) {
      results[i] = udf.evaluate(buildExecArgs(jsonValues.get(i), pathExpr, returnType, defaultVal, null, null));
    }
    return new ObjectPair<>(resultObjectInspector, results);
  }

  private ObjectPair<ObjectInspector, Object[]> test(List<String> jsonValues, String pathExpr, String returnType,
                                                     ObjectInspector defaultValOI, Object defaultVal,
                                                     Boolean errorOnError) throws HiveException {
    GenericUDFJsonValue udf = new GenericUDFJsonValue();
    ObjectInspector resultObjectInspector = udf.initialize(buildInitArgs(pathExpr, returnType, defaultValOI, errorOnError, null));
    Object[] results = new Object[jsonValues.size()];
    for (int i = 0; i < jsonValues.size(); i++) {
      results[i] = udf.evaluate(buildExecArgs(jsonValues.get(i), pathExpr, returnType, defaultVal, errorOnError, null));
    }
    return new ObjectPair<>(resultObjectInspector, results);
  }

  private ObjectPair<ObjectInspector, Object[]> test(List<String> jsonValues, String pathExpr, String returnType,
                                                     ObjectInspector defaultValOI, Object defaultVal,
                                                     Boolean errorOnError, Map<String, ObjectInspector> passingOIs,
                                                     Map<String, Object> passing) throws HiveException {
    GenericUDFJsonValue udf = new GenericUDFJsonValue();
    ObjectInspector resultObjectInspector = udf.initialize(buildInitArgs(pathExpr, returnType, defaultValOI, errorOnError, passingOIs));
    Object[] results = new Object[jsonValues.size()];
    for (int i = 0; i < jsonValues.size(); i++) {
      results[i] = udf.evaluate(buildExecArgs(jsonValues.get(i), pathExpr, returnType, defaultVal, errorOnError, passing));
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
    else if (obj instanceof Double) return new DeferredJavaObject(new DoubleWritable((Double)obj));
    else if (obj instanceof Boolean) return new DeferredJavaObject(new BooleanWritable((Boolean)obj));
    else throw new RuntimeException("what?");
  }
}
