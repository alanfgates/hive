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
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaConstantStringObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
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

  List<String> json;

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
  public void stringNullOnEmptyErrorNoPassing() throws HiveException {
    ObjectPair<ObjectInspector, Object[]> results =
        testNullOnEmptyErrorNoPassing(json, "$.name", serdeConstants.STRING_TYPE_NAME);
    Assert.assertEquals(6, results.getSecond().length);
    Assert.assertTrue(results.getFirst() instanceof PrimitiveObjectInspector);
    PrimitiveObjectInspector poi = (PrimitiveObjectInspector)results.getFirst();
    Assert.assertEquals(String.class, poi.getJavaPrimitiveClass());
    Assert.assertEquals("chris", poi.getPrimitiveJavaObject(results.getSecond()[0]));
    Assert.assertEquals("tracy", poi.getPrimitiveJavaObject(results.getSecond()[1]));
    Assert.assertEquals("kim", poi.getPrimitiveJavaObject(results.getSecond()[2]));
    Assert.assertNull(poi.getPrimitiveJavaObject(results.getSecond()[3]));
    Assert.assertEquals("madison", poi.getPrimitiveJavaObject(results.getSecond()[4]));
    Assert.assertNull(poi.getPrimitiveJavaObject(results.getSecond()[5]));
  }

  private ObjectPair<ObjectInspector, Object[]> testNullOnEmptyErrorNoPassing(
      List<String> jsonValues, String pathExpr, String returning) throws HiveException {
    return test(jsonValues, pathExpr, returning, "NULL", "", null, "NULL", "", null, null);
  }

  private ObjectPair<ObjectInspector, Object[]> test(
      List<String> jsonValues, String pathExpr, String returning, String onEmpty, Object onEmptyDefault,
      ObjectInspector onEmptyDefaultObjectInspector, String onError, Object onErrorDefault,
      ObjectInspector onErrorDefaultObjectInspector,
      List<Map<String, ObjectPair<Object,ObjectInspector>>> passing) throws HiveException {
    assert passing == null || passing.size() == 0 || jsonValues.size() == passing.size();

    GenericUDFJsonValue udf = new GenericUDFJsonValue();

    // determine argument length
    List<ObjectInspector> initArgs = new ArrayList<>();
    initArgs.add(PrimitiveObjectInspectorFactory.writableStringObjectInspector);
    initArgs.add(new JavaConstantStringObjectInspector(pathExpr));
    initArgs.add(new JavaConstantStringObjectInspector(returning));
    initArgs.add(new JavaConstantStringObjectInspector(onEmpty));
    initArgs.add(onEmptyDefaultObjectInspector);
    initArgs.add(new JavaConstantStringObjectInspector(onError));
    initArgs.add(onErrorDefaultObjectInspector);
    if (passing != null && passing.size() > 0) {
      for (Map.Entry<String, ObjectPair<Object, ObjectInspector>> entry : passing.get(0).entrySet()) {
        initArgs.add(new JavaConstantStringObjectInspector(entry.getKey()));
        initArgs.add(entry.getValue().getSecond());
      }
    }

    ObjectInspector resultObjectInspector = udf.initialize(initArgs.toArray(new ObjectInspector[0]));

    Object[] results = new Object[jsonValues.size()];
    for (int i = 0; i < jsonValues.size(); i++) {
      List<DeferredObject> execArgs = new ArrayList<>();
      execArgs.add(wrapInDeferred(jsonValues.get(i)));
      execArgs.add(wrapInDeferred(pathExpr));
      execArgs.add(wrapInDeferred(returning));
      execArgs.add(wrapInDeferred(onEmpty));
      execArgs.add(wrapInDeferred(onEmptyDefault));
      execArgs.add(wrapInDeferred(onError));
      execArgs.add(wrapInDeferred(onErrorDefault));
      if (passing != null) {
        for (Map.Entry<String, ObjectPair<Object, ObjectInspector>> entry : passing.get(i).entrySet()) {
          execArgs.add(wrapInDeferred(entry.getKey()));
          execArgs.add(wrapInDeferred(entry.getValue().getFirst()));
        }
      }
      results[i] = udf.evaluate(execArgs.toArray(new DeferredObject[0]));
    }

    for (Object o : results) System.out.println("XXX " + o.toString());

    return new ObjectPair<>(resultObjectInspector, results);
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
