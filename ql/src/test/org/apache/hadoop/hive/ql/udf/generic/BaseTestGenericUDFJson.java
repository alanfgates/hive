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
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaConstantStringObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject;
import static org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredJavaObject;
import static org.apache.hadoop.hive.ql.udf.generic.GenericUDFJsonValue.WhatToReturn;

abstract class BaseTestGenericUDFJson {
  protected List<String> json;

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
            "\"boollist\"     : [ true, false ]," +
            "\"longlist\"     : [ 3, 26 ]," +
            "\"doublelist\"   : [ 3.52, 2.86 ]," +
            "\"multilist\"    : [ \"string\", 1, 2.3, false, null ]," +
            "\"longstring\"   : \"42\"," +
            "\"doublestring\" : \"3.1415\"," +
            "\"boolstring\"   :\"true\"," +
            "\"subobj\"       : { " +
                "\"str\"      : \"strval\"," +
                "\"long\"     : -1," +
                "\"decimal\"  : -2.2," +
                "\"bool\"     : true" +
            "}," +
            "\"nested\"     : { " +
                "\"nestedlist\" : [ {" +
                    "\"anothernest\" : [ 10, 100, 1000 ]" +
                "}" +
            "] }" +
        "}"
    );
  }


  protected ObjectPair<ObjectInspector, Object[]> test(List<String> jsonValues, String pathExpr) throws HiveException {
    return test(jsonValues, pathExpr, null);
  }

  protected ObjectPair<ObjectInspector, Object[]> test(List<String> jsonValues, String pathExpr, List<Object> defaultVals)
      throws HiveException {
    return test(jsonValues, pathExpr, defaultVals, null);
  }

  protected ObjectPair<ObjectInspector, Object[]> test(List<String> jsonValues, String pathExpr, List<Object> defaultVals,
                                                       WhatToReturn onEmpty) throws HiveException {
    return test(jsonValues, pathExpr, defaultVals, onEmpty, null);
  }

  protected ObjectPair<ObjectInspector, Object[]> test(List<String> jsonValues, String pathExpr, List<Object> defaultVals,
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
  protected ObjectPair<ObjectInspector, Object[]> test(List<String> jsonValues, String pathExpr, List<Object> defaultVals,
                                                       WhatToReturn onEmpty, WhatToReturn onError,
                                                       List<Map<String, Object>> passing) throws HiveException {
    GenericUDFJsonValue udf = getUdf();
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
    else if (obj instanceof Map) return new DeferredJavaObject(new ArrayList<>(((Map)obj).values()));
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

  protected <T> List<T> wrapInList(T element) {
    List<T> list = new ArrayList<>();
    list.add(element);
    return list;
  }

  abstract protected GenericUDFJsonValue getUdf();
}
