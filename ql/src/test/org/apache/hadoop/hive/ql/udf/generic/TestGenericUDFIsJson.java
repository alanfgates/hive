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

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class TestGenericUDFIsJson {

  // Goal here isn't to test JSON permutations, TestJsonValueParser handles that.  Just want to test that the
  // is_json UDF works

  @Test
  public void nullJson() throws HiveException {
    Assert.assertNull(test(null));
  }

  @Test
  public void emptyJson() throws HiveException {
    Assert.assertFalse(test(""));
  }

  @Test
  public void badJson() throws HiveException {
    Assert.assertFalse(test("bad json!"));
  }

  @Test
  public void goodJson() throws HiveException {
    Assert.assertTrue(test("{ \"name\" : \"fred\" }"));
  }

  @Test
  public void multipleLines() throws HiveException {
    Map<String, Boolean> inputs = new HashMap<>();
    inputs.put("{ \"name\" : \"fred\" }", true);
    inputs.put("{ \"age\" : 35 }", true);
    inputs.put("{ \"classes\" : [\"algebra\", \"painting\" ] }", true);
    inputs.put("{ bad : }", false);
    inputs.put("{ \"gpa\" : 1.35 }", true);

    GenericUDFIsJson isJson = new GenericUDFIsJson();
    ObjectInspector jsonOI = PrimitiveObjectInspectorFactory.writableStringObjectInspector;
    ObjectInspector[] initArgs = {jsonOI};
    isJson.initialize(initArgs);

    for (Map.Entry<String, Boolean> input : inputs.entrySet()) {
      GenericUDF.DeferredObject jsonStr = new GenericUDF.DeferredJavaObject(new Text(input.getKey()));
      GenericUDF.DeferredObject[] execArgs = {jsonStr};
      BooleanWritable result = (BooleanWritable)isJson.evaluate(execArgs);
      Assert.assertNotNull(result);
      Assert.assertEquals(input.getValue(), result.get());
    }
  }

  private Boolean test(String json) throws HiveException {
    GenericUDFIsJson isJson = new GenericUDFIsJson();
    ObjectInspector jsonOI = PrimitiveObjectInspectorFactory.writableStringObjectInspector;
    ObjectInspector[] initArgs = {jsonOI};
    isJson.initialize(initArgs);

    GenericUDF.DeferredObject jsonStr = new GenericUDF.DeferredJavaObject(json == null ? null : new Text(json));
    GenericUDF.DeferredObject[] execArgs = {jsonStr};
    BooleanWritable result = (BooleanWritable)isJson.evaluate(execArgs);
    return result == null ? null : result.get();
  }
}
