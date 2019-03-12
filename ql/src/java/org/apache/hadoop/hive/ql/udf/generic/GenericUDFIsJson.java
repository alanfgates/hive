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

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.sqljsonpath.ErrorListener;
import org.apache.hadoop.hive.ql.udf.generic.sqljsonpath.JsonValueParser;
import org.apache.hadoop.hive.ql.udf.generic.sqljsonpath.ParseException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorConverter;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.BooleanWritable;

import java.io.IOException;

@Description(name = "isjson",
             value = "_FUNC_(json) - Parses the given string to see if it is valid JSON",
             extended = "Returns null if json is null, otherwise true or false")
public class GenericUDFIsJson extends GenericUDF {

  private PrimitiveObjectInspectorConverter.TextConverter inputConverter;
  private JsonValueParser parser;
  private BooleanWritable result;

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    checkArgsSize(arguments, 1, 1);
    checkArgPrimitive(arguments, 0);

    inputConverter = new PrimitiveObjectInspectorConverter.TextConverter((PrimitiveObjectInspector)arguments[0]);
    parser = new JsonValueParser(new ErrorListener());
    result = new BooleanWritable();

    return PrimitiveObjectInspectorFactory.writableBooleanObjectInspector;
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    Object arg = arguments[0].get();
    if (arg == null) return null;
    try {
      String json = inputConverter.convert(arg).toString();
      if (json.trim().length() == 0) {
        result.set(false);
      } else {
        parser.parse(json);
        result.set(true);
      }
    } catch (ParseException e) {
      result.set(false);
    } catch (IOException e) {
      throw new HiveException("Error using JSON parser: " + e.getMessage(), e);
    }
    return result;
  }

  @Override
  public String getDisplayString(String[] children) {
    return getStandardDisplayString("isjson", children);
  }
}
