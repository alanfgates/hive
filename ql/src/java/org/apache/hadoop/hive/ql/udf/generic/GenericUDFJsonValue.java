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

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.sqljsonpath.ErrorListener;
import org.apache.hadoop.hive.ql.udf.generic.sqljsonpath.JsonConversionException;
import org.apache.hadoop.hive.ql.udf.generic.sqljsonpath.JsonPathException;
import org.apache.hadoop.hive.ql.udf.generic.sqljsonpath.JsonSequence;
import org.apache.hadoop.hive.ql.udf.generic.sqljsonpath.JsonSequenceConverter;
import org.apache.hadoop.hive.ql.udf.generic.sqljsonpath.JsonValueParser;
import org.apache.hadoop.hive.ql.udf.generic.sqljsonpath.PathExecutor;
import org.apache.hadoop.hive.ql.udf.generic.sqljsonpath.PathParseResult;
import org.apache.hadoop.hive.ql.udf.generic.sqljsonpath.PathParser;
import org.apache.hadoop.hive.serde2.objectinspector.ConstantObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorConverter;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

@Description(name = "json_value",
             value = "_FUNC_(json_value, path_expression [, return_datatype [, on_empty [, on_error [, passing key, passing value...]]])\n",
             extended = "json_value, string\n" +
                        "path_expression, constant string.\n" +
                        "return_datatype optional: returning data type.  This should be an instance of the type to" +
                        " return.  It can be an instance of string, bigint, int, double, boolean," +
                        " array <any of these>, named_struct<any of these>.  If a type  other than these is desired," +
                        " the output can be cast.  When on_empty or on_error are set to 'default' this value will" +
                        " be returned as the default value.  Note that this does not have to be constant.  If not provided" +
                        " or a null is passed, defaults to string.\n" +
                        "on_empty optional, constant string: what to return when the result is empty.  Valid values" +
                        " are 'null', 'default', and 'error'.  'null' will return a NULL, this is the default." +
                        " 'default' will return the instance provided in return_datatype.  'error' will throw an" +
                        " error.  'error' is not recommended as JSON Path expression can often return empty results.\n" +
                        "on_error optional, constant string: what to return when the path expression" +
                        " encounters an error.  For example if the path expression '$.address.zip == 94100' encounters" +
                        " a zip code encoded as a String, it will throw an error.  This can be set to the same values" +
                        " as on_empty with the same results.  In general it is not recommended to set this to 'error'" +
                        " as JSON's loose type model makes such errors easy to generate.  The default is 'null'." +
                        " Note that this does not refer to errors in parsing the path expression" +
                        " itself.  If you passed a path expression of '$.name.nosuchfunc()' this would result in a" +
                        " compile time error since the path expression is invalid.\n" +
                        "passing key, passing value optional, passing key constant string, passing value one of " +
                        " string, int, long, double, boolean: passed in values to be used to fill in variables in" +
                        " the path expression.  For example, if you had a path expression '$.$keyname' you could pass in " +
                        " ..., 'keyname', name_col) to dynamically fill out the name of the key from another" +
                        " column in the row.  If this is not provided or NULL is passed no values will be plugged" +
                        " into the path expression.  Note that using this slows down the UDF since it has to translate" +
                        " the passed in values on every invocation.  Don't use this for stylistic reasons.  Only use" +
                        " it if you really require dynamic clauses in your path expression."
                        )
public class GenericUDFJsonValue extends GenericUDF {

  private static final Logger LOG = LoggerFactory.getLogger(GenericUDFJsonValue.class);

  private static final int JSON_VALUE = 0;
  private static final int PATH_EXPR = 1;
  private static final int RETURNING = 2;
  private static final int ON_EMPTY = 3;
  private static final int ON_ERROR = 4;
  private static final int START_PASSING = 5;

  @VisibleForTesting
  enum WhatToReturn { NULL, DEFAULT, ERROR }

  private PrimitiveObjectInspectorConverter.TextConverter jsonValueConverter;
  private transient PathParseResult parseResult;  // Antlr parse trees aren't serializable
  private String pathExpr;
  private PathExecutor pathExecutor;
  private JsonValueParser jsonParser;
  private WhatToReturn onEmpty;
  private WhatToReturn onError;
  private Map<String, ObjectInspector> passingOIs;
  // This OI servers as a template for the ObjectInspector we'll return.  It also is used to decode the default value
  // we've been passed.
  private ObjectInspector returnOI;
  private Object constantDefaultVal;
  private JsonSequenceConverter jsonConverter;

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    if (arguments.length < 2) {
      throw new UDFArgumentLengthException(getFuncName() + " requires at least json_value and path expression");
    }

    // Json Value, needs to be a string
    checkArgPrimitive(arguments, JSON_VALUE);
    jsonValueConverter = new PrimitiveObjectInspectorConverter.TextConverter((PrimitiveObjectInspector)arguments[JSON_VALUE]);

    // Path expression, should be a constant
    checkArgPrimitive(arguments, PATH_EXPR);
    pathExpr = getConstantStringValue(arguments, PATH_EXPR);
    if (pathExpr == null) {
      throw new UDFArgumentTypeException(PATH_EXPR, getFuncName() + " requires JSON path expression to be constant");
    }
    // We can't keep the parse expression because it doesn't serialize, but we still parse it here up front to make
    // sure it will work.
    parse();

    returnOI = arguments.length > RETURNING ? arguments[RETURNING]
        : PrimitiveObjectInspectorFactory.writableStringObjectInspector;

    onEmpty = getOnEmptyOrOnError(arguments, ON_EMPTY);
    onError = getOnEmptyOrOnError(arguments, ON_ERROR);

    // We only have to translate the default value if we might need to return it
    if (ObjectInspectorUtils.isConstantObjectInspector(returnOI) && (onEmpty == WhatToReturn.DEFAULT || onError == WhatToReturn.DEFAULT)) {
      ConstantObjectInspector coi = (ConstantObjectInspector) returnOI;
      constantDefaultVal = coi.getWritableConstantValue();
    }

    if (arguments.length > START_PASSING) {
      passingOIs = new HashMap<>();
      for (int i = START_PASSING; i < arguments.length; i += 2) {
        if (arguments.length <= i + 1) {
          throw new UDFArgumentLengthException("You must pass a matched set of passing variable names and values");
        }

        checkArgPrimitive(arguments, i);
        String keyName = getConstantStringValue(arguments, i);
        if (keyName == null) throw new UDFArgumentTypeException(i, "Passing variable name must be a constant string");
        passingOIs.put(keyName, arguments[i + 1]);
      }
    } else {
      passingOIs = Collections.emptyMap();
    }

    return returnOI;
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    Object jsonObj = arguments[JSON_VALUE].get();
    if (jsonObj == null) return null; // Per spec top of page 312, null input = null output
    JsonSequence jsonValue;
    String input = jsonValueConverter.convert(jsonObj).toString();
    if (LOG.isDebugEnabled()) LOG.debug("Evaluating with " + input);

    // The first time through we have to reparse, since we couldn't serialize the parse tree.  The second branch
    // the 'or' will never be activated in production, but without it unit tests fail since the UDF isn't
    // serialized between initialize and evaluate in unit tests.
    if (parseResult == null || pathExecutor == null) {
      parse();
      pathExecutor = new PathExecutor();
      jsonParser = new JsonValueParser(new ErrorListener());
      jsonConverter = new JsonSequenceConverter(returnOI);
    }

    Map<String, JsonSequence> passing = arguments.length > START_PASSING ?
        translatePassingObjects(arguments) : Collections.emptyMap();

    try {
      jsonValue = jsonParser.parse(input);
    } catch (JsonPathException|IOException e) {
      LOG.warn("Failed to parse input " + input + " as JSON", e);
      return getOnError(arguments, input, "Failed to parse input as JSON: " + e.getMessage());
    }

    try {
      JsonSequence result = pathExecutor.execute(parseResult, jsonValue, passing);
      if (LOG.isDebugEnabled()) LOG.debug("Received back: " + result.toString());
      return result.isEmpty() ? getOnEmpty(arguments, input) : jsonConverter.convert(result);
    } catch (JsonPathException e) {
      LOG.warn("Failed to execute path expression for input " + input, e);
      return getOnError(arguments, input, e.getMessage());
    } catch (JsonConversionException e) {
      LOG.info("Conversion failure", e);
      return getOnError(arguments, input, e.getMessage());
    }
  }

  @Override
  public String getDisplayString(String[] children) {
    return getStandardDisplayString("json_value", children);
  }

  private Object getOnError(DeferredObject[] arguments, String jsonValue, String error) throws HiveException {
    return onErrorOrEmpty(arguments, jsonValue, error, onError);
  }

  private Object getOnEmpty(DeferredObject[] arguments, String jsonValue) throws HiveException {
    return onErrorOrEmpty(arguments, jsonValue, "Result of path expression is empty", onEmpty);
  }

  private Object onErrorOrEmpty(DeferredObject[] arguments, String jsonValue, String error, WhatToReturn errorOrEmpty) throws HiveException {
    switch (errorOrEmpty) {
      case ERROR: throw new HiveException("Error for input: " + jsonValue + ": " + error);
      case NULL: return null;
      case DEFAULT: return getDefaultValue(arguments);
      default: throw new RuntimeException("programming error");
    }
  }

  private Object getDefaultValue(DeferredObject[] arguments) throws HiveException {
    if (constantDefaultVal != null) return constantDefaultVal;
    else return arguments[RETURNING].get();
  }

  // TODO could optimize this for constants, not sure it's worth it
  private Map<String, JsonSequence> translatePassingObjects(DeferredObject[] args) throws HiveException {
    Map<String, JsonSequence> passingObjs = new HashMap<>();
    for (int i = START_PASSING; i < args.length; i += 2) {
      assert i + 1 < args.length;
      String argName = PrimitiveObjectInspectorUtils.getString(args[i].get(), PrimitiveObjectInspectorFactory.writableStringObjectInspector);
      passingObjs.put(argName, JsonSequence.fromObjectInspector(passingOIs.get(argName), args[i + 1].get()));
    }
    return passingObjs;
  }

  private void parse() throws UDFArgumentException {
    try {
      PathParser parser = new PathParser();
      LOG.debug("Parsing " + pathExpr);
      parseResult = parser.parse(pathExpr);
    } catch (IOException | JsonPathException e) {
      LOG.info("Failed to parse JSON path exception: " + e.getMessage(), e);
      throw new UDFArgumentException("Failed to parse JSON path exception: " + e.getMessage());
    }
  }

  private WhatToReturn getOnEmptyOrOnError(ObjectInspector[] arguments, int index) throws UDFArgumentTypeException {
    if (arguments.length > index) {
      checkArgPrimitive(arguments, index);
      String str = getConstantStringValue(arguments, index);
      try {
        return WhatToReturn.valueOf(str.toUpperCase());
      } catch (IllegalArgumentException e) {
        throw new UDFArgumentTypeException(index, "Unknown onEmpty or onError specification " + str);
      }
    } else {
      return WhatToReturn.NULL;
    }
  }
}
