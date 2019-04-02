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
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.sqljsonpath.ErrorListener;
import org.apache.hadoop.hive.ql.udf.generic.sqljsonpath.JsonPathException;
import org.apache.hadoop.hive.ql.udf.generic.sqljsonpath.JsonSequence;
import org.apache.hadoop.hive.ql.udf.generic.sqljsonpath.JsonValueParser;
import org.apache.hadoop.hive.ql.udf.generic.sqljsonpath.ListJsonSequenceObjectInspector;
import org.apache.hadoop.hive.ql.udf.generic.sqljsonpath.PathExecutor;
import org.apache.hadoop.hive.ql.udf.generic.sqljsonpath.PathParseResult;
import org.apache.hadoop.hive.ql.udf.generic.sqljsonpath.PathParser;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.objectinspector.ConstantObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
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
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

// TODO fix description
@Description(name = "json_value",
             value = "_FUNC_(json_value, path_expression [, return_datatype [, default_value [, error_on_error [, passing key, passing value...]]])\n",
             extended = "json_value, string\n" +
                        "path_expression, constant string.\n" +
                        "return_datatype optional, constant string: returning data type.  Supported types are 'string', 'int', " +
                        " 'long', 'double', 'boolean', 'array<string|int|long|double|boolean>'.  If you want a type" +
                        " other than these, you can cast the output.  If not provided or a null is passed," +
                        " defaults to string.\n" +
                        "default_value optional, must match return type, need not be constant: default value to return" +
                        " when the result of the expression is empty.  If not provided this defaults to NULL.\n" +
                        "error_on_error optional, constant boolean: whether to raise an error when the path expression" +
                        " encounters an error.  For example if the path expression '$.address.zip == 94100' encounters" +
                        " a zip code encoded as a String, it will throw an error.  In general it is not recommended" +
                        " to set this as JSON's loose type model makes such errors easy to generate.  This value" +
                        " defaults to false.  Note that this does not refer to errors in parsing the path expression" +
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
  private static final int DEFAULT_VAL = 3;
  private static final int ERROR_ON_ERROR = 4;
  private static final int START_PASSING = 5;
  private static final Pattern listSubtypePattern = Pattern.compile("[^<]+<([^>]+)>");

  private PrimitiveObjectInspectorConverter.TextConverter jsonValueConverter;
  private PathParseResult parseResult;
  private PathExecutor pathExecutor;
  private JsonValueParser jsonParser;
  private boolean errorOnError;
  private Map<String, ObjectInspector> passingOIs;
  private PrimitiveObjectInspector defaultValOI;
  private JsonSequence jsonDefaultVal;
  private ObjectInspector resultObjectInspector;
  private Function<JsonSequence, Object> resultResolver;

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    if (arguments.length < 2) {
      throw new UDFArgumentLengthException(getFuncName() + " requires at least json_value and path expression");
    }

    pathExecutor = new PathExecutor();
    jsonParser = new JsonValueParser(new ErrorListener());

    // Json Value, needs to be a string
    checkArgPrimitive(arguments, JSON_VALUE);
    jsonValueConverter = new PrimitiveObjectInspectorConverter.TextConverter((PrimitiveObjectInspector)arguments[JSON_VALUE]);

    // Path expression, should be a constant
    checkArgPrimitive(arguments, PATH_EXPR);
    String pathExpr = getConstantStringValue(arguments, PATH_EXPR);
    if (pathExpr == null) {
      throw new UDFArgumentTypeException(PATH_EXPR, getFuncName() + " requires JSON path expression to be constant");
    }
    try {
      PathParser parser = new PathParser();
      LOG.debug("Parsing " + pathExpr);
      parseResult = parser.parse(pathExpr);
    } catch (IOException | JsonPathException e) {
      LOG.info("Failed to parse JSON path exception: " + e.getMessage(), e);
      throw new UDFArgumentException("Failed to parse JSON path exception: " + e.getMessage());
    }

    String returnType;
    if (arguments.length > RETURNING) {
      checkArgPrimitive(arguments, RETURNING);
      returnType = getConstantStringValue(arguments, RETURNING);
    } else {
      returnType = serdeConstants.STRING_TYPE_NAME;
    }

    if (arguments.length > DEFAULT_VAL) {
      checkArgPrimitive(arguments, DEFAULT_VAL);
      defaultValOI = (PrimitiveObjectInspector)arguments[DEFAULT_VAL];
      if (defaultValOI instanceof ConstantObjectInspector) {
        jsonDefaultVal = JsonSequence.fromWritable(((ConstantObjectInspector)defaultValOI).getWritableConstantValue());
      }
    }

    errorOnError = arguments.length > ERROR_ON_ERROR ? getConstantBooleanValue(arguments, ERROR_ON_ERROR) : false;

    // Can't translate until we know the value of errorOnError
    translateObjectInspector(returnType);

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

    return resultObjectInspector;
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    Object jsonObj = arguments[JSON_VALUE].get();
    if (jsonObj == null) return null; // Per spec top of page 312, null input = null output
    JsonSequence jsonValue;
    String input = jsonValueConverter.convert(jsonObj).toString();
    if (LOG.isDebugEnabled()) LOG.debug("Evaluating with " + input);

    Map<String, JsonSequence> passing = arguments.length > START_PASSING ?
        translatePassingObjects(arguments) : Collections.emptyMap();

    try {
      jsonValue = jsonParser.parse(input);
    } catch (JsonPathException|IOException e) {
      LOG.warn("Failed to parse input " + input + " as JSON", e);
      return onErrorResult(getDefaultValue(arguments), input, "Failed to parse input as JSON: " + e.getMessage());
    }

    try {
      JsonSequence result = pathExecutor.execute(parseResult, jsonValue, passing);
      if (LOG.isDebugEnabled()) LOG.debug("Received back: " + result.toString());
      JsonSequence toReturn = result.isEmpty() ? getDefaultValue(arguments) : result;
      return resultResolver.apply(toReturn);
    } catch (JsonPathException e) {
      LOG.warn("Failed to execute path expression for input " + input, e);
      return onErrorResult(getDefaultValue(arguments), input, e.getMessage());
    }
  }

  @Override
  public String getDisplayString(String[] children) {
    return getStandardDisplayString("json_value", children);
  }

  private JsonSequence getDefaultValue(DeferredObject[] arguments) {
    if (arguments.length <= DEFAULT_VAL) return JsonSequence.nullJsonSequence;
    if (jsonDefaultVal != null) return jsonDefaultVal;
    return JsonSequence.fromWritable(defaultValOI.getPrimitiveWritableObject(arguments[DEFAULT_VAL]));
  }

  private void translateObjectInspector(String returnType) throws UDFArgumentException {
    returnType = returnType.toLowerCase().trim();

    if (returnType.equals(serdeConstants.STRING_TYPE_NAME)) {
      resultResolver = jsonSequence -> jsonSequence.castToString(errorOnError);
      resultObjectInspector = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
    } else if (returnType.equals(serdeConstants.INT_TYPE_NAME)) {
      resultResolver = jsonSequence -> jsonSequence.castToInt(errorOnError);
      resultObjectInspector = PrimitiveObjectInspectorFactory.javaIntObjectInspector;
    } else if (returnType.equals(serdeConstants.BIGINT_TYPE_NAME)) {
      resultResolver = jsonSequence -> jsonSequence.castToLong(errorOnError);
      resultObjectInspector = PrimitiveObjectInspectorFactory.javaLongObjectInspector;
    } else if (returnType.equals(serdeConstants.DOUBLE_TYPE_NAME)) {
      resultResolver = jsonSequence -> jsonSequence.castToDouble(errorOnError);
      resultObjectInspector = PrimitiveObjectInspectorFactory.javaDoubleObjectInspector;
    } else if (returnType.equals(serdeConstants.BOOLEAN_TYPE_NAME)) {
      resultResolver = jsonSequence -> jsonSequence.castToBool(errorOnError);
      resultObjectInspector = PrimitiveObjectInspectorFactory.javaBooleanObjectInspector;
    } else if (returnType.startsWith(serdeConstants.LIST_TYPE_NAME)) {
      translateObjectInspector(getListSubtype(returnType));
      resultObjectInspector = new ListJsonSequenceObjectInspector(resultObjectInspector, resultResolver);
      resultResolver = jsonSequence -> jsonSequence.castToList(errorOnError);
    } else {
      throw new UDFArgumentTypeException(RETURNING, getFuncName() +
          " can return primitive type of list of primitive types");
    }
  }

  private String getListSubtype(String returnType) throws UDFArgumentException {
    Matcher matcher = listSubtypePattern.matcher(returnType);
    if (!matcher.find()) {
      throw new UDFArgumentException("Unknown return type " + returnType);
    }
    return matcher.group(1);
  }

  private Object onErrorResult(JsonSequence defaultVal, String jsonValue, String error) throws HiveException {
    if (errorOnError) throw new HiveException("Error for input: " + jsonValue + ": " + error);
    return resultResolver.apply(defaultVal);
  }

  private Map<String, JsonSequence> translatePassingObjects(DeferredObject[] args) throws HiveException {
    Map<String, JsonSequence> passingObjs = new HashMap<>();
    for (int i = START_PASSING; i < args.length; i += 2) {
      assert i + 1 < args.length;
      String argName = PrimitiveObjectInspectorUtils.getString(args[i].get(), PrimitiveObjectInspectorFactory.writableStringObjectInspector);
      passingObjs.put(argName, objToJsonSequence(args[i + 1].get(), passingOIs.get(argName), i));
    }
    return passingObjs;
  }

  private JsonSequence objToJsonSequence(Object arg, ObjectInspector objInspector, int argNum) throws UDFArgumentTypeException {
    if (arg == null) return JsonSequence.nullJsonSequence;

    if (objInspector.getCategory() != ObjectInspector.Category.PRIMITIVE) {
      throw new IllegalArgumentException("Can only create a JsonSequence from a primitive type");
    }
    PrimitiveObjectInspector poi = (PrimitiveObjectInspector)objInspector;
    switch (poi.getPrimitiveCategory()) {
      case BOOLEAN:
        return PrimitiveObjectInspectorUtils.getBoolean(arg, poi) ? JsonSequence.trueJsonSequence :
            JsonSequence.falseJsonSequence;

      case INT:
      case LONG:
        return new JsonSequence(PrimitiveObjectInspectorUtils.getLong(arg, poi));

      case FLOAT:
      case DOUBLE:
      case DECIMAL:
        return new JsonSequence(PrimitiveObjectInspectorUtils.getDouble(arg, poi));

      case STRING:
      case VARCHAR:
      case CHAR:
        return new JsonSequence(PrimitiveObjectInspectorUtils.getString(arg, poi));

      default:
        throw new UDFArgumentTypeException(argNum, "Not a supported type for the PASSING clause " + objInspector.getTypeName());
    }

  }
}
