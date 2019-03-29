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
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.sqljsonpath.AbstractJsonSequenceObjectInspector;
import org.apache.hadoop.hive.ql.udf.generic.sqljsonpath.BooleanJsonSequenceObjectInspector;
import org.apache.hadoop.hive.ql.udf.generic.sqljsonpath.DoubleJsonSequenceObjectInspector;
import org.apache.hadoop.hive.ql.udf.generic.sqljsonpath.EmptyOrErrorBehavior;
import org.apache.hadoop.hive.ql.udf.generic.sqljsonpath.ErrorListener;
import org.apache.hadoop.hive.ql.udf.generic.sqljsonpath.JsonPathException;
import org.apache.hadoop.hive.ql.udf.generic.sqljsonpath.JsonSequence;
import org.apache.hadoop.hive.ql.udf.generic.sqljsonpath.JsonValueParser;
import org.apache.hadoop.hive.ql.udf.generic.sqljsonpath.ListJsonSequenceObjectInspector;
import org.apache.hadoop.hive.ql.udf.generic.sqljsonpath.LongJsonSequenceObjectInspector;
import org.apache.hadoop.hive.ql.udf.generic.sqljsonpath.PathExecutor;
import org.apache.hadoop.hive.ql.udf.generic.sqljsonpath.PathParseResult;
import org.apache.hadoop.hive.ql.udf.generic.sqljsonpath.PathParser;
import org.apache.hadoop.hive.ql.udf.generic.sqljsonpath.StringJsonSequenceObjectInspector;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorConverter;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

@Description(name = "json_value",
             value = "_FUNC_(json_value, path_expression [PASSING id = val[, id = val...]] [RETURNING datatype]" +
                     " [ERROR|NULL|DEFAULT value ON EMPTY] [ERROR|NULL|DEFAULT value ON ERROR] )",
             extended = "json_value is assumed to be a string type." +
                        " path_expression is assumed to be constant and only parsed the first time the UDF is called." +
                        " PASSING is a set of key/value pairs providing values variables in the JSON. " +
                        " RETURNING defines what datatype to return; valid options are boolean, bigint, int, double," +
                        " string, char, varchar, array, and struct; it defaults to STRING if not provided." +
                        " ON EMPTY defines what to return if the result is empty (that is, the JSON path expression" +
                        " does not match the provided JSON); it defaults to NULL. " +
                        " ON ERROR defines what to return if there is an error; the error can occur in parsing the" +
                        " JSON, or in parsing or executing the path expression; it defaults to NULL.")
public class GenericUDFJsonValue extends GenericUDF {

  private static final Logger LOG = LoggerFactory.getLogger(GenericUDFJsonValue.class);

  private static final int JSON_VALUE = 0;
  private static final int PATH_EXPR = 1;
  private static final int RETURNING = 2;
  private static final int ON_EMPTY = 3;
  private static final int ON_EMPTY_DEFAULT_VAL = 4;
  private static final int ON_ERROR = 5;
  private static final int ON_ERROR_DEFAULT_VAL = 6;

  private PrimitiveObjectInspectorConverter.TextConverter jsonValueConverter;
  private PathParseResult parseResult;
  private PathExecutor pathExecutor;
  private JsonValueParser jsonParser;
  private EmptyOrErrorBehavior onEmpty;
  private ObjectInspector onEmptyObjInspector;
  private EmptyOrErrorBehavior onError;
  private ObjectInspector onErrorObjInspector;
  private Map<Integer, ObjectPair<String, ObjectInspector>> passingOIs;

  // The argument layout for this is a mess due to the possibility of passing any number of key/value pairs in the
  // PASSING clause.
  // arg 0: json_value, assumed to be a string
  // arg 1: path_expression, assumed to be a constant string
  // arg 2: returning data type, assumed to be a constant string
  // arg 3: action on empty, assumed to be a constant string
  // arg 4: value for default value on empty, can be null if error or null was chosen as action on empty
  // arg 5: action on error, assumed to be a constant string
  // arg 6: value for default value on error, can be null if error or null was chosen as action on error
  // arg 7... optional, if present odd arguments will be taken as keys (and assumed to be string constants) and
  //          even arguments assumed to be value mapped to those keys (can be of any type).
  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    checkArgsSize(arguments, 7, 100);

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

    // For now only support primitive types, and list<primitive type> (ie, no structs,  no embedded lists).
    checkArgPrimitive(arguments, RETURNING);
    String returnType = getConstantStringValue(arguments, RETURNING);
    if (returnType == null) {
      throw new UDFArgumentTypeException(RETURNING, getFuncName() + " requires RETURNING specification to be constant");
    }
    AbstractJsonSequenceObjectInspector resultObjectInspector = translateTypeNameToObjectInspector(returnType);

    // ON EMPTY
    checkArgPrimitive(arguments, ON_EMPTY);
    String onEmptyStr = getConstantStringValue(arguments, ON_EMPTY);
    if (onEmptyStr == null) {
      throw new UDFArgumentTypeException(ON_EMPTY, getFuncName() + " requires ON EMPTY specification to be constant");
    }
    try {
      onEmpty = EmptyOrErrorBehavior.valueOf(onEmptyStr);
    } catch (IllegalArgumentException e) {
      throw new UDFArgumentException("Unrecognized ON EMPTY behavior " + onEmptyStr +
          ", options are NULL, ERROR, DEFAULT <value>");
    }
    if (onEmpty == EmptyOrErrorBehavior.DEFAULT) {
      onEmptyObjInspector = arguments[ON_EMPTY_DEFAULT_VAL];
    }

    // ON ERROR
    checkArgPrimitive(arguments, ON_ERROR);
    String onErrorStr = getConstantStringValue(arguments, ON_ERROR);
    if (onErrorStr == null) {
      throw new UDFArgumentTypeException(ON_ERROR, getFuncName() + " requires ON ERROR specification to be constant");
    }
    try {
      onError = EmptyOrErrorBehavior.valueOf(onErrorStr);
    } catch (IllegalArgumentException e) {
      throw new UDFArgumentException("Unrecognized ON ERROR behavior " + onErrorStr +
          ", options are NULL, ERROR, DEFAULT <value>");
    }
    if (onError == EmptyOrErrorBehavior.DEFAULT) {
      onErrorObjInspector = arguments[ON_ERROR_DEFAULT_VAL];
    }

    // PASSING
    for (int arg = 7; arg < arguments.length; arg += 2) {
      passingOIs = new HashMap<>();
      checkArgPrimitive(arguments, arg);
      String key = getConstantStringValue(arguments, arg);
      if (key == null) {
        throw new UDFArgumentTypeException(arg, getFuncName() + " requires keys in the PASSING clause to be constant strings");
      }
      passingOIs.put(arg + 1, new ObjectPair<>(key, arguments[arg + 1]));
    }

    // Need to return whatever object inspector we came up with
    return resultObjectInspector;
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    Object jsonObj = arguments[JSON_VALUE].get();
    if (jsonObj == null) return null; // Per spec top of page 312, null input = null output
    JsonSequence jsonValue;
    String input = jsonValueConverter.convert(jsonObj).toString();
    if (LOG.isDebugEnabled()) LOG.debug("Evaluating with " + input);
    try {
      jsonValue = jsonParser.parse(input);
    } catch (JsonPathException|IOException e) {
      LOG.warn("Failed to parse input " + input + " as JSON", e);
      return onErrorResult(arguments[ON_ERROR_DEFAULT_VAL].get(), input, "Failed to parse input as JSON: " + e.getMessage());
    }

    // Look for args for each item in the passing clause
    Map<String, JsonSequence> passing;
    if (passingOIs != null) {
      passing = new HashMap<>();
      for (Map.Entry<Integer, ObjectPair<String, ObjectInspector>> entry : passingOIs.entrySet()) {
        if (arguments.length < entry.getKey()) {
          LOG.warn("Insufficient number of keys/values, returning ON ERROR result");
          return onErrorResult(arguments[ON_ERROR_DEFAULT_VAL].get(), input, "Passing expected at least " +
              entry.getKey() + " values, but only found " + arguments.length);
        }
        passing.put(entry.getValue().getFirst(), argToJsonSequence(arguments[entry.getKey()].get(), entry.getValue().getSecond(), entry.getKey()));
      }
    } else {
      passing = Collections.emptyMap();
    }

    try {
      JsonSequence result = pathExecutor.execute(parseResult, jsonValue, passing);
      if (LOG.isDebugEnabled()) LOG.debug("Received back: " + result.toString());
      if (result.isEmpty()) {
        return onEmptyResult(arguments[ON_EMPTY_DEFAULT_VAL].get(), input);
      } else {
        return result;
      }
    } catch (JsonPathException e) {
      LOG.error("Failed to execute path expression for input " + input, e);
      return onErrorResult(arguments[ON_ERROR_DEFAULT_VAL].get(), input, e.getMessage());
    }
  }

  @Override
  public String getDisplayString(String[] children) {
    return getStandardDisplayString("json_value", children);
  }

  private AbstractJsonSequenceObjectInspector translateTypeNameToObjectInspector(String typeName) throws UDFArgumentTypeException {
    typeName = typeName.toLowerCase();

    if (typeName.startsWith(serdeConstants.LIST_TYPE_NAME)) {
      AbstractJsonSequenceObjectInspector subObjInspector = translateTypeNameToObjectInspector(typeName.substring(serdeConstants.LIST_TYPE_NAME.length() + 1));
      return new ListJsonSequenceObjectInspector(subObjInspector);
    } else if (typeName.equals(serdeConstants.BOOLEAN_TYPE_NAME)) {
      return new BooleanJsonSequenceObjectInspector();
    } else if (typeName.equals(serdeConstants.BIGINT_TYPE_NAME)) {
      return new LongJsonSequenceObjectInspector();
    } else if (typeName.equals(serdeConstants.DOUBLE_TYPE_NAME)) {
      return new DoubleJsonSequenceObjectInspector();
    } else if (typeName.equals(serdeConstants.STRING_TYPE_NAME)) {
      return new StringJsonSequenceObjectInspector();
    } else {
      throw new UDFArgumentTypeException(RETURNING, getFuncName() + " RETURNING currently only supports " +
          serdeConstants.BOOLEAN_TYPE_NAME + ", " +
          serdeConstants.BIGINT_TYPE_NAME + ", " +
          serdeConstants.DOUBLE_TYPE_NAME + ", " +
          serdeConstants.STRING_TYPE_NAME + ", or " +
          serdeConstants.LIST_TYPE_NAME + " of one of the above");
    }
  }

  // TODO - in most cases the default value will likely be constant.  We should be checking for that and
  // parsing it once rather than doing the conversion on every error or empty.  Note that we can't assume
  // it's constant as non-constants are allowed here.
  private Object onErrorResult(Object defaultVal, String jsonValue, String error) throws HiveException {
    switch (onError) {
      case ERROR: throw new HiveException("Error for input: " + jsonValue + ": " + error);
      case NULL: return null;
      case DEFAULT: return argToJsonSequence(defaultVal, onErrorObjInspector, ON_ERROR_DEFAULT_VAL);
      default: throw new RuntimeException("programming error");
    }
  }

  private Object onEmptyResult(Object defaultVal, String jsonValue) throws HiveException {
    switch (onEmpty) {
      case ERROR: throw new HiveException("Empty result for input: " + jsonValue);
      case NULL: return null;
      case DEFAULT: return argToJsonSequence(defaultVal, onEmptyObjInspector, ON_EMPTY_DEFAULT_VAL);
      default: throw new RuntimeException("programming error");
    }
  }

  private JsonSequence argToJsonSequence(Object arg, ObjectInspector objInspector, int argNum) throws UDFArgumentTypeException {
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
