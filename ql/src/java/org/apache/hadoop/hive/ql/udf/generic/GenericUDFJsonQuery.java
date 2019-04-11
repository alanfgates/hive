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
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.udf.generic.sqljsonpath.JsonQueryConverter;
import org.apache.hadoop.hive.ql.udf.generic.sqljsonpath.JsonSequenceConverter;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;

/**
 * JsonQuery is almost identical to JsonValue except that it only supports strings.  Thus it can be used to
 * easily fetch fragments of JSON.  The same thing can be achieved by calling JsonValue with a return type of string,
 * as it will handle all the casting for you.  That's in fact what this class does.  It extends GenericUDFJsonValue,
 * the only functionality it adds is to check that the default value (if present) is a string.
 */
@Description(name = "json_query",
    value = "_FUNC_(json_query, path_expression [, default_value [, on_empty [, on_error [, passing key, passing value...]]])\n",
    extended = "json_value, string\n" +
        "path_expression, constant string.\n" +
        "default_value optional, string: When on_empty or on_error are set to 'default' this value will" +
        " be returned as the default value.  Note that this does not have to be constant.  If not provided" +
        " or a null is passed, defaults to the empty string.\n" +
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
public class GenericUDFJsonQuery extends GenericUDFJsonValue {

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    if (arguments.length > RETURNING) {
      switch (arguments[RETURNING].getCategory()) {
        case PRIMITIVE:
          PrimitiveObjectInspector poi = (PrimitiveObjectInspector)arguments[RETURNING];
          switch (poi.getPrimitiveCategory()) {
            case STRING:
            case CHAR:
            case VARCHAR:
              break;

            default:
              throw new UDFArgumentTypeException(RETURNING, getUdfName() + " only returns String, Char, or Varchar");
          }
          break;

        default:
          throw new UDFArgumentTypeException(RETURNING, getUdfName() + " only returns String, Char, or Varchar");
      }
    }
    return super.initialize(arguments);
  }

  @Override
  protected JsonSequenceConverter getConverter(ObjectInspector oi) {
    return new JsonQueryConverter(oi);
  }

}
