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
package org.apache.hadoop.hive.ql.udf.generic.sqljsonpath;

import java.util.List;
import java.util.Map;
import java.util.function.DoubleBinaryOperator;
import java.util.function.LongBinaryOperator;

/**
 * JsonSequence tracks the JSON value being returned from a section of the parse tree.  Since the value being returned
 * can change type as it moves through tree, JsonSequence can change its type as it goes along.
 *
 * Many methods are marked final to help the compiler inline methods, as we want operations on this to be as fast
 * possible since they'll be in the inner loop.
 *
 * Ideally we'd like to determine the types as part of the parse and not do the type branching for things like
 * arithmetic operations on every row.  Since JSON does not guarantee static types (eg, the key "salary" could be
 * an int in one record and a double in the next) this is not completely possible.  For constants and such the hope
 * is that the branch prediction on the chip will kick in and save us.  It's worth experimenting in the future to see
 * if this could be sped up by at least generating typed methods for constants.
 */
public class JsonSequence {

  private enum Type { LONG, DOUBLE, BOOL, STRING, LIST, OBJECT, NULL };

  /**
   * Represents the JSON null "key" : null
   */
  static JsonSequence nullJsonSequence = new JsonSequence();

  /**
   * Represents the JSON true "key" : true
   */
  static JsonSequence trueJsonSequence = new JsonSequence(true);

  /**
   * Represents the JSON false "key" : false
   */
  static JsonSequence falseJsonSequence = new JsonSequence(false);

  private Type type;
  private Object val;

  /**
   * Private because we don't want users creating new nulls
   */
  private JsonSequence() {
    type = Type.NULL;
    val = null;
  }

  /**
   * Private because we don't want users creating new true and false values
   * @param val true or false
   */
  private JsonSequence(boolean val) {
    this.val = val;
    type = Type.BOOL;
  }

  /**
   * Create a new JsonSequence that represents an integer value.
   * @param val integer value (as a long)
   */
  JsonSequence(long val) {
    this.val = val;
    type = Type.LONG;
  }

  /**
   * Creates a new JsonSequence that represents a decimal value.
   * @param val decimal value
   */
  JsonSequence(double val) {
    this.val = val;
    type = Type.DOUBLE;
  }

  /**
   * Creates a new JsonSequence that represents a string value.
   * @param val string value
   */
  JsonSequence(String val) {
    this.val = val;
    type = Type.STRING;
  }

  /**
   * Creates a new JsonSequence that represents an array
   * @param val array value (as a list)
   */
  JsonSequence(List<JsonSequence> val) {
    this.val = val;
    type = Type.LIST;
  }

  /**
   * Creates a new JsonSequence that represents a JSON object
   * @param val object value (as a map)
   */
  JsonSequence(Map<String, JsonSequence> val) {
    this.val = val;
    type = Type.OBJECT;
  }

  /**
   * Copy constructor.  This is a shallow copy, the underlying val Object is not copied.
   * @param template JsonSequence to use as a template.
   */
  JsonSequence(JsonSequence template) {
    this.val = template.val;
    this.type = template.type;
  }

  public final boolean isLong() {
    return type == Type.LONG;
  }

  public final boolean isDouble() {
    return type == Type.DOUBLE;
  }

  public final boolean isBool() {
    return type == Type.BOOL;
  }

  public final boolean isString() {
    return type == Type.STRING;
  }

  public final boolean isList() {
    return type == Type.LIST;
  }

  public final boolean isObject() {
    return type == Type.OBJECT;
  }

  public final boolean isNull() {
    return type == Type.NULL;
  }

  public final long asLong() {
    assert val instanceof Long;
    return (Long)val;
  }

  public final boolean asBool() {
    assert val instanceof Boolean;
    return (Boolean)val;
  }

  public final double asDouble() {
    assert val instanceof Double;
    return (Double)val;

  }
  public final String asString() {
    assert val instanceof String;
    return (String)val;
  }

  public final List<JsonSequence> asList() {
    assert val instanceof List;
    return (List<JsonSequence>)val;
  }

  public final Map<String, JsonSequence> asObject() {
    assert val instanceof Map;
    return (Map<String, JsonSequence>)val;
  }

  final void add(JsonSequence other, ErrorListener errorListener) {
    arithmetic(other, (left, right) -> left + right, (left, right) -> left + right, false, errorListener);
  }

  final void subtract(JsonSequence other, ErrorListener errorListener) {
    arithmetic(other, (left, right) -> left - right, (left, right) -> left - right, false, errorListener);
  }

  final void multiply(JsonSequence other, ErrorListener errorListener) {
    arithmetic(other, (left, right) -> left * right, (left, right) -> left * right, false, errorListener);
  }

  final void divide(JsonSequence other, ErrorListener errorListener) {
    arithmetic(other, (left, right) -> left / right, (left, right) -> left / right, true, errorListener);
  }

  final void modulo(JsonSequence other, ErrorListener errorListener) {
    switch (type) {
      case LONG:
        switch (other.type) {
          case LONG:
            if (other.asLong() == 0) {
              errorListener.runtimeError("Division by zero");
              setNull();
            } else {
              val = asLong() % other.asLong();
            }
            break;

          default:
            errorListener.semanticError("You cannot do mod on a " + other.type.name().toLowerCase());
            setNull();
            break;
        }
        break;

      default:
        errorListener.semanticError("You cannot do mod on a " + type.name().toLowerCase());
        setNull();
        break;
    }
  }

  final void negate(ErrorListener errorListener) {
    switch (type) {
      case LONG:
        val = asLong() * -1;
        break;

      case DOUBLE:
        val = asDouble() * -1.0;
        break;

      default:
        errorListener.semanticError("You cannot do arithmetic on a " + type.name().toLowerCase());
        setNull();
        break;
    }
  }

  private void arithmetic(JsonSequence other, LongBinaryOperator longOp, DoubleBinaryOperator doubleOp, boolean zeroCheck, ErrorListener errorListener) {
    switch (type) {
      case LONG:
        switch (other.type) {
          case LONG:
            // TODO fix zero check and is null checks
            if (zeroCheck && other.asLong() == 0) {
              errorListener.runtimeError("Division by zero");
              setNull();
            } else {
              val = longOp.applyAsLong(asLong(), other.asLong());
            }
            break;

          case DOUBLE:
            if (zeroCheck && other.asDouble() == 0.0) {
              errorListener.runtimeError("Division by zero");
              setNull();
            } else {
              type = Type.DOUBLE;
              val = doubleOp.applyAsDouble((double)asLong(), other.asDouble());
            }
            break;

          default:
            errorListener.semanticError("You cannot do arithmetic on a " + other.type.name().toLowerCase());
            setNull();
            break;
        }
        break;

      case DOUBLE:
        switch (other.type) {
          case LONG:
            if (zeroCheck && other.asLong() == 0) {
              errorListener.runtimeError("Division by zero");
              setNull();
            } else {
              val = doubleOp.applyAsDouble(asDouble(), (double)other.asLong());
            }
            break;

          case DOUBLE:
            if (zeroCheck && other.asDouble() == 0.0) {
              errorListener.runtimeError("Division by zero");
              setNull();
            } else {
              val = doubleOp.applyAsDouble(asDouble(), other.asDouble());
            }
            break;

          default:
            errorListener.semanticError("You cannot do arithmetic on a " + other.type.name().toLowerCase());
            setNull();
            break;
        }
        break;

      default:
        errorListener.semanticError("You cannot do arithmetic on a " + type.name().toLowerCase());
        setNull();
        break;
    }
  }

  private void setNull() {
    type = Type.NULL;
    val = null;
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof JsonSequence)) return false;
    JsonSequence other = (JsonSequence)obj;
    if (isNull() && other.isNull()) return true;
    else return type == other.type && val.equals(other.val);
  }

  @Override
  public String toString() {
    return prettyPrint(0);
  }

  private String prettyPrint(int in) {
    if (val == null) return "null";
    StringBuilder buf = new StringBuilder();
    switch (type) {
      case LONG:
      case DOUBLE:
      case BOOL:
      case STRING:
        return val.toString();

      case LIST:
        indent(buf, in);
        buf.append("[\n");
        boolean first = true;
        for (JsonSequence element : asList()) {
          if (first) first = false;
          else buf.append(",\n");
          indent(buf, in);
          buf.append(element.prettyPrint(in + 1));
        }
        buf.append("\n");
        indent(buf, in);
        buf.append("]");
        return buf.toString();

      case OBJECT:
        indent(buf, in);
        buf.append("{\n");
        first = true;
        for (Map.Entry<String, JsonSequence> entry : asObject().entrySet()) {
          if (first) first = false;
          else buf.append(",\n");
          indent(buf, in);
          buf.append("\"")
              .append(entry.getKey())
              .append("\" : ")
              .append(entry.getValue().prettyPrint(in + 1));
        }
        buf.append("\n");
        indent(buf, in);
        buf.append("}");
        return buf.toString();

      default:
        throw new RuntimeException("Programming error");
    }
  }

  private void indent(StringBuilder buf, int in) {
    for (int i = 0; i < in; i++) buf.append("  ");
  }

}
