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
import java.util.function.BinaryOperator;
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

  private static JsonSequence nullJsonSequence;
  private enum Type { LONG, DOUBLE, BOOL, STRING, LIST, OBJECT };

  private final ErrorListener errorListener;

  private Type type;
  private Object val;

  JsonSequence(long val, ErrorListener errorListener) {
    this.val = val;
    type = Type.LONG;
    this.errorListener = errorListener;
  }

  JsonSequence(double val, ErrorListener errorListener) {
    this.val = val;
    type = Type.DOUBLE;
    this.errorListener = errorListener;
  }

  JsonSequence(boolean val, ErrorListener errorListener) {
    this.val = val;
    type = Type.BOOL;
    this.errorListener = errorListener;
  }

  JsonSequence(String val, ErrorListener errorListener) {
    this.val = val;
    type = Type.STRING;
    this.errorListener = errorListener;
  }

  JsonSequence(List<JsonSequence> val, ErrorListener errorListener) {
    this.val = val;
    type = Type.LIST;
    this.errorListener = errorListener;
  }

  JsonSequence(Map<String, JsonSequence> val, ErrorListener errorListener) {
    this.val = val;
    type = Type.OBJECT;
    this.errorListener = errorListener;
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
    return val == null;
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

  final void add(JsonSequence other) {
    arithmetic(other, (left, right) -> left + right, (left, right) -> left + right, false);
  }

  final void subtract(JsonSequence other) {
    arithmetic(other, (left, right) -> left - right, (left, right) -> left - right, false);
  }

  final void multiply(JsonSequence other) {
    arithmetic(other, (left, right) -> left * right, (left, right) -> left * right, false);
  }

  final void divide(JsonSequence other) {
    arithmetic(other, (left, right) -> left / right, (left, right) -> left / right, true);
  }

  final void modulo(JsonSequence other) {
    switch (type) {
      case LONG:
        // TODO not sure this is right.  Should null carry SQL like semantics where it's viral.  Should it be
        // treated like a 0?  Something else?
        if (isNull() || other.isNull()) {
          val = null;
        } else {
          switch (other.type) {
            case LONG:
              val = (Long)val % (Long)other.val;
              break;

            default:
              errorListener.semanticError("You cannot do mod on a " + other.type.name().toLowerCase());
              break;
          }
        }
        break;

      default:
        errorListener.semanticError("You cannot do mod on a " + type.name().toLowerCase());
        break;
    }
  }

  final void negate() {
    switch (type) {
      case LONG:
        if (!isNull()) {
          val = (Long) val * -1L;
        }
        break;

      case DOUBLE:
        if (!isNull()) {
          val = (Double) val * -1.0;
        }
        break;

      default:
        errorListener.semanticError("You cannot do arithmetic on a " + type.name().toLowerCase());
        break;
    }
  }

  final void not(){
    switch (type) {
      case BOOL:
        if (!isNull()) {
          val = !((Boolean)val);
        }
        break;

      default:
        errorListener.semanticError("You cannot do logical operation on " + type.name());
        break;
    }
  }

  final void and(JsonSequence other) {
    logic(other, (left, right) -> left && right);

  }
  final void or(JsonSequence other) {
    logic(other, (left, right) -> left || right);
  }

  private void arithmetic(JsonSequence other, LongBinaryOperator longOp, DoubleBinaryOperator doubleOp, boolean zeroCheck) {
    switch (type) {
      case LONG:
        if (isNull() || other.isNull()) {
          val = null;
        } else {
          switch (other.type) {
            case LONG:
              val = longOp.applyAsLong((Long)val, (Long)other.val);
              break;

            case DOUBLE:
              type = Type.DOUBLE;
              if (zeroCheck && (Double)other.val == 0) {
                errorListener.runtimeError("Division by zero");
              } else {
                val = doubleOp.applyAsDouble(((Long) val).doubleValue(), (Double) other.val);
              }
              break;

            default:
              errorListener.semanticError("You cannot do arithmetic on a " + other.type.name().toLowerCase());
              break;
          }
        }
        break;

      case DOUBLE:
        if (isNull() || other.isNull()) {
          val = null;
        } else {
          switch (other.type) {
            case LONG:
              val = doubleOp.applyAsDouble((Double)val, ((Long)other.val).doubleValue());
              break;

            case DOUBLE:
              val = doubleOp.applyAsDouble((Double)val, (Double)other.val);
              break;

            default:
              errorListener.semanticError("You cannot do arithmetic on a " + other.type.name().toLowerCase());
              break;
          }
        }
        break;

      default:
        errorListener.semanticError("You cannot do arithmetic on a " + type.name().toLowerCase());
        break;
    }
  }

  private void logic(JsonSequence other, BinaryOperator<Boolean> op) {
    switch (type) {
      case BOOL:
        if (isNull() || other.isNull()) {
          val = null;
        } else {
          switch (other.type) {
            case BOOL:
              val = op.apply((Boolean)val, (Boolean)other.val);
              break;

            default:
              errorListener.semanticError("You cannot do logical operation on a " + other.type.name().toLowerCase());
              break;
          }
        }
        break;

      default:
        errorListener.semanticError("You cannot do logical operation on a " + type.name().toLowerCase());
        break;
    }
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

  public static JsonSequence nullValue(ErrorListener listener) {
    if (nullJsonSequence == null) {
      nullJsonSequence = new JsonSequence((String)null, listener);
    }
    return nullJsonSequence;
  }

}
