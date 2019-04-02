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

import org.antlr.v4.runtime.ParserRuleContext;
import org.apache.hadoop.hive.common.ObjectPair;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.DoubleBinaryOperator;
import java.util.function.Function;
import java.util.function.LongBinaryOperator;

/**
 * JsonSequence tracks the JSON value being returned from a section of the parse tree.  Since the value being returned
 * can change type as it moves through tree, JsonSequence can change its type as it goes along.
 *
 * The class is final to help the compiler inline methods, as we want operations on this to be as fast
 * possible since they'll be in the inner loop.
 *
 * Ideally we'd like to determine the types as part of the parse and not do the type branching for things like
 * arithmetic operations on every row.  Since JSON does not guarantee static types (eg, the key "salary" could be
 * an int in one record and a double in the next) this is not completely possible.  For constants and such the hope
 * is that the branch prediction on the chip will kick in and save us.  It's worth experimenting in the future to see
 * if this could be sped up by at least generating typed methods for constants.
 */
public final class JsonSequence {

  enum Type {
    LONG,
    DOUBLE,
    BOOL,
    STRING,
    LIST,   // Represents JSON array, but called list since it is a list in Java and have asArray() return a List just too confusing
    OBJECT, // Represents JSON object, {}
    NULL,   // Represents the JSON null literal
    EMPTY_RESULT // This is not a JSON type.  It represents the result of Path query that did not match anything.
                 // It is returned separately from null so that the caller can decide how to deal with errors.
  }
  private static final Logger LOG = LoggerFactory.getLogger(JsonSequence.class);

  private static final Map<String, ListObjectInspector> LIST_OBJECT_INSPECTOR_CACHE = new HashMap<>();

  /**
   * Represents the JSON null "key" : null
   */
  public static final JsonSequence nullJsonSequence = new JsonSequence(Type.NULL);

  public static final JsonSequence emptyResult = new JsonSequence(Type.EMPTY_RESULT);

  /**
   * Represents the JSON true "key" : true
   */
  public static final JsonSequence trueJsonSequence = new JsonSequence(true);

  /**
   * Represents the JSON false "key" : false
   */
  public static final JsonSequence falseJsonSequence = new JsonSequence(false);

  private Type type;
  private Object val;

  /**
   * Private because we don't want users creating new nulls
   */
  private JsonSequence(Type type) {
    this.type = type;
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
  public JsonSequence(long val) {
    this.val = val;
    type = Type.LONG;
  }

  /**
   * Creates a new JsonSequence that represents a decimal value.
   * @param val decimal value
   */
  public JsonSequence(double val) {
    this.val = val;
    type = Type.DOUBLE;
  }

  /**
   * Creates a new JsonSequence that represents a string value.
   * @param val string value
   */
  public JsonSequence(String val) {
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

  public static JsonSequence fromWritable(Object writable) {
    if (writable instanceof Text) return new JsonSequence(writable.toString());
    if (writable instanceof LongWritable) return new JsonSequence(((LongWritable)writable).get());
    if (writable instanceof IntWritable) return new JsonSequence(((IntWritable)writable).get());
    if (writable instanceof DoubleWritable) return new JsonSequence(((DoubleWritable)writable).get());
    if (writable instanceof BooleanWritable) return new JsonSequence(((BooleanWritable)writable).get());
    else return null;
  }

  public boolean isLong() {
    return type == Type.LONG;
  }

  public boolean isDouble() {
    return type == Type.DOUBLE;
  }

  public boolean isBool() {
    return type == Type.BOOL;
  }

  public boolean isString() {
    return type == Type.STRING;
  }

  public boolean isList() {
    return type == Type.LIST;
  }

  public boolean isObject() {
    return type == Type.OBJECT;
  }

  public boolean isNull() {
    return type == Type.NULL;
  }

  public boolean isEmpty() {
    return type == Type.EMPTY_RESULT;
  }

  /**
   * Get as a long.  Will assert (or throw ClassCastException) if not really a long.
   * @return as a long
   */
  public long asLong() {
    assert val instanceof Long;
    return (Long)val;
  }

  /**
   * Get as a boolean.  Will assert (or throw ClassCastException) if not really a boolean.
   * @return as a boolean
   */
  public boolean asBool() {
    assert val instanceof Boolean;
    return (Boolean)val;
  }

  /**
   * Get as a double.  Will assert (or throw ClassCastException) if not really a double.
   * @return as a double
   */
  public double asDouble() {
    assert val instanceof Double;
    return (Double)val;

  }

  /**
   * Get as a string.  Will assert (or throw ClassCastException) if not really a string.
   * @return as a string
   */
  public String asString() {
    assert val instanceof String;
    return (String)val;
  }

  /**
   * Get as a list.  Will assert (or throw ClassCastException) if not really a list.
   * @return as a list
   */
  public List<JsonSequence> asList() {
    assert val instanceof List;
    return (List<JsonSequence>)val;
  }

  /**
   * Get as an object.  Will assert (or throw ClassCastException) if not really an object.
   * @return as an object
   */
  public Map<String, JsonSequence> asObject() {
    assert val instanceof Map;
    return (Map<String, JsonSequence>)val;
  }

  public final Boolean castToBool(boolean errorOnBadCast) {
    if (isBool()) return asBool();
    if (isNull() || isEmpty()) return null;
    if (isString()) return Boolean.valueOf(asString());
    if (errorOnBadCast) throw new ClassCastException("Attempt to cast " + type.name().toLowerCase() + " as bool");
    else return null;
  }

  public final Long castToLong(boolean errorOnBadCast) {
    if (isLong()) return asLong();
    if (isNull() || isEmpty()) return null;
    if (isDouble()) return (long)asDouble();
    if (isString()) {
      try {
        return Long.valueOf(asString());
      } catch (NumberFormatException e) {
        if (errorOnBadCast) {
          throw e;
        } else {
          return null;
        }
      }
    }
    if (errorOnBadCast) throw new ClassCastException("Attempt to cast " + type.name().toLowerCase() + " as long");
    else return null;
  }

  public final Integer castToInt(boolean errorOnBadCast) {
    Long lg = castToLong(errorOnBadCast);
    if (lg == null) return null;
    if (lg > Integer.MAX_VALUE) {
      if (errorOnBadCast) throw new NumberFormatException("Value too large to fit in integer");
      else return null;
    }
    return (int)lg.longValue();
  }

  public final Double castToDouble(boolean errorOnBadCast) {
    if (isDouble()) return asDouble();
    if (isLong()) return (double)asLong();
    if (isNull() || isEmpty()) return null;
    if (isString()) {
      try {
        return Double.valueOf(asString());
      } catch (NumberFormatException e) {
        if (errorOnBadCast) {
          throw e;
        } else {
          return null;
        }
      }
    }
    if (errorOnBadCast) throw new ClassCastException("Attempt to cast " + type.name().toLowerCase() + " as double");
    else return null;
  }

  public final String castToString(boolean errorOnBadCast) {
    if (isNull() || isEmpty()) return null;
    if (isString()) return asString();
    if (isDouble() || isLong() || isBool()) return val.toString();
    if (errorOnBadCast) throw new ClassCastException("Attempt to cast " + type.name().toLowerCase() + " as string");
    else return null;
  }

  public final List<JsonSequence> castToList(boolean errorOnBadCast) {
    if (isList()) return asList();
    if (isNull() || isEmpty()) return null;
    if (errorOnBadCast) throw new ClassCastException("Attempt to cast " + type.name().toLowerCase() + " as list");
    else return null;
  }

  void add(JsonSequence other, ErrorListener errorListener, ParserRuleContext ctx) {
    arithmetic(other, (left, right) -> left + right, (left, right) -> left + right, false, errorListener, ctx);
  }

  void subtract(JsonSequence other, ErrorListener errorListener, ParserRuleContext ctx) {
    arithmetic(other, (left, right) -> left - right, (left, right) -> left - right, false, errorListener, ctx);
  }

  void multiply(JsonSequence other, ErrorListener errorListener, ParserRuleContext ctx) {
    arithmetic(other, (left, right) -> left * right, (left, right) -> left * right, false, errorListener, ctx);
  }

  void divide(JsonSequence other, ErrorListener errorListener, ParserRuleContext ctx) {
    arithmetic(other, (left, right) -> left / right, (left, right) -> left / right, true, errorListener, ctx);
  }

  void modulo(JsonSequence other, ErrorListener errorListener, ParserRuleContext ctx) {
    switch (type) {
      case LONG:
        switch (other.type) {
          case LONG:
            if (other.asLong() == 0) {
              errorListener.runtimeError("Division by zero at ", ctx);
              setNull();
            } else {
              val = asLong() % other.asLong();
            }
            break;

          default:
            errorListener.semanticError("You cannot do mod on a " + other.type.name().toLowerCase(), ctx);
            setNull();
            break;
        }
        break;

      default:
        errorListener.semanticError("You cannot do mod on a " + type.name().toLowerCase(), ctx);
        setNull();
        break;
    }
  }

  void negate(ErrorListener errorListener, ParserRuleContext ctx) {
    switch (type) {
      case LONG:
        val = asLong() * -1;
        break;

      case DOUBLE:
        val = asDouble() * -1.0;
        break;

      default:
        errorListener.semanticError("You cannot do arithmetic on a " + type.name().toLowerCase(), ctx);
        setNull();
        break;
    }
  }

  /**
   * this is more than equals().  It checks to assure the types are the same or converts where possible.  If the
   * types cannot be compared a semantic error is raised in errorListener.
   * @param other other value
   * @param errorListener error listener to log errors to
   * @return either trueJsonSequence or falseJsonSequence
   */
  JsonSequence equalsOp(JsonSequence other, ErrorListener errorListener, ParserRuleContext ctx) {
    // Null requires special handling, because if two things are null they are immediately equal
    if (type == Type.NULL || other.type == Type.NULL) {
      return type == Type.NULL && other.type == Type.NULL ? JsonSequence.trueJsonSequence : JsonSequence.falseJsonSequence;
    }
    return equalityOperator(other, Object::equals, errorListener, ctx) ? JsonSequence.trueJsonSequence : JsonSequence.falseJsonSequence;
  }

  /**
   * This is more than !equals.  It checks types to make sure this comparison is sensible.  If it is not a semantic
   * error is returned.
   * @param other other JsonSequence.
   * @param errorListener error listener to log errors to
   * @return either trueJsonSequence or falseJsonSequence
   */
  JsonSequence notEqualsOp(JsonSequence other, ErrorListener errorListener, ParserRuleContext ctx) {
    if (type == Type.NULL || other.type == Type.NULL) {
      return type == Type.NULL && other.type == Type.NULL ? JsonSequence.falseJsonSequence : JsonSequence.trueJsonSequence;
    }
    return equalityOperator(other, (obj1, obj2) -> !obj1.equals(obj2), errorListener, ctx) ? JsonSequence.trueJsonSequence :
        JsonSequence.falseJsonSequence;
  }

  JsonSequence greaterThanOp(JsonSequence other, ErrorListener errorListener, ParserRuleContext ctx) {
    return compareTo(other, errorListener, ctx) > 0 ? JsonSequence.trueJsonSequence : JsonSequence.falseJsonSequence;
  }

  JsonSequence greaterThanEqualOp(JsonSequence other, ErrorListener errorListener, ParserRuleContext ctx) {
    return compareTo(other, errorListener, ctx) >= 0 ? JsonSequence.trueJsonSequence : JsonSequence.falseJsonSequence;
  }

  JsonSequence lessThanOp(JsonSequence other, ErrorListener errorListener, ParserRuleContext ctx) {
    return compareTo(other, errorListener, ctx) < 0 ? JsonSequence.trueJsonSequence : JsonSequence.falseJsonSequence;
  }

  JsonSequence lessThanEqualOp(JsonSequence other, ErrorListener errorListener, ParserRuleContext ctx) {
    return compareTo(other, errorListener, ctx) <= 0 ? JsonSequence.trueJsonSequence : JsonSequence.falseJsonSequence;
  }

  Type getType() {
    return type;
  }

  private void arithmetic(JsonSequence other, LongBinaryOperator longOp, DoubleBinaryOperator doubleOp,
                          boolean zeroCheck, ErrorListener errorListener, ParserRuleContext ctx) {
    switch (type) {
      case LONG:
        switch (other.type) {
          case LONG:
            if (zeroCheck && other.asLong() == 0) {
              errorListener.runtimeError("Division by zero at ", ctx);
              setNull();
            } else {
              val = longOp.applyAsLong(asLong(), other.asLong());
            }
            break;

          case DOUBLE:
            if (zeroCheck && other.asDouble() == 0.0) {
              errorListener.runtimeError("Division by zero at ", ctx);
              setNull();
            } else {
              type = Type.DOUBLE;
              val = doubleOp.applyAsDouble((double)asLong(), other.asDouble());
            }
            break;

          default:
            errorListener.semanticError("You cannot do arithmetic on a " + other.type.name().toLowerCase(), ctx);
            setNull();
            break;
        }
        break;

      case DOUBLE:
        switch (other.type) {
          case LONG:
            if (zeroCheck && other.asLong() == 0) {
              errorListener.runtimeError("Division by zero at ", ctx);
              setNull();
            } else {
              val = doubleOp.applyAsDouble(asDouble(), (double)other.asLong());
            }
            break;

          case DOUBLE:
            if (zeroCheck && other.asDouble() == 0.0) {
              errorListener.runtimeError("Division by zero at ", ctx);
              setNull();
            } else {
              val = doubleOp.applyAsDouble(asDouble(), other.asDouble());
            }
            break;

          default:
            errorListener.semanticError("You cannot do arithmetic on a " + other.type.name().toLowerCase(), ctx);
            setNull();
            break;
        }
        break;

      default:
        errorListener.semanticError("You cannot do arithmetic on a " + type.name().toLowerCase(), ctx);
        setNull();
        break;
    }
  }

  private boolean equalityOperator(JsonSequence other, BiFunction<Object, Object, Boolean> comparator,
                                   ErrorListener errorListener, ParserRuleContext ctx) {
    switch (type) {
      case LONG:
        switch (other.type) {
          case LONG:
            return comparator.apply(asLong(), other.asLong());

          case DOUBLE:
            return comparator.apply((double)asLong(), other.asDouble());

          default:
            errorListener.semanticError("Cannot compare a long to a non-numeric type", ctx);
            return false;
        }

      case DOUBLE:
        switch (other.type) {
          case DOUBLE:
            return comparator.apply(asDouble(), other.asDouble());

          case LONG:
            return comparator.apply(asDouble(), (double)other.asLong());

          default:
            errorListener.semanticError("Cannot compare a double to a non-numeric type", ctx);
            return false;

        }

      case NULL:
        // Null requires special handling because we cannot call the .equals method on its val.
        throw new RuntimeException("Programming error");

      case BOOL:
      case STRING:
      case LIST:
      case OBJECT:
        if (type != other.type) {
          errorListener.semanticError("Cannot compare a " + type.name().toLowerCase() + " to a " +
              other.type.name().toLowerCase(), ctx);
          return false;
        }
        return comparator.apply(val, other.val);

      case EMPTY_RESULT:
        return false;

      default:
        throw new RuntimeException("Programming error");
    }
  }

  // This comparison doesn't handle type checking or coercion.  Look at lessThanOp etc. for that.
  private int compareTo(JsonSequence other, ErrorListener errorListener, ParserRuleContext ctx) {
    switch (type) {
      case LONG:
        switch (other.type) {
          case LONG:
            return ((Long)val).compareTo(other.asLong());

          case DOUBLE:
            Double d = (double)asLong();
            return d.compareTo(other.asDouble());

          default:
            errorListener.semanticError("Cannot compare a long to a " + other.type.name().toLowerCase(), ctx);
            return 0;
        }

      case DOUBLE:
        switch (other.type) {
          case DOUBLE:
            return ((Double)val).compareTo(other.asDouble());

          case LONG:
            return ((Double)val).compareTo((double)other.asLong());

          default:
            errorListener.semanticError("Cannot compare a decimal to a " + other.type.name().toLowerCase(), ctx);
            return 0;
        }

      case STRING:
        if (other.isString()) return ((String)val).compareTo(other.asString());
        errorListener.semanticError("Cannot compare a string to a " + other.type.name().toLowerCase(), ctx);
        return 0;

      default:
        errorListener.semanticError("Cannot apply an inequality operator to a " + type.name().toLowerCase(), ctx);
        return 0;
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
    else if (isEmpty() && other.isEmpty()) return true;
    else return type == other.type && val.equals(other.val);
  }

  @Override
  public String toString() {
    return prettyPrint(0);
  }

  private String prettyPrint(int in) {
    StringBuilder buf = new StringBuilder();
    switch (type) {
      case LONG:
      case DOUBLE:
      case BOOL:
        return val.toString();

      case STRING:
        return '"' + val.toString() + '"';

      case NULL:
        return "null";

      case EMPTY_RESULT:
        return "empty result";

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
