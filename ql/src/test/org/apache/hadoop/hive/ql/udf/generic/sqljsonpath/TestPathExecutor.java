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

import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;
import org.apache.hadoop.hive.ql.udf.generic.SqlJsonPathLexer;
import org.apache.hadoop.hive.ql.udf.generic.SqlJsonPathParser;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class TestPathExecutor {

  private static JsonValueParser valueParser;
  private static JsonSequence    emptyJson;

  @BeforeClass
  public static void buildValueParser() throws IOException, ParseException {
    ErrorListener listener = new ErrorListener();
    valueParser = new JsonValueParser(listener);
    emptyJson = valueParser.parse("{ }");
  }

  @Test
  public void syntaxError() throws IOException {
    try {
      parse("fizzbot");
    } catch (ParseException e) {
      Assert.assertEquals("'fizzbot' produced a syntax error: no viable alternative at input 'fizzbot' on line 1 at position 0", e.getMessage());
    }

  }

  @Test
  public void laxDefault() throws IOException, ParseException {
    Context context = parseAndExecute("$.a", emptyJson);
    Assert.assertEquals(Mode.LAX, context.executor.getMode());
  }

  @Test
  public void laxSpecified() throws IOException, ParseException {
    Context context = parseAndExecute("lax $.a", emptyJson);
    Assert.assertEquals(Mode.LAX, context.executor.getMode());
  }

  @Test
  public void strict() throws IOException, ParseException {
    Context context = parseAndExecute("strict $.a", emptyJson);
    Assert.assertEquals(Mode.STRICT, context.executor.getMode());
  }

  @Test
  public void longLiteral() throws IOException, ParseException {
    Context context = parseAdditiveExpr("5");
    Assert.assertEquals(5L, context.val.asLong());
  }

  @Test
  public void doubleLiteral() throws IOException, ParseException {
    Context context = parseAdditiveExpr("5.1");
    Assert.assertEquals(5.1, context.val.asDouble(), 0.00001);
  }

  @Test
  public void booleanLiteral() throws IOException, ParseException {
    Context context = parseAdditiveExpr("true");
    Assert.assertTrue(context.val.asBool());
  }

  @Test
  public void nullLiteral() throws IOException, ParseException {
    Context context = parseAdditiveExpr("null");
    Assert.assertTrue(context.val.isNull());
  }

  @Test
  public void singleQuoteStringLiteral() throws IOException, ParseException {
    Context context = parseAdditiveExpr("'fred'");
    Assert.assertEquals("fred", context.val.asString());
  }

  @Test
  public void doubleQuoteStringLiteral() throws IOException, ParseException {
    Context context = parseAdditiveExpr("\"fred\"");
    Assert.assertEquals("fred", context.val.asString());
  }

  @Test
  public void addLong() throws IOException, ParseException {
    Context context = parseAdditiveExpr("5 + 6");
    Assert.assertTrue(context.val.isLong());
    Assert.assertEquals(11L, context.val.asLong());
  }

  @Test
  public void subtractLong() throws IOException, ParseException {
    Context context = parseAdditiveExpr("8 - 4");
    Assert.assertTrue(context.val.isLong());
    Assert.assertEquals(4L, context.val.asLong());
  }

  @Test
  public void multiplyLong() throws IOException, ParseException {
    Context context = parseAdditiveExpr("9 * 10");
    Assert.assertTrue(context.val.isLong());
    Assert.assertEquals(90L, context.val.asLong());
  }

  @Test
  public void divideLong() throws IOException, ParseException {
    Context context = parseAdditiveExpr("9 / 3");
    Assert.assertTrue(context.val.isLong());
    Assert.assertEquals(3L, context.val.asLong());
  }

  @Test
  public void modLong() throws IOException, ParseException {
    Context context = parseAdditiveExpr("10 % 3");
    Assert.assertTrue(context.val.isLong());
    Assert.assertEquals(1L, context.val.asLong());
  }

  @Test
  public void addDouble() throws IOException, ParseException {
    Context context = parseAdditiveExpr("5.1 + 7.2");
    Assert.assertTrue(context.val.isDouble());
    Assert.assertEquals(12.3, context.val.asDouble(), 0.00001);
  }

  @Test
  public void subtractDouble() throws IOException, ParseException {
    Context context = parseAdditiveExpr("10.0 - .2");
    Assert.assertTrue(context.val.isDouble());
    Assert.assertEquals(9.8, context.val.asDouble(), 0.00001);
  }

  @Test
  public void multiplyDouble() throws IOException, ParseException {
    Context context = parseAdditiveExpr("2.0 * 3.141592654");
    Assert.assertTrue(context.val.isDouble());
    Assert.assertEquals(6.283185308, context.val.asDouble(), 0.001);
  }

  @Test
  public void divideDouble() throws IOException, ParseException {
    Context context = parseAdditiveExpr("20.0 / 3.0");
    Assert.assertTrue(context.val.isDouble());
    Assert.assertEquals(6.66666, context.val.asDouble(), 0.001);
  }

  @Test
  public void addLongAndDouble() throws IOException, ParseException {
    Context context = parseAdditiveExpr("5 + 7.2");
    Assert.assertTrue(context.val.isDouble());
    Assert.assertEquals(12.2, context.val.asDouble(), 0.00001);
  }

  @Test
  public void subtractLongAndDouble() throws IOException, ParseException {
    Context context = parseAdditiveExpr("10 - 7.2");
    Assert.assertTrue(context.val.isDouble());
    Assert.assertEquals(2.8, context.val.asDouble(), 0.00001);
  }

  @Test
  public void multiplyLongAndDouble() throws IOException, ParseException {
    Context context = parseAdditiveExpr("10 * 1.238273");
    Assert.assertTrue(context.val.isDouble());
    Assert.assertEquals(12.38273, context.val.asDouble(), 0.00001);
  }

  @Test
  public void divideLongAndDouble() throws IOException, ParseException {
    Context context = parseAdditiveExpr("20 / 1.238273");
    Assert.assertTrue(context.val.isDouble());
    Assert.assertEquals(16.151527167272484, context.val.asDouble(), 0.00001);
  }

  @Test
  public void addDoubleAndLong() throws IOException, ParseException {
    Context context = parseAdditiveExpr("5.2 + 7");
    Assert.assertTrue(context.val.isDouble());
    Assert.assertEquals(12.2, context.val.asDouble(), 0.00001);
  }

  @Test
  public void subtractDoubleAndLong() throws IOException, ParseException {
    Context context = parseAdditiveExpr("10.2 - 7");
    Assert.assertTrue(context.val.isDouble());
    Assert.assertEquals(3.2, context.val.asDouble(), 0.00001);
  }

  @Test
  public void multiplyDoubleAndLong() throws IOException, ParseException {
    Context context = parseAdditiveExpr("1.238273 * 10");
    Assert.assertTrue(context.val.isDouble());
    Assert.assertEquals(12.38273, context.val.asDouble(), 0.00001);
  }

  @Test
  public void divideDoubleAndLong() throws IOException, ParseException {
    Context context = parseAdditiveExpr("20.238273 / 3");
    Assert.assertTrue(context.val.isDouble());
    Assert.assertEquals(6.746091, context.val.asDouble(), 0.00001);
  }

  @Test
  public void longUnaryPlus() throws IOException, ParseException {
    Context context = parseAdditiveExpr("+3");
    Assert.assertTrue(context.val.isLong());
    Assert.assertEquals(3L, context.val.asLong());
  }

  @Test
  public void longUnaryMinus() throws IOException, ParseException {
    Context context = parseAdditiveExpr("-3");
    Assert.assertTrue(context.val.isLong());
    Assert.assertEquals(-3L, context.val.asLong());
  }

  @Test
  public void doubleUnaryPlus() throws IOException, ParseException {
    Context context = parseAdditiveExpr("+20.238273");
    Assert.assertTrue(context.val.isDouble());
    Assert.assertEquals(20.238273, context.val.asDouble(), 0.00001);
  }

  @Test
  public void doubleUnaryMinus() throws IOException, ParseException {
    Context context = parseAdditiveExpr("-20.238273");
    Assert.assertTrue(context.val.isDouble());
    Assert.assertEquals(-20.238273, context.val.asDouble(), 0.00001);
  }

  @Test
  public void badLongAdd() throws IOException, ParseException {
    String pathExpr = "20 + 'fred'";
    Context context = parseAndExecute(pathExpr, emptyJson);
    try {
      context.executor.getErrorListener().checkForErrors(pathExpr);
      Assert.fail();
    } catch (ParseException e) {
      Assert.assertEquals("'20 + 'fred'' produced a semantic error: You cannot do arithmetic on a string", e.getMessage());
    }
  }

  @Test
  public void badLongSubtract() throws IOException, ParseException {
    String pathExpr = "20 - 'fred'";
    Context context = parseAndExecute(pathExpr, emptyJson);
    try {
      context.executor.getErrorListener().checkForErrors(pathExpr);
      Assert.fail();
    } catch (ParseException e) {
      Assert.assertEquals("'20 - 'fred'' produced a semantic error: You cannot do arithmetic on a string", e.getMessage());
    }
  }

  @Test
  public void badLongMultiply() throws IOException, ParseException {
    String pathExpr = "20 * true";
    Context context = parseAndExecute(pathExpr, emptyJson);
    try {
      context.executor.getErrorListener().checkForErrors(pathExpr);
      Assert.fail();
    } catch (ParseException e) {
      Assert.assertEquals("'20 * true' produced a semantic error: You cannot do arithmetic on a bool", e.getMessage());
    }
  }

  @Test
  public void badLongDivide() throws IOException, ParseException {
    String pathExpr = "20 / 'bob'";
    Context context = parseAndExecute(pathExpr, emptyJson);
    try {
      context.executor.getErrorListener().checkForErrors(pathExpr);
      Assert.fail();
    } catch (ParseException e) {
      Assert.assertEquals("'20 / 'bob'' produced a semantic error: You cannot do arithmetic on a string", e.getMessage());
    }
  }

  @Test
  public void badMod() throws IOException, ParseException {
    String pathExpr = "20 % 3.0";
    Context context = parseAndExecute(pathExpr, emptyJson);
    try {
      context.executor.getErrorListener().checkForErrors(pathExpr);
      Assert.fail();
    } catch (ParseException e) {
      Assert.assertEquals("'20 % 3.0' produced a semantic error: You cannot do mod on a double", e.getMessage());
    }
  }

  @Test
  public void badStringAdd() throws IOException, ParseException {
    String pathExpr = "'fred' + 3.0";
    Context context = parseAndExecute(pathExpr, emptyJson);
    try {
      context.executor.getErrorListener().checkForErrors(pathExpr);
      Assert.fail();
    } catch (ParseException e) {
      Assert.assertEquals("''fred' + 3.0' produced a semantic error: You cannot do arithmetic on a string", e.getMessage());
    }
  }

  @Test
  public void badStringSubtract() throws IOException, ParseException {
    String pathExpr = "'fred' - 3.0";
    Context context = parseAndExecute(pathExpr, emptyJson);
    try {
      context.executor.getErrorListener().checkForErrors(pathExpr);
      Assert.fail();
    } catch (ParseException e) {
      Assert.assertEquals("''fred' - 3.0' produced a semantic error: You cannot do arithmetic on a string", e.getMessage());
    }
  }

  @Test
  public void badStringMultiply() throws IOException, ParseException {
    String pathExpr = "'fred' * 3.0";
    Context context = parseAndExecute(pathExpr, emptyJson);
    try {
      context.executor.getErrorListener().checkForErrors(pathExpr);
      Assert.fail();
    } catch (ParseException e) {
      Assert.assertEquals("''fred' * 3.0' produced a semantic error: You cannot do arithmetic on a string", e.getMessage());
    }
  }

  @Test
  public void badStringDivide() throws IOException, ParseException {
    String pathExpr = "'fred' / 3.0";
    Context context = parseAndExecute(pathExpr, emptyJson);
    try {
      context.executor.getErrorListener().checkForErrors(pathExpr);
      Assert.fail();
    } catch (ParseException e) {
      Assert.assertEquals("''fred' / 3.0' produced a semantic error: You cannot do arithmetic on a string", e.getMessage());
    }
  }

  @Test
  public void badStringMod() throws IOException, ParseException {
    String pathExpr = "'fred' % 3.0";
    Context context = parseAndExecute(pathExpr, emptyJson);
    try {
      context.executor.getErrorListener().checkForErrors(pathExpr);
      Assert.fail();
    } catch (ParseException e) {
      Assert.assertEquals("''fred' % 3.0' produced a semantic error: You cannot do mod on a string", e.getMessage());
    }
  }

  @Test
  public void addNull() throws IOException, ParseException {
    Context context = parseAdditiveExpr("20 + null");
    Assert.assertTrue(context.val.isNull());
  }

  @Test
  public void subtractNull() throws IOException, ParseException {
    Context context = parseAdditiveExpr("20.0 - null");
    Assert.assertTrue(context.val.isNull());
  }

  @Test
  public void multiplyNull() throws IOException, ParseException {
    Context context = parseAdditiveExpr("20 * null");
    Assert.assertTrue(context.val.isNull());
  }

  @Test
  public void divideNull() throws IOException, ParseException {
    Context context = parseAdditiveExpr("20 / null");
    Assert.assertTrue(context.val.isNull());
  }

  @Test
  public void modNull() throws IOException, ParseException {
    Context context = parseAdditiveExpr("20 % null");
    Assert.assertTrue(context.val.isNull());
  }

  @Test
  public void nullAdd() throws IOException, ParseException {
    Context context = parseAdditiveExpr("null + 20");
    Assert.assertTrue(context.val.isNull());
  }

  @Test
  public void nullSubtract() throws IOException, ParseException {
    Context context = parseAdditiveExpr("null - 20.0");
    Assert.assertTrue(context.val.isNull());
  }

  @Test
  public void nullMultiply() throws IOException, ParseException {
    Context context = parseAdditiveExpr("null * 20");
    Assert.assertTrue(context.val.isNull());
  }

  @Test
  public void nullDivide() throws IOException, ParseException {
    Context context = parseAdditiveExpr("null / 20");
    Assert.assertTrue(context.val.isNull());
  }

  @Test
  public void nullMod() throws IOException, ParseException {
    Context context = parseAdditiveExpr("null % 20");
    Assert.assertTrue(context.val.isNull());
  }

  @Test
  public void pathNamedVariable() throws IOException, ParseException {
    JsonSequence json = valueParser.parse("{ \"name\" : \"fred\", \"classes\" : [ \"pe\", \"history\" ] }");
    Context context = parseAndExecute("$.classes[$i]", json, Collections.singletonMap("i", new JsonSequence(1L)));

    JsonSequence wrappedExpected = valueParser.parse(" { \"k\" : \"history\" }");
    Assert.assertEquals(wrappedExpected.asObject().get("k"), context.val);
  }

  @Test
  public void pathNamedVariableNoMatchingId() throws IOException, ParseException {
    String pathExpr = "$fred";
    Context context = parseAndExecute(pathExpr, emptyJson, Collections.singletonMap("bob", new JsonSequence(5L)));
    try {
      context.executor.getErrorListener().checkForErrors(pathExpr);
      Assert.fail();
    } catch (ParseException e) {
      Assert.assertEquals("'" + pathExpr + "' produced a semantic error: Variable fred" +
          " referenced in path expression but no matching id found in passing clause", e.getMessage());
    }
  }

  @Test
  public void pathNamedVariableNullPassing() throws IOException, ParseException {
    String pathExpr = "$fred";
    Context context = parseAndExecute(pathExpr, emptyJson);
    try {
      context.executor.getErrorListener().checkForErrors(pathExpr);
      Assert.fail();
    } catch (ParseException e) {
      Assert.assertEquals("'" + pathExpr + "' produced a semantic error: Variable fred" +
          " referenced in path expression but no matching id found in passing clause", e.getMessage());
    }
  }

  @Test
  public void pathNamedVariableEmptyPassing() throws IOException, ParseException {
    String pathExpr = "$fred";
    Context context = parseAndExecute(pathExpr, emptyJson, Collections.emptyMap());
    try {
      context.executor.getErrorListener().checkForErrors(pathExpr);
      Assert.fail();
    } catch (ParseException e) {
      Assert.assertEquals("'" + pathExpr + "' produced a semantic error: Variable fred" +
          " referenced in path expression but no matching id found in passing clause", e.getMessage());
    }
  }

  @Test
  public void fullMatch() throws IOException, ParseException {
    JsonSequence json = valueParser.parse(" { \"name\" : \"fred\" }");
    Context context = parseAndExecute("$", json);
    Assert.assertEquals(json, context.val);
  }

  @Test
  public void matchKeyString() throws IOException, ParseException {
    JsonSequence json = valueParser.parse(" { \"name\" : \"fred\" }");
    Context context = parseAndExecute("$.name", json);
    Assert.assertTrue(context.val.isString());
    Assert.assertEquals("fred", context.val.asString());
  }

  @Test
  public void matchKeyLong() throws IOException, ParseException {
    JsonSequence json = valueParser.parse(" { \"name\" : \"fred\", \"age\" : 35 }");
    Context context = parseAndExecute("$.age", json);
    Assert.assertTrue(context.val.isLong());
    Assert.assertEquals(35, context.val.asLong());
  }

  @Test
  public void matchKeyDouble() throws IOException, ParseException {
    JsonSequence json = valueParser.parse(" { \"name\" : \"fred\", \"gpa\" : 2.73 }");
    Context context = parseAndExecute("$.gpa", json);
    Assert.assertTrue(context.val.isDouble());
    Assert.assertEquals(2.73, context.val.asDouble(), 0.001);
  }

  @Test
  public void matchKeyBool() throws IOException, ParseException {
    JsonSequence json = valueParser.parse(" { \"name\" : \"fred\", \"honor roll\" : false }");
    Context context = parseAndExecute("$.\"honor roll\"", json);
    Assert.assertTrue(context.val.isBool());
    Assert.assertFalse(context.val.asBool());
  }

  @Test
  public void matchKeyNull() throws IOException, ParseException {
    JsonSequence json = valueParser.parse(" { \"name\" : \"fred\", \"sports\" : null }");
    Context context = parseAndExecute("$.sports", json);
    Assert.assertTrue(context.val.isNull());
  }

  @Test
  public void matchKeyQuotes() throws IOException, ParseException {
    JsonSequence json = valueParser.parse(" { \"name\" : \"fred\" }");
    Context context = parseAndExecute("$.\"name\"", json);
    Assert.assertTrue(context.val.isString());
    Assert.assertEquals("fred", context.val.asString());
  }

  @Test
  public void noMatchKey() throws IOException, ParseException {
    JsonSequence json = valueParser.parse(" { \"name\" : \"fred\" }");
    Context context = parseAndExecute("$.address", json);
    Assert.assertTrue(context.val.isEmpty());
  }

  @Test
  public void noMatchKeyQuotes() throws IOException, ParseException {
    JsonSequence json = valueParser.parse(" { \"name\" : \"fred\" }");
    Context context = parseAndExecute("$.\"address\"", json);
    Assert.assertTrue(context.val.isEmpty());
  }

  @Test
  public void objectWildcard() throws IOException, ParseException {
    JsonSequence json = valueParser.parse(" { \"name\" : \"fred\", \"age\" : 35 }");
    JsonSequence expected = new JsonSequence(json);
    Context context = parseAndExecute("$.*", json);


    Assert.assertEquals(expected, context.val);
  }

  @Test
  public void objectWildcardEmpty() throws IOException, ParseException {
    Context context = parseAndExecute("$.*", emptyJson);
    Assert.assertEquals(emptyJson, context.val);
  }

  @Test
  public void simpleSubscriptList() throws IOException, ParseException {
    JsonSequence json = valueParser.parse("{ \"name\" : \"fred\", \"classes\" : [ \"science\", \"art\" ] }");
    Context context = parseAndExecute("$.classes[0]", json);

    JsonSequence wrappedExpected = valueParser.parse(" { \"k\" : \"science\" }");
    Assert.assertEquals(wrappedExpected.asObject().get("k"), context.val);
  }

  @Test
  public void simpleSubscriptObject() throws IOException, ParseException {
    JsonSequence json = valueParser.parse("{ \"name\" : \"fred\"," +
        "\"classes\" : [ \"science\", \"art\" ]," +
        "\"sports\"  : [ \"swimming\", \"baseball\" ] }");
    Context context = parseAndExecute("$.*[1]", json);

    JsonSequence expected = valueParser.parse(" { \"classes\" : \"art\", \"sports\" : \"baseball\" }");
    Assert.assertEquals(expected, context.val);
  }

  @Test
  public void lastSubscriptList() throws IOException, ParseException {
    JsonSequence json = valueParser.parse("{ \"name\" : \"fred\", \"classes\" : [ \"science\", \"art\" ] }");
    Context context = parseAndExecute("$.classes[last]", json);

    JsonSequence wrappedExpected = valueParser.parse(" { \"k\" : \"art\" }");
    Assert.assertEquals(wrappedExpected.asObject().get("k"), context.val);
  }

  @Test
  public void lastSubscriptObject() throws IOException, ParseException {
    JsonSequence json = valueParser.parse("{ \"name\" : \"fred\"," +
        "\"classes\" : [ \"science\", \"art\" ]," +
        "\"sports\"  : [ \"swimming\", \"baseball\" ] }");
    Context context = parseAndExecute("$.*[last]", json);

    JsonSequence expected = valueParser.parse(" { \"classes\" : \"art\", \"sports\" : \"baseball\" }");
    Assert.assertEquals(expected, context.val);
  }

  @Test
  public void toSubscriptList() throws IOException, ParseException {
    JsonSequence json = valueParser.parse("{ \"name\" : \"fred\"," +
        "\"classes\" : [ \"science\", \"art\", \"math\", \"history\", \"writing\" ] }");
    Context context = parseAndExecute("$.classes[1 to 3]", json);

    JsonSequence wrappedExpected = valueParser.parse(" { \"k\" : [ \"art\", \"math\", \"history\" ] }");
    Assert.assertEquals(wrappedExpected.asObject().get("k"), context.val);
  }

  @Test
  public void toSubscriptObject() throws IOException, ParseException {
    JsonSequence json = valueParser.parse("{ \"name\" : \"fred\"," +
        "\"classes\" : [ \"science\", \"art\", \"math\", \"history\", \"writing\" ]," +
        "\"sports\"  : [ \"swimming\", \"baseball\", \"volleyball\", \"soccer\" ] }");
    Context context = parseAndExecute("$.*[1 to 3]", json);

    JsonSequence expected = valueParser.parse(" { \"classes\" : [ \"art\", \"math\", \"history\"]," +
                                                 "\"sports\" : [ \"baseball\", \"volleyball\", \"soccer\" ] }");
    Assert.assertEquals(expected, context.val);
  }

  @Test
  public void toLastSubscriptList() throws IOException, ParseException {
    JsonSequence json = valueParser.parse("{ \"name\" : \"fred\"," +
        "\"classes\" : [ \"science\", \"art\", \"math\", \"history\", \"writing\" ] }");
    Context context = parseAndExecute("$.classes[1 to last]", json);

    JsonSequence wrappedExpected = valueParser.parse(" { \"k\" : [ \"art\", \"math\", \"history\", \"writing\" ] }");
    Assert.assertEquals(wrappedExpected.asObject().get("k"), context.val);
  }

  @Test
  public void toLastSubscriptObject() throws IOException, ParseException {
    JsonSequence json = valueParser.parse("{ \"name\" : \"fred\"," +
        "\"classes\" : [ \"science\", \"art\", \"math\", \"history\", \"writing\" ]," +
        "\"sports\"  : [ \"swimming\", \"baseball\", \"volleyball\", \"soccer\" ] }");
    Context context = parseAndExecute("$.*[1 to last]", json);

    JsonSequence expected = valueParser.parse(" { \"classes\" : [ \"art\", \"math\", \"history\", \"writing\"]," +
        "                                         \"sports\" : [ \"baseball\", \"volleyball\", \"soccer\" ] }");
    Assert.assertEquals(expected, context.val);
  }

  @Test
  public void notAnArraySubscriptList() throws IOException, ParseException {
    JsonSequence json = valueParser.parse("{ \"name\" : \"fred\", \"classes\" : [ \"science\", \"art\" ] }");
    Context context = parseAndExecute("$.name[0]", json);
    Assert.assertTrue(context.val.isEmpty());
  }

  @Test
  public void notAnArraySubscriptObject() throws IOException, ParseException {
    JsonSequence json = valueParser.parse("{ \"name\" : \"fred\" }");
    Context context = parseAndExecute("$.*[1]", json);

    Assert.assertEquals(emptyJson, context.val);
  }

  @Test
  public void offEndSubscriptList() throws IOException, ParseException {
    JsonSequence json = valueParser.parse("{ \"name\" : \"fred\", \"classes\" : [ \"science\", \"art\" ] }");
    Context context = parseAndExecute("$.classes[3]", json);
    Assert.assertTrue(context.val.isEmpty());
  }

  @Test
  public void offEndSubscriptObject() throws IOException, ParseException {
    JsonSequence json = valueParser.parse("{ \"name\" : \"fred\"," +
        "\"classes\" : [ \"science\", \"art\" ]," +
        "\"sports\"  : [ \"swimming\", \"baseball\", \"soccer\" ] }");
    Context context = parseAndExecute("$.*[2]", json);

    JsonSequence expected = valueParser.parse(" { \"sports\" : \"soccer\" }");
    Assert.assertEquals(expected, context.val);
  }

  @Test
  public void toOffEndSubscriptList() throws IOException, ParseException {
    JsonSequence json = valueParser.parse("{ \"name\" : \"fred\"," +
        "\"classes\" : [ \"science\", \"art\", \"math\", \"history\", \"writing\" ] }");
    Context context = parseAndExecute("$.classes[3 to 5]", json);

    JsonSequence wrappedExpected = valueParser.parse(" { \"k\" : [ \"history\", \"writing\" ] }");
    Assert.assertEquals(wrappedExpected.asObject().get("k"), context.val);
  }

  @Test
  public void toOffEndSubscriptObject() throws IOException, ParseException {
    JsonSequence json = valueParser.parse("{ \"name\" : \"fred\"," +
        "\"classes\" : [ \"science\", \"art\", \"math\", \"history\", \"writing\" ]," +
        "\"sports\"  : [ \"swimming\", \"baseball\", \"volleyball\", \"soccer\" ] }");
    Context context = parseAndExecute("$.*[3 to 5]", json);

    JsonSequence expected = valueParser.parse(" { \"classes\" : [ \"history\", \"writing\"], \"sports\" : [ \"soccer\" ] }");
    Assert.assertEquals(expected, context.val);
  }

  @Test
  public void listSubscriptList() throws IOException, ParseException {
    JsonSequence json = valueParser.parse("{ \"name\" : \"fred\"," +
        "\"classes\" : [ \"science\", \"art\", \"math\", \"history\", \"writing\" ] }");
    Context context = parseAndExecute("$.classes[1, 4]", json);

    JsonSequence wrappedExpected = valueParser.parse(" { \"k\" : [ \"art\", \"writing\" ] }");
    Assert.assertEquals(wrappedExpected.asObject().get("k"), context.val);
  }

  @Test
  public void listSubscriptObject() throws IOException, ParseException {
    JsonSequence json = valueParser.parse("{ \"name\" : \"fred\"," +
        "\"classes\" : [ \"science\", \"art\", \"math\", \"history\", \"writing\" ]," +
        "\"sports\"  : [ \"swimming\", \"baseball\", \"volleyball\", \"soccer\" ] }");
    Context context = parseAndExecute("$.*[1, 4]", json);

    JsonSequence expected = valueParser.parse(" { \"classes\" : [ \"art\", \"writing\"], \"sports\" : [ \"baseball\" ] }");
    Assert.assertEquals(expected, context.val);
  }

  @Test
  public void listAndToSubscriptList() throws IOException, ParseException {
    JsonSequence json = valueParser.parse("{ \"name\" : \"fred\"," +
        "\"classes\" : [ \"science\", \"art\", \"math\", \"history\", \"writing\" ] }");
    Context context = parseAndExecute("$.classes[0, 3 to 5]", json);

    JsonSequence wrappedExpected = valueParser.parse(" { \"k\" : [ \"science\", \"history\", \"writing\" ] }");
    Assert.assertEquals(wrappedExpected.asObject().get("k"), context.val);
  }

  @Test
  public void listAndToSubscriptObject() throws IOException, ParseException {
    JsonSequence json = valueParser.parse("{ \"name\" : \"fred\"," +
        "\"classes\" : [ \"science\", \"art\", \"math\", \"history\", \"writing\" ]," +
        "\"sports\"  : [ \"swimming\", \"baseball\", \"volleyball\", \"soccer\" ] }");
    Context context = parseAndExecute("$.*[0, 2 to last]", json);

    JsonSequence expected = valueParser.parse(" { \"classes\" : [ \"science\", \"math\", \"history\", \"writing\"]," +
        "                                         \"sports\" : [ \"swimming\", \"volleyball\", \"soccer\" ] }");
    Assert.assertEquals(expected, context.val);
  }

  @Test
  public void arithmeticSubscriptList() throws IOException, ParseException {
    JsonSequence json = valueParser.parse("{ \"name\" : \"fred\"," +
        "\"classes\" : [ \"science\", \"art\", \"math\", \"history\", \"writing\" ] }");
    Context context = parseAndExecute("$.classes[1 + 1]", json);

    JsonSequence wrappedExpected = valueParser.parse(" { \"k\" : \"math\" }");
    Assert.assertEquals(wrappedExpected.asObject().get("k"), context.val);
  }

  @Test
  public void wildcardSubscriptList() throws IOException, ParseException {
    JsonSequence json = valueParser.parse("{ \"name\" : \"fred\"," +
        "\"classes\" : [ \"science\", \"art\", \"math\" ] }");
    Context context = parseAndExecute("$.classes[*]", json);

    JsonSequence wrappedExpected = valueParser.parse(" { \"k\" : [ \"science\", \"art\", \"math\" ] }");
    Assert.assertEquals(wrappedExpected.asObject().get("k"), context.val);
  }

  @Test
  public void wildcardSubscriptObject() throws IOException, ParseException {
    JsonSequence json = valueParser.parse("{ \"name\" : \"fred\"," +
        "\"classes\" : [ \"science\", \"art\", \"math\" ]," +
        "\"sports\"  : [ \"swimming\", \"baseball\" ] }");
    Context context = parseAndExecute("$.*[*]", json);

    JsonSequence expected = valueParser.parse(" { \"classes\" : [ \"science\", \"art\", \"math\"]," +
        "                                         \"sports\" : [ \"swimming\", \"baseball\" ] }");
    Assert.assertEquals(expected, context.val);
  }

  @Test
  public void typeLong() throws IOException, ParseException {
    JsonSequence json = valueParser.parse("{ \"name\" : \"fred\", \"age\" : 35 }");
    Context context = parseAndExecute("$.age.type()", json);
    Assert.assertTrue(context.val.isString());
    Assert.assertEquals("number", context.val.asString());
  }

  @Test
  public void typeDouble() throws IOException, ParseException {
    JsonSequence json = valueParser.parse("{ \"name\" : \"fred\", \"gpa\" : 3.58 }");
    Context context = parseAndExecute("$.gpa.type()", json);
    Assert.assertTrue(context.val.isString());
    Assert.assertEquals("number", context.val.asString());
  }

  @Test
  public void typeString() throws IOException, ParseException {
    JsonSequence json = valueParser.parse("{ \"name\" : \"fred\", \"gpa\" : 3.58 }");
    Context context = parseAndExecute("$.name.type()", json);
    Assert.assertTrue(context.val.isString());
    Assert.assertEquals("string", context.val.asString());
  }

  @Test
  public void typeNull() throws IOException, ParseException {
    JsonSequence json = valueParser.parse("{ \"name\" : \"fred\", \"age\" : 35, \"sports\" : null }");
    Context context = parseAndExecute("$.sports.type()", json);
    Assert.assertTrue(context.val.isString());
    Assert.assertEquals("null", context.val.asString());
  }

  @Test
  public void typeBoolean() throws IOException, ParseException {
    JsonSequence json = valueParser.parse("{ \"name\" : \"fred\", \"age\" : 35, \"honor roll\" : true }");
    Context context = parseAndExecute("$.\'honor roll\'.type()", json);
    Assert.assertTrue(context.val.isString());
    Assert.assertEquals("boolean", context.val.asString());
  }

  @Test
  public void typeList() throws IOException, ParseException {
    JsonSequence json = valueParser.parse("{ \"name\" : \"fred\", \"classes\" : [ \"art\", \"math\" ] }");
    Context context = parseAndExecute("$.classes.type()", json);
    Assert.assertTrue(context.val.isString());
    Assert.assertEquals("array", context.val.asString());
  }

  @Test
  public void typeObject() throws IOException, ParseException {
    JsonSequence json = valueParser.parse("{ \"name\" : \"fred\", \"address\" : { \"street\" : \"123 main\", \"city\" : \"phoenix\" } }");
    Context context = parseAndExecute("$.address.type()", json);
    Assert.assertTrue(context.val.isString());
    Assert.assertEquals("object", context.val.asString());
  }

  @Test
  public void typeEmpty() throws IOException, ParseException {
    JsonSequence json = valueParser.parse("{ \"name\" : \"fred\", \"age\" : 35 }");
    Context context = parseAndExecute("$.gpa.type()", json);
    Assert.assertTrue(context.val.isEmpty());
  }

  @Test
  public void sizeScalar() throws IOException, ParseException {
    JsonSequence json = valueParser.parse("{ \"name\" : \"fred\", \"age\" : 35, \"honor roll\" : true }");
    Context context = parseAndExecute("$.\'honor roll\'.size()", json);
    Assert.assertTrue(context.val.isLong());
    Assert.assertEquals(1, context.val.asLong());
  }

  @Test
  public void sizeList() throws IOException, ParseException {
    JsonSequence json = valueParser.parse("{ \"name\" : \"fred\", \"classes\" : [ \"art\", \"math\" ] }");
    Context context = parseAndExecute("$.classes.size()", json);
    Assert.assertTrue(context.val.isLong());
    Assert.assertEquals(2, context.val.asLong());
  }

  @Test
  public void sizeObject() throws IOException, ParseException {
    JsonSequence json = valueParser.parse("{ \"name\" : \"fred\", \"address\" : { \"street\" : \"123 main\", \"city\" : \"phoenix\" } }");
    Context context = parseAndExecute("$.address.size()", json);
    Assert.assertTrue(context.val.isLong());
    Assert.assertEquals(2, context.val.asLong());
  }

  @Test
  public void sizeEmpty() throws IOException, ParseException {
    JsonSequence json = valueParser.parse("{ \"name\" : \"fred\", \"age\" : 35 }");
    Context context = parseAndExecute("$.gpa.size()", json);
    Assert.assertTrue(context.val.isEmpty());
  }

  @Test
  public void doubleLong() throws IOException, ParseException {
    JsonSequence json = valueParser.parse("{ \"name\" : \"fred\", \"age\" : 35 }");
    Context context = parseAndExecute("$.age.double()", json);
    Assert.assertTrue(context.val.isDouble());
    Assert.assertEquals(35.0, context.val.asDouble(), 0.001);
  }

  @Test
  public void doubleDouble() throws IOException, ParseException {
    JsonSequence json = valueParser.parse("{ \"name\" : \"fred\", \"gpa\" : 3.58 }");
    Context context = parseAndExecute("$.gpa.double()", json);
    Assert.assertTrue(context.val.isDouble());
    Assert.assertEquals(3.58, context.val.asDouble(), 0.001);
  }

  @Test
  public void doubleString() throws IOException, ParseException {
    JsonSequence json = valueParser.parse("{ \"name\" : \"fred\", \"gpa\" : \"3.58\" }");
    Context context = parseAndExecute("$.gpa.double()", json);
    Assert.assertTrue(context.val.isDouble());
    Assert.assertEquals(3.58, context.val.asDouble(), 0.001);
  }

  @Test
  public void doubleNotStringOrNumeric() throws IOException, ParseException {
    JsonSequence json = valueParser.parse("{ \"name\" : \"fred\", \"honor roll\" : true }");
    Context context = parseAndExecute("$.\"honor roll\".double()", json);
    try {
      context.errorListener.checkForErrors("$.\"honor roll\".double()");
      Assert.fail();
    } catch (ParseException e) {
      Assert.assertEquals("'$.\"honor roll\".double()' produced a runtime error: Double method" +
          " requires numeric or string argument, passed a bool", e.getMessage());

    }
  }

  @Test
  public void doubleEmpty() throws IOException, ParseException {
    JsonSequence json = valueParser.parse("{ \"name\" : \"fred\", \"gpa\" : 3.58 }");
    Context context = parseAndExecute("$.sports.double()", json);
    Assert.assertTrue(context.val.isEmpty());
  }

  @Test
  public void intLong() throws IOException, ParseException {
    JsonSequence json = valueParser.parse("{ \"name\" : \"fred\", \"age\" : 35 }");
    Context context = parseAndExecute("$.age.integer()", json);
    Assert.assertTrue(context.val.isLong());
    Assert.assertEquals(35, context.val.asLong());
  }

  @Test
  public void intDouble() throws IOException, ParseException {
    JsonSequence json = valueParser.parse("{ \"name\" : \"fred\", \"gpa\" : 3.58 }");
    Context context = parseAndExecute("$.gpa.integer()", json);
    Assert.assertTrue(context.val.isLong());
    Assert.assertEquals(3, context.val.asLong());
  }

  @Test
  public void intString() throws IOException, ParseException {
    JsonSequence json = valueParser.parse("{ \"name\" : \"fred\", \"age\" : \"35\" }");
    Context context = parseAndExecute("$.age.integer()", json);
    Assert.assertTrue(context.val.isLong());
    Assert.assertEquals(35, context.val.asLong());
  }

  @Test
  public void intNotStringOrNumeric() throws IOException, ParseException {
    JsonSequence json = valueParser.parse("{ \"name\" : \"fred\", \"honor roll\" : true }");
    Context context = parseAndExecute("$.\"honor roll\".integer()", json);
    try {
      context.errorListener.checkForErrors("$.\"honor roll\".integer()");
      Assert.fail();
    } catch (ParseException e) {
      Assert.assertEquals("'$.\"honor roll\".integer()' produced a runtime error: Integer method" +
          " requires numeric or string argument, passed a bool", e.getMessage());

    }
  }

  @Test
  public void intEmpty() throws IOException, ParseException {
    JsonSequence json = valueParser.parse("{ \"name\" : \"fred\", \"gpa\" : 3.58 }");
    Context context = parseAndExecute("$.sports.integer()", json);
    Assert.assertTrue(context.val.isEmpty());
  }

  @Test
  public void ceilingLong() throws IOException, ParseException {
    JsonSequence json = valueParser.parse("{ \"name\" : \"fred\", \"age\" : 35 }");
    Context context = parseAndExecute("$.age.ceiling()", json);
    Assert.assertTrue(context.val.isLong());
    Assert.assertEquals(35, context.val.asLong());
  }

  @Test
  public void ceilingDouble() throws IOException, ParseException {
    JsonSequence json = valueParser.parse("{ \"name\" : \"fred\", \"gpa\" : 3.58 }");
    Context context = parseAndExecute("$.gpa.ceiling()", json);
    Assert.assertTrue(context.val.isLong());
    Assert.assertEquals(4, context.val.asLong());
  }

  @Test
  public void ceilingNotNumeric() throws IOException, ParseException {
    JsonSequence json = valueParser.parse("{ \"name\" : \"fred\", \"honor roll\" : true }");
    Context context = parseAndExecute("$.name.ceiling()", json);
    try {
      context.errorListener.checkForErrors("$.name.ceiling()");
      Assert.fail();
    } catch (ParseException e) {
      Assert.assertEquals("'$.name.ceiling()' produced a runtime error: Ceiling method" +
          " requires numeric argument, passed a string", e.getMessage());

    }
  }

  @Test
  public void ceilingEmpty() throws IOException, ParseException {
    JsonSequence json = valueParser.parse("{ \"name\" : \"fred\", \"gpa\" : 3.58 }");
    Context context = parseAndExecute("$.sports.ceiling()", json);
    Assert.assertTrue(context.val.isEmpty());
  }

  @Test
  public void floorLong() throws IOException, ParseException {
    JsonSequence json = valueParser.parse("{ \"name\" : \"fred\", \"age\" : 35 }");
    Context context = parseAndExecute("$.age.floor()", json);
    Assert.assertTrue(context.val.isLong());
    Assert.assertEquals(35, context.val.asLong());
  }

  @Test
  public void floorDouble() throws IOException, ParseException {
    JsonSequence json = valueParser.parse("{ \"name\" : \"fred\", \"gpa\" : 3.58 }");
    Context context = parseAndExecute("$.gpa.floor()", json);
    Assert.assertTrue(context.val.isLong());
    Assert.assertEquals(3, context.val.asLong());
  }

  @Test
  public void floorNotNumeric() throws IOException, ParseException {
    JsonSequence json = valueParser.parse("{ \"name\" : \"fred\", \"honor roll\" : true }");
    Context context = parseAndExecute("$.name.floor()", json);
    try {
      context.errorListener.checkForErrors("$.name.floor()");
      Assert.fail();
    } catch (ParseException e) {
      Assert.assertEquals("'$.name.floor()' produced a runtime error: Floor method" +
          " requires numeric argument, passed a string", e.getMessage());

    }
  }

  @Test
  public void floorEmpty() throws IOException, ParseException {
    JsonSequence json = valueParser.parse("{ \"name\" : \"fred\", \"gpa\" : 3.58 }");
    Context context = parseAndExecute("$.sports.floor()", json);
    Assert.assertTrue(context.val.isEmpty());
  }

  @Test
  public void absLong() throws IOException, ParseException {
    JsonSequence json = valueParser.parse("{ \"name\" : \"fred\", \"age\" : 35 }");
    Context context = parseAndExecute("$.age.abs()", json);
    Assert.assertTrue(context.val.isLong());
    Assert.assertEquals(35, context.val.asLong());
  }

  @Test
  public void absDouble() throws IOException, ParseException {
    JsonSequence json = valueParser.parse("{ \"name\" : \"fred\", \"gpa\" : -3.58 }");
    Context context = parseAndExecute("$.gpa.abs()", json);
    Assert.assertTrue(context.val.isDouble());
    Assert.assertEquals(3.58, context.val.asDouble(), 0.001);
  }

  @Test
  public void absNotNumeric() throws IOException, ParseException {
    JsonSequence json = valueParser.parse("{ \"name\" : \"fred\", \"honor roll\" : true }");
    Context context = parseAndExecute("$.name.abs()", json);
    try {
      context.errorListener.checkForErrors("$.name.abs()");
      Assert.fail();
    } catch (ParseException e) {
      Assert.assertEquals("'$.name.abs()' produced a runtime error: Abs method" +
          " requires numeric argument, passed a string", e.getMessage());

    }
  }

  @Test
  public void absEmpty() throws IOException, ParseException {
    JsonSequence json = valueParser.parse("{ \"name\" : \"fred\", \"gpa\" : 3.58 }");
    Context context = parseAndExecute("$.sports.abs()", json);
    Assert.assertTrue(context.val.isEmpty());
  }

  @Test
  public void bigHarryDeepThing() throws IOException, ParseException {
    JsonSequence json = valueParser.parse(
        "{" +
        "  \"name\" : \"fred\"," +
        "  \"classes\" : [ " +
        "    {" +
        "      \"name\"      : \"science\"," +
        "      \"professor\" : \"d. who\"," +
        "      \"texts\"     : [" +
        "         {" +
        "            \"title\"  : \"intro to physics\"," +
        "            \"author\" : \"i. newton\"" +
        "         }, {" +
        "            \"title\"  : \"intro to biology\"," +
        "            \"author\" : \"c. darwin\"" +
        "         }" +
        "       ]" +
        "    }, {" +
        "      \"name\"      : \"art\"," +
        "      \"professor\" : \"v. van gogh\"" +
        "    }" +
        "  ]" +
        "}");
    Map<String, JsonSequence> passing = new HashMap<>();
    passing.put("class", new JsonSequence(0));
    passing.put("text", new JsonSequence(1));
    Context context = parseAndExecute("$.classes[$class].texts[$text].author", json, passing);

    JsonSequence wrappedExpected = valueParser.parse(" { \"k\" : \"c. darwin\" }");
    Assert.assertEquals(wrappedExpected.asObject().get("k"), context.val);
  }

  @Test
  public void bigHarryDeepMultiThing() throws IOException, ParseException {
    JsonSequence json = valueParser.parse(
        "{" +
            "  \"name\" : \"fred\"," +
            "  \"classes\" : [ " +
            "    {" +
            "      \"name\"      : \"science\"," +
            "      \"professor\" : \"d. who\"," +
            "      \"texts\"     : [" +
            "         {" +
            "            \"title\"  : \"intro to physics\"," +
            "            \"author\" : \"i. newton\"" +
            "         }, {" +
            "            \"title\"  : \"intro to biology\"," +
            "            \"author\" : \"c. darwin\"" +
            "         }" +
            "       ]" +
            "    }, {" +
            "      \"name\"      : \"art\"," +
            "      \"professor\" : \"v. van gogh\"" +
            "    }" +
            "  ]" +
            "}");
    Map<String, JsonSequence> passing = new HashMap<>();
    passing.put("class", new JsonSequence(0));
    passing.put("text", new JsonSequence(1));
    Context context = parseAndExecute("$.classes[$class].texts[*].author", json, passing);

    JsonSequence wrappedExpected = valueParser.parse("{ \"k\" : [ \"i. newton\", \"c. darwin\" ] }");
    Assert.assertEquals(wrappedExpected.asObject().get("k"), context.val);
  }

  @Test
  public void filterExists() throws IOException, ParseException {
    JsonSequence json = valueParser.parse(
        "{" +
            "\"name\" : \"fred\"," +
            "\"address\" : {" +
                "\"street\" : \"123 Main\"," +
                "\"city\"   : \"Springfield\"," +
                "\"zip\"    : 12345" +
            "}" +
        "}");
    Context context = parseAndExecute("$.address?(exists(@.city))", json);

    JsonSequence expected = valueParser.parse(
        "{" +
            "\"street\" : \"123 Main\"," +
            "\"city\"   : \"Springfield\"," +
            "\"zip\"    : 12345" +
        "}");
    Assert.assertEquals(expected, context.val);
  }

  @Test
  public void filterNonExistent() throws IOException, ParseException {
    JsonSequence json = valueParser.parse(
        "{" +
            "\"name\" : \"fred\"," +
            "\"address\" : {" +
            "\"street\" : \"123 Main\"," +
            "\"city\"   : \"Springfield\"," +
            "\"zip\"    : 12345" +
            "}" +
            "}");
    Context context = parseAndExecute("$.address?(exists(@.state))", json);
    Assert.assertEquals(JsonSequence.emptyResult, context.val);
  }

  @Test
  public void filterNotExists() throws IOException, ParseException {
    JsonSequence json = valueParser.parse(
        "{" +
            "\"name\" : \"fred\"," +
            "\"address\" : {" +
            "\"street\" : \"123 Main\"," +
            "\"city\"   : \"Springfield\"," +
            "\"zip\"    : 12345" +
            "}" +
            "}");
    Context context = parseAndExecute("$.address?(!exists(@.state))", json);

    JsonSequence expected = valueParser.parse(
        "{" +
            "\"street\" : \"123 Main\"," +
            "\"city\"   : \"Springfield\"," +
            "\"zip\"    : 12345" +
            "}");
    Assert.assertEquals(expected, context.val);
  }

  @Test
  public void filterAndTrue() throws IOException, ParseException {
    JsonSequence json = valueParser.parse(
        "{" +
            "\"name\" : \"fred\"," +
            "\"address\" : {" +
            "\"street\" : \"123 Main\"," +
            "\"city\"   : \"Springfield\"," +
            "\"zip\"    : 12345" +
            "}" +
            "}");
    Context context = parseAndExecute("$.address?(exists(@.city) && exists(@.zip))", json);

    JsonSequence expected = valueParser.parse(
        "{" +
            "\"street\" : \"123 Main\"," +
            "\"city\"   : \"Springfield\"," +
            "\"zip\"    : 12345" +
            "}");
    Assert.assertEquals(expected, context.val);
  }

  @Test
  public void filterAndFirstFalse() throws IOException, ParseException {
    JsonSequence json = valueParser.parse(
        "{" +
            "\"name\" : \"fred\"," +
            "\"address\" : {" +
            "\"street\" : \"123 Main\"," +
            "\"city\"   : \"Springfield\"," +
            "\"zip\"    : 12345" +
            "}" +
            "}");
    Context context = parseAndExecute("$.address?(exists(@.state) && exists(@.zip))", json);

    Assert.assertEquals(JsonSequence.emptyResult, context.val);
  }

  @Test
  public void filterAndSecondFalse() throws IOException, ParseException {
    JsonSequence json = valueParser.parse(
        "{" +
            "\"name\" : \"fred\"," +
            "\"address\" : {" +
            "\"street\" : \"123 Main\"," +
            "\"city\"   : \"Springfield\"," +
            "\"zip\"    : 12345" +
            "}" +
            "}");
    Context context = parseAndExecute("$.address?(exists(@.city) && exists(@.state))", json);

    Assert.assertEquals(JsonSequence.emptyResult, context.val);
  }

  @Test
  public void filterOrBothTrue() throws IOException, ParseException {
    JsonSequence json = valueParser.parse(
        "{" +
            "\"name\" : \"fred\"," +
            "\"address\" : {" +
            "\"street\" : \"123 Main\"," +
            "\"city\"   : \"Springfield\"," +
            "\"zip\"    : 12345" +
            "}" +
            "}");
    Context context = parseAndExecute("$.address?(exists(@.city) || exists(@.zip))", json);

    JsonSequence expected = valueParser.parse(
        "{" +
            "\"street\" : \"123 Main\"," +
            "\"city\"   : \"Springfield\"," +
            "\"zip\"    : 12345" +
            "}");
    Assert.assertEquals(expected, context.val);
  }

  @Test
  public void filterOrFirstTrue() throws IOException, ParseException {
    JsonSequence json = valueParser.parse(
        "{" +
            "\"name\" : \"fred\"," +
            "\"address\" : {" +
            "\"street\" : \"123 Main\"," +
            "\"city\"   : \"Springfield\"," +
            "\"zip\"    : 12345" +
            "}" +
            "}");
    Context context = parseAndExecute("$.address?(exists(@.city) || exists(@.state))", json);

    JsonSequence expected = valueParser.parse(
        "{" +
            "\"street\" : \"123 Main\"," +
            "\"city\"   : \"Springfield\"," +
            "\"zip\"    : 12345" +
            "}");
    Assert.assertEquals(expected, context.val);
  }

  @Test
  public void filterOrSecondTrue() throws IOException, ParseException {
    JsonSequence json = valueParser.parse(
        "{" +
            "\"name\" : \"fred\"," +
            "\"address\" : {" +
            "\"street\" : \"123 Main\"," +
            "\"city\"   : \"Springfield\"," +
            "\"zip\"    : 12345" +
            "}" +
            "}");
    Context context = parseAndExecute("$.address?(exists(@.state) || exists(@.zip))", json);

    JsonSequence expected = valueParser.parse(
        "{" +
            "\"street\" : \"123 Main\"," +
            "\"city\"   : \"Springfield\"," +
            "\"zip\"    : 12345" +
            "}");
    Assert.assertEquals(expected, context.val);
  }

  @Test
  public void filterBothFalse() throws IOException, ParseException {
    JsonSequence json = valueParser.parse(
        "{" +
            "\"name\" : \"fred\"," +
            "\"address\" : {" +
            "\"street\" : \"123 Main\"," +
            "\"city\"   : \"Springfield\"," +
            "\"zip\"    : 12345" +
            "}" +
            "}");
    Context context = parseAndExecute("$.address?(exists(@.state) || exists(@.country))", json);

    Assert.assertEquals(JsonSequence.emptyResult, context.val);
  }

  // The following is used in many of the following (in)equality tests.
  private String equalityJson =
      "{" +
          "\"name\"     : \"fred\"," +
          "\"education\" : {" +
              "\"school\"          : \"usc\"," +
              "\"years attended\"  : 4," +
              "\"gpa\"             : 3.29," +
              "\"graduated\"       : true," +
              "\"sports\"          : null," +
              "\"activities\"      : [ \"pkg\", \"ddd\" ]," +
              "\"clubs\"           : [ \"pkg\", \"ddd\" ]," +
              "\"extracurricular\" : [ \"soup kitchen volunteer\" ]," +
              "\"best class\"      : { \"dept\" : \"math\", \"course\" : \"101\" }," +
              "\"favorite class\"  : { \"dept\" : \"math\", \"course\" : \"101\" }," +
              "\"worst class\"     : { \"dept\" : \"art\", \"course\" : \"201\" }" +
          "}" +
      "}";

  private String expectedEqualityJson =
      "{" +
        "\"school\"          : \"usc\"," +
        "\"years attended\"  : 4," +
        "\"gpa\"             : 3.29," +
        "\"graduated\"       : true," +
        "\"sports\"          : null," +
        "\"activities\"      : [ \"pkg\", \"ddd\" ]," +
        "\"clubs\"           : [ \"pkg\", \"ddd\" ]," +
        "\"extracurricular\" : [ \"soup kitchen volunteer\" ]," +
        "\"best class\"      : { \"dept\" : \"math\", \"course\" : \"101\" }," +
        "\"favorite class\"  : { \"dept\" : \"math\", \"course\" : \"101\" }," +
        "\"worst class\"     : { \"dept\" : \"art\", \"course\" : \"201\" }" +
      "}";

  @Test
  public void filterLongEquals() throws IOException, ParseException {
    JsonSequence json = valueParser.parse(equalityJson);
    Context context = parseAndExecute("$.education?(@.\"years attended\" == 4)", json);

    JsonSequence expected = valueParser.parse(expectedEqualityJson);
    Assert.assertEquals(expected, context.val);
  }

  @Test
  public void filterLongEqualsFails() throws IOException, ParseException {
    JsonSequence json = valueParser.parse(equalityJson);
    Context context = parseAndExecute("$.education?(@.\"years attended\" == 3)", json);

    Assert.assertEquals(JsonSequence.emptyResult, context.val);
  }

  @Test
  public void filterLongDoubleEquals() throws IOException, ParseException {
    JsonSequence json = valueParser.parse(equalityJson);
    Context context = parseAndExecute("$.education?(@.\"years attended\" == 4.0)", json);

    JsonSequence expected = valueParser.parse(expectedEqualityJson);
    Assert.assertEquals(expected, context.val);
  }

  @Test
  public void filterLongDoubleEqualsFails() throws IOException, ParseException {
    JsonSequence json = valueParser.parse(equalityJson);
    Context context = parseAndExecute("$.education?(@.\"years attended\" == 3.0)", json);

    Assert.assertEquals(JsonSequence.emptyResult, context.val);
  }

  @Test
  public void filterDoubleEquals() throws IOException, ParseException {
    JsonSequence json = valueParser.parse(equalityJson);
    Context context = parseAndExecute("$.education?(@.gpa == 3.29)", json);

    JsonSequence expected = valueParser.parse(expectedEqualityJson);
    Assert.assertEquals(expected, context.val);
  }

  @Test
  public void filterDoubleEqualsFails() throws IOException, ParseException {
    JsonSequence json = valueParser.parse(equalityJson);
    Context context = parseAndExecute("$.education?(@.gpa == 3.00)", json);

    Assert.assertEquals(JsonSequence.emptyResult, context.val);
  }

  @Test
  public void filterStringEquals() throws IOException, ParseException {
    JsonSequence json = valueParser.parse(equalityJson);
    Context context = parseAndExecute("$.education?(@.school == \"usc\")", json);

    JsonSequence expected = valueParser.parse(expectedEqualityJson);
    Assert.assertEquals(expected, context.val);
  }

  @Test
  public void filterStringEqualsFails() throws IOException, ParseException {
    JsonSequence json = valueParser.parse(equalityJson);
    Context context = parseAndExecute("$.education?(@.school == \"ucla\")", json);

    Assert.assertEquals(JsonSequence.emptyResult, context.val);
  }

  @Test
  public void filterBoolEquals() throws IOException, ParseException {
    JsonSequence json = valueParser.parse(equalityJson);
    Context context = parseAndExecute("$.education?(@.graduated == true)", json);

    JsonSequence expected = valueParser.parse(expectedEqualityJson);
    Assert.assertEquals(expected, context.val);
  }

  @Test
  public void filterBoolEqualsFails() throws IOException, ParseException {
    JsonSequence json = valueParser.parse(equalityJson);
    Context context = parseAndExecute("$.education?(@.graduated == false)", json);

    Assert.assertEquals(JsonSequence.emptyResult, context.val);
  }

  @Test
  public void filterNullEquals() throws IOException, ParseException {
    JsonSequence json = valueParser.parse(equalityJson);
    Context context = parseAndExecute("$.education?(@.sports == null)", json);

    JsonSequence expected = valueParser.parse(expectedEqualityJson);
    Assert.assertEquals(expected, context.val);
  }

  @Test
  public void filterNullEqualsFails() throws IOException, ParseException {
    JsonSequence json = valueParser.parse(equalityJson);
    Context context = parseAndExecute("$.education?(@.graduated == null)", json);

    Assert.assertEquals(JsonSequence.emptyResult, context.val);
  }

  @Test
  public void filterEmptyEquals() throws IOException, ParseException {
    // Test that the right thing happens when the preceding path is empty
    JsonSequence json = valueParser.parse(equalityJson);
    Context context = parseAndExecute("$.nosuch?(@.graduated == true)", json);

    Assert.assertEquals(JsonSequence.emptyResult, context.val);
  }

  @Test
  public void filterBadTypesEquals() throws IOException, ParseException {
    String path = "$.education?(@.graduated == 3.141592654)";
    JsonSequence json = valueParser.parse(equalityJson);
    Context context = parseAndExecute(path, json);
    Assert.assertEquals(JsonSequence.emptyResult, context.val);
    try {
      context.errorListener.checkForErrors(path);
      Assert.fail();
    } catch (ParseException e) {
      Assert.assertEquals("'$.education?(@.graduated == 3.141592654)' produced a semantic error: Cannot compare a bool to a double", e.getMessage());
    }
  }

  @Test
  public void filterLongNe() throws IOException, ParseException {
    JsonSequence json = valueParser.parse(equalityJson);
    Context context = parseAndExecute("$.education?(@.\"years attended\" != 5)", json);

    JsonSequence expected = valueParser.parse(expectedEqualityJson);
    Assert.assertEquals(expected, context.val);
  }

  @Test
  public void filterLongNeFails() throws IOException, ParseException {
    JsonSequence json = valueParser.parse(equalityJson);
    Context context = parseAndExecute("$.education?(@.\"years attended\" != 4)", json);

    Assert.assertEquals(JsonSequence.emptyResult, context.val);
  }

  @Test
  public void filterLongDoubleNe() throws IOException, ParseException {
    JsonSequence json = valueParser.parse(equalityJson);
    Context context = parseAndExecute("$.education?(@.\"years attended\" != 4.1)", json);

    JsonSequence expected = valueParser.parse(expectedEqualityJson);
    Assert.assertEquals(expected, context.val);
  }

  @Test
  public void filterLongDoubleNeFails() throws IOException, ParseException {
    JsonSequence json = valueParser.parse(equalityJson);
    Context context = parseAndExecute("$.education?(@.\"years attended\" != 4.0)", json);

    Assert.assertEquals(JsonSequence.emptyResult, context.val);
  }

  @Test
  public void filterDoubleNe() throws IOException, ParseException {
    JsonSequence json = valueParser.parse(equalityJson);
    Context context = parseAndExecute("$.education?(@.gpa != 3.19)", json);

    JsonSequence expected = valueParser.parse(expectedEqualityJson);
    Assert.assertEquals(expected, context.val);
  }

  @Test
  public void filterDoubleNeFails() throws IOException, ParseException {
    JsonSequence json = valueParser.parse(equalityJson);
    Context context = parseAndExecute("$.education?(@.gpa != 3.29)", json);

    Assert.assertEquals(JsonSequence.emptyResult, context.val);
  }

  @Test
  public void filterStringNe() throws IOException, ParseException {
    JsonSequence json = valueParser.parse(equalityJson);
    Context context = parseAndExecute("$.education?(@.school != \"ucla\")", json);

    JsonSequence expected = valueParser.parse(expectedEqualityJson);
    Assert.assertEquals(expected, context.val);
  }

  @Test
  public void filterStringNeFails() throws IOException, ParseException {
    JsonSequence json = valueParser.parse(equalityJson);
    Context context = parseAndExecute("$.education?(@.school != \"usc\")", json);

    Assert.assertEquals(JsonSequence.emptyResult, context.val);
  }

  @Test
  public void filterBoolNe() throws IOException, ParseException {
    JsonSequence json = valueParser.parse(equalityJson);
    Context context = parseAndExecute("$.education?(@.graduated != false)", json);

    JsonSequence expected = valueParser.parse(expectedEqualityJson);
    Assert.assertEquals(expected, context.val);
  }

  @Test
  public void filterBoolNeFails() throws IOException, ParseException {
    JsonSequence json = valueParser.parse(equalityJson);
    Context context = parseAndExecute("$.education?(@.graduated <> true)", json);

    Assert.assertEquals(JsonSequence.emptyResult, context.val);
  }

  @Test
  public void filterNullNe() throws IOException, ParseException {
    JsonSequence json = valueParser.parse(equalityJson);
    Context context = parseAndExecute("$.education?(@.graduated <> null)", json);

    JsonSequence expected = valueParser.parse(expectedEqualityJson);
    Assert.assertEquals(expected, context.val);
  }

  @Test
  public void filterNullNeFails() throws IOException, ParseException {
    JsonSequence json = valueParser.parse(equalityJson);
    Context context = parseAndExecute("$.education?(@.sports != null)", json);

    Assert.assertEquals(JsonSequence.emptyResult, context.val);
  }

  @Test
  public void filterEmptyNe() throws IOException, ParseException {
    // Test that the right thing happens when the preceding path is empty
    JsonSequence json = valueParser.parse(equalityJson);
    Context context = parseAndExecute("$.nosuch?(@.graduated != true)", json);

    Assert.assertEquals(JsonSequence.emptyResult, context.val);
  }

  @Test
  public void filterBadTypesNe() throws IOException, ParseException {
    String path = "$.education?(@.graduated != 3.141592654)";
    JsonSequence json = valueParser.parse(equalityJson);
    Context context = parseAndExecute(path, json);
    Assert.assertEquals(JsonSequence.emptyResult, context.val);
    try {
      context.errorListener.checkForErrors(path);
      Assert.fail();
    } catch (ParseException e) {
      Assert.assertEquals("'$.education?(@.graduated != 3.141592654)' produced a semantic error: Cannot compare a bool to a double", e.getMessage());
    }
  }

  @Test
  public void filterLongLt() throws IOException, ParseException {
    JsonSequence json = valueParser.parse(equalityJson);
    Context context = parseAndExecute("$.education?(@.\"years attended\" < 5)", json);

    JsonSequence expected = valueParser.parse(expectedEqualityJson);
    Assert.assertEquals(expected, context.val);
  }

  @Test
  public void filterLongLtFails() throws IOException, ParseException {
    JsonSequence json = valueParser.parse(equalityJson);
    Context context = parseAndExecute("$.education?(@.\"years attended\" < 4)", json);

    Assert.assertEquals(JsonSequence.emptyResult, context.val);
  }

  @Test
  public void filterLongDoubleLt() throws IOException, ParseException {
    JsonSequence json = valueParser.parse(equalityJson);
    Context context = parseAndExecute("$.education?(@.\"years attended\" < 4.1)", json);

    JsonSequence expected = valueParser.parse(expectedEqualityJson);
    Assert.assertEquals(expected, context.val);
  }

  @Test
  public void filterLongDoubleLtFails() throws IOException, ParseException {
    JsonSequence json = valueParser.parse(equalityJson);
    Context context = parseAndExecute("$.education?(@.\"years attended\" < 4.0)", json);

    Assert.assertEquals(JsonSequence.emptyResult, context.val);
  }

  @Test
  public void filterDoubleLt() throws IOException, ParseException {
    JsonSequence json = valueParser.parse(equalityJson);
    Context context = parseAndExecute("$.education?(@.gpa < 3.50)", json);

    JsonSequence expected = valueParser.parse(expectedEqualityJson);
    Assert.assertEquals(expected, context.val);
  }

  @Test
  public void filterDoubleLtFails() throws IOException, ParseException {
    JsonSequence json = valueParser.parse(equalityJson);
    Context context = parseAndExecute("$.education?(@.gpa < 2.29)", json);

    Assert.assertEquals(JsonSequence.emptyResult, context.val);
  }

  @Test
  public void filterStringLt() throws IOException, ParseException {
    JsonSequence json = valueParser.parse(equalityJson);
    Context context = parseAndExecute("$.education?(@.school < \"yyy\")", json);

    JsonSequence expected = valueParser.parse(expectedEqualityJson);
    Assert.assertEquals(expected, context.val);
  }

  @Test
  public void filterStringLtFails() throws IOException, ParseException {
    JsonSequence json = valueParser.parse(equalityJson);
    Context context = parseAndExecute("$.education?(@.school < \"asc\")", json);

    Assert.assertEquals(JsonSequence.emptyResult, context.val);
  }

  @Test
  public void filterLtBadType() throws IOException, ParseException {
    String path = "$.education?(@.graduated < false)";
    JsonSequence json = valueParser.parse(equalityJson);
    Context context = parseAndExecute(path, json);
    Assert.assertEquals(JsonSequence.emptyResult, context.val);

    try {
      context.errorListener.checkForErrors(path);
      Assert.fail();
    } catch (ParseException e) {
      Assert.assertEquals("'$.education?(@.graduated < false)' produced a semantic error: Cannot apply an inequality operator to a bool", e.getMessage());
    }
  }

  @Test
  public void filterEmptyLt() throws IOException, ParseException {
    // Test that the right thing happens when the preceding path is empty
    JsonSequence json = valueParser.parse(equalityJson);
    Context context = parseAndExecute("$.nosuch?(@.gpa < 4.00)", json);

    Assert.assertEquals(JsonSequence.emptyResult, context.val);
  }

  @Test
  public void filterBadTypesLt() throws IOException, ParseException {
    String path = "$.education?(@.gpa < 'abc')";
    JsonSequence json = valueParser.parse(equalityJson);
    Context context = parseAndExecute(path, json);
    Assert.assertEquals(JsonSequence.emptyResult, context.val);
    try {
      context.errorListener.checkForErrors(path);
      Assert.fail();
    } catch (ParseException e) {
      Assert.assertEquals("'$.education?(@.gpa < 'abc')' produced a semantic error: Cannot compare a decimal to a string", e.getMessage());
    }
  }

  @Test
  public void filterLongLe() throws IOException, ParseException {
    JsonSequence json = valueParser.parse(equalityJson);
    Context context = parseAndExecute("$.education?(@.\"years attended\" <= 4)", json);

    JsonSequence expected = valueParser.parse(expectedEqualityJson);
    Assert.assertEquals(expected, context.val);
  }

  @Test
  public void filterLongLeFails() throws IOException, ParseException {
    JsonSequence json = valueParser.parse(equalityJson);
    Context context = parseAndExecute("$.education?(@.\"years attended\" <= 3)", json);

    Assert.assertEquals(JsonSequence.emptyResult, context.val);
  }

  @Test
  public void filterLongDoubleLe() throws IOException, ParseException {
    JsonSequence json = valueParser.parse(equalityJson);
    Context context = parseAndExecute("$.education?(@.\"years attended\" <= 4.1)", json);

    JsonSequence expected = valueParser.parse(expectedEqualityJson);
    Assert.assertEquals(expected, context.val);
  }

  @Test
  public void filterLongDoubleLeFails() throws IOException, ParseException {
    JsonSequence json = valueParser.parse(equalityJson);
    Context context = parseAndExecute("$.education?(@.\"years attended\" <= 3.99)", json);

    Assert.assertEquals(JsonSequence.emptyResult, context.val);
  }

  @Test
  public void filterDoubleLe() throws IOException, ParseException {
    JsonSequence json = valueParser.parse(equalityJson);
    Context context = parseAndExecute("$.education?(@.gpa <= 3.50)", json);

    JsonSequence expected = valueParser.parse(expectedEqualityJson);
    Assert.assertEquals(expected, context.val);
  }

  @Test
  public void filterDoubleLeFails() throws IOException, ParseException {
    JsonSequence json = valueParser.parse(equalityJson);
    Context context = parseAndExecute("$.education?(@.gpa <= 2.29)", json);

    Assert.assertEquals(JsonSequence.emptyResult, context.val);
  }

  @Test
  public void filterStringLe() throws IOException, ParseException {
    JsonSequence json = valueParser.parse(equalityJson);
    Context context = parseAndExecute("$.education?(@.school <= \"uscaaab\")", json);

    JsonSequence expected = valueParser.parse(expectedEqualityJson);
    Assert.assertEquals(expected, context.val);
  }

  @Test
  public void filterStringLeFails() throws IOException, ParseException {
    JsonSequence json = valueParser.parse(equalityJson);
    Context context = parseAndExecute("$.education?(@.school <= \"us\")", json);

    Assert.assertEquals(JsonSequence.emptyResult, context.val);
  }

  @Test
  public void filterLeBadType() throws IOException, ParseException {
    String path = "$.education?(@.graduated <= false)";
    JsonSequence json = valueParser.parse(equalityJson);
    Context context = parseAndExecute(path, json);

    try {
      context.errorListener.checkForErrors(path);
      Assert.fail();
    } catch (ParseException e) {
      Assert.assertEquals("'$.education?(@.graduated <= false)' produced a semantic error: Cannot apply an inequality operator to a bool", e.getMessage());
    }
  }

  @Test
  public void filterEmptyLe() throws IOException, ParseException {
    // Test that the right thing happens when the preceding path is empty
    JsonSequence json = valueParser.parse(equalityJson);
    Context context = parseAndExecute("$.nosuch?(@.gpa <= 4.00)", json);

    Assert.assertEquals(JsonSequence.emptyResult, context.val);
  }

  @Test
  public void filterBadTypesLe() throws IOException, ParseException {
    String path = "$.education?(@.gpa <= 'abc')";
    JsonSequence json = valueParser.parse(equalityJson);
    Context context = parseAndExecute(path, json);
    try {
      context.errorListener.checkForErrors(path);
      Assert.fail();
    } catch (ParseException e) {
      Assert.assertEquals("'$.education?(@.gpa <= 'abc')' produced a semantic error: Cannot compare a decimal to a string", e.getMessage());
    }
  }

  @Test
  public void filterLongGt() throws IOException, ParseException {
    JsonSequence json = valueParser.parse(equalityJson);
    Context context = parseAndExecute("$.education?(@.\"years attended\" > 3)", json);

    JsonSequence expected = valueParser.parse(expectedEqualityJson);
    Assert.assertEquals(expected, context.val);
  }

  @Test
  public void filterLongGtFails() throws IOException, ParseException {
    JsonSequence json = valueParser.parse(equalityJson);
    Context context = parseAndExecute("$.education?(@.\"years attended\" > 4)", json);

    Assert.assertEquals(JsonSequence.emptyResult, context.val);
  }

  @Test
  public void filterLongDoubleGt() throws IOException, ParseException {
    JsonSequence json = valueParser.parse(equalityJson);
    Context context = parseAndExecute("$.education?(@.\"years attended\" > 3.9)", json);

    JsonSequence expected = valueParser.parse(expectedEqualityJson);
    Assert.assertEquals(expected, context.val);
  }

  @Test
  public void filterLongDoubleGtFails() throws IOException, ParseException {
    JsonSequence json = valueParser.parse(equalityJson);
    Context context = parseAndExecute("$.education?(@.\"years attended\" > 4.0)", json);

    Assert.assertEquals(JsonSequence.emptyResult, context.val);
  }

  @Test
  public void filterDoubleGt() throws IOException, ParseException {
    JsonSequence json = valueParser.parse(equalityJson);
    Context context = parseAndExecute("$.education?(@.gpa > 3.00)", json);

    JsonSequence expected = valueParser.parse(expectedEqualityJson);
    Assert.assertEquals(expected, context.val);
  }

  @Test
  public void filterDoubleGtFails() throws IOException, ParseException {
    JsonSequence json = valueParser.parse(equalityJson);
    Context context = parseAndExecute("$.education?(@.gpa > 3.79)", json);

    Assert.assertEquals(JsonSequence.emptyResult, context.val);
  }

  @Test
  public void filterStringGt() throws IOException, ParseException {
    JsonSequence json = valueParser.parse(equalityJson);
    Context context = parseAndExecute("$.education?(@.school > \"u\")", json);

    JsonSequence expected = valueParser.parse(expectedEqualityJson);
    Assert.assertEquals(expected, context.val);
  }

  @Test
  public void filterStringGtFails() throws IOException, ParseException {
    JsonSequence json = valueParser.parse(equalityJson);
    Context context = parseAndExecute("$.education?(@.school > \"z\")", json);

    Assert.assertEquals(JsonSequence.emptyResult, context.val);
  }

  @Test
  public void filterGtBadType() throws IOException, ParseException {
    String path = "$.education?(@.graduated > false)";
    JsonSequence json = valueParser.parse(equalityJson);
    Context context = parseAndExecute(path, json);
    Assert.assertEquals(JsonSequence.emptyResult, context.val);

    try {
      context.errorListener.checkForErrors(path);
      Assert.fail();
    } catch (ParseException e) {
      Assert.assertEquals("'$.education?(@.graduated > false)' produced a semantic error: Cannot apply an inequality operator to a bool", e.getMessage());
    }
  }

  @Test
  public void filterEmptyGt() throws IOException, ParseException {
    // Test that the right thing happens when the preceding path is empty
    JsonSequence json = valueParser.parse(equalityJson);
    Context context = parseAndExecute("$.nosuch?(@.gpa > 4.00)", json);

    Assert.assertEquals(JsonSequence.emptyResult, context.val);
  }

  @Test
  public void filterBadTypesGt() throws IOException, ParseException {
    String path = "$.education?(@.gpa > 'abc')";
    JsonSequence json = valueParser.parse(equalityJson);
    Context context = parseAndExecute(path, json);
    Assert.assertEquals(JsonSequence.emptyResult, context.val);
    try {
      context.errorListener.checkForErrors(path);
      Assert.fail();
    } catch (ParseException e) {
      Assert.assertEquals("'$.education?(@.gpa > 'abc')' produced a semantic error: Cannot compare a decimal to a string", e.getMessage());
    }
  }

  @Test
  public void filterLongGe() throws IOException, ParseException {
    JsonSequence json = valueParser.parse(equalityJson);
    Context context = parseAndExecute("$.education?(@.\"years attended\" >= 4)", json);

    JsonSequence expected = valueParser.parse(expectedEqualityJson);
    Assert.assertEquals(expected, context.val);
  }

  @Test
  public void filterLongGeFails() throws IOException, ParseException {
    JsonSequence json = valueParser.parse(equalityJson);
    Context context = parseAndExecute("$.education?(@.\"years attended\" >= 5)", json);

    Assert.assertEquals(JsonSequence.emptyResult, context.val);
  }

  @Test
  public void filterLongDoubleGe() throws IOException, ParseException {
    JsonSequence json = valueParser.parse(equalityJson);
    Context context = parseAndExecute("$.education?(@.\"years attended\" >= 4.0)", json);

    JsonSequence expected = valueParser.parse(expectedEqualityJson);
    Assert.assertEquals(expected, context.val);
  }

  @Test
  public void filterLongDoubleGeFails() throws IOException, ParseException {
    JsonSequence json = valueParser.parse(equalityJson);
    Context context = parseAndExecute("$.education?(@.\"years attended\" >= 4.99)", json);

    Assert.assertEquals(JsonSequence.emptyResult, context.val);
  }

  @Test
  public void filterDoubleGe() throws IOException, ParseException {
    JsonSequence json = valueParser.parse(equalityJson);
    Context context = parseAndExecute("$.education?(@.gpa >= 3.29)", json);

    JsonSequence expected = valueParser.parse(expectedEqualityJson);
    Assert.assertEquals(expected, context.val);
  }

  @Test
  public void filterDoubleGeFails() throws IOException, ParseException {
    JsonSequence json = valueParser.parse(equalityJson);
    Context context = parseAndExecute("$.education?(@.gpa >= 3.99)", json);

    Assert.assertEquals(JsonSequence.emptyResult, context.val);
  }

  @Test
  public void filterStringGe() throws IOException, ParseException {
    JsonSequence json = valueParser.parse(equalityJson);
    Context context = parseAndExecute("$.education?(@.school >= \"usc\")", json);

    JsonSequence expected = valueParser.parse(expectedEqualityJson);
    Assert.assertEquals(expected, context.val);
  }

  @Test
  public void filterStringGeFails() throws IOException, ParseException {
    JsonSequence json = valueParser.parse(equalityJson);
    Context context = parseAndExecute("$.education?(@.school >= \"uxxx\")", json);

    Assert.assertEquals(JsonSequence.emptyResult, context.val);
  }

  @Test
  public void filterGeBadType() throws IOException, ParseException {
    String path = "$.education?(@.graduated >= false)";
    JsonSequence json = valueParser.parse(equalityJson);
    Context context = parseAndExecute(path, json);

    try {
      context.errorListener.checkForErrors(path);
      Assert.fail();
    } catch (ParseException e) {
      Assert.assertEquals("'$.education?(@.graduated >= false)' produced a semantic error: Cannot apply an inequality operator to a bool", e.getMessage());
    }
  }

  @Test
  public void filterEmptyGe() throws IOException, ParseException {
    // Test that the right thing happens when the preceding path is empty
    JsonSequence json = valueParser.parse(equalityJson);
    Context context = parseAndExecute("$.nosuch?(@.gpa >= 4.00)", json);

    Assert.assertEquals(JsonSequence.emptyResult, context.val);
  }

  @Test
  public void filterBadTypesGe() throws IOException, ParseException {
    String path = "$.education?(@.gpa >= 'abc')";
    JsonSequence json = valueParser.parse(equalityJson);
    Context context = parseAndExecute(path, json);
    try {
      context.errorListener.checkForErrors(path);
      Assert.fail();
    } catch (ParseException e) {
      Assert.assertEquals("'$.education?(@.gpa >= 'abc')' produced a semantic error: Cannot compare a decimal to a string", e.getMessage());
    }
  }

  // TODO test equals double -long
  // TODO test equals list
  // TODO test equals object
  // TODO test ne double -long
  // TODO test ne list
  // TODO test ne object

  private ParseTree parse(String path) throws IOException, ParseException {
    PathParser parser = new PathParser();
    parser.parse(path);
    return parser.getTree();
  }

  private Context parseAndExecute(String path, JsonSequence value) throws IOException, ParseException {
    return parseAndExecute(path, value, null);
  }

  private Context parseAndExecute(String path, JsonSequence value, Map<String, JsonSequence> passing)
      throws IOException, ParseException {
    ParseTree tree = parse(path);
    ErrorListener errorListener = new ErrorListener();
    Context context = new Context(tree, errorListener);
    PathExecutor executor = new PathExecutor(errorListener);
    context.executor = executor;
    context.val = executor.execute(context.tree, value, passing);
    return context;
  }

  // Useful for testing just the arithmetic portions
  private Context parseAdditiveExpr(String path) throws IOException, ParseException {
    ErrorListener errorListener = new ErrorListener();
    SqlJsonPathLexer scanner = new SqlJsonPathLexer(new ANTLRInputStream(new ByteArrayInputStream(path.getBytes())));
    CommonTokenStream tokens = new CommonTokenStream(scanner);
    SqlJsonPathParser parser = new SqlJsonPathParser(tokens);
    parser.addErrorListener(errorListener);
    ParseTree tree = parser.additive_expression();
    errorListener.checkForErrors(path);
    PathExecutor executor = new PathExecutor(errorListener);
    Context context =  new Context(tree, errorListener);
    context.val = executor.visit(tree);
    return context;
  }

  private static class Context {
    final ParseTree tree;
    final ErrorListener errorListener;
    PathExecutor executor;
    JsonSequence val;

    public Context(ParseTree tree, ErrorListener errorListener) {
      this.tree = tree;
      this.errorListener = errorListener;
      executor = null;
    }

  }
}
