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
    System.out.println("val is " + context.val.toString());
    Assert.assertEquals(json, context.val);
  }

  @Test
  public void matchKey() throws IOException, ParseException {
    JsonSequence json = valueParser.parse(" { \"name\" : \"fred\" }");
    Context context = parseAndExecute("$.name", json);
    Assert.assertTrue(context.val.isString());
    Assert.assertEquals("fred", context.val.asString());
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
    System.out.println("null val is " + context.val.toString());
    Assert.assertTrue(context.val.isNull());
  }

  @Test
  public void noMatchKeyQuotes() throws IOException, ParseException {
    JsonSequence json = valueParser.parse(" { \"name\" : \"fred\" }");
    Context context = parseAndExecute("$.\"address\"", json);
    Assert.assertTrue(context.val.isNull());
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
    Assert.assertTrue(context.val.isNull());
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

    JsonSequence wrappedExpected = valueParser.parse(" { \"k\" : null }");
    Assert.assertEquals(wrappedExpected.asObject().get("k"), context.val);
  }

  @Test
  public void offEndSubscriptObject() throws IOException, ParseException {
    JsonSequence json = valueParser.parse("{ \"name\" : \"fred\"," +
        "\"classes\" : [ \"science\", \"art\" ]," +
        "\"sports\"  : [ \"swimming\", \"baseball\", \"soccer\" ] }");
    Context context = parseAndExecute("$.*[2]", json);

    JsonSequence expected = valueParser.parse(" { \"classes\" : null, \"sports\" : \"soccer\" }");
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

  private ParseTree parse(String path) throws IOException, ParseException {
    PathParser parser = new PathParser();
    parser.parse(path);
    return parser.getTree();
  }

  private Context parseAndExecute(String path, JsonSequence value) throws IOException, ParseException {
    return parseAndExecute(path, value, null, EmptyOrErrorBehavior.NULL, EmptyOrErrorBehavior.NULL);
  }

  private Context parseAndExecute(String path, JsonSequence value, Map<String, JsonSequence> passing) throws IOException, ParseException {
    return parseAndExecute(path, value, passing, EmptyOrErrorBehavior.NULL, EmptyOrErrorBehavior.NULL);
  }

  private Context parseAndExecute(String path, JsonSequence value, Map<String, JsonSequence> passing, EmptyOrErrorBehavior onEmpty,
                                  EmptyOrErrorBehavior onError)
      throws IOException, ParseException {
    ParseTree tree = parse(path);
    ErrorListener errorListener = new ErrorListener();
    Context context = new Context(tree, errorListener);
    PathExecutor executor = new PathExecutor(errorListener);
    context.executor = executor;
    context.val = executor.execute(context.tree, value, passing, onEmpty, onError);
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
