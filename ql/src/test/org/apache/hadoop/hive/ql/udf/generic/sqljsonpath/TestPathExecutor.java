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

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class TestPathExecutor {

  private static JsonValueParser valueParser;
  private static JsonSequence    emptyJson;

  @BeforeClass
  public static void buildValueParser() throws IOException, JsonPathException {
    ErrorListener listener = new ErrorListener();
    valueParser = new JsonValueParser(listener);
    emptyJson = valueParser.parse("{ }");
  }

  @Test
  public void syntaxError() throws IOException {
    try {
      PathParser parser = new PathParser();
      parser.parse("fizzbot");
    } catch (JsonPathException e) {
      Assert.assertEquals("'fizzbot' produced a syntax error: no viable alternative at input 'fizzbot' on line 1 at position 0", e.getMessage());
    }

  }

  @Test
  public void laxDefault() throws IOException, JsonPathException {
    PathExecutionResult pathExecResult = parseAndExecute("$.a");
    Assert.assertEquals(Mode.LAX, pathExecResult.executor.getMode());
  }

  @Test
  public void laxSpecified() throws IOException, JsonPathException {
    PathExecutionResult pathExecResult = parseAndExecute("lax $.a");
    Assert.assertEquals(Mode.LAX, pathExecResult.executor.getMode());
  }

  @Test
  public void strict() throws IOException, JsonPathException {
    PathExecutionResult pathExecResult = parseAndExecute("strict $.a");
    Assert.assertEquals(Mode.STRICT, pathExecResult.executor.getMode());
  }

  @Test
  public void longLiteral() throws IOException, JsonPathException {
    PathExecutionResult pathExecResult = parseAndExecute("5");
    Assert.assertTrue(pathExecResult.executor.returnedByVisit.isLong());
    Assert.assertEquals(5, pathExecResult.executor.returnedByVisit.asLong());

  }

  @Test
  public void doubleLiteral() throws IOException, JsonPathException {
    PathExecutionResult pathExecResult = parseAndExecute("5.1");
    Assert.assertTrue(pathExecResult.executor.returnedByVisit.isDouble());
    Assert.assertEquals(5.1, pathExecResult.executor.returnedByVisit.asDouble(), 0.001);
  }

  @Test
  public void booleanLiteral() throws IOException, JsonPathException {
    PathExecutionResult pathExecResult = parseAndExecute("true");
    Assert.assertTrue(pathExecResult.executor.returnedByVisit.isBool());
    Assert.assertTrue(pathExecResult.executor.returnedByVisit.asBool());
  }

  @Test
  public void nullLiteral() throws IOException, JsonPathException {
    PathExecutionResult pathExecResult = parseAndExecute("null");
    Assert.assertTrue(pathExecResult.executor.returnedByVisit.isNull());
  }

  @Test
  public void singleQuoteStringLiteral() throws IOException, JsonPathException {
    PathExecutionResult pathExecResult = parseAndExecute("'fred'");
    Assert.assertTrue(pathExecResult.executor.returnedByVisit.isString());
    Assert.assertEquals("fred", pathExecResult.executor.returnedByVisit.asString());
  }

  @Test
  public void doubleQuoteStringLiteral() throws IOException, JsonPathException {
    PathExecutionResult pathExecResult = parseAndExecute("\"fred\"");
    Assert.assertTrue(pathExecResult.executor.returnedByVisit.isString());
    Assert.assertEquals("fred", pathExecResult.executor.returnedByVisit.asString());
  }

  @Test
  public void addLong() throws IOException, JsonPathException {
    PathExecutionResult pathExecResult = parseAndExecute("5 + 6");
    Assert.assertTrue(pathExecResult.executor.returnedByVisit.isLong());
    Assert.assertEquals(11L, pathExecResult.executor.returnedByVisit.asLong());
  }

  @Test
  public void subtractLong() throws IOException, JsonPathException {
    PathExecutionResult pathExecResult = parseAndExecute("8 - 4");
    Assert.assertTrue(pathExecResult.executor.returnedByVisit.isLong());
    Assert.assertEquals(4L, pathExecResult.executor.returnedByVisit.asLong());
  }

  @Test
  public void multiplyLong() throws IOException, JsonPathException {
    PathExecutionResult pathExecResult = parseAndExecute("9 * 10");
    Assert.assertTrue(pathExecResult.executor.returnedByVisit.isLong());
    Assert.assertEquals(90L, pathExecResult.executor.returnedByVisit.asLong());
  }

  @Test
  public void divideLong() throws IOException, JsonPathException {
    PathExecutionResult pathExecResult = parseAndExecute("9 / 3");
    Assert.assertTrue(pathExecResult.executor.returnedByVisit.isLong());
    Assert.assertEquals(3L, pathExecResult.executor.returnedByVisit.asLong());
  }

  @Test
  public void modLong() throws IOException, JsonPathException {
    PathExecutionResult pathExecResult = parseAndExecute("10 % 3");
    Assert.assertTrue(pathExecResult.executor.returnedByVisit.isLong());
    Assert.assertEquals(1L, pathExecResult.executor.returnedByVisit.asLong());
  }

  @Test
  public void addDouble() throws IOException, JsonPathException {
    PathExecutionResult pathExecResult = parseAndExecute("5.1 + 7.2");
    Assert.assertTrue(pathExecResult.executor.returnedByVisit.isDouble());
    Assert.assertEquals(12.3, pathExecResult.executor.returnedByVisit.asDouble(), 0.00001);
  }

  @Test
  public void subtractDouble() throws IOException, JsonPathException {
    PathExecutionResult pathExecResult = parseAndExecute("10.0 - .2");
    Assert.assertTrue(pathExecResult.executor.returnedByVisit.isDouble());
    Assert.assertEquals(9.8, pathExecResult.executor.returnedByVisit.asDouble(), 0.00001);
  }

  @Test
  public void multiplyDouble() throws IOException, JsonPathException {
    PathExecutionResult pathExecResult = parseAndExecute("2.0 * 3.141592654");
    Assert.assertTrue(pathExecResult.executor.returnedByVisit.isDouble());
    Assert.assertEquals(6.283185308, pathExecResult.executor.returnedByVisit.asDouble(), 0.001);
  }

  @Test
  public void divideDouble() throws IOException, JsonPathException {
    PathExecutionResult pathExecResult = parseAndExecute("20.0 / 3.0");
    Assert.assertTrue(pathExecResult.executor.returnedByVisit.isDouble());
    Assert.assertEquals(6.66666, pathExecResult.executor.returnedByVisit.asDouble(), 0.001);
  }

  @Test
  public void addLongAndDouble() throws IOException, JsonPathException {
    PathExecutionResult pathExecResult = parseAndExecute("5 + 7.2");
    Assert.assertTrue(pathExecResult.executor.returnedByVisit.isDouble());
    Assert.assertEquals(12.2, pathExecResult.executor.returnedByVisit.asDouble(), 0.00001);
  }

  @Test
  public void subtractLongAndDouble() throws IOException, JsonPathException {
    PathExecutionResult pathExecResult = parseAndExecute("10 - 7.2");
    Assert.assertTrue(pathExecResult.executor.returnedByVisit.isDouble());
    Assert.assertEquals(2.8, pathExecResult.executor.returnedByVisit.asDouble(), 0.00001);
  }

  @Test
  public void multiplyLongAndDouble() throws IOException, JsonPathException {
    PathExecutionResult pathExecResult = parseAndExecute("10 * 1.238273");
    Assert.assertTrue(pathExecResult.executor.returnedByVisit.isDouble());
    Assert.assertEquals(12.38273, pathExecResult.executor.returnedByVisit.asDouble(), 0.00001);
  }

  @Test
  public void divideLongAndDouble() throws IOException, JsonPathException {
    PathExecutionResult pathExecResult = parseAndExecute("20 / 1.238273");
    Assert.assertTrue(pathExecResult.executor.returnedByVisit.isDouble());
    Assert.assertEquals(16.151527167272484, pathExecResult.executor.returnedByVisit.asDouble(), 0.00001);
  }

  @Test
  public void addDoubleAndLong() throws IOException, JsonPathException {
    PathExecutionResult pathExecResult = parseAndExecute("5.2 + 7");
    Assert.assertTrue(pathExecResult.executor.returnedByVisit.isDouble());
    Assert.assertEquals(12.2, pathExecResult.executor.returnedByVisit.asDouble(), 0.00001);

  }

  @Test
  public void subtractDoubleAndLong() throws IOException, JsonPathException {
    PathExecutionResult pathExecResult = parseAndExecute("10.2 - 7");
    Assert.assertTrue(pathExecResult.executor.returnedByVisit.isDouble());
    Assert.assertEquals(3.2, pathExecResult.executor.returnedByVisit.asDouble(), 0.00001);
  }

  @Test
  public void multiplyDoubleAndLong() throws IOException, JsonPathException {
    PathExecutionResult pathExecResult = parseAndExecute("1.238273 * 10");
    Assert.assertTrue(pathExecResult.executor.returnedByVisit.isDouble());
    Assert.assertEquals(12.38273, pathExecResult.executor.returnedByVisit.asDouble(), 0.00001);
  }

  @Test
  public void divideDoubleAndLong() throws IOException, JsonPathException {
    PathExecutionResult pathExecResult = parseAndExecute("20.238273 / 3");
    Assert.assertTrue(pathExecResult.executor.returnedByVisit.isDouble());
    Assert.assertEquals(6.746091, pathExecResult.executor.returnedByVisit.asDouble(), 0.00001);
  }

  @Test
  public void longUnaryPlus() throws IOException, JsonPathException {
    PathExecutionResult pathExecResult = parseAndExecute("+3");
    Assert.assertTrue(pathExecResult.executor.returnedByVisit.isLong());
    Assert.assertEquals(3L, pathExecResult.executor.returnedByVisit.asLong());
  }

  @Test
  public void longUnaryMinus() throws IOException, JsonPathException {
    PathExecutionResult pathExecResult = parseAndExecute("-3");
    Assert.assertTrue(pathExecResult.executor.returnedByVisit.isLong());
    Assert.assertEquals(-3L, pathExecResult.executor.returnedByVisit.asLong());
  }

  @Test
  public void doubleUnaryPlus() throws IOException, JsonPathException {
    PathExecutionResult pathExecResult = parseAndExecute("+20.238273");
    Assert.assertTrue(pathExecResult.executor.returnedByVisit.isDouble());
    Assert.assertEquals(20.238273, pathExecResult.executor.returnedByVisit.asDouble(), 0.00001);
  }

  @Test
  public void doubleUnaryMinus() throws IOException, JsonPathException {
    PathExecutionResult pathExecResult = parseAndExecute("-20.238273");
    Assert.assertTrue(pathExecResult.executor.returnedByVisit.isDouble());
    Assert.assertEquals(-20.238273, pathExecResult.executor.returnedByVisit.asDouble(), 0.00001);
  }

  @Test
  public void badLongAdd() throws IOException {
    try {
      String pathExpr = "20 + 'fred'";
      parseAndExecute(pathExpr);
      Assert.fail();
    } catch (JsonPathException e) {
      Assert.assertEquals("'20 + 'fred'' produced a semantic error: You cannot do arithmetic on a string at \"20 + 'fred'\"", e.getMessage());
    }
  }

  @Test
  public void badLongSubtract() throws IOException {
    try {
      String pathExpr = "20 - 'fred'";
      parseAndExecute(pathExpr);
      Assert.fail();
    } catch (JsonPathException e) {
      Assert.assertEquals("'20 - 'fred'' produced a semantic error: You cannot do arithmetic on a string at \"20 - 'fred'\"", e.getMessage());
    }
  }

  @Test
  public void badLongMultiply() throws IOException {
    try {
      String pathExpr = "20 * true";
      parseAndExecute(pathExpr);
      Assert.fail();
    } catch (JsonPathException e) {
      Assert.assertEquals("'20 * true' produced a semantic error: You cannot do arithmetic on a bool at \"20 * true\"", e.getMessage());
    }
  }

  @Test
  public void badLongDivide() throws IOException {
    try {
      String pathExpr = "20 / 'bob'";
      parseAndExecute(pathExpr);
      Assert.fail();
    } catch (JsonPathException e) {
      Assert.assertEquals("'20 / 'bob'' produced a semantic error: You cannot do arithmetic on a string at \"20 / 'bob'\"", e.getMessage());
    }
  }

  @Test
  public void badMod() throws IOException {
    try {
      String pathExpr = "20 % 3.0";
      parseAndExecute(pathExpr);
      Assert.fail();
    } catch (JsonPathException e) {
      Assert.assertEquals("'20 % 3.0' produced a semantic error: You cannot do mod on a double at \"20 % 3.0\"", e.getMessage());
    }
  }

  @Test
  public void badStringAdd() throws IOException {
    try {
      String pathExpr = "'fred' + 3.0";
      parseAndExecute(pathExpr);
      Assert.fail();
    } catch (JsonPathException e) {
      Assert.assertEquals("''fred' + 3.0' produced a semantic error: You cannot do arithmetic on a string at \"'fred' + 3.0\"", e.getMessage());
    }
  }

  @Test
  public void badStringSubtract() throws IOException {
    try {
      String pathExpr = "'fred' - 3.0";
      parseAndExecute(pathExpr);
      Assert.fail();
    } catch (JsonPathException e) {
      Assert.assertEquals("''fred' - 3.0' produced a semantic error: You cannot do arithmetic on a string at \"'fred' - 3.0\"", e.getMessage());
    }
  }

  @Test
  public void badStringMultiply() throws IOException {
    try {
      String pathExpr = "'fred' * 3.0";
      parseAndExecute(pathExpr);
      Assert.fail();
    } catch (JsonPathException e) {
      Assert.assertEquals("''fred' * 3.0' produced a semantic error: You cannot do arithmetic on a string at \"'fred' * 3.0\"", e.getMessage());
    }
  }

  @Test
  public void badStringDivide() throws IOException {
    try {
      String pathExpr = "'fred' / 3.0";
      parseAndExecute(pathExpr);
      Assert.fail();
    } catch (JsonPathException e) {
      Assert.assertEquals("''fred' / 3.0' produced a semantic error: You cannot do arithmetic on a string at \"'fred' / 3.0\"", e.getMessage());
    }
  }

  @Test
  public void badStringMod() throws IOException {
    try {
      String pathExpr = "'fred' % 3.0";
      parseAndExecute(pathExpr);
      Assert.fail();
    } catch (JsonPathException e) {
      Assert.assertEquals("''fred' % 3.0' produced a semantic error: You cannot do mod on a string at \"'fred' % 3.0\"", e.getMessage());
    }
  }

  @Test
  public void addNull() throws IOException {
    try {
      parseAndExecute("20 + null");
      Assert.fail();
    } catch (JsonPathException e) {
      Assert.assertEquals("'20 + null' produced a semantic error: You cannot do arithmetic on a null at \"20 + null\"", e.getMessage());
    }
  }

  @Test
  public void subtractNull() throws IOException {
    try {
      parseAndExecute("20.0 - null");
      Assert.fail();
    } catch (JsonPathException e) {
      Assert.assertEquals("'20.0 - null' produced a semantic error: You cannot do arithmetic on a null at \"20.0 - null\"", e.getMessage());
    }
  }

  @Test
  public void multiplyNull() throws IOException {
    try {
      parseAndExecute("20 * null");
      Assert.fail();
    } catch (JsonPathException e) {
      Assert.assertEquals("'20 * null' produced a semantic error: You cannot do arithmetic on a null at \"20 * null\"", e.getMessage());
    }
  }

  @Test
  public void divideNull() throws IOException {
    try {
      parseAndExecute("20 / null");
      Assert.fail();
    } catch (JsonPathException e) {
      Assert.assertEquals("'20 / null' produced a semantic error: You cannot do arithmetic on a null at \"20 / null\"", e.getMessage());
    }
  }

  @Test
  public void modNull() throws IOException {
    try {
      parseAndExecute("20 % null");
      Assert.fail();
    } catch (JsonPathException e) {
      Assert.assertEquals("'20 % null' produced a semantic error: You cannot do mod on a null at \"20 % null\"", e.getMessage());
    }
  }

  @Test
  public void nullAdd() throws IOException {
    try {
      parseAndExecute("null + 20");
      Assert.fail();
    } catch (JsonPathException e) {
      Assert.assertEquals("'null + 20' produced a semantic error: You cannot do arithmetic on a null at \"null + 20\"", e.getMessage());
    }
  }

  @Test
  public void nullSubtract() throws IOException {
    try {
      parseAndExecute("null - 20.0");
      Assert.fail();
    } catch (JsonPathException e) {
      Assert.assertEquals("'null - 20.0' produced a semantic error: You cannot do arithmetic on a null at \"null - 20.0\"", e.getMessage());
    }
  }

  @Test
  public void nullMultiply() throws IOException {
    try {
      parseAndExecute("null * 20");
      Assert.fail();
    } catch (JsonPathException e) {
      Assert.assertEquals("'null * 20' produced a semantic error: You cannot do arithmetic on a null at \"null * 20\"", e.getMessage());
    }
  }

  @Test
  public void nullDivide() throws IOException {
    try {
      parseAndExecute("null / 20");
      Assert.fail();
    } catch (JsonPathException e) {
      Assert.assertEquals("'null / 20' produced a semantic error: You cannot do arithmetic on a null at \"null / 20\"", e.getMessage());
    }
  }

  @Test
  public void nullMod() throws IOException {
    try {
      parseAndExecute("null % 20");
      Assert.fail();
    } catch (JsonPathException e) {
      Assert.assertEquals("'null % 20' produced a semantic error: You cannot do mod on a null at \"null % 20\"", e.getMessage());
    }
  }

  @Test
  public void pathNamedVariable() throws IOException, JsonPathException {
    JsonSequence json = valueParser.parse("{ \"name\" : \"fred\", \"classes\" : [ \"pe\", \"history\" ] }");
    PathExecutionResult pathExecResult = parseAndExecute("$.classes[$i]", json, Collections.singletonMap("i", new JsonSequence(1L)));

    JsonSequence wrappedExpected = valueParser.parse(" { \"k\" : \"history\" }");
    Assert.assertEquals(wrappedExpected.asObject().get("k"), pathExecResult.match);
  }

  @Test
  public void pathNamedVariableNoMatchingId() throws IOException {
    String pathExpr = "$fred";
    try {
      parseAndExecute(pathExpr, emptyJson, Collections.singletonMap("bob", new JsonSequence(5L)));
      Assert.fail();
    } catch (JsonPathException e) {
      Assert.assertEquals("'" + pathExpr + "' produced a semantic error: Variable fred" +
          " referenced in path expression but no matching id found in passing clause at \"$ fred\"", e.getMessage());
    }
  }

  @Test
  public void pathNamedVariableNullPassing() throws IOException {
    String pathExpr = "$fred";
    try {
      parseAndExecute(pathExpr, emptyJson);
      Assert.fail();
    } catch (JsonPathException e) {
      Assert.assertEquals("'" + pathExpr + "' produced a semantic error: Variable fred" +
          " referenced in path expression but no matching id found in passing clause at \"$ fred\"", e.getMessage());
    }
  }

  @Test
  public void pathNamedVariableEmptyPassing() throws IOException {
    String pathExpr = "$fred";
    try {
      parseAndExecute(pathExpr, emptyJson, Collections.emptyMap());
      Assert.fail();
    } catch (JsonPathException e) {
      Assert.assertEquals("'" + pathExpr + "' produced a semantic error: Variable fred" +
          " referenced in path expression but no matching id found in passing clause at \"$ fred\"", e.getMessage());
    }
  }

  @Test
  public void fullMatch() throws IOException, JsonPathException {
    JsonSequence json = valueParser.parse(" { \"name\" : \"fred\" }");
    PathExecutionResult pathExecResult = parseAndExecute("$", json);
    Assert.assertEquals(json, pathExecResult.match);
  }

  @Test
  public void matchKeyString() throws IOException, JsonPathException {
    JsonSequence json = valueParser.parse(" { \"name\" : \"fred\" }");
    PathExecutionResult pathExecResult = parseAndExecute("$.name", json);
    Assert.assertTrue(pathExecResult.match.isString());
    Assert.assertEquals("fred", pathExecResult.match.asString());
  }

  @Test
  public void matchKeyLong() throws IOException, JsonPathException {
    JsonSequence json = valueParser.parse(" { \"name\" : \"fred\", \"age\" : 35 }");
    PathExecutionResult pathExecResult = parseAndExecute("$.age", json);
    Assert.assertTrue(pathExecResult.match.isLong());
    Assert.assertEquals(35, pathExecResult.match.asLong());
  }

  @Test
  public void matchKeyDouble() throws IOException, JsonPathException {
    JsonSequence json = valueParser.parse(" { \"name\" : \"fred\", \"gpa\" : 2.73 }");
    PathExecutionResult pathExecResult = parseAndExecute("$.gpa", json);
    Assert.assertTrue(pathExecResult.match.isDouble());
    Assert.assertEquals(2.73, pathExecResult.match.asDouble(), 0.001);
  }

  @Test
  public void matchKeyBool() throws IOException, JsonPathException {
    JsonSequence json = valueParser.parse(" { \"name\" : \"fred\", \"honor roll\" : false }");
    PathExecutionResult pathExecResult = parseAndExecute("$.\"honor roll\"", json);
    Assert.assertTrue(pathExecResult.match.isBool());
    Assert.assertFalse(pathExecResult.match.asBool());
  }

  @Test
  public void matchKeyNull() throws IOException, JsonPathException {
    JsonSequence json = valueParser.parse(" { \"name\" : \"fred\", \"sports\" : null }");
    PathExecutionResult pathExecResult = parseAndExecute("$.sports", json);
    Assert.assertTrue(pathExecResult.match.isNull());
  }

  @Test
  public void matchKeyQuotes() throws IOException, JsonPathException {
    JsonSequence json = valueParser.parse(" { \"name\" : \"fred\" }");
    PathExecutionResult pathExecResult = parseAndExecute("$.\"name\"", json);
    Assert.assertTrue(pathExecResult.match.isString());
    Assert.assertEquals("fred", pathExecResult.match.asString());
  }

  @Test
  public void noMatchKey() throws IOException, JsonPathException {
    JsonSequence json = valueParser.parse(" { \"name\" : \"fred\" }");
    PathExecutionResult pathExecResult = parseAndExecute("$.address", json);
    Assert.assertTrue(pathExecResult.match.isEmpty());
  }

  @Test
  public void noMatchKeyQuotes() throws IOException, JsonPathException {
    JsonSequence json = valueParser.parse(" { \"name\" : \"fred\" }");
    PathExecutionResult pathExecResult = parseAndExecute("$.\"address\"", json);
    Assert.assertTrue(pathExecResult.match.isEmpty());
  }

  @Test
  public void objectWildcard() throws IOException, JsonPathException {
    JsonSequence json = valueParser.parse(" { \"name\" : \"fred\", \"age\" : 35 }");
    JsonSequence expected = new JsonSequence(json);
    PathExecutionResult pathExecResult = parseAndExecute("$.*", json);


    Assert.assertEquals(expected, pathExecResult.match);
  }

  @Test
  public void objectWildcardEmpty() throws IOException, JsonPathException {
    PathExecutionResult pathExecResult = parseAndExecute("$.*", emptyJson);
    Assert.assertEquals(emptyJson, pathExecResult.match);
  }

  @Test
  public void simpleSubscriptList() throws IOException, JsonPathException {
    JsonSequence json = valueParser.parse("{ \"name\" : \"fred\", \"classes\" : [ \"science\", \"art\" ] }");
    PathExecutionResult pathExecResult = parseAndExecute("$.classes[0]", json);

    JsonSequence wrappedExpected = valueParser.parse(" { \"k\" : \"science\" }");
    Assert.assertEquals(wrappedExpected.asObject().get("k"), pathExecResult.match);
  }

  @Test
  public void simpleSubscriptObject() throws IOException, JsonPathException {
    JsonSequence json = valueParser.parse("{ \"name\" : \"fred\"," +
        "\"classes\" : [ \"science\", \"art\" ]," +
        "\"sports\"  : [ \"swimming\", \"baseball\" ] }");
    PathExecutionResult pathExecResult = parseAndExecute("$.*[1]", json);

    JsonSequence expected = valueParser.parse(" { \"classes\" : \"art\", \"sports\" : \"baseball\" }");
    Assert.assertEquals(expected, pathExecResult.match);
  }

  @Test
  public void lastSubscriptList() throws IOException, JsonPathException {
    JsonSequence json = valueParser.parse("{ \"name\" : \"fred\", \"classes\" : [ \"science\", \"art\" ] }");
    PathExecutionResult pathExecResult = parseAndExecute("$.classes[last]", json);

    JsonSequence wrappedExpected = valueParser.parse(" { \"k\" : \"art\" }");
    Assert.assertEquals(wrappedExpected.asObject().get("k"), pathExecResult.match);
  }

  @Test
  public void lastSubscriptObject() throws IOException, JsonPathException {
    JsonSequence json = valueParser.parse("{ \"name\" : \"fred\"," +
        "\"classes\" : [ \"science\", \"art\" ]," +
        "\"sports\"  : [ \"swimming\", \"baseball\" ] }");
    PathExecutionResult pathExecResult = parseAndExecute("$.*[last]", json);

    JsonSequence expected = valueParser.parse(" { \"classes\" : \"art\", \"sports\" : \"baseball\" }");
    Assert.assertEquals(expected, pathExecResult.match);
  }

  @Test
  public void toSubscriptList() throws IOException, JsonPathException {
    JsonSequence json = valueParser.parse("{ \"name\" : \"fred\"," +
        "\"classes\" : [ \"science\", \"art\", \"math\", \"history\", \"writing\" ] }");
    PathExecutionResult pathExecResult = parseAndExecute("$.classes[1 to 3]", json);

    JsonSequence wrappedExpected = valueParser.parse(" { \"k\" : [ \"art\", \"math\", \"history\" ] }");
    Assert.assertEquals(wrappedExpected.asObject().get("k"), pathExecResult.match);
  }

  @Test
  public void toSubscriptObject() throws IOException, JsonPathException {
    JsonSequence json = valueParser.parse("{ \"name\" : \"fred\"," +
        "\"classes\" : [ \"science\", \"art\", \"math\", \"history\", \"writing\" ]," +
        "\"sports\"  : [ \"swimming\", \"baseball\", \"volleyball\", \"soccer\" ] }");
    PathExecutionResult pathExecResult = parseAndExecute("$.*[1 to 3]", json);

    JsonSequence expected = valueParser.parse(" { \"classes\" : [ \"art\", \"math\", \"history\"]," +
                                                 "\"sports\" : [ \"baseball\", \"volleyball\", \"soccer\" ] }");
    Assert.assertEquals(expected, pathExecResult.match);
  }

  @Test
  public void toLastSubscriptList() throws IOException, JsonPathException {
    JsonSequence json = valueParser.parse("{ \"name\" : \"fred\"," +
        "\"classes\" : [ \"science\", \"art\", \"math\", \"history\", \"writing\" ] }");
    PathExecutionResult pathExecResult = parseAndExecute("$.classes[1 to last]", json);

    JsonSequence wrappedExpected = valueParser.parse(" { \"k\" : [ \"art\", \"math\", \"history\", \"writing\" ] }");
    Assert.assertEquals(wrappedExpected.asObject().get("k"), pathExecResult.match);
  }

  @Test
  public void toLastSubscriptObject() throws IOException, JsonPathException {
    JsonSequence json = valueParser.parse("{ \"name\" : \"fred\"," +
        "\"classes\" : [ \"science\", \"art\", \"math\", \"history\", \"writing\" ]," +
        "\"sports\"  : [ \"swimming\", \"baseball\", \"volleyball\", \"soccer\" ] }");
    PathExecutionResult pathExecResult = parseAndExecute("$.*[1 to last]", json);

    JsonSequence expected = valueParser.parse(" { \"classes\" : [ \"art\", \"math\", \"history\", \"writing\"]," +
        "                                         \"sports\" : [ \"baseball\", \"volleyball\", \"soccer\" ] }");
    Assert.assertEquals(expected, pathExecResult.match);
  }

  @Test
  public void notAnArraySubscriptList() throws IOException, JsonPathException {
    JsonSequence json = valueParser.parse("{ \"name\" : \"fred\", \"classes\" : [ \"science\", \"art\" ] }");
    PathExecutionResult pathExecResult = parseAndExecute("$.name[0]", json);
    Assert.assertTrue(pathExecResult.match.isEmpty());
  }

  @Test
  public void notAnArraySubscriptObject() throws IOException, JsonPathException {
    JsonSequence json = valueParser.parse("{ \"name\" : \"fred\" }");
    PathExecutionResult pathExecResult = parseAndExecute("$.*[1]", json);

    Assert.assertEquals(emptyJson, pathExecResult.match);
  }

  @Test
  public void offEndSubscriptList() throws IOException, JsonPathException {
    JsonSequence json = valueParser.parse("{ \"name\" : \"fred\", \"classes\" : [ \"science\", \"art\" ] }");
    PathExecutionResult pathExecResult = parseAndExecute("$.classes[3]", json);
    Assert.assertTrue(pathExecResult.match.isEmpty());
  }

  @Test
  public void offEndSubscriptObject() throws IOException, JsonPathException {
    JsonSequence json = valueParser.parse("{ \"name\" : \"fred\"," +
        "\"classes\" : [ \"science\", \"art\" ]," +
        "\"sports\"  : [ \"swimming\", \"baseball\", \"soccer\" ] }");
    PathExecutionResult pathExecResult = parseAndExecute("$.*[2]", json);

    JsonSequence expected = valueParser.parse(" { \"sports\" : \"soccer\" }");
    Assert.assertEquals(expected, pathExecResult.match);
  }

  @Test
  public void toOffEndSubscriptList() throws IOException, JsonPathException {
    JsonSequence json = valueParser.parse("{ \"name\" : \"fred\"," +
        "\"classes\" : [ \"science\", \"art\", \"math\", \"history\", \"writing\" ] }");
    PathExecutionResult pathExecResult = parseAndExecute("$.classes[3 to 5]", json);

    JsonSequence wrappedExpected = valueParser.parse(" { \"k\" : [ \"history\", \"writing\" ] }");
    Assert.assertEquals(wrappedExpected.asObject().get("k"), pathExecResult.match);
  }

  @Test
  public void toOffEndSubscriptObject() throws IOException, JsonPathException {
    JsonSequence json = valueParser.parse("{ \"name\" : \"fred\"," +
        "\"classes\" : [ \"science\", \"art\", \"math\", \"history\", \"writing\" ]," +
        "\"sports\"  : [ \"swimming\", \"baseball\", \"volleyball\", \"soccer\" ] }");
    PathExecutionResult pathExecResult = parseAndExecute("$.*[3 to 5]", json);

    JsonSequence expected = valueParser.parse(" { \"classes\" : [ \"history\", \"writing\"], \"sports\" : [ \"soccer\" ] }");
    Assert.assertEquals(expected, pathExecResult.match);
  }

  @Test
  public void listSubscriptList() throws IOException, JsonPathException {
    JsonSequence json = valueParser.parse("{ \"name\" : \"fred\"," +
        "\"classes\" : [ \"science\", \"art\", \"math\", \"history\", \"writing\" ] }");
    PathExecutionResult pathExecResult = parseAndExecute("$.classes[1, 4]", json);

    JsonSequence wrappedExpected = valueParser.parse(" { \"k\" : [ \"art\", \"writing\" ] }");
    Assert.assertEquals(wrappedExpected.asObject().get("k"), pathExecResult.match);
  }

  @Test
  public void listSubscriptObject() throws IOException, JsonPathException {
    JsonSequence json = valueParser.parse("{ \"name\" : \"fred\"," +
        "\"classes\" : [ \"science\", \"art\", \"math\", \"history\", \"writing\" ]," +
        "\"sports\"  : [ \"swimming\", \"baseball\", \"volleyball\", \"soccer\" ] }");
    PathExecutionResult pathExecResult = parseAndExecute("$.*[1, 4]", json);

    JsonSequence expected = valueParser.parse(" { \"classes\" : [ \"art\", \"writing\"], \"sports\" : [ \"baseball\" ] }");
    Assert.assertEquals(expected, pathExecResult.match);
  }

  @Test
  public void listAndToSubscriptList() throws IOException, JsonPathException {
    JsonSequence json = valueParser.parse("{ \"name\" : \"fred\"," +
        "\"classes\" : [ \"science\", \"art\", \"math\", \"history\", \"writing\" ] }");
    PathExecutionResult pathExecResult = parseAndExecute("$.classes[0, 3 to 5]", json);

    JsonSequence wrappedExpected = valueParser.parse(" { \"k\" : [ \"science\", \"history\", \"writing\" ] }");
    Assert.assertEquals(wrappedExpected.asObject().get("k"), pathExecResult.match);
  }

  @Test
  public void listAndToSubscriptObject() throws IOException, JsonPathException {
    JsonSequence json = valueParser.parse("{ \"name\" : \"fred\"," +
        "\"classes\" : [ \"science\", \"art\", \"math\", \"history\", \"writing\" ]," +
        "\"sports\"  : [ \"swimming\", \"baseball\", \"volleyball\", \"soccer\" ] }");
    PathExecutionResult pathExecResult = parseAndExecute("$.*[0, 2 to last]", json);

    JsonSequence expected = valueParser.parse(" { \"classes\" : [ \"science\", \"math\", \"history\", \"writing\"]," +
        "                                         \"sports\" : [ \"swimming\", \"volleyball\", \"soccer\" ] }");
    Assert.assertEquals(expected, pathExecResult.match);
  }

  @Test
  public void arithmeticSubscriptList() throws IOException, JsonPathException {
    JsonSequence json = valueParser.parse("{ \"name\" : \"fred\"," +
        "\"classes\" : [ \"science\", \"art\", \"math\", \"history\", \"writing\" ] }");
    PathExecutionResult pathExecResult = parseAndExecute("$.classes[1 + 1]", json);

    JsonSequence wrappedExpected = valueParser.parse(" { \"k\" : \"math\" }");
    Assert.assertEquals(wrappedExpected.asObject().get("k"), pathExecResult.match);
  }

  @Test
  public void wildcardSubscriptList() throws IOException, JsonPathException {
    JsonSequence json = valueParser.parse("{ \"name\" : \"fred\"," +
        "\"classes\" : [ \"science\", \"art\", \"math\" ] }");
    PathExecutionResult pathExecResult = parseAndExecute("$.classes[*]", json);

    JsonSequence wrappedExpected = valueParser.parse(" { \"k\" : [ \"science\", \"art\", \"math\" ] }");
    Assert.assertEquals(wrappedExpected.asObject().get("k"), pathExecResult.match);
  }

  @Test
  public void wildcardSubscriptObject() throws IOException, JsonPathException {
    JsonSequence json = valueParser.parse("{ \"name\" : \"fred\"," +
        "\"classes\" : [ \"science\", \"art\", \"math\" ]," +
        "\"sports\"  : [ \"swimming\", \"baseball\" ] }");
    PathExecutionResult pathExecResult = parseAndExecute("$.*[*]", json);

    JsonSequence expected = valueParser.parse(" { \"classes\" : [ \"science\", \"art\", \"math\"]," +
        "                                         \"sports\" : [ \"swimming\", \"baseball\" ] }");
    Assert.assertEquals(expected, pathExecResult.match);
  }

  @Test
  public void typeLong() throws IOException, JsonPathException {
    JsonSequence json = valueParser.parse("{ \"name\" : \"fred\", \"age\" : 35 }");
    PathExecutionResult pathExecResult = parseAndExecute("$.age.type()", json);
    Assert.assertTrue(pathExecResult.match.isString());
    Assert.assertEquals("number", pathExecResult.match.asString());
  }

  @Test
  public void typeDouble() throws IOException, JsonPathException {
    JsonSequence json = valueParser.parse("{ \"name\" : \"fred\", \"gpa\" : 3.58 }");
    PathExecutionResult pathExecResult = parseAndExecute("$.gpa.type()", json);
    Assert.assertTrue(pathExecResult.match.isString());
    Assert.assertEquals("number", pathExecResult.match.asString());
  }

  @Test
  public void typeString() throws IOException, JsonPathException {
    JsonSequence json = valueParser.parse("{ \"name\" : \"fred\", \"gpa\" : 3.58 }");
    PathExecutionResult pathExecResult = parseAndExecute("$.name.type()", json);
    Assert.assertTrue(pathExecResult.match.isString());
    Assert.assertEquals("string", pathExecResult.match.asString());
  }

  @Test
  public void typeNull() throws IOException, JsonPathException {
    JsonSequence json = valueParser.parse("{ \"name\" : \"fred\", \"age\" : 35, \"sports\" : null }");
    PathExecutionResult pathExecResult = parseAndExecute("$.sports.type()", json);
    Assert.assertTrue(pathExecResult.match.isString());
    Assert.assertEquals("null", pathExecResult.match.asString());
  }

  @Test
  public void typeBoolean() throws IOException, JsonPathException {
    JsonSequence json = valueParser.parse("{ \"name\" : \"fred\", \"age\" : 35, \"honor roll\" : true }");
    PathExecutionResult pathExecResult = parseAndExecute("$.\'honor roll\'.type()", json);
    Assert.assertTrue(pathExecResult.match.isString());
    Assert.assertEquals("boolean", pathExecResult.match.asString());
  }

  @Test
  public void typeList() throws IOException, JsonPathException {
    JsonSequence json = valueParser.parse("{ \"name\" : \"fred\", \"classes\" : [ \"art\", \"math\" ] }");
    PathExecutionResult pathExecResult = parseAndExecute("$.classes.type()", json);
    Assert.assertTrue(pathExecResult.match.isString());
    Assert.assertEquals("array", pathExecResult.match.asString());
  }

  @Test
  public void typeObject() throws IOException, JsonPathException {
    JsonSequence json = valueParser.parse("{ \"name\" : \"fred\", \"address\" : { \"street\" : \"123 main\", \"city\" : \"phoenix\" } }");
    PathExecutionResult pathExecResult = parseAndExecute("$.address.type()", json);
    Assert.assertTrue(pathExecResult.match.isString());
    Assert.assertEquals("object", pathExecResult.match.asString());
  }

  @Test
  public void typeEmpty() throws IOException, JsonPathException {
    JsonSequence json = valueParser.parse("{ \"name\" : \"fred\", \"age\" : 35 }");
    PathExecutionResult pathExecResult = parseAndExecute("$.gpa.type()", json);
    Assert.assertTrue(pathExecResult.match.isEmpty());
  }

  @Test
  public void sizeScalar() throws IOException, JsonPathException {
    JsonSequence json = valueParser.parse("{ \"name\" : \"fred\", \"age\" : 35, \"honor roll\" : true }");
    PathExecutionResult pathExecResult = parseAndExecute("$.\'honor roll\'.size()", json);
    Assert.assertTrue(pathExecResult.match.isLong());
    Assert.assertEquals(1, pathExecResult.match.asLong());
  }

  @Test
  public void sizeList() throws IOException, JsonPathException {
    JsonSequence json = valueParser.parse("{ \"name\" : \"fred\", \"classes\" : [ \"art\", \"math\" ] }");
    PathExecutionResult pathExecResult = parseAndExecute("$.classes.size()", json);
    Assert.assertTrue(pathExecResult.match.isLong());
    Assert.assertEquals(2, pathExecResult.match.asLong());
  }

  @Test
  public void sizeObject() throws IOException, JsonPathException {
    JsonSequence json = valueParser.parse("{ \"name\" : \"fred\", \"address\" : { \"street\" : \"123 main\", \"city\" : \"phoenix\" } }");
    PathExecutionResult pathExecResult = parseAndExecute("$.address.size()", json);
    Assert.assertTrue(pathExecResult.match.isLong());
    Assert.assertEquals(2, pathExecResult.match.asLong());
  }

  @Test
  public void sizeEmpty() throws IOException, JsonPathException {
    JsonSequence json = valueParser.parse("{ \"name\" : \"fred\", \"age\" : 35 }");
    PathExecutionResult pathExecResult = parseAndExecute("$.gpa.size()", json);
    Assert.assertTrue(pathExecResult.match.isEmpty());
  }

  @Test
  public void doubleLong() throws IOException, JsonPathException {
    JsonSequence json = valueParser.parse("{ \"name\" : \"fred\", \"age\" : 35 }");
    PathExecutionResult pathExecResult = parseAndExecute("$.age.double()", json);
    Assert.assertTrue(pathExecResult.match.isDouble());
    Assert.assertEquals(35.0, pathExecResult.match.asDouble(), 0.001);
  }

  @Test
  public void doubleDouble() throws IOException, JsonPathException {
    JsonSequence json = valueParser.parse("{ \"name\" : \"fred\", \"gpa\" : 3.58 }");
    PathExecutionResult pathExecResult = parseAndExecute("$.gpa.double()", json);
    Assert.assertTrue(pathExecResult.match.isDouble());
    Assert.assertEquals(3.58, pathExecResult.match.asDouble(), 0.001);
  }

  @Test
  public void doubleString() throws IOException, JsonPathException {
    JsonSequence json = valueParser.parse("{ \"name\" : \"fred\", \"gpa\" : \"3.58\" }");
    PathExecutionResult pathExecResult = parseAndExecute("$.gpa.double()", json);
    Assert.assertTrue(pathExecResult.match.isDouble());
    Assert.assertEquals(3.58, pathExecResult.match.asDouble(), 0.001);
  }

  @Test
  public void doubleNotStringOrNumeric() throws IOException {
    try {
      JsonSequence json = valueParser.parse("{ \"name\" : \"fred\", \"honor roll\" : true }");
      parseAndExecute("$.\"honor roll\".double()", json);
      Assert.fail();
    } catch (JsonPathException e) {
      Assert.assertEquals("'$.\"honor roll\".double()' produced a runtime error: Double method" +
          " requires numeric or string argument, passed a bool at \"double ( )\"", e.getMessage());

    }
  }

  @Test
  public void doubleEmpty() throws IOException, JsonPathException {
    JsonSequence json = valueParser.parse("{ \"name\" : \"fred\", \"gpa\" : 3.58 }");
    PathExecutionResult pathExecResult = parseAndExecute("$.sports.double()", json);
    Assert.assertTrue(pathExecResult.match.isEmpty());
  }

  @Test
  public void intLong() throws IOException, JsonPathException {
    JsonSequence json = valueParser.parse("{ \"name\" : \"fred\", \"age\" : 35 }");
    PathExecutionResult pathExecResult = parseAndExecute("$.age.integer()", json);
    Assert.assertTrue(pathExecResult.match.isLong());
    Assert.assertEquals(35, pathExecResult.match.asLong());
  }

  @Test
  public void intDouble() throws IOException, JsonPathException {
    JsonSequence json = valueParser.parse("{ \"name\" : \"fred\", \"gpa\" : 3.58 }");
    PathExecutionResult pathExecResult = parseAndExecute("$.gpa.integer()", json);
    Assert.assertTrue(pathExecResult.match.isLong());
    Assert.assertEquals(3, pathExecResult.match.asLong());
  }

  @Test
  public void intString() throws IOException, JsonPathException {
    JsonSequence json = valueParser.parse("{ \"name\" : \"fred\", \"age\" : \"35\" }");
    PathExecutionResult pathExecResult = parseAndExecute("$.age.integer()", json);
    Assert.assertTrue(pathExecResult.match.isLong());
    Assert.assertEquals(35, pathExecResult.match.asLong());
  }

  @Test
  public void intNotStringOrNumeric() throws IOException {
    try {
      JsonSequence json = valueParser.parse("{ \"name\" : \"fred\", \"honor roll\" : true }");
      parseAndExecute("$.\"honor roll\".integer()", json);
      Assert.fail();
    } catch (JsonPathException e) {
      Assert.assertEquals("'$.\"honor roll\".integer()' produced a runtime error: Integer method" +
          " requires numeric or string argument, passed a bool at \"integer ( )\"", e.getMessage());

    }
  }

  @Test
  public void intEmpty() throws IOException, JsonPathException {
    JsonSequence json = valueParser.parse("{ \"name\" : \"fred\", \"gpa\" : 3.58 }");
    PathExecutionResult pathExecResult = parseAndExecute("$.sports.integer()", json);
    Assert.assertTrue(pathExecResult.match.isEmpty());
  }

  @Test
  public void ceilingLong() throws IOException, JsonPathException {
    JsonSequence json = valueParser.parse("{ \"name\" : \"fred\", \"age\" : 35 }");
    PathExecutionResult pathExecResult = parseAndExecute("$.age.ceiling()", json);
    Assert.assertTrue(pathExecResult.match.isLong());
    Assert.assertEquals(35, pathExecResult.match.asLong());
  }

  @Test
  public void ceilingDouble() throws IOException, JsonPathException {
    JsonSequence json = valueParser.parse("{ \"name\" : \"fred\", \"gpa\" : 3.58 }");
    PathExecutionResult pathExecResult = parseAndExecute("$.gpa.ceiling()", json);
    Assert.assertTrue(pathExecResult.match.isLong());
    Assert.assertEquals(4, pathExecResult.match.asLong());
  }

  @Test
  public void ceilingNotNumeric() throws IOException {
    try {
      JsonSequence json = valueParser.parse("{ \"name\" : \"fred\", \"honor roll\" : true }");
      parseAndExecute("$.name.ceiling()", json);
      Assert.fail();
    } catch (JsonPathException e) {
      Assert.assertEquals("'$.name.ceiling()' produced a runtime error: Ceiling method" +
          " requires numeric argument, passed a string at \"ceiling ( )\"", e.getMessage());

    }
  }

  @Test
  public void ceilingEmpty() throws IOException, JsonPathException {
    JsonSequence json = valueParser.parse("{ \"name\" : \"fred\", \"gpa\" : 3.58 }");
    PathExecutionResult pathExecResult = parseAndExecute("$.sports.ceiling()", json);
    Assert.assertTrue(pathExecResult.match.isEmpty());
  }

  @Test
  public void floorLong() throws IOException, JsonPathException {
    JsonSequence json = valueParser.parse("{ \"name\" : \"fred\", \"age\" : 35 }");
    PathExecutionResult pathExecResult = parseAndExecute("$.age.floor()", json);
    Assert.assertTrue(pathExecResult.match.isLong());
    Assert.assertEquals(35, pathExecResult.match.asLong());
  }

  @Test
  public void floorDouble() throws IOException, JsonPathException {
    JsonSequence json = valueParser.parse("{ \"name\" : \"fred\", \"gpa\" : 3.58 }");
    PathExecutionResult pathExecResult = parseAndExecute("$.gpa.floor()", json);
    Assert.assertTrue(pathExecResult.match.isLong());
    Assert.assertEquals(3, pathExecResult.match.asLong());
  }

  @Test
  public void floorNotNumeric() throws IOException {
    try {
      JsonSequence json = valueParser.parse("{ \"name\" : \"fred\", \"honor roll\" : true }");
      parseAndExecute("$.name.floor()", json);
      Assert.fail();
    } catch (JsonPathException e) {
      Assert.assertEquals("'$.name.floor()' produced a runtime error: Floor method" +
          " requires numeric argument, passed a string at \"floor ( )\"", e.getMessage());

    }
  }

  @Test
  public void floorEmpty() throws IOException, JsonPathException {
    JsonSequence json = valueParser.parse("{ \"name\" : \"fred\", \"gpa\" : 3.58 }");
    PathExecutionResult pathExecResult = parseAndExecute("$.sports.floor()", json);
    Assert.assertTrue(pathExecResult.match.isEmpty());
  }

  @Test
  public void absLong() throws IOException, JsonPathException {
    JsonSequence json = valueParser.parse("{ \"name\" : \"fred\", \"age\" : 35 }");
    PathExecutionResult pathExecResult = parseAndExecute("$.age.abs()", json);
    Assert.assertTrue(pathExecResult.match.isLong());
    Assert.assertEquals(35, pathExecResult.match.asLong());
  }

  @Test
  public void absDouble() throws IOException, JsonPathException {
    JsonSequence json = valueParser.parse("{ \"name\" : \"fred\", \"gpa\" : -3.58 }");
    PathExecutionResult pathExecResult = parseAndExecute("$.gpa.abs()", json);
    Assert.assertTrue(pathExecResult.match.isDouble());
    Assert.assertEquals(3.58, pathExecResult.match.asDouble(), 0.001);
  }

  @Test
  public void absNotNumeric() throws IOException {
    try {
      JsonSequence json = valueParser.parse("{ \"name\" : \"fred\", \"honor roll\" : true }");
      parseAndExecute("$.name.abs()", json);
      Assert.fail();
    } catch (JsonPathException e) {
      Assert.assertEquals("'$.name.abs()' produced a runtime error: Abs method" +
          " requires numeric argument, passed a string at \"abs ( )\"", e.getMessage());

    }
  }

  @Test
  public void absEmpty() throws IOException, JsonPathException {
    JsonSequence json = valueParser.parse("{ \"name\" : \"fred\", \"gpa\" : 3.58 }");
    PathExecutionResult pathExecResult = parseAndExecute("$.sports.abs()", json);
    Assert.assertTrue(pathExecResult.match.isEmpty());
  }

  @Test
  public void bigHarryDeepThing() throws IOException, JsonPathException {
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
    PathExecutionResult pathExecResult = parseAndExecute("$.classes[$class].texts[$text].author", json, passing);

    JsonSequence wrappedExpected = valueParser.parse(" { \"k\" : \"c. darwin\" }");
    Assert.assertEquals(wrappedExpected.asObject().get("k"), pathExecResult.match);
  }

  @Test
  public void bigHarryDeepMultiThing() throws IOException, JsonPathException {
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
    PathExecutionResult pathExecResult = parseAndExecute("$.classes[$class].texts[*].author", json, passing);

    JsonSequence wrappedExpected = valueParser.parse("{ \"k\" : [ \"i. newton\", \"c. darwin\" ] }");
    Assert.assertEquals(wrappedExpected.asObject().get("k"), pathExecResult.match);
  }

  @Test
  public void filterExists() throws IOException, JsonPathException {
    JsonSequence json = valueParser.parse(
        "{" +
            "\"name\" : \"fred\"," +
            "\"address\" : {" +
                "\"street\" : \"123 Main\"," +
                "\"city\"   : \"Springfield\"," +
                "\"zip\"    : 12345" +
            "}" +
        "}");
    PathExecutionResult pathExecResult = parseAndExecute("$.address?(exists(@.city))", json);

    JsonSequence expected = valueParser.parse(
        "{" +
            "\"street\" : \"123 Main\"," +
            "\"city\"   : \"Springfield\"," +
            "\"zip\"    : 12345" +
        "}");
    Assert.assertEquals(expected, pathExecResult.match);
  }

  @Test
  public void filterListExists() throws IOException, JsonPathException {
    JsonSequence json = valueParser.parse(
        "{" +
          "\"classes\" : [" +
            "{" +
              "\"department\"    : \"history\"," +
              "\"number\"        : 202," +
              "\"professor\"     : \"Who\"," +
              "\"prerequisites\" : [ \"history 201\" ]" +
            "}, {" +
              "\"department\"    : \"music\"," +
              "\"number\"        : 101," +
              "\"professor\"     : \"Beethoven\"" +
            "}" +
          "]" +
        "}");

    JsonSequence expected = valueParser.parse(
        "{" +
            "\"bogus\" : [" +
              "{" +
                "\"department\"    : \"history\"," +
                "\"number\"        : 202," +
                "\"professor\"     : \"Who\"," +
                "\"prerequisites\" : [ \"history 201\" ]" +
              "}" +
            "]" +
        "}");

    PathExecutionResult pathExecResult = parseAndExecute("$.classes[*]?(exists(@.prerequisites))", json);
    Assert.assertEquals(expected.asObject().get("bogus"), pathExecResult.match);

  }

  @Test
  public void filterNonExistent() throws IOException, JsonPathException {
    JsonSequence json = valueParser.parse(
        "{" +
            "\"name\" : \"fred\"," +
            "\"address\" : {" +
            "\"street\" : \"123 Main\"," +
            "\"city\"   : \"Springfield\"," +
            "\"zip\"    : 12345" +
            "}" +
            "}");
    PathExecutionResult pathExecResult = parseAndExecute("$.address?(exists(@.state))", json);
    Assert.assertEquals(JsonSequence.emptyResult, pathExecResult.match);
  }

  @Test
  public void filterNotExists() throws IOException, JsonPathException {
    JsonSequence json = valueParser.parse(
        "{" +
            "\"name\" : \"fred\"," +
            "\"address\" : {" +
            "\"street\" : \"123 Main\"," +
            "\"city\"   : \"Springfield\"," +
            "\"zip\"    : 12345" +
            "}" +
            "}");
    PathExecutionResult pathExecResult = parseAndExecute("$.address?(!exists(@.state))", json);

    JsonSequence expected = valueParser.parse(
        "{" +
            "\"street\" : \"123 Main\"," +
            "\"city\"   : \"Springfield\"," +
            "\"zip\"    : 12345" +
            "}");
    Assert.assertEquals(expected, pathExecResult.match);
  }

  @Test
  public void filterAndTrue() throws IOException, JsonPathException {
    JsonSequence json = valueParser.parse(
        "{" +
            "\"name\" : \"fred\"," +
            "\"address\" : {" +
            "\"street\" : \"123 Main\"," +
            "\"city\"   : \"Springfield\"," +
            "\"zip\"    : 12345" +
            "}" +
            "}");
    PathExecutionResult pathExecResult = parseAndExecute("$.address?(exists(@.city) && exists(@.zip))", json);

    JsonSequence expected = valueParser.parse(
        "{" +
            "\"street\" : \"123 Main\"," +
            "\"city\"   : \"Springfield\"," +
            "\"zip\"    : 12345" +
            "}");
    Assert.assertEquals(expected, pathExecResult.match);
  }

  @Test
  public void filterAndFirstFalse() throws IOException, JsonPathException {
    JsonSequence json = valueParser.parse(
        "{" +
            "\"name\" : \"fred\"," +
            "\"address\" : {" +
            "\"street\" : \"123 Main\"," +
            "\"city\"   : \"Springfield\"," +
            "\"zip\"    : 12345" +
            "}" +
            "}");
    PathExecutionResult pathExecResult = parseAndExecute("$.address?(exists(@.state) && exists(@.zip))", json);

    Assert.assertEquals(JsonSequence.emptyResult, pathExecResult.match);
  }

  @Test
  public void filterAndSecondFalse() throws IOException, JsonPathException {
    JsonSequence json = valueParser.parse(
        "{" +
            "\"name\" : \"fred\"," +
            "\"address\" : {" +
            "\"street\" : \"123 Main\"," +
            "\"city\"   : \"Springfield\"," +
            "\"zip\"    : 12345" +
            "}" +
            "}");
    PathExecutionResult pathExecResult = parseAndExecute("$.address?(exists(@.city) && exists(@.state))", json);

    Assert.assertEquals(JsonSequence.emptyResult, pathExecResult.match);
  }

  @Test
  public void filterOrBothTrue() throws IOException, JsonPathException {
    JsonSequence json = valueParser.parse(
        "{" +
            "\"name\" : \"fred\"," +
            "\"address\" : {" +
            "\"street\" : \"123 Main\"," +
            "\"city\"   : \"Springfield\"," +
            "\"zip\"    : 12345" +
            "}" +
            "}");
    PathExecutionResult pathExecResult = parseAndExecute("$.address?(exists(@.city) || exists(@.zip))", json);

    JsonSequence expected = valueParser.parse(
        "{" +
            "\"street\" : \"123 Main\"," +
            "\"city\"   : \"Springfield\"," +
            "\"zip\"    : 12345" +
            "}");
    Assert.assertEquals(expected, pathExecResult.match);
  }

  @Test
  public void filterOrFirstTrue() throws IOException, JsonPathException {
    JsonSequence json = valueParser.parse(
        "{" +
            "\"name\" : \"fred\"," +
            "\"address\" : {" +
            "\"street\" : \"123 Main\"," +
            "\"city\"   : \"Springfield\"," +
            "\"zip\"    : 12345" +
            "}" +
            "}");
    PathExecutionResult pathExecResult = parseAndExecute("$.address?(exists(@.city) || exists(@.state))", json);

    JsonSequence expected = valueParser.parse(
        "{" +
            "\"street\" : \"123 Main\"," +
            "\"city\"   : \"Springfield\"," +
            "\"zip\"    : 12345" +
            "}");
    Assert.assertEquals(expected, pathExecResult.match);
  }

  @Test
  public void filterOrSecondTrue() throws IOException, JsonPathException {
    JsonSequence json = valueParser.parse(
        "{" +
            "\"name\" : \"fred\"," +
            "\"address\" : {" +
            "\"street\" : \"123 Main\"," +
            "\"city\"   : \"Springfield\"," +
            "\"zip\"    : 12345" +
            "}" +
            "}");
    PathExecutionResult pathExecResult = parseAndExecute("$.address?(exists(@.state) || exists(@.zip))", json);

    JsonSequence expected = valueParser.parse(
        "{" +
            "\"street\" : \"123 Main\"," +
            "\"city\"   : \"Springfield\"," +
            "\"zip\"    : 12345" +
            "}");
    Assert.assertEquals(expected, pathExecResult.match);
  }

  @Test
  public void filterBothFalse() throws IOException, JsonPathException {
    JsonSequence json = valueParser.parse(
        "{" +
            "\"name\" : \"fred\"," +
            "\"address\" : {" +
            "\"street\" : \"123 Main\"," +
            "\"city\"   : \"Springfield\"," +
            "\"zip\"    : 12345" +
            "}" +
            "}");
    PathExecutionResult pathExecResult = parseAndExecute("$.address?(exists(@.state) || exists(@.country))", json);

    Assert.assertEquals(JsonSequence.emptyResult, pathExecResult.match);
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
  public void filterLongEquals() throws IOException, JsonPathException {
    JsonSequence json = valueParser.parse(equalityJson);
    PathExecutionResult pathExecResult = parseAndExecute("$.education?(@.\"years attended\" == 4)", json);

    JsonSequence expected = valueParser.parse(expectedEqualityJson);
    Assert.assertEquals(expected, pathExecResult.match);
  }

  @Test
  public void filterLongEqualsFails() throws IOException, JsonPathException {
    JsonSequence json = valueParser.parse(equalityJson);
    PathExecutionResult pathExecResult = parseAndExecute("$.education?(@.\"years attended\" == 3)", json);

    Assert.assertEquals(JsonSequence.emptyResult, pathExecResult.match);
  }

  @Test
  public void filterLongDoubleEquals() throws IOException, JsonPathException {
    JsonSequence json = valueParser.parse(equalityJson);
    PathExecutionResult pathExecResult = parseAndExecute("$.education?(@.\"years attended\" == 4.0)", json);

    JsonSequence expected = valueParser.parse(expectedEqualityJson);
    Assert.assertEquals(expected, pathExecResult.match);
  }

  @Test
  public void filterLongDoubleEqualsFails() throws IOException, JsonPathException {
    JsonSequence json = valueParser.parse(equalityJson);
    PathExecutionResult pathExecResult = parseAndExecute("$.education?(@.\"years attended\" == 3.0)", json);

    Assert.assertEquals(JsonSequence.emptyResult, pathExecResult.match);
  }

  @Test
  public void filterDoubleEquals() throws IOException, JsonPathException {
    JsonSequence json = valueParser.parse(equalityJson);
    PathExecutionResult pathExecResult = parseAndExecute("$.education?(@.gpa == 3.29)", json);

    JsonSequence expected = valueParser.parse(expectedEqualityJson);
    Assert.assertEquals(expected, pathExecResult.match);
  }

  @Test
  public void filterDoubleEqualsFails() throws IOException, JsonPathException {
    JsonSequence json = valueParser.parse(equalityJson);
    PathExecutionResult pathExecResult = parseAndExecute("$.education?(@.gpa == 3.00)", json);

    Assert.assertEquals(JsonSequence.emptyResult, pathExecResult.match);
  }

  @Test
  public void filterStringEquals() throws IOException, JsonPathException {
    JsonSequence json = valueParser.parse(equalityJson);
    PathExecutionResult pathExecResult = parseAndExecute("$.education?(@.school == \"usc\")", json);

    JsonSequence expected = valueParser.parse(expectedEqualityJson);
    Assert.assertEquals(expected, pathExecResult.match);
  }

  @Test
  public void filterStringEqualsFails() throws IOException, JsonPathException {
    JsonSequence json = valueParser.parse(equalityJson);
    PathExecutionResult pathExecResult = parseAndExecute("$.education?(@.school == \"ucla\")", json);

    Assert.assertEquals(JsonSequence.emptyResult, pathExecResult.match);
  }

  @Test
  public void filterBoolEquals() throws IOException, JsonPathException {
    JsonSequence json = valueParser.parse(equalityJson);
    PathExecutionResult pathExecResult = parseAndExecute("$.education?(@.graduated == true)", json);

    JsonSequence expected = valueParser.parse(expectedEqualityJson);
    Assert.assertEquals(expected, pathExecResult.match);
  }

  @Test
  public void filterBoolEqualsFails() throws IOException, JsonPathException {
    JsonSequence json = valueParser.parse(equalityJson);
    PathExecutionResult pathExecResult = parseAndExecute("$.education?(@.graduated == false)", json);

    Assert.assertEquals(JsonSequence.emptyResult, pathExecResult.match);
  }

  @Test
  public void filterNullEquals() throws IOException, JsonPathException {
    JsonSequence json = valueParser.parse(equalityJson);
    PathExecutionResult pathExecResult = parseAndExecute("$.education?(@.sports == null)", json);

    JsonSequence expected = valueParser.parse(expectedEqualityJson);
    Assert.assertEquals(expected, pathExecResult.match);
  }

  @Test
  public void filterNullEqualsFails() throws IOException, JsonPathException {
    JsonSequence json = valueParser.parse(equalityJson);
    PathExecutionResult pathExecResult = parseAndExecute("$.education?(@.graduated == null)", json);

    Assert.assertEquals(JsonSequence.emptyResult, pathExecResult.match);
  }

  @Test
  public void filterListEquals() throws IOException, JsonPathException {
    JsonSequence json = valueParser.parse(equalityJson);
    PathExecutionResult pathExecResult = parseAndExecute("$.education?(@.activities == @.clubs)", json);

    JsonSequence expected = valueParser.parse(expectedEqualityJson);
    Assert.assertEquals(expected, pathExecResult.match);
  }

  @Test
  public void filterListEqualsFails() throws IOException, JsonPathException {
    JsonSequence json = valueParser.parse(equalityJson);
    PathExecutionResult pathExecResult = parseAndExecute("$.education?(@.activities == @.extracurricular)", json);

    Assert.assertEquals(JsonSequence.emptyResult, pathExecResult.match);
  }

  @Test
  public void filterObjectEquals() throws IOException, JsonPathException {
    JsonSequence json = valueParser.parse(equalityJson);
    PathExecutionResult pathExecResult = parseAndExecute("$.education?(@.\"best class\" == @.\"favorite class\")", json);

    JsonSequence expected = valueParser.parse(expectedEqualityJson);
    Assert.assertEquals(expected, pathExecResult.match);
  }

  @Test
  public void filterObjectEqualsFails() throws IOException, JsonPathException {
    JsonSequence json = valueParser.parse(equalityJson);
    PathExecutionResult pathExecResult = parseAndExecute("$.education?(@.\"best class\" == @.\"worst class\")", json);

    Assert.assertEquals(JsonSequence.emptyResult, pathExecResult.match);
  }

  @Test
  public void filterEmptyEquals() throws IOException, JsonPathException {
    // Test that the right thing happens when the preceding path is empty
    JsonSequence json = valueParser.parse(equalityJson);
    PathExecutionResult pathExecResult = parseAndExecute("$.nosuch?(@.graduated == true)", json);

    Assert.assertEquals(JsonSequence.emptyResult, pathExecResult.match);
  }

  @Test
  public void filterBadTypesEquals() throws IOException {
    try {
      JsonSequence json = valueParser.parse(equalityJson);
      PathExecutionResult pathExecResult = parseAndExecute("$.education?(@.graduated == 3.141592654)", json);
      Assert.assertEquals(JsonSequence.emptyResult, pathExecResult.match);
      Assert.fail();
    } catch (JsonPathException e) {
      Assert.assertEquals("'$.education?(@.graduated == 3.141592654)' produced a semantic error: Cannot compare a bool to a double at \"@.graduated == 3.141592654\"", e.getMessage());
    }
  }

  @Test
  public void filterLongNe() throws IOException, JsonPathException {
    JsonSequence json = valueParser.parse(equalityJson);
    PathExecutionResult pathExecResult = parseAndExecute("$.education?(@.\"years attended\" != 5)", json);

    JsonSequence expected = valueParser.parse(expectedEqualityJson);
    Assert.assertEquals(expected, pathExecResult.match);
  }

  @Test
  public void filterLongNeFails() throws IOException, JsonPathException {
    JsonSequence json = valueParser.parse(equalityJson);
    PathExecutionResult pathExecResult = parseAndExecute("$.education?(@.\"years attended\" != 4)", json);

    Assert.assertEquals(JsonSequence.emptyResult, pathExecResult.match);
  }

  @Test
  public void filterLongDoubleNe() throws IOException, JsonPathException {
    JsonSequence json = valueParser.parse(equalityJson);
    PathExecutionResult pathExecResult = parseAndExecute("$.education?(@.\"years attended\" != 4.1)", json);

    JsonSequence expected = valueParser.parse(expectedEqualityJson);
    Assert.assertEquals(expected, pathExecResult.match);
  }

  @Test
  public void filterLongDoubleNeFails() throws IOException, JsonPathException {
    JsonSequence json = valueParser.parse(equalityJson);
    PathExecutionResult pathExecResult = parseAndExecute("$.education?(@.\"years attended\" != 4.0)", json);

    Assert.assertEquals(JsonSequence.emptyResult, pathExecResult.match);
  }

  @Test
  public void filterDoubleNe() throws IOException, JsonPathException {
    JsonSequence json = valueParser.parse(equalityJson);
    PathExecutionResult pathExecResult = parseAndExecute("$.education?(@.gpa != 3.19)", json);

    JsonSequence expected = valueParser.parse(expectedEqualityJson);
    Assert.assertEquals(expected, pathExecResult.match);
  }

  @Test
  public void filterDoubleNeFails() throws IOException, JsonPathException {
    JsonSequence json = valueParser.parse(equalityJson);
    PathExecutionResult pathExecResult = parseAndExecute("$.education?(@.gpa != 3.29)", json);

    Assert.assertEquals(JsonSequence.emptyResult, pathExecResult.match);
  }

  @Test
  public void filterStringNe() throws IOException, JsonPathException {
    JsonSequence json = valueParser.parse(equalityJson);
    PathExecutionResult pathExecResult = parseAndExecute("$.education?(@.school != \"ucla\")", json);

    JsonSequence expected = valueParser.parse(expectedEqualityJson);
    Assert.assertEquals(expected, pathExecResult.match);
  }

  @Test
  public void filterStringNeFails() throws IOException, JsonPathException {
    JsonSequence json = valueParser.parse(equalityJson);
    PathExecutionResult pathExecResult = parseAndExecute("$.education?(@.school != \"usc\")", json);

    Assert.assertEquals(JsonSequence.emptyResult, pathExecResult.match);
  }

  @Test
  public void filterBoolNe() throws IOException, JsonPathException {
    JsonSequence json = valueParser.parse(equalityJson);
    PathExecutionResult pathExecResult = parseAndExecute("$.education?(@.graduated != false)", json);

    JsonSequence expected = valueParser.parse(expectedEqualityJson);
    Assert.assertEquals(expected, pathExecResult.match);
  }

  @Test
  public void filterBoolNeFails() throws IOException, JsonPathException {
    JsonSequence json = valueParser.parse(equalityJson);
    PathExecutionResult pathExecResult = parseAndExecute("$.education?(@.graduated <> true)", json);

    Assert.assertEquals(JsonSequence.emptyResult, pathExecResult.match);
  }

  @Test
  public void filterNullNe() throws IOException, JsonPathException {
    JsonSequence json = valueParser.parse(equalityJson);
    PathExecutionResult pathExecResult = parseAndExecute("$.education?(@.graduated <> null)", json);

    JsonSequence expected = valueParser.parse(expectedEqualityJson);
    Assert.assertEquals(expected, pathExecResult.match);
  }

  @Test
  public void filterNullNeFails() throws IOException, JsonPathException {
    JsonSequence json = valueParser.parse(equalityJson);
    PathExecutionResult pathExecResult = parseAndExecute("$.education?(@.sports != null)", json);

    Assert.assertEquals(JsonSequence.emptyResult, pathExecResult.match);
  }

  @Test
  public void filterListNe() throws IOException, JsonPathException {
    JsonSequence json = valueParser.parse(equalityJson);
    PathExecutionResult pathExecResult = parseAndExecute("$.education?(@.activities != @.extracurricular)", json);

    JsonSequence expected = valueParser.parse(expectedEqualityJson);
    Assert.assertEquals(expected, pathExecResult.match);
  }

  @Test
  public void filterListNeFails() throws IOException, JsonPathException {
    JsonSequence json = valueParser.parse(equalityJson);
    PathExecutionResult pathExecResult = parseAndExecute("$.education?(@.activities != @.clubs)", json);

    Assert.assertEquals(JsonSequence.emptyResult, pathExecResult.match);
  }

  @Test
  public void filterObjectNe() throws IOException, JsonPathException {
    JsonSequence json = valueParser.parse(equalityJson);
    PathExecutionResult pathExecResult = parseAndExecute("$.education?(@.\"best class\" != @.\"worst class\")", json);

    JsonSequence expected = valueParser.parse(expectedEqualityJson);
    Assert.assertEquals(expected, pathExecResult.match);
  }

  @Test
  public void filterObjectNeFails() throws IOException, JsonPathException {
    JsonSequence json = valueParser.parse(equalityJson);
    PathExecutionResult pathExecResult = parseAndExecute("$.education?(@.\"best class\" != @.\"favorite class\")", json);

    Assert.assertEquals(JsonSequence.emptyResult, pathExecResult.match);
  }

  @Test
  public void filterEmptyNe() throws IOException, JsonPathException {
    // Test that the right thing happens when the preceding path is empty
    JsonSequence json = valueParser.parse(equalityJson);
    PathExecutionResult pathExecResult = parseAndExecute("$.nosuch?(@.graduated != true)", json);

    Assert.assertEquals(JsonSequence.emptyResult, pathExecResult.match);
  }

  @Test
  public void filterBadTypesNe() throws IOException {
    try {
      String path = "$.education?(@.graduated != 3.141592654)";
      JsonSequence json = valueParser.parse(equalityJson);
      PathExecutionResult pathExecResult = parseAndExecute(path, json);
      Assert.assertEquals(JsonSequence.emptyResult, pathExecResult.match);
      Assert.fail();
    } catch (JsonPathException e) {
      Assert.assertEquals("'$.education?(@.graduated != 3.141592654)' produced a semantic error: Cannot compare a bool to a double at \"@.graduated != 3.141592654\"", e.getMessage());
    }
  }

  @Test
  public void filterLongLt() throws IOException, JsonPathException {
    JsonSequence json = valueParser.parse(equalityJson);
    PathExecutionResult pathExecResult = parseAndExecute("$.education?(@.\"years attended\" < 5)", json);

    JsonSequence expected = valueParser.parse(expectedEqualityJson);
    Assert.assertEquals(expected, pathExecResult.match);
  }

  @Test
  public void filterLongLtFails() throws IOException, JsonPathException {
    JsonSequence json = valueParser.parse(equalityJson);
    PathExecutionResult pathExecResult = parseAndExecute("$.education?(@.\"years attended\" < 4)", json);

    Assert.assertEquals(JsonSequence.emptyResult, pathExecResult.match);
  }

  @Test
  public void filterLongDoubleLt() throws IOException, JsonPathException {
    JsonSequence json = valueParser.parse(equalityJson);
    PathExecutionResult pathExecResult = parseAndExecute("$.education?(@.\"years attended\" < 4.1)", json);

    JsonSequence expected = valueParser.parse(expectedEqualityJson);
    Assert.assertEquals(expected, pathExecResult.match);
  }

  @Test
  public void filterLongDoubleLtFails() throws IOException, JsonPathException {
    JsonSequence json = valueParser.parse(equalityJson);
    PathExecutionResult pathExecResult = parseAndExecute("$.education?(@.\"years attended\" < 4.0)", json);

    Assert.assertEquals(JsonSequence.emptyResult, pathExecResult.match);
  }

  @Test
  public void filterDoubleLt() throws IOException, JsonPathException {
    JsonSequence json = valueParser.parse(equalityJson);
    PathExecutionResult pathExecResult = parseAndExecute("$.education?(@.gpa < 3.50)", json);

    JsonSequence expected = valueParser.parse(expectedEqualityJson);
    Assert.assertEquals(expected, pathExecResult.match);
  }

  @Test
  public void filterDoubleLtFails() throws IOException, JsonPathException {
    JsonSequence json = valueParser.parse(equalityJson);
    PathExecutionResult pathExecResult = parseAndExecute("$.education?(@.gpa < 2.29)", json);

    Assert.assertEquals(JsonSequence.emptyResult, pathExecResult.match);
  }

  @Test
  public void filterStringLt() throws IOException, JsonPathException {
    JsonSequence json = valueParser.parse(equalityJson);
    PathExecutionResult pathExecResult = parseAndExecute("$.education?(@.school < \"yyy\")", json);

    JsonSequence expected = valueParser.parse(expectedEqualityJson);
    Assert.assertEquals(expected, pathExecResult.match);
  }

  @Test
  public void filterStringLtFails() throws IOException, JsonPathException {
    JsonSequence json = valueParser.parse(equalityJson);
    PathExecutionResult pathExecResult = parseAndExecute("$.education?(@.school < \"asc\")", json);

    Assert.assertEquals(JsonSequence.emptyResult, pathExecResult.match);
  }

  @Test
  public void filterLtBadType() throws IOException {
    try {
      String path = "$.education?(@.graduated < false)";
      JsonSequence json = valueParser.parse(equalityJson);
      PathExecutionResult pathExecResult = parseAndExecute(path, json);
      Assert.assertEquals(JsonSequence.emptyResult, pathExecResult.match);
      Assert.fail();
    } catch (JsonPathException e) {
      Assert.assertEquals("'$.education?(@.graduated < false)' produced a semantic error: Cannot apply an inequality operator to a bool at \"@.graduated < false\"", e.getMessage());
    }
  }

  @Test
  public void filterEmptyLt() throws IOException, JsonPathException {
    // Test that the right thing happens when the preceding path is empty
    JsonSequence json = valueParser.parse(equalityJson);
    PathExecutionResult pathExecResult = parseAndExecute("$.nosuch?(@.gpa < 4.00)", json);

    Assert.assertEquals(JsonSequence.emptyResult, pathExecResult.match);
  }

  @Test
  public void filterBadTypesLt() throws IOException {
    try {
      String path = "$.education?(@.gpa < 'abc')";
      JsonSequence json = valueParser.parse(equalityJson);
      PathExecutionResult pathExecResult = parseAndExecute(path, json);
      Assert.assertEquals(JsonSequence.emptyResult, pathExecResult.match);
      Assert.fail();
    } catch (JsonPathException e) {
      Assert.assertEquals("'$.education?(@.gpa < 'abc')' produced a semantic error: Cannot compare a decimal to a string at \"@.gpa < 'abc'\"", e.getMessage());
    }
  }

  @Test
  public void filterLongLe() throws IOException, JsonPathException {
    JsonSequence json = valueParser.parse(equalityJson);
    PathExecutionResult pathExecResult = parseAndExecute("$.education?(@.\"years attended\" <= 4)", json);

    JsonSequence expected = valueParser.parse(expectedEqualityJson);
    Assert.assertEquals(expected, pathExecResult.match);
  }

  @Test
  public void filterLongLeFails() throws IOException, JsonPathException {
    JsonSequence json = valueParser.parse(equalityJson);
    PathExecutionResult pathExecResult = parseAndExecute("$.education?(@.\"years attended\" <= 3)", json);

    Assert.assertEquals(JsonSequence.emptyResult, pathExecResult.match);
  }

  @Test
  public void filterLongDoubleLe() throws IOException, JsonPathException {
    JsonSequence json = valueParser.parse(equalityJson);
    PathExecutionResult pathExecResult = parseAndExecute("$.education?(@.\"years attended\" <= 4.1)", json);

    JsonSequence expected = valueParser.parse(expectedEqualityJson);
    Assert.assertEquals(expected, pathExecResult.match);
  }

  @Test
  public void filterLongDoubleLeFails() throws IOException, JsonPathException {
    JsonSequence json = valueParser.parse(equalityJson);
    PathExecutionResult pathExecResult = parseAndExecute("$.education?(@.\"years attended\" <= 3.99)", json);

    Assert.assertEquals(JsonSequence.emptyResult, pathExecResult.match);
  }

  @Test
  public void filterDoubleLe() throws IOException, JsonPathException {
    JsonSequence json = valueParser.parse(equalityJson);
    PathExecutionResult pathExecResult = parseAndExecute("$.education?(@.gpa <= 3.50)", json);

    JsonSequence expected = valueParser.parse(expectedEqualityJson);
    Assert.assertEquals(expected, pathExecResult.match);
  }

  @Test
  public void filterDoubleLeFails() throws IOException, JsonPathException {
    JsonSequence json = valueParser.parse(equalityJson);
    PathExecutionResult pathExecResult = parseAndExecute("$.education?(@.gpa <= 2.29)", json);

    Assert.assertEquals(JsonSequence.emptyResult, pathExecResult.match);
  }

  @Test
  public void filterStringLe() throws IOException, JsonPathException {
    JsonSequence json = valueParser.parse(equalityJson);
    PathExecutionResult pathExecResult = parseAndExecute("$.education?(@.school <= \"uscaaab\")", json);

    JsonSequence expected = valueParser.parse(expectedEqualityJson);
    Assert.assertEquals(expected, pathExecResult.match);
  }

  @Test
  public void filterStringLeFails() throws IOException, JsonPathException {
    JsonSequence json = valueParser.parse(equalityJson);
    PathExecutionResult pathExecResult = parseAndExecute("$.education?(@.school <= \"us\")", json);

    Assert.assertEquals(JsonSequence.emptyResult, pathExecResult.match);
  }

  @Test
  public void filterLeBadType() throws IOException {
    try {
      String path = "$.education?(@.graduated <= false)";
      JsonSequence json = valueParser.parse(equalityJson);
      parseAndExecute(path, json);
      Assert.fail();
    } catch (JsonPathException e) {
      Assert.assertEquals("'$.education?(@.graduated <= false)' produced a semantic error: Cannot apply an inequality operator to a bool at \"@.graduated <= false\"", e.getMessage());
    }
  }

  @Test
  public void filterEmptyLe() throws IOException, JsonPathException {
    // Test that the right thing happens when the preceding path is empty
    JsonSequence json = valueParser.parse(equalityJson);
    PathExecutionResult pathExecResult = parseAndExecute("$.nosuch?(@.gpa <= 4.00)", json);

    Assert.assertEquals(JsonSequence.emptyResult, pathExecResult.match);
  }

  @Test
  public void filterBadTypesLe() throws IOException {
    try {
      String path = "$.education?(@.gpa <= 'abc')";
      JsonSequence json = valueParser.parse(equalityJson);
      parseAndExecute(path, json);
      Assert.fail();
    } catch (JsonPathException e) {
      Assert.assertEquals("'$.education?(@.gpa <= 'abc')' produced a semantic error: Cannot compare a decimal to a string at \"@.gpa <= 'abc'\"", e.getMessage());
    }
  }

  @Test
  public void filterLongGt() throws IOException, JsonPathException {
    JsonSequence json = valueParser.parse(equalityJson);
    PathExecutionResult pathExecResult = parseAndExecute("$.education?(@.\"years attended\" > 3)", json);

    JsonSequence expected = valueParser.parse(expectedEqualityJson);
    Assert.assertEquals(expected, pathExecResult.match);
  }

  @Test
  public void filterLongGtFails() throws IOException, JsonPathException {
    JsonSequence json = valueParser.parse(equalityJson);
    PathExecutionResult pathExecResult = parseAndExecute("$.education?(@.\"years attended\" > 4)", json);

    Assert.assertEquals(JsonSequence.emptyResult, pathExecResult.match);
  }

  @Test
  public void filterLongDoubleGt() throws IOException, JsonPathException {
    JsonSequence json = valueParser.parse(equalityJson);
    PathExecutionResult pathExecResult = parseAndExecute("$.education?(@.\"years attended\" > 3.9)", json);

    JsonSequence expected = valueParser.parse(expectedEqualityJson);
    Assert.assertEquals(expected, pathExecResult.match);
  }

  @Test
  public void filterLongDoubleGtFails() throws IOException, JsonPathException {
    JsonSequence json = valueParser.parse(equalityJson);
    PathExecutionResult pathExecResult = parseAndExecute("$.education?(@.\"years attended\" > 4.0)", json);

    Assert.assertEquals(JsonSequence.emptyResult, pathExecResult.match);
  }

  @Test
  public void filterDoubleGt() throws IOException, JsonPathException {
    JsonSequence json = valueParser.parse(equalityJson);
    PathExecutionResult pathExecResult = parseAndExecute("$.education?(@.gpa > 3.00)", json);

    JsonSequence expected = valueParser.parse(expectedEqualityJson);
    Assert.assertEquals(expected, pathExecResult.match);
  }

  @Test
  public void filterDoubleGtFails() throws IOException, JsonPathException {
    JsonSequence json = valueParser.parse(equalityJson);
    PathExecutionResult pathExecResult = parseAndExecute("$.education?(@.gpa > 3.79)", json);

    Assert.assertEquals(JsonSequence.emptyResult, pathExecResult.match);
  }

  @Test
  public void filterStringGt() throws IOException, JsonPathException {
    JsonSequence json = valueParser.parse(equalityJson);
    PathExecutionResult pathExecResult = parseAndExecute("$.education?(@.school > \"u\")", json);

    JsonSequence expected = valueParser.parse(expectedEqualityJson);
    Assert.assertEquals(expected, pathExecResult.match);
  }

  @Test
  public void filterStringGtFails() throws IOException, JsonPathException {
    JsonSequence json = valueParser.parse(equalityJson);
    PathExecutionResult pathExecResult = parseAndExecute("$.education?(@.school > \"z\")", json);

    Assert.assertEquals(JsonSequence.emptyResult, pathExecResult.match);
  }

  @Test
  public void filterGtBadType() throws IOException {
    try {
      String path = "$.education?(@.graduated > false)";
      JsonSequence json = valueParser.parse(equalityJson);
      PathExecutionResult pathExecResult = parseAndExecute(path, json);
      Assert.assertEquals(JsonSequence.emptyResult, pathExecResult.match);
      Assert.fail();
    } catch (JsonPathException e) {
      Assert.assertEquals("'$.education?(@.graduated > false)' produced a semantic error: Cannot apply an inequality operator to a bool at \"@.graduated > false\"", e.getMessage());
    }
  }

  @Test
  public void filterEmptyGt() throws IOException, JsonPathException {
    // Test that the right thing happens when the preceding path is empty
    JsonSequence json = valueParser.parse(equalityJson);
    PathExecutionResult pathExecResult = parseAndExecute("$.nosuch?(@.gpa > 4.00)", json);

    Assert.assertEquals(JsonSequence.emptyResult, pathExecResult.match);
  }

  @Test
  public void filterBadTypesGt() throws IOException {
    try {
      String path = "$.education?(@.gpa > 'abc')";
      JsonSequence json = valueParser.parse(equalityJson);
      PathExecutionResult pathExecResult = parseAndExecute(path, json);
      Assert.assertEquals(JsonSequence.emptyResult, pathExecResult.match);
      Assert.fail();
    } catch (JsonPathException e) {
      Assert.assertEquals("'$.education?(@.gpa > 'abc')' produced a semantic error: Cannot compare a decimal to a string at \"@.gpa > 'abc'\"", e.getMessage());
    }
  }

  @Test
  public void filterLongGe() throws IOException, JsonPathException {
    JsonSequence json = valueParser.parse(equalityJson);
    PathExecutionResult pathExecResult = parseAndExecute("$.education?(@.\"years attended\" >= 4)", json);

    JsonSequence expected = valueParser.parse(expectedEqualityJson);
    Assert.assertEquals(expected, pathExecResult.match);
  }

  @Test
  public void filterLongGeFails() throws IOException, JsonPathException {
    JsonSequence json = valueParser.parse(equalityJson);
    PathExecutionResult pathExecResult = parseAndExecute("$.education?(@.\"years attended\" >= 5)", json);

    Assert.assertEquals(JsonSequence.emptyResult, pathExecResult.match);
  }

  @Test
  public void filterLongDoubleGe() throws IOException, JsonPathException {
    JsonSequence json = valueParser.parse(equalityJson);
    PathExecutionResult pathExecResult = parseAndExecute("$.education?(@.\"years attended\" >= 4.0)", json);

    JsonSequence expected = valueParser.parse(expectedEqualityJson);
    Assert.assertEquals(expected, pathExecResult.match);
  }

  @Test
  public void filterLongDoubleGeFails() throws IOException, JsonPathException {
    JsonSequence json = valueParser.parse(equalityJson);
    PathExecutionResult pathExecResult = parseAndExecute("$.education?(@.\"years attended\" >= 4.99)", json);

    Assert.assertEquals(JsonSequence.emptyResult, pathExecResult.match);
  }

  @Test
  public void filterDoubleGe() throws IOException, JsonPathException {
    JsonSequence json = valueParser.parse(equalityJson);
    PathExecutionResult pathExecResult = parseAndExecute("$.education?(@.gpa >= 3.29)", json);

    JsonSequence expected = valueParser.parse(expectedEqualityJson);
    Assert.assertEquals(expected, pathExecResult.match);
  }

  @Test
  public void filterDoubleGeFails() throws IOException, JsonPathException {
    JsonSequence json = valueParser.parse(equalityJson);
    PathExecutionResult pathExecResult = parseAndExecute("$.education?(@.gpa >= 3.99)", json);

    Assert.assertEquals(JsonSequence.emptyResult, pathExecResult.match);
  }

  @Test
  public void filterStringGe() throws IOException, JsonPathException {
    JsonSequence json = valueParser.parse(equalityJson);
    PathExecutionResult pathExecResult = parseAndExecute("$.education?(@.school >= \"usc\")", json);

    JsonSequence expected = valueParser.parse(expectedEqualityJson);
    Assert.assertEquals(expected, pathExecResult.match);
  }

  @Test
  public void filterStringGeFails() throws IOException, JsonPathException {
    JsonSequence json = valueParser.parse(equalityJson);
    PathExecutionResult pathExecResult = parseAndExecute("$.education?(@.school >= \"uxxx\")", json);

    Assert.assertEquals(JsonSequence.emptyResult, pathExecResult.match);
  }

  @Test
  public void filterGeBadType() throws IOException {
    try {
      String path = "$.education?(@.graduated >= false)";
      JsonSequence json = valueParser.parse(equalityJson);
      parseAndExecute(path, json);
      Assert.fail();
    } catch (JsonPathException e) {
      Assert.assertEquals("'$.education?(@.graduated >= false)' produced a semantic error: Cannot apply an inequality operator to a bool at \"@.graduated >= false\"", e.getMessage());
    }
  }

  @Test
  public void filterEmptyGe() throws IOException, JsonPathException {
    // Test that the right thing happens when the preceding path is empty
    JsonSequence json = valueParser.parse(equalityJson);
    PathExecutionResult pathExecResult = parseAndExecute("$.nosuch?(@.gpa >= 4.00)", json);

    Assert.assertEquals(JsonSequence.emptyResult, pathExecResult.match);
  }

  @Test
  public void filterBadTypesGe() throws IOException {
    try {
      String path = "$.education?(@.gpa >= 'abc')";
      JsonSequence json = valueParser.parse(equalityJson);
      parseAndExecute(path, json);
      Assert.fail();
    } catch (JsonPathException e) {
      Assert.assertEquals("'$.education?(@.gpa >= 'abc')' produced a semantic error: Cannot compare a decimal to a string at \"@.gpa >= 'abc'\"", e.getMessage());
    }
  }

  // TODO test equals double -long
  // TODO test ne double -long

  // TODO test all the comparison predicates against a list
  private String listEqualityJson =
      "{" +
          "\"name\"    : \"fred\"," +
          "\"classes\" : [" +
            "{" +
              "\"department\"     : \"math\"," +
              "\"number\"         : 101," +
              "\"avg attendance\" : 287.5," +
              "\"honors\"         : false," +
              "\"prerequisites\"  : null" +
            "}, {" +
              "\"department\"     : \"art\"," +
              "\"number\"         : 401," +
              "\"avg attendance\" : 7.0," +
              "\"honors\"         : true," +
              "\"prerequisites\"  : [ \"art 301\" ]" +
            "}" +
          "]" +
      "}";

  private String listEqualityJsonExpectedFirst =
      "{ \"bogus\" : [" +
        "{" +
          "\"department\"     : \"math\"," +
          "\"number\"         : 101," +
          "\"avg attendance\" : 287.5," +
          "\"honors\"         : false," +
          "\"prerequisites\"  : null" +
        "}" +
     "] }";

  private String listEqualityJsonExpectedSecond =
      "{ \"bogus\" : [" +
        "{" +
          "\"department\"     : \"art\"," +
          "\"number\"         : 401," +
          "\"avg attendance\" : 7.0," +
          "\"honors\"         : true," +
          "\"prerequisites\"  : [ \"art 301\" ]" +
        "}" +
     "] }";

  private String listEqualityJsonExpectedBoth =
      "{ \"bogus\" : [" +
          "{" +
            "\"department\"     : \"math\"," +
            "\"number\"         : 101," +
            "\"avg attendance\" : 287.5," +
            "\"honors\"         : false," +
            "\"prerequisites\"  : null" +
          "}, {" +
            "\"department\"     : \"art\"," +
            "\"number\"         : 401," +
            "\"avg attendance\" : 7.0," +
            "\"honors\"         : true," +
            "\"prerequisites\"  : [ \"art 301\" ]" +
          "}" +
     "] }";

  @Test
  public void filterListLongEquals() throws IOException, JsonPathException {
    JsonSequence json = valueParser.parse(listEqualityJson);
    PathExecutionResult pathExecResult = parseAndExecute("$.classes[*]?(@.number == 101)", json);

    JsonSequence expected = valueParser.parse(listEqualityJsonExpectedFirst);
    Assert.assertEquals(expected.asObject().get("bogus"), pathExecResult.match);
  }

  @Test
  public void filterListStringNe() throws IOException, JsonPathException {
    JsonSequence json = valueParser.parse(listEqualityJson);
    PathExecutionResult pathExecResult = parseAndExecute("$.classes[*]?(@.department != \"science\")", json);

    JsonSequence expected = valueParser.parse(listEqualityJsonExpectedBoth);
    Assert.assertEquals(expected.asObject().get("bogus"), pathExecResult.match);
  }

  @Test
  public void filterListDoubleGt() throws IOException, JsonPathException {
    JsonSequence json = valueParser.parse(listEqualityJson);
    PathExecutionResult pathExecResult = parseAndExecute("$.classes[*]?(@.\"avg attendance\" > 50)", json);

    JsonSequence expected = valueParser.parse(listEqualityJsonExpectedFirst);
    Assert.assertEquals(expected.asObject().get("bogus"), pathExecResult.match);
  }

  @Test
  public void filterListNullNe() throws IOException, JsonPathException {
    JsonSequence json = valueParser.parse(listEqualityJson);
    PathExecutionResult pathExecResult = parseAndExecute("$.classes[*]?(@.prerequisites != null)", json);

    JsonSequence expected = valueParser.parse(listEqualityJsonExpectedSecond);
    Assert.assertEquals(expected.asObject().get("bogus"), pathExecResult.match);
  }

  @Test
  public void filterListOr() throws IOException, JsonPathException {
    JsonSequence json = valueParser.parse(listEqualityJson);
    PathExecutionResult pathExecResult = parseAndExecute("$.classes[*]?(@.department == \"art\" || @.department == \"math\")", json);

    JsonSequence expected = valueParser.parse(listEqualityJsonExpectedBoth);
    Assert.assertEquals(expected.asObject().get("bogus"), pathExecResult.match);
  }

  @Test
  public void filterListAnd() throws IOException, JsonPathException {
    JsonSequence json = valueParser.parse(listEqualityJson);
    PathExecutionResult pathExecResult = parseAndExecute("$.classes[*]?(@.number >= 200 && @.\"avg attendance\" > 20)", json);
    Assert.assertEquals(JsonSequence.emptyResult, pathExecResult.match);
  }

  @Test
  public void filterRegex() throws IOException, JsonPathException {
    JsonSequence json = valueParser.parse(equalityJson);
    PathExecutionResult pathExecResult = parseAndExecute("$.education?(@.school like_regex \"u.c\")", json);

    JsonSequence expected = valueParser.parse(expectedEqualityJson);
    Assert.assertEquals(expected, pathExecResult.match);
  }

  @Test
  public void filterRegexFails() throws IOException, JsonPathException {
    JsonSequence json = valueParser.parse(equalityJson);
    PathExecutionResult pathExecResult = parseAndExecute("$.education?(@.school like_regex \"u[x|y]c\")", json);
    Assert.assertEquals(JsonSequence.emptyResult, pathExecResult.match);
  }

  @Test
  public void filterRegexList() throws IOException, JsonPathException {
    JsonSequence json = valueParser.parse(listEqualityJson);
    PathExecutionResult pathExecResult = parseAndExecute("$.classes[*]?(@.department like_regex \"^a.*\")", json);

    JsonSequence expected = valueParser.parse(listEqualityJsonExpectedSecond);
    Assert.assertEquals(expected.asObject().get("bogus"), pathExecResult.match);
  }

  @Test
  public void filterRegexBadtype() throws IOException {
    try {
      String path = "$.education?(@.gpa like_regex 'abc.*')";
      JsonSequence json = valueParser.parse(equalityJson);
      parseAndExecute(path, json);
      Assert.fail();
    } catch (JsonPathException e) {
      Assert.assertEquals("'$.education?(@.gpa like_regex 'abc.*')' produced a semantic error: Regular expressions can only be used on strings at \"@.gpa like_regex 'abc.*'\"", e.getMessage());
    }
  }

  // TODO - figure out a regular expression that throws an error in the Java parser but not path parser
  /*
  @Test
  public void filterRegexSyntaxError() throws IOException, JsonPathException {
    String path = "$.education?(@.school like_regex '\\\\u12x abc')";
    JsonSequence json = valueParser.parse(equalityJson);
    PathExecutionResult pathExecResult = parseAndExecute(path, json);
    try {
      pathExecResult.errorListener.checkForErrors(path);
      Assert.fail();
    } catch (JsonPathException e) {
      Assert.assertEquals("", e.getMessage());
    }
  }
  */

  @Test
  public void filterStartsWith() throws IOException, JsonPathException {
    JsonSequence json = valueParser.parse(equalityJson);
    PathExecutionResult pathExecResult = parseAndExecute("$.education?(@.school starts with \"us\")", json);

    JsonSequence expected = valueParser.parse(expectedEqualityJson);
    Assert.assertEquals(expected, pathExecResult.match);
  }

  @Test
  public void filterStartsWithVal() throws IOException, JsonPathException {
    JsonSequence json = valueParser.parse(equalityJson);
    PathExecutionResult pathExecResult = parseAndExecute("$.education?(@.school starts with $match)", json, Collections.singletonMap("match", new JsonSequence("us")));

    JsonSequence expected = valueParser.parse(expectedEqualityJson);
    Assert.assertEquals(expected, pathExecResult.match);
  }

  @Test
  public void filterStartsWithFails() throws IOException, JsonPathException {
    JsonSequence json = valueParser.parse(equalityJson);
    PathExecutionResult pathExecResult = parseAndExecute("$.education?(@.school starts with \"oregon\")", json);
    Assert.assertEquals(JsonSequence.emptyResult, pathExecResult.match);
  }

  @Test
  public void filterStartsWithList() throws IOException, JsonPathException {
    JsonSequence json = valueParser.parse(listEqualityJson);
    PathExecutionResult pathExecResult = parseAndExecute("$.classes[*]?(@.department starts with \"a\")", json);

    JsonSequence expected = valueParser.parse(listEqualityJsonExpectedSecond);
    Assert.assertEquals(expected.asObject().get("bogus"), pathExecResult.match);
  }

  @Test
  public void filterStartsWithBadtype() throws IOException {
    try {
      String path = "$.education?(@.gpa starts with 'abc')";
      JsonSequence json = valueParser.parse(equalityJson);
      parseAndExecute(path, json);
      Assert.fail();
    } catch (JsonPathException e) {
      Assert.assertEquals("'$.education?(@.gpa starts with 'abc')' produced a semantic error: Starts with can only be used with strings at \"@.gpa starts with 'abc'\"", e.getMessage());
    }
  }

  @Test
  public void filterStartsWithBadtype2() throws IOException {
    try {
      String path = "$.education?(@.school starts with $match)";
      JsonSequence json = valueParser.parse(equalityJson);
      parseAndExecute(path, json, Collections.singletonMap("match", new JsonSequence(3.14)));
      Assert.fail();
    } catch (JsonPathException e) {
      Assert.assertEquals("'$.education?(@.school starts with $match)' produced a semantic error: Starts with can only be used with strings at \"@.school starts with $match\"", e.getMessage());
    }
  }

  // TODO - add test where several json values are run against the same parser/executor to test that everything resets properly.

  private PathExecutionResult parseAndExecute(String path) throws IOException, JsonPathException {
    return parseAndExecute(path, emptyJson, null);
  }

  private PathExecutionResult parseAndExecute(String path, JsonSequence values) throws IOException, JsonPathException {
    return parseAndExecute(path, values, null);
  }

  private PathExecutionResult parseAndExecute(String path, JsonSequence values, Map<String, JsonSequence> passing)
      throws IOException, JsonPathException {
    PathParser parser = new PathParser();
    PathParseResult parseResult = parser.parse(path);
    PathExecutor executor = new PathExecutor();
    JsonSequence match = executor.execute(parseResult, values, passing);
    return new PathExecutionResult(parseResult, executor, match);
  }

  private static class PathExecutionResult {
    final PathParseResult parseResult;
    final PathExecutor executor;
    final JsonSequence match;

    PathExecutionResult(PathParseResult parseResult, PathExecutor executor, JsonSequence match) {
      this.parseResult = parseResult;
      this.executor = executor;
      this.match = match;
    }
  }
}
