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

import org.antlr.v4.runtime.tree.ParseTree;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

public class TestExecutor {

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
    Context context = parseAndValidate("$.a");
    Assert.assertEquals(Mode.LAX, context.validator.getMode());
  }

  @Test
  public void laxSpecified() throws IOException, ParseException {
    Context context = parseAndValidate("lax $.a");
    Assert.assertEquals(Mode.LAX, context.validator.getMode());
  }

  @Test
  public void strict() throws IOException, ParseException {
    Context context = parseAndValidate("strict $.a");
    Assert.assertEquals(Mode.STRICT, context.validator.getMode());
  }

  @Test
  public void longLiteral() throws IOException, ParseException {
    Context context = parseValidateExecute("5");
    Assert.assertEquals(5L, context.val.asLong());
  }

  @Test
  public void doubleLiteral() throws IOException, ParseException {
    Context context = parseValidateExecute("5.1");
    Assert.assertEquals(5.1, context.val.asDouble(), 0.00001);
  }

  @Test
  public void booleanLiteral() throws IOException, ParseException {
    Context context = parseValidateExecute("true");
    Assert.assertTrue(context.val.asBool());
  }

  @Test
  public void nullLiteral() throws IOException, ParseException {
    Context context = parseValidateExecute("null");
    Assert.assertTrue(context.val.isNull());
  }

  @Test
  public void singleQuoteStringLiteral() throws IOException, ParseException {
    Context context = parseValidateExecute("'fred'");
    Assert.assertEquals("fred", context.val.asString());
  }

  @Test
  public void doubleQuoteStringLiteral() throws IOException, ParseException {
    Context context = parseValidateExecute("\"fred\"");
    Assert.assertEquals("fred", context.val.asString());
  }

  @Test
  public void addLong() throws IOException, ParseException {
    Context context = parseValidateExecute("5 + 6");
    Assert.assertTrue(context.val.isLong());
    Assert.assertEquals(11L, context.val.asLong());
  }

  @Test
  public void subtractLong() throws IOException, ParseException {
    Context context = parseValidateExecute("8 - 4");
    Assert.assertTrue(context.val.isLong());
    Assert.assertEquals(4L, context.val.asLong());
  }

  @Test
  public void multiplyLong() throws IOException, ParseException {
    Context context = parseValidateExecute("9 * 10");
    Assert.assertTrue(context.val.isLong());
    Assert.assertEquals(90L, context.val.asLong());
  }

  @Test
  public void divideLong() throws IOException, ParseException {
    Context context = parseValidateExecute("9 / 3");
    Assert.assertTrue(context.val.isLong());
    Assert.assertEquals(3L, context.val.asLong());
  }

  @Test
  public void modLong() throws IOException, ParseException {
    Context context = parseValidateExecute("10 % 3");
    Assert.assertTrue(context.val.isLong());
    Assert.assertEquals(1L, context.val.asLong());
  }

  @Test
  public void addDouble() throws IOException, ParseException {
    Context context = parseValidateExecute("5.1 + 7.2");
    Assert.assertTrue(context.val.isDouble());
    Assert.assertEquals(12.3, context.val.asDouble(), 0.00001);
  }

  @Test
  public void subtractDouble() throws IOException, ParseException {
    Context context = parseValidateExecute("10.0 - .2");
    Assert.assertTrue(context.val.isDouble());
    Assert.assertEquals(9.8, context.val.asDouble(), 0.00001);
  }

  @Test
  public void multiplyDouble() throws IOException, ParseException {
    Context context = parseValidateExecute("2.0 * 3.141592654");
    Assert.assertTrue(context.val.isDouble());
    Assert.assertEquals(6.283185308, context.val.asDouble(), 0.001);
  }

  @Test
  public void divideDouble() throws IOException, ParseException {
    Context context = parseValidateExecute("20.0 / 3.0");
    Assert.assertTrue(context.val.isDouble());
    Assert.assertEquals(6.66666, context.val.asDouble(), 0.001);
  }

  @Test
  public void addLongAndDouble() throws IOException, ParseException {
    Context context = parseValidateExecute("5 + 7.2");
    Assert.assertTrue(context.val.isDouble());
    Assert.assertEquals(12.2, context.val.asDouble(), 0.00001);
  }

  @Test
  public void subtractLongAndDouble() throws IOException, ParseException {
    Context context = parseValidateExecute("10 - 7.2");
    Assert.assertTrue(context.val.isDouble());
    Assert.assertEquals(2.8, context.val.asDouble(), 0.00001);
  }

  @Test
  public void multiplyLongAndDouble() throws IOException, ParseException {
    Context context = parseValidateExecute("10 * 1.238273");
    Assert.assertTrue(context.val.isDouble());
    Assert.assertEquals(12.38273, context.val.asDouble(), 0.00001);
  }

  @Test
  public void divideLongAndDouble() throws IOException, ParseException {
    Context context = parseValidateExecute("20 / 1.238273");
    Assert.assertTrue(context.val.isDouble());
    Assert.assertEquals(16.151527167272484, context.val.asDouble(), 0.00001);
  }

  @Test
  public void addDoubleAndLong() throws IOException, ParseException {
    Context context = parseValidateExecute("5.2 + 7");
    Assert.assertTrue(context.val.isDouble());
    Assert.assertEquals(12.2, context.val.asDouble(), 0.00001);
  }

  @Test
  public void subtractDoubleAndLong() throws IOException, ParseException {
    Context context = parseValidateExecute("10.2 - 7");
    Assert.assertTrue(context.val.isDouble());
    Assert.assertEquals(3.2, context.val.asDouble(), 0.00001);
  }

  @Test
  public void multiplyDoubleAndLong() throws IOException, ParseException {
    Context context = parseValidateExecute("1.238273 * 10");
    Assert.assertTrue(context.val.isDouble());
    Assert.assertEquals(12.38273, context.val.asDouble(), 0.00001);
  }

  @Test
  public void divideDoubleAndLong() throws IOException, ParseException {
    Context context = parseValidateExecute("20.238273 / 3");
    Assert.assertTrue(context.val.isDouble());
    Assert.assertEquals(6.746091, context.val.asDouble(), 0.00001);
  }

  @Test
  public void longUnaryPlus() throws IOException, ParseException {
    Context context = parseValidateExecute("+3");
    Assert.assertTrue(context.val.isLong());
    Assert.assertEquals(3L, context.val.asLong());
  }

  @Test
  public void longUnaryMinus() throws IOException, ParseException {
    Context context = parseValidateExecute("-3");
    Assert.assertTrue(context.val.isLong());
    Assert.assertEquals(-3L, context.val.asLong());
  }

  @Test
  public void doubleUnaryPlus() throws IOException, ParseException {
    Context context = parseValidateExecute("+20.238273");
    Assert.assertTrue(context.val.isDouble());
    Assert.assertEquals(20.238273, context.val.asDouble(), 0.00001);
  }

  @Test
  public void doubleUnaryMinus() throws IOException, ParseException {
    Context context = parseValidateExecute("-20.238273");
    Assert.assertTrue(context.val.isDouble());
    Assert.assertEquals(-20.238273, context.val.asDouble(), 0.00001);
  }

  @Test
  public void badLongAdd() throws IOException, ParseException {
    String pathExpr = "20 + 'fred'";
    Context context = parseValidateExecute(pathExpr);
    try {
      context.executor.errorListener.checkForErrors(pathExpr);
      Assert.fail();
    } catch (ParseException e) {
      Assert.assertEquals("'20 + 'fred'' produced a semantic error: You cannot do arithmetic on a string", e.getMessage());
    }
  }

  @Test
  public void badLongSubtract() throws IOException, ParseException {
    String pathExpr = "20 - 'fred'";
    Context context = parseValidateExecute(pathExpr);
    try {
      context.executor.errorListener.checkForErrors(pathExpr);
      Assert.fail();
    } catch (ParseException e) {
      Assert.assertEquals("'20 - 'fred'' produced a semantic error: You cannot do arithmetic on a string", e.getMessage());
    }
  }

  @Test
  public void badLongMultiply() throws IOException, ParseException {
    String pathExpr = "20 * true";
    Context context = parseValidateExecute(pathExpr);
    try {
      context.executor.errorListener.checkForErrors(pathExpr);
      Assert.fail();
    } catch (ParseException e) {
      Assert.assertEquals("'20 * true' produced a semantic error: You cannot do arithmetic on a bool", e.getMessage());
    }
  }

  @Test
  public void badLongDivide() throws IOException, ParseException {
    String pathExpr = "20 / 'bob'";
    Context context = parseValidateExecute(pathExpr);
    try {
      context.executor.errorListener.checkForErrors(pathExpr);
      Assert.fail();
    } catch (ParseException e) {
      Assert.assertEquals("'20 / 'bob'' produced a semantic error: You cannot do arithmetic on a string", e.getMessage());
    }
  }

  @Test
  public void badMod() throws IOException, ParseException {
    String pathExpr = "20 % 3.0";
    Context context = parseValidateExecute(pathExpr);
    try {
      context.executor.errorListener.checkForErrors(pathExpr);
      Assert.fail();
    } catch (ParseException e) {
      Assert.assertEquals("'20 % 3.0' produced a semantic error: You cannot do mod on a double", e.getMessage());
    }
  }

  @Test
  public void badStringAdd() throws IOException, ParseException {
    String pathExpr = "'fred' + 3.0";
    Context context = parseValidateExecute(pathExpr);
    try {
      context.executor.errorListener.checkForErrors(pathExpr);
      Assert.fail();
    } catch (ParseException e) {
      Assert.assertEquals("''fred' + 3.0' produced a semantic error: You cannot do arithmetic on a string", e.getMessage());
    }
  }

  @Test
  public void badStringSubtract() throws IOException, ParseException {
    String pathExpr = "'fred' - 3.0";
    Context context = parseValidateExecute(pathExpr);
    try {
      context.executor.errorListener.checkForErrors(pathExpr);
      Assert.fail();
    } catch (ParseException e) {
      Assert.assertEquals("''fred' - 3.0' produced a semantic error: You cannot do arithmetic on a string", e.getMessage());
    }
  }

  @Test
  public void badStringMultiply() throws IOException, ParseException {
    String pathExpr = "'fred' * 3.0";
    Context context = parseValidateExecute(pathExpr);
    try {
      context.executor.errorListener.checkForErrors(pathExpr);
      Assert.fail();
    } catch (ParseException e) {
      Assert.assertEquals("''fred' * 3.0' produced a semantic error: You cannot do arithmetic on a string", e.getMessage());
    }
  }

  @Test
  public void badStringDivide() throws IOException, ParseException {
    String pathExpr = "'fred' / 3.0";
    Context context = parseValidateExecute(pathExpr);
    try {
      context.executor.errorListener.checkForErrors(pathExpr);
      Assert.fail();
    } catch (ParseException e) {
      Assert.assertEquals("''fred' / 3.0' produced a semantic error: You cannot do arithmetic on a string", e.getMessage());
    }
  }

  @Test
  public void badStringMod() throws IOException, ParseException {
    String pathExpr = "'fred' % 3.0";
    Context context = parseValidateExecute(pathExpr);
    try {
      context.executor.errorListener.checkForErrors(pathExpr);
      Assert.fail();
    } catch (ParseException e) {
      Assert.assertEquals("''fred' % 3.0' produced a semantic error: You cannot do mod on a string", e.getMessage());
    }
  }

  @Test
  public void addNull() throws IOException, ParseException {
    Context context = parseValidateExecute("20 + null");
    Assert.assertTrue(context.val.isNull());
  }

  @Test
  public void subtractNull() throws IOException, ParseException {
    Context context = parseValidateExecute("20.0 - null");
    Assert.assertTrue(context.val.isNull());
  }

  @Test
  public void multiplyNull() throws IOException, ParseException {
    Context context = parseValidateExecute("20 * null");
    Assert.assertTrue(context.val.isNull());
  }

  @Test
  public void divideNull() throws IOException, ParseException {
    Context context = parseValidateExecute("20 / null");
    Assert.assertTrue(context.val.isNull());
  }

  @Test
  public void modNull() throws IOException, ParseException {
    Context context = parseValidateExecute("20 % null");
    Assert.assertTrue(context.val.isNull());
  }

  @Test
  public void nullAdd() throws IOException, ParseException {
    Context context = parseValidateExecute("null + 20");
    Assert.assertTrue(context.val.isNull());
  }

  @Test
  public void nullSubtract() throws IOException, ParseException {
    Context context = parseValidateExecute("null - 20.0");
    Assert.assertTrue(context.val.isNull());
  }

  @Test
  public void nullMultiply() throws IOException, ParseException {
    Context context = parseValidateExecute("null * 20");
    Assert.assertTrue(context.val.isNull());
  }

  @Test
  public void nullDivide() throws IOException, ParseException {
    Context context = parseValidateExecute("null / 20");
    Assert.assertTrue(context.val.isNull());
  }

  @Test
  public void nullMod() throws IOException, ParseException {
    Context context = parseValidateExecute("null % 20");
    Assert.assertTrue(context.val.isNull());
  }

  @Test
  public void pathNamedVariable() throws IOException, ParseException {
    Context context = parseValidateExecute("$fred", null, Collections.singletonMap("fred", new JsonSequence(5L, new ErrorListener())));
    Assert.assertTrue(context.val.isLong());
    Assert.assertEquals(5L, context.val.asLong());
  }

  @Test
  public void pathNamedVariableNoMatchingId() throws IOException, ParseException {
    String pathExpr = "$fred";
    Context context = parseValidateExecute(pathExpr, null, Collections.singletonMap("bob", new JsonSequence(5L, new ErrorListener())));
    try {
      context.executor.errorListener.checkForErrors(pathExpr);
      Assert.fail();
    } catch (ParseException e) {
      Assert.assertEquals("'" + pathExpr + "' produced a semantic error: Variable fred" +
          " referenced in path expression but no matching id found in passing clause", e.getMessage());
    }
  }

  @Test
  public void pathNamedVariableNullPassing() throws IOException, ParseException {
    String pathExpr = "$fred";
    Context context = parseValidateExecute(pathExpr);
    try {
      context.executor.errorListener.checkForErrors(pathExpr);
      Assert.fail();
    } catch (ParseException e) {
      Assert.assertEquals("'" + pathExpr + "' produced a semantic error: Variable fred" +
          " referenced in path expression but no matching id found in passing clause", e.getMessage());
    }
  }

  @Test
  public void pathNamedVariableEmptyPassing() throws IOException, ParseException {
    String pathExpr = "$fred";
    Context context = parseValidateExecute(pathExpr, null, Collections.emptyMap());
    try {
      context.executor.errorListener.checkForErrors(pathExpr);
      Assert.fail();
    } catch (ParseException e) {
      Assert.assertEquals("'" + pathExpr + "' produced a semantic error: Variable fred" +
          " referenced in path expression but no matching id found in passing clause", e.getMessage());
    }
  }

  private ParseTree parse(String path) throws IOException, ParseException {
    PathParser parser = new PathParser();
    parser.parse(path);
    return parser.getTree();
  }

  private Context parseAndValidate(String path) throws IOException, ParseException {
    return parseAndValidate(path, null);
  }

  private Context parseAndValidate(String path, Map<String, JsonSequence> passing) throws IOException, ParseException {
    ParseTree tree = parse(path);
    ErrorListener errorListener = new ErrorListener();
    PathValidator validator = new PathValidator(errorListener);
    validator.validate(tree, passing);
    return new Context(tree, validator, errorListener);
  }

  private Context parseValidateExecute(String path) throws IOException, ParseException {
    return parseValidateExecute(path, null, null, EmptyOrErrorBehavior.NULL, EmptyOrErrorBehavior.NULL);
  }

  private Context parseValidateExecute(String path, JsonSequence value, Map<String, JsonSequence> passing) throws IOException, ParseException {
    return parseValidateExecute(path, value, passing, EmptyOrErrorBehavior.NULL, EmptyOrErrorBehavior.NULL);
  }

  private Context parseValidateExecute(String path, JsonSequence value, Map<String, JsonSequence> passing, EmptyOrErrorBehavior onEmpty,
                                       EmptyOrErrorBehavior onError)
      throws IOException, ParseException {
    Context context = parseAndValidate(path, passing);
    ErrorListener errorListener = new ErrorListener();
    PathExecutor executor = new PathExecutor(errorListener);
    context.executor = executor;
    context.val = executor.execute(context.tree, value, passing, onEmpty, onError, context.validator);
    return context;
  }

  private static class Context {
    final ParseTree tree;
    final PathValidator validator;
    final ErrorListener errorListener;
    PathExecutor executor;
    JsonSequence val;

    public Context(ParseTree tree, PathValidator validator, ErrorListener errorListener) {
      this.tree = tree;
      this.validator = validator;
      this.errorListener = errorListener;
      executor = null;
    }

  }
}
