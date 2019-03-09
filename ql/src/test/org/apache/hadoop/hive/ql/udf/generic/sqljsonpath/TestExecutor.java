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
import java.util.Map;

public class TestExecutor {

  @Test
  public void syntaxError() throws IOException {
    try {
      parse("fizzbot");
    } catch (ParseException e) {
      Assert.assertEquals("Path expression 'fizzbot' produced scan or parse errors: no viable alternative at input 'fizzbot' on line 1 at position 0", e.getMessage());
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
    Assert.assertEquals(5L, (long)context.val.asLong());
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
    Assert.assertEquals(11L, (long)context.val.asLong());
  }

  @Test
  public void addDouble() throws IOException, ParseException {
    Context context = parseValidateExecute("5.1 + 7.2");
    Assert.assertEquals(12.3, context.val.asDouble(), 0.00001);
  }

  // TODO test arithmetic on non-integer types
  // TODO test casts long <-> double


  private ParseTree parse(String path) throws IOException, ParseException {
    Parser parser = new Parser();
    parser.parse(path);
    return parser.getTree();
  }

  private Context parseAndValidate(String path) throws IOException, ParseException {
    ParseTree tree = parse(path);
    ErrorListener errorListener = new ErrorListener();
    Validator validator = new Validator(errorListener);
    validator.validate(tree);
    return new Context(tree, validator, errorListener);
  }

  private Context parseValidateExecute(String path) throws IOException, ParseException {
    return parseValidateExecute(path, null, EmptyOrErrorBehavior.NULL, EmptyOrErrorBehavior.NULL);
  }

  private Context parseValidateExecute(String path, Map<String, String> passing, EmptyOrErrorBehavior onEmpty,
                                       EmptyOrErrorBehavior onError)
      throws IOException, ParseException {
    Context context = parseAndValidate(path);
    ErrorListener errorListener = new ErrorListener();
    Executor executor = new Executor(errorListener);
    context.executor = executor;
    context.val = executor.execute(context.tree, path, passing, onEmpty, onError, context.validator);
    return context;
  }

  private static class Context {
    final ParseTree tree;
    final Validator validator;
    final ErrorListener errorListener;
    Executor executor;
    ValueUnion val;

    public Context(ParseTree tree, Validator validator, ErrorListener errorListener) {
      this.tree = tree;
      this.validator = validator;
      this.errorListener = errorListener;
      executor = null;
    }

  }
}
