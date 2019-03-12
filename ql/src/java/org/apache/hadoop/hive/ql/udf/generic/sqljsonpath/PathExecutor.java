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
import org.apache.hadoop.hive.ql.udf.generic.SqlJsonPathBaseVisitor;
import org.apache.hadoop.hive.ql.udf.generic.SqlJsonPathParser;

import java.util.Collections;
import java.util.Map;

public class PathExecutor extends SqlJsonPathBaseVisitor<JsonSequence> {

  protected JsonSequence value;
  protected Map<String, JsonSequence> passing;
  protected EmptyOrErrorBehavior onEmpty;
  protected EmptyOrErrorBehavior onError;
  protected PathValidator validator;
  protected ErrorListener errorListener;

  public PathExecutor(ErrorListener errorListener) {
    this.errorListener = errorListener;

  }

  public JsonSequence execute(ParseTree tree, JsonSequence value, Map<String, JsonSequence> passing, PathValidator validator) {
    return execute(tree, value, passing, EmptyOrErrorBehavior.NULL, EmptyOrErrorBehavior.NULL, validator);
  }

  /**
   * Execute a SQL/JSON Path statement against a particular bit of JSON
   * @param tree the parse tree for the SQL/JSON Path
   * @param value JSON value to execute the Path statement against
   * @param passing map of arguments defined in the parse tree
   * @param onEmpty behavior when a specified key is not present
   * @param onError behavior when an error occurs
   * @return value of executing the Path statement against the value
   */
  public JsonSequence execute(ParseTree tree, JsonSequence value, Map<String, JsonSequence> passing, EmptyOrErrorBehavior onEmpty,
                              EmptyOrErrorBehavior onError, PathValidator validator) {
    this.value = value;
    this.passing = passing == null ? Collections.emptyMap() : passing;
    this.onEmpty = onEmpty;
    this.onError = onError;
    this.validator = validator;
    return visit(tree);
  }

  @Override
  public JsonSequence visitAdditive_expression(SqlJsonPathParser.Additive_expressionContext ctx) {
    if (ctx.getChildCount() == 1) return visit(ctx.getChild(0));
    assert ctx.getChildCount() == 3;
    ParseTree operator = ctx.getChild(1);
    JsonSequence val1 = visit(ctx.getChild(0));
    JsonSequence val2 = visit(ctx.getChild(2));
    switch (operator.getText()) {
      case "+": val1.add(val2); break;
      case "-": val1.subtract(val2); break;
      default: throw new RuntimeException("Programming error");
    }
    return val1;
  }

  @Override
  public JsonSequence visitMultiplicative_expression(SqlJsonPathParser.Multiplicative_expressionContext ctx) {
    if (ctx.getChildCount() == 1) return visit(ctx.getChild(0));
    assert ctx.getChildCount() == 3;
    ParseTree operator = ctx.getChild(1);
    JsonSequence val1 = visit(ctx.getChild(0));
    JsonSequence val2 = visit(ctx.getChild(2));
    switch (operator.getText()) {
      case "*": val1.multiply(val2); break;
      case "/": val1.divide(val2); break;
      case "%": val1.modulo(val2); break;
      default: throw new RuntimeException("Programming error");
    }
    return val1;
  }

  @Override
  public JsonSequence visitUnary_expression(SqlJsonPathParser.Unary_expressionContext ctx) {
    if (ctx.getChildCount() == 1) return visit(ctx.getChild(0));
    assert ctx.getChildCount() == 2;
    ParseTree operator = ctx.getChild(0);
    JsonSequence val = visit(ctx.getChild(1));
    switch (operator.getText()) {
      case "+": break;
      case "-": val.negate(); break;
      default: throw new RuntimeException("Programming error");
    }
    return val;
  }

  @Override
  public JsonSequence visitPath_null_literal(SqlJsonPathParser.Path_null_literalContext ctx) {
    return JsonSequence.nullValue(errorListener);
  }

  @Override
  public JsonSequence visitPath_boolean_literal(SqlJsonPathParser.Path_boolean_literalContext ctx) {
    if (ctx.getText().equalsIgnoreCase("true")) return new JsonSequence(true, errorListener);
    else if (ctx.getText().equalsIgnoreCase("false")) return new JsonSequence(false, errorListener);
    else throw new RuntimeException("Programming error");
  }

  @Override
  public JsonSequence visitPath_integer_literal(SqlJsonPathParser.Path_integer_literalContext ctx) {
    return new JsonSequence(Long.valueOf(ctx.getText()), errorListener);
  }

  @Override
  public JsonSequence visitPath_decimal_literal(SqlJsonPathParser.Path_decimal_literalContext ctx) {
    return new JsonSequence(Double.valueOf(ctx.getText()), errorListener);
  }

  @Override
  public JsonSequence visitPath_string_literal(SqlJsonPathParser.Path_string_literalContext ctx) {
    String val = ctx.getText();
    return new JsonSequence(val.substring(1, val.length() - 1), errorListener);
  }

  @Override
  public JsonSequence visitPath_named_variable(SqlJsonPathParser.Path_named_variableContext ctx) {
    String id = ctx.getChild(1).getText();
    JsonSequence val = passing.get(id);
    if (val == null) {
      errorListener.semanticError("Variable " + id +
          " referenced in path expression but no matching id found in passing clause");
      return JsonSequence.nullValue(errorListener);
    } else {
      return val;
    }
  }
}
