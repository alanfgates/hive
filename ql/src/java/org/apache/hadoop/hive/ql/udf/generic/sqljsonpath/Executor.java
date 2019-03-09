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

import java.util.Map;

public class Executor extends SqlJsonPathBaseVisitor<ValueUnion> {

  protected String value;
  protected Map<String, String> passing;
  protected EmptyOrErrorBehavior onEmpty;
  protected EmptyOrErrorBehavior onError;
  protected Validator validator;
  protected ErrorListener errorListener;

  public Executor(ErrorListener errorListener) {
    this.errorListener = errorListener;

  }

  public ValueUnion execute(ParseTree tree, String value, Map<String, String> passing, Validator validator) {
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
  public ValueUnion execute(ParseTree tree, String value, Map<String, String> passing, EmptyOrErrorBehavior onEmpty,
                   EmptyOrErrorBehavior onError, Validator validator) {
    this.value = value;
    this.passing = passing;
    this.onEmpty = onEmpty;
    this.onError = onError;
    this.validator = validator;
    return visit(tree);
  }

  @Override
  public ValueUnion visitAdditive_expression(SqlJsonPathParser.Additive_expressionContext ctx) {
    if (ctx.getChildCount() == 1) return visit(ctx.getChild(0));
    assert ctx.getChildCount() == 3;
    ParseTree operator = ctx.getChild(1);
    ValueUnion val1 = visit(ctx.getChild(0));
    ValueUnion val2 = visit(ctx.getChild(2));
    switch (operator.getText()) {
      case "+": val1.add(val2); break;
      case "-": val1.subtract(val2); break;
      default: throw new RuntimeException("Programming error");
    }
    return val1;
  }

  @Override
  public ValueUnion visitMultiplicative_expression(SqlJsonPathParser.Multiplicative_expressionContext ctx) {
    if (ctx.getChildCount() == 1) return visit(ctx.getChild(0));
    assert ctx.getChildCount() == 3;
    ParseTree operator = ctx.getChild(1);
    ValueUnion val1 = visit(ctx.getChild(0));
    ValueUnion val2 = visit(ctx.getChild(2));
    switch (operator.getText()) {
      case "*": val1.multiply(val2); break;
      case "/": val1.divide(val2); break;
      case "%": val1.modulo(val2); break;
      default: throw new RuntimeException("Programming error");
    }
    return val1;
  }

  @Override
  public ValueUnion visitUnary_expression(SqlJsonPathParser.Unary_expressionContext ctx) {
    if (ctx.getChildCount() == 1) return visit(ctx.getChild(0));
    assert ctx.getChildCount() == 2;
    ParseTree operator = ctx.getChild(0);
    ValueUnion val = visit(ctx.getChild(1));
    switch (operator.getText()) {
      case "+": break;
      case "-": val.negate();
      default: throw new RuntimeException("Programming error");
    }
    return val;
  }

  @Override
  public ValueUnion visitPath_null_literal(SqlJsonPathParser.Path_null_literalContext ctx) {
    return ValueUnion.nullValue(errorListener);
  }

  @Override
  public ValueUnion visitPath_boolean_literal(SqlJsonPathParser.Path_boolean_literalContext ctx) {
    if (ctx.getText().equalsIgnoreCase("true")) return new ValueUnion(true, errorListener);
    else if (ctx.getText().equalsIgnoreCase("false")) return new ValueUnion(false, errorListener);
    else throw new RuntimeException("Programming error");
  }

  @Override
  public ValueUnion visitPath_integer_literal(SqlJsonPathParser.Path_integer_literalContext ctx) {
    return new ValueUnion(Long.valueOf(ctx.getText()), errorListener);
  }

  @Override
  public ValueUnion visitPath_decimal_literal(SqlJsonPathParser.Path_decimal_literalContext ctx) {
    return new ValueUnion(Double.valueOf(ctx.getText()), errorListener);
  }

  @Override
  public ValueUnion visitPath_string_literal(SqlJsonPathParser.Path_string_literalContext ctx) {
    String val = ctx.getText();
    return new ValueUnion(val.substring(1, val.length() - 1), errorListener);
  }
}
