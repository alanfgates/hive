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

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import java.util.Map;

public class PathExecutor extends SqlJsonPathBaseVisitor<JsonSequence> {

  protected JsonSequence value;
  protected Map<String, JsonSequence> passing;
  protected EmptyOrErrorBehavior onEmpty;
  protected EmptyOrErrorBehavior onError;
  protected PathValidator validator;
  protected ErrorListener errorListener;
  protected JsonSequence matching;
  protected Deque<List<Integer>> subscriptStack;

  public PathExecutor(ErrorListener errorListener) {
    this.errorListener = errorListener;
    subscriptStack = new ArrayDeque<>();
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
    matching = null;
    subscriptStack.clear();
    return visit(tree);
  }

  // The following (up until visitAccessor_expression) return scalar values we find in the tree.  These are not
  // part of the match

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
    return new JsonSequence(stripQuotes(val), errorListener);
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

  // IMPORTANT -- This is the method that returns the match.  Returns from other methods are just to pass things
  // up the tree
  @Override
  public JsonSequence visitAccessor_expression(SqlJsonPathParser.Accessor_expressionContext ctx) {
    visitChildren(ctx);
    if (matching != null) return matching;
    else return JsonSequence.nullValue(errorListener);
  }

  @Override
  public JsonSequence visitPath_context_variable(SqlJsonPathParser.Path_context_variableContext ctx) {
    matching = value;
    return null;
  }

  @Override
  public JsonSequence visitMember_accessor_id(SqlJsonPathParser.Member_accessor_idContext ctx) {
    if (matching != null && matching.isObject()) {
      Map<String, JsonSequence> m = matching.asObject();
      JsonSequence next = m.get(ctx.getChild(1).getText());
      if (next != null) {
        matching = next;
        return null;
      }
    }
    matching = JsonSequence.nullValue(errorListener);
    return null;
  }

  @Override
  public JsonSequence visitMember_accessor_string(SqlJsonPathParser.Member_accessor_stringContext ctx) {
    if (matching != null && matching.isObject()) {
      Map<String, JsonSequence> m = matching.asObject();
      JsonSequence next = m.get(stripQuotes(ctx.getChild(1).getText()));
      if (next != null) {
        matching = next;
        return null;
      }
    }
    matching = JsonSequence.nullValue(errorListener);
    return null;
  }

  @Override
  public JsonSequence visitWildcard_member_accessor(SqlJsonPathParser.Wildcard_member_accessorContext ctx) {
    if (matching != null && matching.isObject()) {
      Map<String, JsonSequence> m = matching.asObject();
      matching = new JsonSequence(new ArrayList<>(m.values()), errorListener);
      return matching;
    }
    matching = JsonSequence.nullValue(errorListener);
    return null;
  }

  @Override
  public JsonSequence visitArray_accessor(SqlJsonPathParser.Array_accessorContext ctx) {
    if (matching == null || matching.isNull()) return null;
    if (!matching.isList()) {
      // TODO - should this be an error in strict mode?
      matching = JsonSequence.nullValue(errorListener);
      return null;
    }
    subscriptStack.push(new ArrayList<>());
    visit(ctx.getChild(1));
    List<Integer> subscripts = subscriptStack.pop();
    List<JsonSequence> matches = new ArrayList<>();
    for (int subscript : subscripts) {
      // TODO is it an error in strict mode to go over the end of the array?
      if (matching.asList().size() > subscript) {
        matches.add(matching.asList().get(subscript));
      }
    }
    matching = new JsonSequence(matches, errorListener);
    return null;
  }

  @Override
  public JsonSequence visitSubscript_simple(SqlJsonPathParser.Subscript_simpleContext ctx) {
    try {
      int subscript = checkSubscript(visitChildren(ctx));
      assert subscriptStack.size() > 0;
      subscriptStack.peek().add(subscript);
    } catch (IOException e) {
      errorListener.runtimeError(e.getMessage());
    }
    return null;
  }

  @Override
  public JsonSequence visitSubscript_to(SqlJsonPathParser.Subscript_toContext ctx) {
    try {
      int startSub = checkSubscript(visit(ctx.getChild(0)));
      int endSub = checkSubscript(visit(ctx.getChild(2)));
      assert subscriptStack.size() > 0;
      for (int i = startSub; i <= endSub; i++) subscriptStack.peek().add(i);
    } catch (IOException e) {
      errorListener.runtimeError(e.getMessage());
    }
    return null;
  }

  // TODO I have a huge problem here.  The way I'm using matching won't work as soon as there's multiple
  // keys.  Something like $.*[3] will bork it.  Need to rethink this and make sure I'm handling it

  // Array subscript methods are expected to return the value of the subscript

  @Override
  public JsonSequence visitSubscript_last(SqlJsonPathParser.Subscript_lastContext ctx) {
    // If we're here an Array should current be in matching, just put in its last value
    assert matching != null && matching.isList();
    return new JsonSequence(matching.asList().size() - 1, errorListener);
  }

  private int checkSubscript(JsonSequence subscript) throws IOException {
    if (!subscript.isLong()) {
      throw new IOException("Subscripts must be integer values");
    }
    if (subscript.asLong() > Integer.MAX_VALUE) {
      throw new IOException("Subscripts cannot exceed " + Integer.MAX_VALUE);
    }
    return (int)subscript.asLong();
  }


  // TODO last, array should be in matching, just put in it's number
  private String stripQuotes(String quotedStr) {
    return quotedStr.substring(1, quotedStr.length() - 1);
  }

}
