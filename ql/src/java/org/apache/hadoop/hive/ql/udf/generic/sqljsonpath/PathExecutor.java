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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class PathExecutor extends SqlJsonPathBaseVisitor<JsonSequence> {

  private static final String START_SUBSCRIPT = "__json_start_subscript";
  private static final String END_SUBSCRIPT = "__json_end_subscript";

  protected JsonSequence value;
  protected Map<String, JsonSequence> passing;
  protected EmptyOrErrorBehavior onEmpty;
  protected EmptyOrErrorBehavior onError;
  protected PathValidator validator;
  protected ErrorListener errorListener;
  protected JsonSequence matching;

  private JsonSequence lastJsonSequence = new JsonSequence("last");

  // TODO I am assuming here that the passed in value is a single bit of JSON.  Is it valid to pass two
  // JSON objects in value?  Or would that need to be in an array?

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
    matching = null;
    visit(tree);
    return matching;
  }

  // Visit methods return JSON sequences.  However, these are not the current match.  These are used for building
  // up values via arithmetic, etc.  The match we have seen so far is built up in the matching member.

  @Override
  public JsonSequence visitAdditive_expression(SqlJsonPathParser.Additive_expressionContext ctx) {
    if (ctx.getChildCount() == 1) return visit(ctx.getChild(0));
    assert ctx.getChildCount() == 3;
    ParseTree operator = ctx.getChild(1);
    JsonSequence val1 = visit(ctx.getChild(0));
    JsonSequence val2 = visit(ctx.getChild(2));
    switch (operator.getText()) {
      case "+": val1.add(val2, errorListener); break;
      case "-": val1.subtract(val2, errorListener); break;
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
      case "*": val1.multiply(val2, errorListener); break;
      case "/": val1.divide(val2, errorListener); break;
      case "%": val1.modulo(val2, errorListener); break;
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
      case "-": val.negate(errorListener); break;
      default: throw new RuntimeException("Programming error");
    }
    return val;
  }

  @Override
  public JsonSequence visitPath_null_literal(SqlJsonPathParser.Path_null_literalContext ctx) {
    return JsonSequence.nullJsonSequence;
  }

  @Override
  public JsonSequence visitPath_boolean_literal(SqlJsonPathParser.Path_boolean_literalContext ctx) {
    if (ctx.getText().equalsIgnoreCase("true")) return JsonSequence.trueJsonSequence;
    else if (ctx.getText().equalsIgnoreCase("false")) return JsonSequence.falseJsonSequence;
    else throw new RuntimeException("Programming error");
  }

  @Override
  public JsonSequence visitPath_integer_literal(SqlJsonPathParser.Path_integer_literalContext ctx) {
    return new JsonSequence(Long.valueOf(ctx.getText()));
  }

  @Override
  public JsonSequence visitPath_decimal_literal(SqlJsonPathParser.Path_decimal_literalContext ctx) {
    return new JsonSequence(Double.valueOf(ctx.getText()));
  }

  @Override
  public JsonSequence visitPath_string_literal(SqlJsonPathParser.Path_string_literalContext ctx) {
    String val = ctx.getText();
    return new JsonSequence(stripQuotes(val));
  }

  @Override
  public JsonSequence visitPath_named_variable(SqlJsonPathParser.Path_named_variableContext ctx) {
    String id = ctx.getChild(1).getText();
    JsonSequence val = passing.get(id);
    if (val == null) {
      errorListener.semanticError("Variable " + id +
          " referenced in path expression but no matching id found in passing clause");
      return JsonSequence.nullJsonSequence;
    } else {
      return val;
    }
  }

  /*
  // IMPORTANT -- This is the method that returns the match.  Returns from other methods are just to pass things
  // up the tree
  @Override
  public JsonSequence visitAccessor_expression(SqlJsonPathParser.Accessor_expressionContext ctx) {
    visitChildren(ctx);
    if (matching != null) return matching;
    else return JsonSequence.nullJsonSequence;
  }
  */

  @Override
  public JsonSequence visitPath_context_variable(SqlJsonPathParser.Path_context_variableContext ctx) {
    // Sets the matching as the root of the tree.
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
    matching = JsonSequence.nullJsonSequence;
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
    matching = JsonSequence.nullJsonSequence;
    return null;
  }

  @Override
  public JsonSequence visitWildcard_member_accessor(SqlJsonPathParser.Wildcard_member_accessorContext ctx) {
    if (matching != null && matching.isObject()) {
      // I think I'm supposed to return the entire object here
      return matching;
    }
    matching = JsonSequence.nullJsonSequence;
    return null;
  }

  @Override
  public JsonSequence visitArray_accessor(SqlJsonPathParser.Array_accessorContext ctx) {
    if (matching == null || matching.isNull()) return null;

    JsonSequence subscripts = visit(ctx.getChild(1));
    assert subscripts.isList();

    // It is legitimate here for matching to be either a list ($.name[3]) or an object ($.*[3]).  In the list
    // case we want to return a list.  In the object case, we want to return an object, with only the fields that
    // are a list.
    if (matching.isList()) {
      matching = applySubscriptsToOneArray(matching, subscripts);
    } else if (matching.isObject()) {
      JsonSequence newMatches = new JsonSequence(new HashMap<>());
      for (Map.Entry<String, JsonSequence> matchingEntry : matching.asObject().entrySet()) {
        if (matchingEntry.getValue().isList()) {
          newMatches.asObject().put(matchingEntry.getKey(), applySubscriptsToOneArray(matchingEntry.getValue(), subscripts));
        }
      }
      matching = newMatches;
    } else {
      matching = JsonSequence.nullJsonSequence;
    }
    return null;
  }

  /**
   *
   * @param ctx
   * @return a list of subscripts.  One or more of these could be objects with start and end value which
   * represent the use 'to'
   */
  @Override
  public JsonSequence visitSubscript_list(SqlJsonPathParser.Subscript_listContext ctx) {
    if (ctx.getChildCount() == 1) {
      // This is the simple subscript case, but it might still be a 'x TO y'
      JsonSequence subscript = visit(ctx.getChild(0));
      return new JsonSequence(Collections.singletonList(subscript));
    } else if (ctx.getChildCount() == 3) {
      JsonSequence subscriptList = visit(ctx.getChild(0));
      JsonSequence subscript = visit(ctx.getChild(2));
      subscriptList.asList().add(subscript);
      return subscriptList;
    } else {
      throw new RuntimeException("Programming error");
    }
  }

  /**
   *
   * @param ctx
   * @return a Json object with start and end tokens
   */
  @Override
  public JsonSequence visitSubscript_to(SqlJsonPathParser.Subscript_toContext ctx) {
    JsonSequence startSeq = visit(ctx.getChild(0));
    JsonSequence endSeq = visit(ctx.getChild(2));
    if (endSeq != lastJsonSequence && endSeq.asLong() < startSeq.asLong()) {
      errorListener.runtimeError("The end subscript must be greater than or equal to the start subscript");
      return JsonSequence.nullJsonSequence;
    }
    Map<String, JsonSequence> subscripts = new HashMap<>();
    subscripts.put(START_SUBSCRIPT, startSeq);
    subscripts.put(END_SUBSCRIPT, endSeq);
    return new JsonSequence(subscripts);
  }

  @Override
  public JsonSequence visitSubscript_last(SqlJsonPathParser.Subscript_lastContext ctx) {
    return lastJsonSequence;
  }

  private int checkSubscript(JsonSequence subscript) throws IOException {
    if (!subscript.isLong()) {
      errorListener.semanticError("Subscripts must be integer values");
      throw new IOException();
    }
    if (subscript.asLong() > Integer.MAX_VALUE) {
      errorListener.runtimeError("Subscripts cannot exceed " + Integer.MAX_VALUE);
      throw new IOException();
    }
    return (int)subscript.asLong();
  }

  private JsonSequence applySubscriptsToOneArray(JsonSequence oneList, JsonSequence subscripts) {
    try {
      JsonSequence newList = new JsonSequence(new ArrayList<>());
      for (JsonSequence sub : subscripts.asList()) {
        // This might be a number, or it might be an object with a start and end subscript, or it might be 'last'.
        if (sub == lastJsonSequence) { // Use of == intentional here
          newList.asList().add(oneList.asList().get(oneList.asList().size() - 1));
        } else if (sub.isLong()) {
          if (sub.asLong() < oneList.asList().size()) { // make sure we don't fly off the end
            newList.asList().add(oneList.asList().get(checkSubscript(sub)));
          }
        } else if (sub.isObject()) {
          assert sub.asObject().containsKey(START_SUBSCRIPT);
          int start = checkSubscript(sub.asObject().get(START_SUBSCRIPT));
          assert sub.asObject().containsKey(END_SUBSCRIPT);
          JsonSequence endSubscript = sub.asObject().get(END_SUBSCRIPT);
          int end = (endSubscript == lastJsonSequence) ? end = oneList.asList().size() - 1 : checkSubscript(endSubscript);
          // visitSubscript_to already checked that start <= end
          for (int i = start; i <= end; i++) {
            if (i >= oneList.asList().size()) break; // Don't fly off the end
            newList.asList().add(oneList.asList().get(i));
          }
        } else {
          throw new RuntimeException("programming error");
        }
      }
      return newList;
    } catch (IOException e) {
      return JsonSequence.nullJsonSequence;
    }
  }


  // TODO last, array should be in matching, just put in it's number
  private String stripQuotes(String quotedStr) {
    return quotedStr.substring(1, quotedStr.length() - 1);
  }

}
