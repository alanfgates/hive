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

import com.google.common.annotations.VisibleForTesting;
import org.antlr.v4.runtime.tree.ParseTree;
import org.apache.hadoop.hive.ql.udf.generic.SqlJsonPathBaseVisitor;
import org.apache.hadoop.hive.ql.udf.generic.SqlJsonPathParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PathExecutor extends SqlJsonPathBaseVisitor<JsonSequence> {

  private static final String START_SUBSCRIPT = "__json_start_subscript";
  private static final String END_SUBSCRIPT = "__json_end_subscript";
  private static final JsonSequence lastJsonSequence = new JsonSequence("last");

  private JsonSequence value;
  private Map<String, JsonSequence> passing;
  private ErrorListener errorListener;
  private JsonSequence matching;
  private Mode mode;

  // TODO I am assuming here that the passed in value is a single bit of JSON.  Is it valid to pass two
  // JSON objects in value?  Or would that need to be in an array?

  public PathExecutor(ErrorListener errorListener) {
    this.errorListener = errorListener;
  }

  /**
   * Execute a SQL/JSON Path statement against a particular bit of JSON
   * @param tree the parse tree for the SQL/JSON Path
   * @param value JSON value to execute the Path statement against
   * @param passing map of arguments defined in the parse tree
   * @return value of executing the Path statement against the value
   */
  public JsonSequence execute(ParseTree tree, JsonSequence value, Map<String, JsonSequence> passing) {
    this.value = value;
    this.passing = passing == null ? Collections.emptyMap() : passing;
    matching = null;
    mode = Mode.LAX;
    visit(tree);
    return matching;
  }

  @Override
  final public JsonSequence visitPath_mode_strict(SqlJsonPathParser.Path_mode_strictContext ctx) {
    mode = Mode.STRICT;
    return JsonSequence.emptyResult;
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

  @Override
  public JsonSequence visitPath_context_variable(SqlJsonPathParser.Path_context_variableContext ctx) {
    // Sets the matching as the root of the tree.
    matching = value;
    return null;
  }

  @Override
  public JsonSequence visitMember_accessor_id(SqlJsonPathParser.Member_accessor_idContext ctx) {
    matching = accessMember(ctx.getChild(1).getText());
    return null;
  }

  @Override
  public JsonSequence visitMember_accessor_string(SqlJsonPathParser.Member_accessor_stringContext ctx) {
    matching = accessMember(stripQuotes(ctx.getChild(1).getText()));
    return null;
  }

  @Override
  public JsonSequence visitWildcard_member_accessor(SqlJsonPathParser.Wildcard_member_accessorContext ctx) {
    if (matching != null && matching.isObject()) {
      // I think I'm supposed to return the entire object here
      return matching;
    }
    matching = JsonSequence.emptyResult;
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
          JsonSequence res = applySubscriptsToOneArray(matchingEntry.getValue(), subscripts);
          if (res != JsonSequence.emptyResult) {
            newMatches.asObject().put(matchingEntry.getKey(), res);
          }
        }
      }
      matching = newMatches;
    } else {
      matching = JsonSequence.emptyResult;
    }
    return null;
  }

  @Override
  public JsonSequence visitWildcard_array_accessor(SqlJsonPathParser.Wildcard_array_accessorContext ctx) {
    if (matching == null || matching.isNull()) return null;

    // It is legitimate here for matching to be either a list ($.name[*]) or an object ($.*[*]).  In the list
    // case we want to return a list.  In the object case, we want to return an object, with only the fields that
    // are a list.
    if (matching.isList()) {
      // NOP
    } else if (matching.isObject()) {
      JsonSequence newMatches = new JsonSequence(new HashMap<>());
      for (Map.Entry<String, JsonSequence> matchingEntry : matching.asObject().entrySet()) {
        if (matchingEntry.getValue().isList()) {
          newMatches.asObject().put(matchingEntry.getKey(), matchingEntry.getValue());
        }
      }
      matching = newMatches;
    } else {
      matching = JsonSequence.emptyResult;
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
      List<JsonSequence> list = new ArrayList<>();
      list.add(subscript);
      return new JsonSequence(list);
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

  @VisibleForTesting
  Mode getMode() {
    return mode;
  }

  @VisibleForTesting
  ErrorListener getErrorListener() {
    return errorListener;
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
      // if only one value was accessed unwrap it
      if (subscripts.asList().size() == 1 && (subscripts.asList().get(0) == lastJsonSequence || subscripts.asList().get(0).isLong())) {
        if (newList.asList().size() > 0) return newList.asList().get(0);
        else return JsonSequence.emptyResult;
      } else {
        return newList;
      }
    } catch (IOException e) {
      return JsonSequence.nullJsonSequence;
    }
  }

  private JsonSequence accessMember(String memberKey) {
    if (matching.isObject()) {
      return accessMemberInObject(matching, memberKey);
    } else if (matching.isList()) {
      JsonSequence newMatching = new JsonSequence(new ArrayList<>());
      for (JsonSequence element : matching.asList()) {
        if (element.isObject()) {
          JsonSequence newMember = accessMemberInObject(element, memberKey);
          if (newMember != JsonSequence.emptyResult) newMatching.asList().add(newMember);
        }
      }
      return newMatching;
    }
    return JsonSequence.emptyResult;

  }

  private JsonSequence accessMemberInObject(JsonSequence object, String memberKey) {
    Map<String, JsonSequence> m = object.asObject();
    JsonSequence next = m.get(memberKey);
    return next == null ? JsonSequence.emptyResult : next;
  }

  private String stripQuotes(String quotedStr) {
    return quotedStr.substring(1, quotedStr.length() - 1);
  }

}
