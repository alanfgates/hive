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
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.tree.ParseTree;
import org.apache.hadoop.hive.ql.udf.generic.SqlJsonPathBaseVisitor;
import org.apache.hadoop.hive.ql.udf.generic.SqlJsonPathParser;

import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BinaryOperator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

/**
 * Evaluates SQL/JSON Path statements against a specific JSON value.  This is done by visiting the parse tree.  This
 * class is not reentrant and is only intended to be used by a single thread at a time.  However, it does not depend
 * on any static state and as many instances as desired can be created.
 * <p>
 *
 * The class is designed to be used repeatedly and thus resets all its state on each call to
 * {@link #execute(PathParseResult, JsonSequence, Map)}.
 * <p>
 *
 * The following digressions from the SQL Spec are noted:
 * <ol>
 *   <li>Values passed in to execute via the passing clause are not parsed to see if they are JSON.  A simple
 *   replace is done in the path expression.</li>
 * </ol>
 */
public class PathExecutor extends SqlJsonPathBaseVisitor<JsonSequence> {

  private static final String START_SUBSCRIPT = "__json_start_subscript";
  private static final String END_SUBSCRIPT = "__json_end_subscript";
  private static final JsonSequence lastJsonSequence = new JsonSequence("last");

  private enum PathElement { START, MEMBER_ACCESSOR, MEMBER_WILDCARD, SINGLE_SUBCRIPT, MULTI_SUBSCRIPT, METHOD }

  private JsonSequence value;
  private Map<String, JsonSequence> passing;
  private ErrorListener errorListener;
  // matching tracks the match we have made so far.  It is modified by many methods in the visitor.  During pre-filter
  // operations it tracks the matches from the path expression.  Once we are in the filter it tracks the match
  // expressions of the filter fragment currently being considered.
  private JsonSequence matching;
  // matchAtFilter caches the match that was made before the filter was considered.
  private JsonSequence matchAtFilter;
  private Mode mode;
  // Cache patterns that we compile as part of regular expression matching.  This gets reset each time we execute
  private Map<byte[], Pattern> regexPatterns;
  private MessageDigest md5;
  private PathElement previousElement;

  @VisibleForTesting
  JsonSequence returnedByVisit;

  // TODO Check against spec on 709 2 near top

  public PathExecutor() {
    regexPatterns = new HashMap<>();
    try {
      md5 = MessageDigest.getInstance("MD5");
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException("Programming error", e);
    }
  }

  /**
   * Execute a SQL/JSON Path statement against a particular bit of JSON
   * @param parseResult info from parsing the expression
   * @param value JSON value to execute the Path statement against.  If you want to pass a SQL NULL in this place
   *              pass a Java null.
   * @param passing map of arguments defined in the parse tree
   * @return value of executing the Path statement against the value
   * @throws JsonPathException if a semantic or runtime error occurs.
   */
  public JsonSequence execute(PathParseResult parseResult, JsonSequence value, Map<String, JsonSequence> passing) throws JsonPathException {
    // Per the spec, if a null value is passed in, the result is empty but successful (p 707, 11.b.i.1.A)
    if (value == null) return JsonSequence.emptyResult;
    this.value = value;
    this.passing = passing == null ? Collections.emptyMap() : passing;
    errorListener = parseResult.errorListener;
    errorListener.clear();
    matching = null;
    mode = Mode.STRICT;  // Strict is default, if the path expression specifies lax it will be set in the visit
    previousElement = PathElement.START;
    regexPatterns.clear();
    returnedByVisit = visit(parseResult.parseTree);
    errorListener.checkForErrors(parseResult.pathExpr);
    return matching;
  }

  @Override
  public JsonSequence visitPath_mode(SqlJsonPathParser.Path_modeContext ctx) {
    mode = Mode.valueOf(ctx.getText().toUpperCase());
    if (mode == Mode.LAX) {
      errorListener.semanticError("lax mode not supported", ctx);
    }
    return null;
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
      case "+": val1.add(val2, errorListener, ctx); break;
      case "-": val1.subtract(val2, errorListener, ctx); break;
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
      case "*": val1.multiply(val2, errorListener, ctx); break;
      case "/": val1.divide(val2, errorListener, ctx); break;
      case "%": val1.modulo(val2, errorListener, ctx); break;
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
      case "-": val.negate(errorListener, ctx); break;
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
          " referenced in path expression but no matching id found in passing clause", ctx);
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
  public JsonSequence visitPath_at_variable(SqlJsonPathParser.Path_at_variableContext ctx) {
    previousElement = PathElement.START;
    matching = matchAtFilter;
    return null;
  }

  @Override
  public JsonSequence visitMember_accessor_id(SqlJsonPathParser.Member_accessor_idContext ctx) {
    matching = accessMember(ctx.getChild(1).getText());
    previousElement = PathElement.MEMBER_ACCESSOR;
    return null;
  }

  @Override
  public JsonSequence visitMember_accessor_string(SqlJsonPathParser.Member_accessor_stringContext ctx) {
    matching = accessMember(stripQuotes(ctx.getChild(1).getText()));
    previousElement = PathElement.MEMBER_ACCESSOR;
    return null;
  }

  @Override
  public JsonSequence visitWildcard_member_accessor(SqlJsonPathParser.Wildcard_member_accessorContext ctx) {
    if (matching != null && matching.isObject()) {
      // I think I'm supposed to return the entire object here
      previousElement = PathElement.MEMBER_WILDCARD;
      return null;
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
      matching = applySubscriptsToOneArray(matching, subscripts, ctx);
    } else if (matching.isObject() && previousElement == PathElement.MEMBER_WILDCARD) {
      JsonSequence newMatches = new JsonSequence(new HashMap<>());
      for (Map.Entry<String, JsonSequence> matchingEntry : matching.asObject().entrySet()) {
        if (matchingEntry.getValue().isList()) {
          JsonSequence res = applySubscriptsToOneArray(matchingEntry.getValue(), subscripts, ctx);
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
    } else if (matching.isObject() && previousElement == PathElement.MEMBER_WILDCARD) {
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
    previousElement = PathElement.MULTI_SUBSCRIPT;
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
      errorListener.runtimeError("The end subscript must be greater than or equal to the start subscript", ctx);
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

  @Override
  public JsonSequence visitMethod_type(SqlJsonPathParser.Method_typeContext ctx) {
    // Note, differences from the SQL spec.  This does not try to guess if something is a datetime
    if (matching != JsonSequence.emptyResult) {
      switch (matching.getType()) {
        case LONG:
        case DOUBLE:
          matching = new JsonSequence("number");
          break;

        case NULL:
          matching = new JsonSequence("null"); // Note, this is a string with null, not the null value
          break;

        case STRING:
          matching = new JsonSequence("string");
          break;

        case BOOL:
          matching = new JsonSequence("boolean");
          break;

        case LIST:
          matching = new JsonSequence("array");
          break;

        case OBJECT:
          matching = new JsonSequence("object");
          break;

        default:
          throw new RuntimeException("Programming error");
      }
    }
    previousElement = PathElement.METHOD;
    return null;
  }

  @Override
  public JsonSequence visitMethod_size(SqlJsonPathParser.Method_sizeContext ctx) {
    if (matching != JsonSequence.emptyResult) {
      if (matching.isList()) {
        matching = new JsonSequence(matching.asList().size());
      } else if (matching.isObject()) {
        // NOTE: this is not to spec.  Per the spec everything but list should return 1, but asking the size
        // of the object seems like a reasonable thing to do.
        matching = new JsonSequence(matching.asObject().size());
      } else {
        matching = new JsonSequence(1);
      }
    }
    return null;
  }

  @Override
  public JsonSequence visitMethod_double(SqlJsonPathParser.Method_doubleContext ctx) {
    switch (matching.getType()) {
      case DOUBLE:
      case EMPTY_RESULT:
        break;

      case LONG:
        matching = new JsonSequence((double)matching.asLong());
        break;

      case STRING:
        matching = new JsonSequence(Double.valueOf(matching.asString()));
        break;

      default:
        errorListener.runtimeError("Double method requires numeric or string argument, passed a " + matching.getType().name().toLowerCase(), ctx);
        break;
    }
    return null;
  }

  @Override
  public JsonSequence visitMethod_int(SqlJsonPathParser.Method_intContext ctx) {
    switch (matching.getType()) {
      case LONG:
      case EMPTY_RESULT:
        break;

      case DOUBLE:
        matching = new JsonSequence((long)matching.asDouble());
        break;

      case STRING:
        matching = new JsonSequence(Long.valueOf(matching.asString()));
        break;

      default:
        errorListener.runtimeError("Integer method requires numeric or string argument, passed a " + matching.getType().name().toLowerCase(), ctx);
        break;
    }
    return null;
  }

  @Override
  public JsonSequence visitMethod_ceiling(SqlJsonPathParser.Method_ceilingContext ctx) {
    switch (matching.getType()) {
      case LONG:
      case EMPTY_RESULT:
        break;

      case DOUBLE:
        // Note, I am going against the standard here by doing a type conversion from double -> long
        matching = new JsonSequence((long)Math.ceil(matching.asDouble()));
        break;

      default:
        errorListener.runtimeError("Ceiling method requires numeric argument, passed a " + matching.getType().name().toLowerCase(), ctx);
        break;
    }
    return null;
  }

  @Override
  public JsonSequence visitMethod_floor(SqlJsonPathParser.Method_floorContext ctx) {
    switch (matching.getType()) {
      case LONG:
      case EMPTY_RESULT:
        break;

      case DOUBLE:
        // Note, I am going against the standard here by doing a type conversion from double -> long
        matching = new JsonSequence((long)Math.floor(matching.asDouble()));
        break;

      default:
        errorListener.runtimeError("Floor method requires numeric argument, passed a " + matching.getType().name().toLowerCase(), ctx);
        break;
    }
    return null;
  }

  @Override
  public JsonSequence visitMethod_abs(SqlJsonPathParser.Method_absContext ctx) {
    switch (matching.getType()) {
      case EMPTY_RESULT:
        break;

      case LONG:
        matching = new JsonSequence(Math.abs(matching.asLong()));
        break;

      case DOUBLE:
        matching = new JsonSequence(Math.abs(matching.asDouble()));
        break;

      default:
        errorListener.runtimeError("Abs method requires numeric argument, passed a " + matching.getType().name().toLowerCase(), ctx);
        break;
    }
    return null;
  }

  @Override
  public JsonSequence visitFilter_expression(SqlJsonPathParser.Filter_expressionContext ctx) {
    if (matching != JsonSequence.emptyResult) {
      if (matching.isList()) {
        // If this is a list, we need to apply the filter to each element in turn and built up a matching list
        JsonSequence matchingList = new JsonSequence(new ArrayList<>());
        for (JsonSequence matchingElement : matching.asList()) {
          matchAtFilter = matchingElement;
          JsonSequence eval = visit(ctx.getChild(2));
          if (eval.asBool()) matchingList.asList().add(matchingElement);
        }
        matching = matchingList.asList().size() > 0 ? matchingList : JsonSequence.emptyResult;
      } else {
        // Cache the match we've seen so far.
        matchAtFilter = matching;
        JsonSequence eval = visit(ctx.getChild(2));
        assert eval.isBool();
        matching = eval.asBool() ? matchAtFilter : JsonSequence.emptyResult;
      }
    }
    return null;
  }

  @Override
  public JsonSequence visitBoolean_disjunction(SqlJsonPathParser.Boolean_disjunctionContext ctx) {
    if (ctx.getChildCount() == 1) {
      return visit(ctx.getChild(0));
    } else {
      JsonSequence left = visit(ctx.getChild(0));
      assert left.isBool();
      if (left.asBool()) return JsonSequence.trueJsonSequence;
      JsonSequence right = visit(ctx.getChild(2));
      assert right.isBool();
      return right;
    }
  }

  @Override
  public JsonSequence visitBoolean_conjunction(SqlJsonPathParser.Boolean_conjunctionContext ctx) {
    if (ctx.getChildCount() == 1) {
      return visit(ctx.getChild(0));
    } else {
      JsonSequence left = visit(ctx.getChild(0));
      assert left.isBool();
      if (!left.asBool()) return JsonSequence.falseJsonSequence;
      JsonSequence right = visit(ctx.getChild(2));
      assert right.isBool();
      return right;
    }
  }

  @Override
  public JsonSequence visitBoolean_negation(SqlJsonPathParser.Boolean_negationContext ctx) {
    if (ctx.getChildCount() == 1) {
      return visit(ctx.getChild(0));
    } else {
      JsonSequence val = visit(ctx.getChild(1));
      assert val.isBool();
      return val.asBool() ? JsonSequence.falseJsonSequence : JsonSequence.trueJsonSequence;
    }
  }

  @Override
  public JsonSequence visitDelimited_predicate(SqlJsonPathParser.Delimited_predicateContext ctx) {
    if (ctx.getChildCount() == 1) {
      return visit(ctx.getChild(0));
    } else {
      return visit(ctx.getChild(1));
    }
  }

  @Override
  public JsonSequence visitExists_path_predicate(SqlJsonPathParser.Exists_path_predicateContext ctx) {
    JsonSequence val = visit(ctx.getChild(2));
    return matching != JsonSequence.emptyResult ? JsonSequence.trueJsonSequence : JsonSequence.falseJsonSequence;
  }

  // In all these comparison predicates the results can come to us in one of two ways.  The node may be a constant
  // which means we'll get the answer back as a result of visiting the node.  It may also be a path match, which
  // means we'll get it by looking in matching.  So look at the result of the visit first, and if it's null, then
  // look at matching.  If both are null throw up our hands.

  @Override
  public JsonSequence visitComparison_predicate_equals(SqlJsonPathParser.Comparison_predicate_equalsContext ctx) {
    return binaryComparisonOperator(ctx, (lf, rt) -> lf.equalsOp(rt, errorListener, ctx));
  }

  @Override
  public JsonSequence visitComparison_predicate_not_equals(SqlJsonPathParser.Comparison_predicate_not_equalsContext ctx) {
    return binaryComparisonOperator(ctx, (lf, rt) -> lf.notEqualsOp(rt, errorListener, ctx));
  }

  @Override
  public JsonSequence visitComparison_predicate_greater_than(SqlJsonPathParser.Comparison_predicate_greater_thanContext ctx) {
    return binaryComparisonOperator(ctx, (lf, rt) -> lf.greaterThanOp(rt, errorListener, ctx));
  }

  @Override
  public JsonSequence visitComparison_predicate_greater_than_equals(SqlJsonPathParser.Comparison_predicate_greater_than_equalsContext ctx) {
    return binaryComparisonOperator(ctx, (lf, rt) -> lf.greaterThanEqualOp(rt, errorListener, ctx));
  }

  @Override
  public JsonSequence visitComparison_predicate_less_than(SqlJsonPathParser.Comparison_predicate_less_thanContext ctx) {
    return binaryComparisonOperator(ctx, (lf, rt) -> lf.lessThanOp(rt, errorListener, ctx));
  }

  @Override
  public JsonSequence visitComparison_predicate_less_than_equals(SqlJsonPathParser.Comparison_predicate_less_than_equalsContext ctx) {
    return binaryComparisonOperator(ctx, (lf, rt) -> lf.lessThanEqualOp(rt, errorListener, ctx));
  }

  @Override
  public JsonSequence visitLike_regex_predicate(SqlJsonPathParser.Like_regex_predicateContext ctx) {
    visit(ctx.getChild(0));
    // The left side should always be the path, and right a constant
    JsonSequence left = matching;
    JsonSequence right = visit(ctx.getChild(2));
    if (!left.isString()) {
      errorListener.semanticError("Regular expressions can only be used on strings", ctx);
      return JsonSequence.falseJsonSequence;
    }
    assert right.isString();

    md5.reset();
    md5.update(right.asString().getBytes());
    byte[] key = md5.digest();
    Pattern pattern = regexPatterns.get(key);
    if (pattern == null) {
      try {
        pattern = Pattern.compile(right.asString());
        regexPatterns.put(key, pattern);
      } catch (PatternSyntaxException e) {
        errorListener.semanticError("Regular expression syntax error " + e.getMessage(), ctx);
        return JsonSequence.falseJsonSequence;
      }
    }
    Matcher m = pattern.matcher(left.asString());
    return m.find(0) ? JsonSequence.trueJsonSequence : JsonSequence.falseJsonSequence;
  }

  @Override
  public JsonSequence visitStarts_with_predicate(SqlJsonPathParser.Starts_with_predicateContext ctx) {
    visit(ctx.getChild(0));
    // The left side should always be the path, and right a constant or passed in variable
    JsonSequence left = matching;
    JsonSequence right = visit(ctx.getChild(3));

    if (!left.isString() || !right.isString()) {
      errorListener.semanticError("Starts with can only be used with strings", ctx);
      return JsonSequence.falseJsonSequence;
    }

    return left.asString().startsWith(right.asString()) ? JsonSequence.trueJsonSequence : JsonSequence.falseJsonSequence;
  }

  @VisibleForTesting
  Mode getMode() {
    return mode;
  }

  private int checkSubscript(JsonSequence subscript, ParserRuleContext ctx) throws IOException {
    if (!subscript.isLong()) {
      errorListener.semanticError("Subscripts must be integer values", ctx);
      throw new IOException();
    }
    if (subscript.asLong() > Integer.MAX_VALUE) {
      errorListener.runtimeError("Subscripts cannot exceed " + Integer.MAX_VALUE, ctx);
      throw new IOException();
    }
    return (int)subscript.asLong();
  }

  private JsonSequence applySubscriptsToOneArray(JsonSequence oneList, JsonSequence subscripts, ParserRuleContext ctx) {
    try {
      JsonSequence newList = new JsonSequence(new ArrayList<>());
      for (JsonSequence sub : subscripts.asList()) {
        // This might be a number, or it might be an object with a start and end subscript, or it might be 'last'.
        if (sub == lastJsonSequence) { // Use of == intentional here
          newList.asList().add(oneList.asList().get(oneList.asList().size() - 1));
        } else if (sub.isLong()) {
          if (sub.asLong() < oneList.asList().size()) { // make sure we don't fly off the end
            newList.asList().add(oneList.asList().get(checkSubscript(sub, ctx)));
          }
        } else if (sub.isObject()) {
          assert sub.asObject().containsKey(START_SUBSCRIPT);
          int start = checkSubscript(sub.asObject().get(START_SUBSCRIPT), ctx);
          assert sub.asObject().containsKey(END_SUBSCRIPT);
          JsonSequence endSubscript = sub.asObject().get(END_SUBSCRIPT);
          int end = (endSubscript == lastJsonSequence) ? end = oneList.asList().size() - 1 : checkSubscript(endSubscript, ctx);
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
        if (newList.asList().size() > 0) {
          previousElement = PathElement.SINGLE_SUBCRIPT;
          return newList.asList().get(0);
        }
        else return JsonSequence.emptyResult;
      } else {
        previousElement = PathElement.MULTI_SUBSCRIPT;
        return newList;
      }
    } catch (IOException e) {
      return JsonSequence.nullJsonSequence;
    }
  }

  private JsonSequence accessMember(String memberKey) {
    if (matching.isObject()) {
      return accessMemberInObject(matching, memberKey);
    } else if (matching.isList() && (previousElement == PathElement.MULTI_SUBSCRIPT)) {
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

  private JsonSequence binaryComparisonOperator(ParserRuleContext ctx, BinaryOperator<JsonSequence> comparisonOp) {
    JsonSequence left = visit(ctx.getChild(0));
    left = left == null ? matching : left;
    JsonSequence right = visit(ctx.getChild(2));
    right = right == null ? matching : right;
    return comparisonOp.apply(left, right);
  }
}
