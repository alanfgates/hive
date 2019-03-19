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
import org.apache.hadoop.hive.ql.udf.generic.JsonBaseVisitor;
import org.apache.hadoop.hive.ql.udf.generic.JsonLexer;
import org.apache.hadoop.hive.ql.udf.generic.JsonParser;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JsonValueParser extends JsonBaseVisitor<JsonSequence> {

  private ErrorListener errorListener;
  private Deque<List<JsonSequence>> arrayStack;
  private Deque<Map<String, JsonSequence>> objStack;

  public JsonValueParser(ErrorListener errorListener) {
    this.errorListener = errorListener;
    arrayStack = new ArrayDeque<>();
    objStack = new ArrayDeque<>();
  }

  public JsonSequence parse(String jsonStr) throws IOException, JsonPathException {
    clear();
    JsonLexer scanner = new JsonLexer(new ANTLRInputStream(new ByteArrayInputStream(jsonStr.getBytes())));
    CommonTokenStream tokens = new CommonTokenStream(scanner);
    JsonParser parser = new JsonParser(tokens);
    parser.addErrorListener(errorListener);
    ParseTree tree = parser.object();
    errorListener.checkForErrors(jsonStr);
    JsonSequence jsonVal = visit(tree);
    errorListener.checkForErrors(jsonStr);
    return jsonVal;
  }

  @Override
  public JsonSequence visitObject(org.apache.hadoop.hive.ql.udf.generic.JsonParser.ObjectContext ctx) {
    objStack.push(new HashMap<>());
    visitChildren(ctx);
    Map<String, JsonSequence> obj = objStack.pop();
    return new JsonSequence(obj);
  }

  @Override
  public JsonSequence visitElement(org.apache.hadoop.hive.ql.udf.generic.JsonParser.ElementContext ctx) {
    JsonSequence element = visit(ctx.getChild(2));
    String key = ctx.getChild(0).getText();
    assert objStack.size() > 0;
    objStack.peek().put(key.substring(1, key.length() - 1), element);
    return JsonSequence.emptyResult;
  }

  @Override
  public JsonSequence visitArray(org.apache.hadoop.hive.ql.udf.generic.JsonParser.ArrayContext ctx) {
    arrayStack.push(new ArrayList<>());
    visitChildren(ctx);
    List<JsonSequence> array = arrayStack.pop();
    return new JsonSequence(array);
  }

  @Override
  public JsonSequence visitArray_element(org.apache.hadoop.hive.ql.udf.generic.JsonParser.Array_elementContext ctx) {
    JsonSequence element = visit(ctx.getChild(0));
    assert arrayStack.size() > 0;
    arrayStack.peek().add(element);
    return JsonSequence.emptyResult;
  }

  @Override
  public JsonSequence visitNull_literal(org.apache.hadoop.hive.ql.udf.generic.JsonParser.Null_literalContext ctx) {
    return JsonSequence.nullJsonSequence;
  }

  @Override
  public JsonSequence visitBoolean_literal(org.apache.hadoop.hive.ql.udf.generic.JsonParser.Boolean_literalContext ctx) {
    if (ctx.getText().equalsIgnoreCase("true")) return JsonSequence.trueJsonSequence;
    else if (ctx.getText().equalsIgnoreCase("false")) return JsonSequence.falseJsonSequence;
    else throw new RuntimeException("Programming error");
  }

  @Override
  public JsonSequence visitInt_literal(org.apache.hadoop.hive.ql.udf.generic.JsonParser.Int_literalContext ctx) {
    return new JsonSequence(Long.valueOf(ctx.getText()));
  }

  @Override
  public JsonSequence visitDecimal_literal(org.apache.hadoop.hive.ql.udf.generic.JsonParser.Decimal_literalContext ctx) {
    return new JsonSequence(Double.valueOf(ctx.getText()));
  }

  @Override
  public JsonSequence visitString_literal(org.apache.hadoop.hive.ql.udf.generic.JsonParser.String_literalContext ctx) {
    String val = ctx.getText();
    return new JsonSequence(val.substring(1, val.length() - 1));
  }

  private void clear() {
    errorListener.clear();
    arrayStack.clear();
    objStack.clear();
  }
}
