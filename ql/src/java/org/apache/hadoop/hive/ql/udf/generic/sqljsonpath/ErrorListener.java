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

import org.antlr.v4.runtime.ANTLRErrorListener;
import org.antlr.v4.runtime.Parser;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;
import org.antlr.v4.runtime.atn.ATNConfigSet;
import org.antlr.v4.runtime.dfa.DFA;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

public class ErrorListener implements ANTLRErrorListener  {
  private List<String> errors;

  public ErrorListener() {
    errors = new ArrayList<>();
  }

  /**
   * Reset this listener so there are no errors.
   */
  void clear() {
    errors.clear();
  }

  /**
   * Check to see if an error occurred.
   * @param expr expression that was parsed.  Not checked here, just used in the error message.
   * @throws JsonPathException if any errors were found.
   */
  void checkForErrors(String expr) throws JsonPathException {
    if (errors.size() > 0) throw new JsonPathException(expr, errors);
  }

  /**
   * Report a semantic error.
   * @param s error string.
   * @param ctx context at the point the error occurred.  This is passed rather than a string to prevent all the work
   *            of constructing the error string unless there is an error.
   */
  void semanticError(String s, ParserRuleContext ctx) {
    errors.add("semantic error: " + s + " at \"" + getErrorText(ctx) + "\"");
  }

  /**
   * Report a runtime error.
   * @param s error string.
   * @param ctx context at the point the error occurred.  This is passed rather than a string to prevent all the work
   *            of constructing the error string unless there is an error.
   */
  void runtimeError(String s, ParserRuleContext ctx) {
    errors.add("runtime error: " + s + " at \"" + getErrorText(ctx) + "\"");
  }

  @Override
  public void syntaxError(Recognizer<?, ?> recognizer, Object o, int line, int charpos, String s, RecognitionException e) {
    errors.add("syntax error: " + s + " on line " + line + " at position " + charpos);
  }

  @Override
  public void reportAmbiguity(Parser parser, DFA dfa, int i, int i1, boolean b, BitSet bitSet, ATNConfigSet atnConfigSet) {
    System.out.println("in reportAmbiguity");

  }

  @Override
  public void reportAttemptingFullContext(Parser parser, DFA dfa, int i, int i1, BitSet bitSet, ATNConfigSet atnConfigSet) {
    System.out.println("in reportAttemptingFullContext");

  }

  @Override
  public void reportContextSensitivity(Parser parser, DFA dfa, int i, int i1, int i2, ATNConfigSet atnConfigSet) {
    System.out.println("in reportContextSensitivity");

  }

  private String getErrorText(ParserRuleContext ctx) {
    // Stolen straight from ParserRuleContext except I put spaces in between tokens for readability.
    if (ctx.getChildCount() == 0) {
      return "";
    } else {
      StringBuilder builder = new StringBuilder();

      boolean first = true;
      for(int i = 0; i < ctx.getChildCount(); ++i) {
        if (first) first = false;
        else builder.append(" ");
        builder.append(ctx.getChild(i).getText());
      }

      return builder.toString();
    }

  }
}
