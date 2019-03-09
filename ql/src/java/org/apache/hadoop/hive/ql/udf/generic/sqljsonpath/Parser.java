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
import org.apache.hadoop.hive.ql.udf.generic.SqlJsonPathLexer;
import org.apache.hadoop.hive.ql.udf.generic.SqlJsonPathParser;

import java.io.ByteArrayInputStream;
import java.io.IOException;

public class Parser {

  private ParseTree tree;

  /**
   * Parse a path expression.
   * @param pathExpression the path expression to parse
   * @throws ParseException if the expression fails to parse
   * @throws IOException if Antlr fails to read the input stream (shouldn't really happen unless you call it with a null string).
   */
  public void parse(String pathExpression) throws ParseException, IOException {
    ErrorListener errorListener = new ErrorListener();
    SqlJsonPathLexer scanner = new SqlJsonPathLexer(new ANTLRInputStream(new ByteArrayInputStream(pathExpression.getBytes())));
    CommonTokenStream tokens = new CommonTokenStream(scanner);
    SqlJsonPathParser parser = new SqlJsonPathParser(tokens);
    parser.addErrorListener(errorListener);
    tree = parser.path_expression();
    errorListener.checkForErrors(pathExpression);

  }

  public ParseTree getTree() {
    return tree;
  }
}
