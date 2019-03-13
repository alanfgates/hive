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
import org.apache.hadoop.hive.ql.udf.generic.SqlJsonPathParser;

import java.util.Collections;
import java.util.Map;

/**
 * The validator serves a couple of purposes.  First, it sets up things like the strict/lax mode so we don't have
 * to set it everytime through.  Second, it dry runs any operations that could cause semantic errors.  For example
 * it will try any addition operations it sees to make sure the types match.  This way we don't return the same
 * error for every row, but rather catch the error up front.
 *
 * It extends PathExecutor because in may cases it does the same thing.  It only overrides methods where it needs to
 * act differently (like supplying dummy values for variables).
 */
public class PathValidator extends PathExecutor {

  protected Mode mode;

  public PathValidator(ErrorListener errorListener) {
    super(errorListener);
    mode = Mode.LAX; // Set to default
  }

  public void validate(ParseTree tree, JsonSequence value, Map<String, JsonSequence> passing) {
    this.value = value;
    this.passing = passing == null ? Collections.emptyMap() : passing;
    visit(tree);
  }

  @Override
  final public JsonSequence visitPath_mode_strict(SqlJsonPathParser.Path_mode_strictContext ctx) {
    mode = Mode.STRICT;
    return JsonSequence.nullValue(errorListener);
  }

  Mode getMode() {
    return mode;
  }

}
