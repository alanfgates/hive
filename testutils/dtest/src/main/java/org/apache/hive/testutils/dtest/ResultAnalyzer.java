/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hive.testutils.dtest;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

class ResultAnalyzer {
  private int succeeded;
  private List<String> failed;
  private List<String> errors;
  private Pattern successLine;
  private Pattern errorLine;
  private Pattern unitTestError;
  private Pattern unitTestFailure;
  private Pattern qTestError;
  private Pattern qTestFailure;

  ResultAnalyzer() {
    succeeded = 0;
    failed = new ArrayList<>();
    errors = new ArrayList<>();
    successLine =
        Pattern.compile("\\[INFO\\] Tests run: ([0-9]+), Failures: ([0-9]+), Errors: ([0-9]+).*");
    errorLine =
        Pattern.compile("\\[ERROR\\] Tests run: ([0-9]+), Failures: ([0-9]+), Errors: ([0-9]+).*");
    unitTestError =
        Pattern.compile("\\[ERROR\\] ([A-Za-z0-9_]+).*\\.(Test[A-Za-z0-9_]+).*ERROR!");
    unitTestFailure =
        Pattern.compile("\\[ERROR\\] ([A-Za-z0-9_]+).*\\.(Test[A-Za-z0-9_]+).*FAILURE!");
    qTestFailure =
        Pattern.compile("\\[ERROR\\] testCliDriver\\[([A-Za-z0-9_]+)\\].*\\.(Test[A-Za-z0-9_]+).*FAILURE!");
    qTestError =
        Pattern.compile("\\[ERROR\\] testCliDriver\\[([A-Za-z0-9_]+)\\].*\\.(Test[A-Za-z0-9_]+).*ERROR!");
  }

  int getSucceeded() {
    return succeeded;
  }

  List<String> getFailed() {
    return failed;
  }

  List<String> getErrors() {
    return errors;
  }

  void analyzeLogLine(String name, String line) {
    count(line, successLine);
    count(line, errorLine);
    if (name.contains("itests-qtest")) {
      findErrorsAndFailures(line, qTestError, qTestFailure);
    } else {
      findErrorsAndFailures(line, unitTestError, unitTestFailure);
    }
  }

  private void count(String line, Pattern pattern) {
    Matcher m = pattern.matcher(line);
    if (m.matches()) {
      int total = Integer.parseInt(m.group(1));
      int failures = Integer.parseInt(m.group(2));
      int errors = Integer.parseInt(m.group(3));
      succeeded += total - failures - errors;
    }
  }

  private void findErrorsAndFailures(String line, Pattern error, Pattern failure) {
    Matcher errorLine = error.matcher(line);
    if (errorLine.matches()) {
      errors.add(errorLine.group(2) + "." + errorLine.group(1));
    } else {
      Matcher failureLine = failure.matcher(line);
      if (failureLine.matches()) {
        failed.add(failureLine.group(2) + "." + failureLine.group(1));
      }
    }
  }
}
