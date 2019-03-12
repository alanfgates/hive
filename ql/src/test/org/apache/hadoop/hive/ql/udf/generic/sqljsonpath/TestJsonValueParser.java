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

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestJsonValueParser {

  private static JsonValueParser parser;
  private static ErrorListener errorListener;

  // Done once to test that re-using the parser works
  @BeforeClass
  public static void createParser() {
    errorListener = new ErrorListener();
    parser = new JsonValueParser(errorListener);
  }

  @Test
  public void empty() throws IOException, ParseException {
    JsonSequence json = parser.parse("{}");
    Assert.assertTrue(json.isObject());
    Assert.assertEquals(0, json.asObject().size());
  }

  @Test
  public void justString() throws IOException, ParseException {
    JsonSequence json = parser.parse("{ \"name\" : \"fred\" }");
    Assert.assertTrue(json.isObject());
    Assert.assertEquals(1, json.asObject().size());
    Assert.assertEquals(new JsonSequence(Collections.singletonMap("name", new JsonSequence("fred", errorListener)), errorListener), json);
  }

  @Test
  public void justInt() throws IOException, ParseException {
    JsonSequence json = parser.parse("{ \"age\" : 10 }");
    Assert.assertTrue(json.isObject());
    Assert.assertEquals(1, json.asObject().size());
    Assert.assertEquals(new JsonSequence(Collections.singletonMap("age", new JsonSequence(10, errorListener)), errorListener), json);
  }

  @Test
  public void simple() throws IOException, ParseException {
    JsonSequence json = parser.parse("{" +
        "\"name\"       : \"clark kent\"," +
        "\"age\"        :  53," +
        "\"gpa\"        :  3.97," +
        "\"honor roll\" : true," +
        "\"major\"      : null," +
        "\"classes\"    : [ \"math 101\", \"history 101\" ]," +
        "\"sports\"     : [ ]" +
        "}");

    Map<String, JsonSequence> m = new HashMap<>();
    m.put("name", new JsonSequence("clark kent", errorListener));
    m.put("age", new JsonSequence(53L, errorListener));
    m.put("gpa", new JsonSequence(3.97, errorListener));
    m.put("honor roll", new JsonSequence(true, errorListener));
    m.put("major", JsonSequence.nullValue(errorListener));
    List<JsonSequence> l = new ArrayList<>();
    l.add(new JsonSequence("math 101", errorListener));
    l.add(new JsonSequence("history 101", errorListener));
    m.put("classes", new JsonSequence(l, errorListener));
    m.put("sports", new JsonSequence(Collections.emptyList(), errorListener));
    JsonSequence expected = new JsonSequence(m, errorListener);

    Assert.assertEquals(expected, json);
  }

  @Test
  public void nested() throws IOException, ParseException {
    JsonSequence json = parser.parse("{" +
        "\"name\"    : \"diana prince\"," +
        "\"address\" : {" +
        "              \"street\" : \"123 amazon street\"," +
        "              \"zip\"    : 12345" +
        "              }," +
        "\"classes\" : [" +
        "                 {" +
        "                   \"class\"     : \"math 101\"," +
        "                   \"professor\" : \"xavier\"" +
        "                 }, {" +
        "                   \"class\"     : \"history 101\"," +
        "                   \"professor\" : \"who\"" +
        "                 }" +
        "              ]" +
        "}");

    Map<String, JsonSequence> m = new HashMap<>();
    m.put("name", new JsonSequence("diana prince", errorListener));
    Map<String, JsonSequence> m1 = new HashMap<>();
    m1.put("street", new JsonSequence("123 amazon street", errorListener));
    m1.put("zip", new JsonSequence(12345L, errorListener));
    m.put("address", new JsonSequence(m1, errorListener));
    List<JsonSequence> l = new ArrayList<>();
    Map<String, JsonSequence> m2 = new HashMap<>();
    m2.put("class", new JsonSequence("math 101", errorListener));
    m2.put("professor", new JsonSequence("xavier", errorListener));
    l.add(new JsonSequence(m2, errorListener));
    Map<String, JsonSequence> m3 = new HashMap<>();
    m3.put("class", new JsonSequence("history 101", errorListener));
    m3.put("professor", new JsonSequence("who", errorListener));
    l.add(new JsonSequence(m3, errorListener));
    m.put("classes", new JsonSequence(l, errorListener));
    JsonSequence expected = new JsonSequence(m, errorListener);

    Assert.assertEquals(expected, json);
  }

  @Test
  public void syntaxError() throws IOException {
    try {
      parser.parse("{ \"oops\" }");
      Assert.fail();
    } catch (ParseException e) {
      Assert.assertEquals("'{ \"oops\" }' produced a syntax error: mismatched input '}' expecting ':' on line 1 at position 9", e.getMessage());
    }
  }
}
