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
  public void empty() throws IOException, JsonPathException {
    JsonSequence json = parser.parse("{}");
    Assert.assertTrue(json.isObject());
    Assert.assertEquals(0, json.asObject().size());
  }

  @Test
  public void justString() throws IOException, JsonPathException {
    JsonSequence json = parser.parse("{ \"name\" : \"fred\" }");
    Assert.assertTrue(json.isObject());
    Assert.assertEquals(1, json.asObject().size());
    Assert.assertEquals(new JsonSequence(Collections.singletonMap("name", new JsonSequence("fred"))), json);
  }

  @Test
  public void justInt() throws IOException, JsonPathException {
    JsonSequence json = parser.parse("{ \"age\" : 10 }");
    Assert.assertTrue(json.isObject());
    Assert.assertEquals(1, json.asObject().size());
    Assert.assertEquals(new JsonSequence(Collections.singletonMap("age", new JsonSequence(10))), json);
  }

  @Test
  public void simple() throws IOException, JsonPathException {
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
    m.put("name", new JsonSequence("clark kent"));
    m.put("age", new JsonSequence(53L));
    m.put("gpa", new JsonSequence(3.97));
    m.put("honor roll", JsonSequence.trueJsonSequence);
    m.put("major", JsonSequence.nullJsonSequence);
    List<JsonSequence> l = new ArrayList<>();
    l.add(new JsonSequence("math 101"));
    l.add(new JsonSequence("history 101"));
    m.put("classes", new JsonSequence(l));
    m.put("sports", new JsonSequence(Collections.emptyList()));
    JsonSequence expected = new JsonSequence(m);

    Assert.assertEquals(expected, json);
  }

  @Test
  public void nested() throws IOException, JsonPathException {
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
    m.put("name", new JsonSequence("diana prince"));
    Map<String, JsonSequence> m1 = new HashMap<>();
    m1.put("street", new JsonSequence("123 amazon street"));
    m1.put("zip", new JsonSequence(12345L));
    m.put("address", new JsonSequence(m1));
    List<JsonSequence> l = new ArrayList<>();
    Map<String, JsonSequence> m2 = new HashMap<>();
    m2.put("class", new JsonSequence("math 101"));
    m2.put("professor", new JsonSequence("xavier"));
    l.add(new JsonSequence(m2));
    Map<String, JsonSequence> m3 = new HashMap<>();
    m3.put("class", new JsonSequence("history 101"));
    m3.put("professor", new JsonSequence("who"));
    l.add(new JsonSequence(m3));
    m.put("classes", new JsonSequence(l));
    JsonSequence expected = new JsonSequence(m);

    Assert.assertEquals(expected, json);
  }

  @Test
  public void syntaxError() throws IOException {
    try {
      parser.parse("{ \"oops\" }");
      Assert.fail();
    } catch (JsonPathException e) {
      Assert.assertEquals("'{ \"oops\" }' produced a syntax error: mismatched input '}' expecting ':' on line 1 at position 9", e.getMessage());
    }
  }
}
