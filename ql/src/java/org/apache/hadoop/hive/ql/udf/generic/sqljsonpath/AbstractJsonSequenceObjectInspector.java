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

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

import java.util.function.Function;

public abstract class AbstractJsonSequenceObjectInspector implements ObjectInspector {

  protected JsonSequence asJsonSequence(Object o) {
    if (o == null) return null;
    if (!(o instanceof JsonSequence)) {
      throw new ClassCastException("Attempt to read object of type " + o.getClass().getName() + " as a JsonSequence");
    }
    return (JsonSequence)o;
  }

  protected <T> T asType(Function<JsonSequence, Boolean> is, Function<JsonSequence, T> as, Object o, String typeName) {
    if (o == null) return null;
    JsonSequence json = asJsonSequence(o);
    if (json.isNull()) return null;
    if (!is.apply(json)) {
      throw new ClassCastException("Attempt to read JsonSequence " + json.toString() + " as a " + typeName);
    }
    return as.apply(json);
  }

  public Object copyObject(Object o) {
    JsonSequence json = asJsonSequence(o);
    return new JsonSequence(json);
  }

}
