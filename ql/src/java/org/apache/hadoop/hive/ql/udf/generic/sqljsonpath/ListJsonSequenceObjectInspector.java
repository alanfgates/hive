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

import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

import java.util.List;
import java.util.function.Function;

public class ListJsonSequenceObjectInspector implements ListObjectInspector {
  private final ObjectInspector elementOI;
  private final Function<JsonSequence, Object> elementResolver;

  public ListJsonSequenceObjectInspector(ObjectInspector elementObjectInspector,
                                         Function<JsonSequence, Object> elementResolver) {
    this.elementOI = elementObjectInspector;
    this.elementResolver = elementResolver;
  }

  @Override
  public ObjectInspector getListElementObjectInspector() {
    return elementOI;
  }

  @Override
  public Object getListElement(Object data, int index) {
    List<JsonSequence> list = asList(data);
    if (list == null || index >= list.size()) return null;
    JsonSequence element = list.get(index);
    return elementResolver.apply(element);
  }

  @Override
  public int getListLength(Object data) {
    List<JsonSequence> list = asList(data);
    return list == null ? 0 : list.size();
  }

  @Override
  public List<?> getList(Object data) {
    // If this is a list of primitive data, we have to convert the JsonSequence's to Objects
    return asList(data);
  }

  @Override
  public String getTypeName() {
    return serdeConstants.LIST_TYPE_NAME + "<" + elementOI.getTypeName() + ">";
  }

  @Override
  public Category getCategory() {
    return Category.LIST;
  }

  private List<JsonSequence> asList(Object o) {
    if (o == null) return null;
    if (!(o instanceof JsonSequence)) return null;
    JsonSequence json = (JsonSequence)o;
    if (!json.isList()) return null;
    return json.asList();
  }
}
