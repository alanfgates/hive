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

public class ListJsonSequenceObjectInspector extends AbstractJsonSequenceObjectInspector implements ListObjectInspector {
  private final AbstractJsonSequenceObjectInspector elementOI;

  public ListJsonSequenceObjectInspector(AbstractJsonSequenceObjectInspector elementObjectInspector) {
    this.elementOI = elementObjectInspector;
  }

  @Override
  public ObjectInspector getListElementObjectInspector() {
    return elementOI;
  }

  @Override
  public Object getListElement(Object data, int index) {
    List<JsonSequence> list = asList(data);
    return list == null || index >= list.size() ? null : list.get(index);
  }

  @Override
  public int getListLength(Object data) {
    List<JsonSequence> list = asList(data);
    return list == null ? 0 : list.size();
  }

  @Override
  public List<?> getList(Object data) {
    return asList(data);
  }

  @Override
  public String getTypeName() {
    return serdeConstants.LIST_TYPE_NAME;
  }

  @Override
  public Category getCategory() {
    return Category.LIST;
  }

  private List<JsonSequence> asList(Object o) {
    return asType(JsonSequence::isList, JsonSequence::asList, o, "list");
  }
}
