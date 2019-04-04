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
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

public class StructJsonSequenceObjectInspector extends StructObjectInspector {
  private final Map<String, JsonSequenceStructField> fields;

  public static Builder builder() {
    return new Builder();
  }

  private StructJsonSequenceObjectInspector(List<JsonSequenceStructField> fields) {
    this.fields = new HashMap<>();
    for (JsonSequenceStructField field : fields) {
      this.fields.put(field.getFieldName(), field);
    }
  }

  @Override
  public List<? extends StructField> getAllStructFieldRefs() {
    return new ArrayList<>(fields.values());
  }

  @Override
  public StructField getStructFieldRef(String fieldName) {
    return fields.get(fieldName);
  }

  @Override
  public Object getStructFieldData(Object data, StructField fieldRef) {
    Map<String, JsonSequence> map = asMap(data);
    if (map == null) return null;
    JsonSequenceStructField sf = fieldRef instanceof JsonSequenceStructField ? (JsonSequenceStructField)fieldRef
        : fields.get(fieldRef.getFieldName());
    JsonSequence json = map.get(sf.getFieldName());
    return json == null ? json : sf.getFieldResolver().apply(json);
  }

  @Override
  public List<Object> getStructFieldsDataAsList(Object data) {
    Map<String, JsonSequence> map = asMap(data);
    if (map == null) return null;
    List<Object> list = new ArrayList<>();
    for (Map.Entry<String, JsonSequence> entry : map.entrySet()) {
      JsonSequenceStructField sf = fields.get(entry.getKey());
      // It's possible that the JsonSequence contains fields that this object inspector doesn't know about.   That
      // isn't a problem, just don't include those fields in the returned list.
      if (sf != null) list.add(sf.getFieldResolver().apply(entry.getValue()));
    }
    return list;
  }

  @Override
  public String getTypeName() {
    return ObjectInspectorUtils.getStandardStructTypeName(this);
  }

  @Override
  public Category getCategory() {
    return Category.STRUCT;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    StructJsonSequenceObjectInspector that = (StructJsonSequenceObjectInspector) o;
    return Objects.equals(fields, that.fields);
  }

  @Override
  public int hashCode() {
    return Objects.hash(fields);
  }

  private Map<String, JsonSequence> asMap(Object o) {
    if (o == null) return null;
    if (!(o instanceof Map)) return null;
    return (Map)o;
  }

  private static class JsonSequenceStructField implements StructField {
    private final String fieldName;
    private final ObjectInspector fieldObjectInspector;
    private final int fieldId;
    private final Function<JsonSequence, Object> fieldResolver;

    public JsonSequenceStructField(String fieldName, ObjectInspector fieldObjectInspector, int fieldId,
                                   Function<JsonSequence, Object> fieldResolver) {
      this.fieldName = fieldName;
      this.fieldObjectInspector = fieldObjectInspector;
      this.fieldId = fieldId;
      this.fieldResolver = fieldResolver;
    }

    @Override
    public String getFieldName() {
      return fieldName;
    }

    @Override
    public ObjectInspector getFieldObjectInspector() {
      return fieldObjectInspector;
    }

    @Override
    public int getFieldID() {
      return fieldId;
    }

    @Override
    public String getFieldComment() {
      return null;
    }

    public Function<JsonSequence, Object> getFieldResolver() {
      return fieldResolver;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      JsonSequenceStructField that = (JsonSequenceStructField) o;
      return fieldId == that.fieldId &&
          Objects.equals(fieldName, that.fieldName) &&
          Objects.equals(fieldObjectInspector, that.fieldObjectInspector);
    }

    @Override
    public int hashCode() {
      return Objects.hash(fieldName, fieldObjectInspector, fieldId);
    }
  }

  public static class Builder {
    private List<JsonSequenceStructField> builderFields = new ArrayList<>();

    /**
     * Add a field to this ObjectInspector.
     * @param name name of the field
     * @param oi object inspector for the field
     * @param resolver resolver to turn a JsonSequence into a Java Object.
     */
    public void addField(String name, ObjectInspector oi, Function<JsonSequence, Object> resolver) {
      builderFields.add(new JsonSequenceStructField(name, oi, builderFields.size(), resolver));
    }

    public StructJsonSequenceObjectInspector build() {
      return new StructJsonSequenceObjectInspector(builderFields);
    }
  }

}
