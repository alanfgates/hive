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

import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Given a JsonSequence and an ObjectInspector, convert the JsonSequence to the expected output type.   This is done
 * as a separate class so that we can keep state across multiple JsonSequences.
 */
public class JsonSequenceConverter {

  /**
   * Convert the JsonSequence to something that can pass on to the rest of Hive.  The conversion will be based on
   * the object inspector.
   * @param outputOI ObjectInspector to use to determine what form the writable should take.
   * @param errorOnBadCast whether to throw an error if the attempted cast fails.  For example, if the underlying type
   *                       is a boolean and the passed in ObjectInspector is a LongObjectInspector that's bad.  If
   *                       true, a {@link ClassCastException} will be thrown.  If false, null is returned.
   * @return an object, not necessary a writable (may be a list or a map), or null.
   */
  public Object convert(ObjectInspector outputOI, JsonSequence json, boolean errorOnBadCast) {
    if (json.isNull() || json.isEmpty()) return null;

    ObjectInspectorConverters.Converter converter;
    ObjectInspector inputOI = getInputObjectInspector(json);
    switch (outputOI.getCategory()) {
      case STRUCT:
        if (json.isObject()) {
          StructObjectInspector soi = (StructObjectInspector)outputOI;
          List<Object> output = new ArrayList<>();
          for (StructField sf : soi.getAllStructFieldRefs()) {
            JsonSequence seq = json.asObject().get(sf.getFieldName());
            output.add(seq == null ? null : convert(sf.getFieldObjectInspector(), seq, errorOnBadCast));
          }
          converter = ObjectInspectorConverters.getConverter(inputOI, soi);
          return converter.convert(output);
        }
        if (errorOnBadCast) throw new ClassCastException("Attempt to cast " + json.getType().name().toLowerCase() + " as object");
        else return null;

      case LIST:
        if (json.isList()) {
          ListObjectInspector loi = (ListObjectInspector)outputOI;
          converter = ObjectInspectorConverters.getConverter(inputOI, loi);
          return converter.convert(json.asList()
              .stream()
              .map(JsonSequence::getVal)
              .collect(Collectors.toList()));
        }
        if (errorOnBadCast) throw new ClassCastException("Attempt to cast " + json.getType().name().toLowerCase() + " as list");
        else return null;

      case PRIMITIVE:
        converter = ObjectInspectorConverters.getConverter(inputOI, outputOI);
        return converter.convert(json.getVal());

      default:
        throw new RuntimeException("Programming error, unexpected category " + outputOI.getCategory());
    }
  }

  private ObjectInspector getInputObjectInspector(JsonSequence json) {
    switch (json.getType()) {
      case OBJECT:
        List<String> names = new ArrayList<>();
        List<ObjectInspector> fieldOIs = new ArrayList<>();
        for (Map.Entry<String, JsonSequence> field : json.asObject().entrySet()) {
          names.add(field.getKey());
          fieldOIs.add(getInputObjectInspector(field.getValue()));
        }
        return ObjectInspectorFactory.getStandardStructObjectInspector(names, fieldOIs);

      case LIST:
        // We have to return a union of all object inspectors needed to read elements of this list.
        Set<ObjectInspectorSetWrapper> uniqueSubOIs = new HashSet<>();
        for (JsonSequence seq : json.asList()) {
          uniqueSubOIs.add(new ObjectInspectorSetWrapper(getInputObjectInspector(seq)));
        }
        ObjectInspector subInspector = uniqueSubOIs.size() == 1 ? uniqueSubOIs.iterator().next().get() :
            ObjectInspectorFactory.getStandardUnionObjectInspector(uniqueSubOIs.stream().map(ObjectInspectorSetWrapper::get).collect(Collectors.toList()));
        return ObjectInspectorFactory.getStandardListObjectInspector(subInspector);

      case BOOL:
        return PrimitiveObjectInspectorFactory.javaBooleanObjectInspector;

      case LONG:
        return PrimitiveObjectInspectorFactory.javaLongObjectInspector;

      case DOUBLE:
        return PrimitiveObjectInspectorFactory.javaDoubleObjectInspector;

      case STRING:
      case NULL: // here to handle case where there's a null in a list or a struct
        return PrimitiveObjectInspectorFactory.javaStringObjectInspector;

      default:
        throw new RuntimeException("Programming error, unexpected type " + json.getType().name());
    }

  }

  private static class ObjectInspectorSetWrapper {
    private final ObjectInspector wrapped;

    ObjectInspectorSetWrapper(ObjectInspector oi) {
      this.wrapped = oi;
    }

    ObjectInspector get() {
      return wrapped;
    }

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof ObjectInspectorSetWrapper)) return false;
      ObjectInspectorSetWrapper other = (ObjectInspectorSetWrapper)o;
      return ObjectInspectorUtils.compareTypes(wrapped, other.wrapped);
    }

    @Override
    public int hashCode() {
      return Objects.hash(wrapped.getTypeName());
    }
  }

}
