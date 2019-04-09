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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
  private static final Logger LOG = LoggerFactory.getLogger(JsonSequenceConverter.class);

  /**
   * Convert the JsonSequence to something that can pass on to the rest of Hive.  The conversion will be based on
   * the object inspector.
   * @param outputOI ObjectInspector to use to determine what form the writable should take.
   * @param json JsonSequence to be converted to an object that can be read by outputOI.
   * @param errorOnBadCast whether to throw an error if the attempted cast fails.  For example, if the underlying type
   *                       is a boolean and the passed in ObjectInspector is a LongObjectInspector that's bad.  If
   *                       true, a {@link ClassCastException} will be thrown.  If false, null is returned.
   * @return an object, not necessary a writable (may be a list or a map), or null.
   */
  public Object convert(ObjectInspector outputOI, JsonSequence json, boolean errorOnBadCast) {
    if (json.isNull() || json.isEmpty()) return null;

    ObjectInspectorConverters.Converter converter;
    ObjectInspector inputOI = getInputObjectInspector(json, outputOI);
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
          List<Object> converted = new ArrayList<>();
          for (JsonSequence element : json.asList()) {
            converted.add(convert(loi.getListElementObjectInspector(), element, errorOnBadCast));
          }
          return converter.convert(converted);
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

  private ObjectInspector getInputObjectInspector(JsonSequence json, ObjectInspector outputOI) {
    switch (json.getType()) {
      case OBJECT:
        if (outputOI.getCategory() == ObjectInspector.Category.STRUCT) {
          StructObjectInspector soi = (StructObjectInspector)outputOI;
          List<String> names = new ArrayList<>();
          List<ObjectInspector> fieldOIs = new ArrayList<>();
          for (Map.Entry<String, JsonSequence> field : json.asObject().entrySet()) {
            names.add(field.getKey());
            StructField sf = soi.getStructFieldRef(field.getKey());
            ObjectInspector fieldInspector = sf == null ? PrimitiveObjectInspectorFactory.javaStringObjectInspector :
                translate(sf.getFieldObjectInspector());
            fieldOIs.add(getInputObjectInspector(field.getValue(), fieldInspector));
          }
          return ObjectInspectorFactory.getStandardStructObjectInspector(names, fieldOIs);
        } else {
          return PrimitiveObjectInspectorFactory.javaStringObjectInspector;
        }

      case LIST:
        if (outputOI.getCategory() == ObjectInspector.Category.LIST) {
          ListObjectInspector loi = (ListObjectInspector) outputOI;
          return ObjectInspectorFactory.getStandardListObjectInspector(translate(loi.getListElementObjectInspector()));
        } else {
          return PrimitiveObjectInspectorFactory.javaStringObjectInspector;
        }

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

  private ObjectInspector translate(ObjectInspector outputOI) {
    switch (outputOI.getCategory()) {
      case STRUCT:
        List<String> names = new ArrayList<>();
        List<ObjectInspector> inspectors = new ArrayList<>();
        StructObjectInspector soi = (StructObjectInspector)outputOI;
        for (StructField sf : soi.getAllStructFieldRefs()) {
          names.add(sf.getFieldName());
          inspectors.add(translate(sf.getFieldObjectInspector()));
        }
        return ObjectInspectorFactory.getStandardStructObjectInspector(names, inspectors);

      case LIST:
        return ObjectInspectorFactory.getStandardListObjectInspector(translate(((ListObjectInspector)outputOI).getListElementObjectInspector()));

      case PRIMITIVE:
        PrimitiveObjectInspector poi = (PrimitiveObjectInspector)outputOI;
        return PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(poi.getPrimitiveCategory());

      default:
        throw new RuntimeException("Programming error, unexpected category " + outputOI.getCategory().name());
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
