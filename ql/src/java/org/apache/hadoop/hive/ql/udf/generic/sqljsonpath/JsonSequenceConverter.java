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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Given a JsonSequence and an ObjectInspector, convert the JsonSequence to the expected output type.   This is done
 * as a separate class so that we can keep state across multiple JsonSequences.
 */
public class JsonSequenceConverter {
  private static final Logger LOG = LoggerFactory.getLogger(JsonSequenceConverter.class);

  private final ObjectInspector outputObjectInspector;
  private final Map<String, ObjectInspector> objectInspectorCache;
  private final Map<String, ObjectInspectorConverters.Converter> converterCache;

  /**
   *
   * @param outputObjectInspector ObjectInspector to use to determine what form the writable should take.
   */
  public JsonSequenceConverter(ObjectInspector outputObjectInspector) {
    this.outputObjectInspector = outputObjectInspector;
    objectInspectorCache = new HashMap<>();
    converterCache = new HashMap<>();
  }

  /**
   * Convert the JsonSequence to something that can pass on to the rest of Hive.  The conversion will be based on
   * the object inspector.
   * @param json JsonSequence to be converted to an object that can be read by outputOI.
   * @return an object, not necessary a writable (may be a list or a map), or null.
   * @throws JsonConversionException if a bad conversion is attempted.
   */
  public Object convert(JsonSequence json) throws JsonConversionException {
    return convert(outputObjectInspector, json, true);
  }

  private Object convert(ObjectInspector outputOI, JsonSequence json, boolean useCache) throws JsonConversionException {
    if (json.isNull() || json.isEmpty()) return null;

    String cacheKey = buildCacheKey(json, outputOI);
    ObjectInspectorConverters.Converter converter = null;
    // Don't use the cache when we're converting nested objects.  Converters have a member object they always return,
    // so for example if you use one converter to convert all the elements of a list you'll end up with everything
    // in your list pointing to the same object, which will have the value of the last thing converted.  We could
    // still cache the converter and then copy the result, but this seems equivalent to not caching the converter.
    if (useCache) converter = converterCache.get(cacheKey);

    // Wrap the whole thing in a try because some of the converters throw RuntimeExceptions if you try a conversion
    // they don't support.  We don't want to blow up the execution with a RuntimeException
    try {
      if (converter == null) {
        ObjectInspector inputObjectInspector = getInputObjectInspector(json, outputOI);
        if (LOG.isDebugEnabled()) {
          LOG.debug("Using output ObjectInspector " +
              ObjectInspectorUtils.getObjectInspectorName(outputOI));
          LOG.debug("Using input ObjectInspector " + ObjectInspectorUtils.getObjectInspectorName(inputObjectInspector));
        }
        converter = ObjectInspectorConverters.getConverter(inputObjectInspector, outputOI);
        converterCache.put(cacheKey, converter);
      }

      switch (outputOI.getCategory()) {
        case STRUCT:
          if (json.isObject()) {
            StructObjectInspector soi = (StructObjectInspector) outputOI;
            List<Object> output = new ArrayList<>();
            for (StructField sf : soi.getAllStructFieldRefs()) {
              JsonSequence seq = json.asObject().get(sf.getFieldName());
              output.add(seq == null ? null : convert(sf.getFieldObjectInspector(), seq, false));
            }
            return converter.convert(output);
          }
          throw new JsonConversionException("Attempt to cast " + json.getType().name().toLowerCase() + " as object");

        case LIST:
          if (json.isList()) {
            ListObjectInspector loi = (ListObjectInspector) outputOI;
            List<Object> converted = new ArrayList<>();
            for (JsonSequence element : json.asList()) {
              converted.add(convert(loi.getListElementObjectInspector(), element, false));
            }
            return converter.convert(converted);
          }
          throw new JsonConversionException("Attempt to cast " + json.getType().name().toLowerCase() + " as list");

        case PRIMITIVE:
          return converter.convert(json.getVal());

        default:
          throw new RuntimeException("Programming error, unexpected category " + outputOI.getCategory());
      }
    } catch (Exception e) {
      throw new JsonConversionException("Failed conversion", e);
    }
  }

  private ObjectInspector getInputObjectInspector(JsonSequence json, ObjectInspector outputOI) {
    String cacheKey = buildCacheKey(json, outputOI);
    ObjectInspector cached = objectInspectorCache.get(cacheKey);
    if (cached != null) return cached;
    ObjectInspector inputOI;
    switch (json.getType()) {
      case OBJECT:
        if (outputOI.getCategory() == ObjectInspector.Category.STRUCT) {
          StructObjectInspector soi = (StructObjectInspector)outputOI;
          List<String> names = new ArrayList<>();
          List<ObjectInspector> fieldOIs = new ArrayList<>();
          for (StructField sf : soi.getAllStructFieldRefs()) {
            names.add(sf.getFieldName());
            ObjectInspector fieldInspector = translateOutputOI(sf.getFieldObjectInspector());
            fieldOIs.add(fieldInspector);
          }
          inputOI = ObjectInspectorFactory.getStandardStructObjectInspector(names, fieldOIs);
        } else {
          inputOI = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
        }
        break;


      case LIST:
        if (outputOI.getCategory() == ObjectInspector.Category.LIST) {
          ListObjectInspector loi = (ListObjectInspector) outputOI;
          inputOI = ObjectInspectorFactory.getStandardListObjectInspector(translateOutputOI(loi.getListElementObjectInspector()));
        } else {
          inputOI = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
        }
        break;

      case BOOL:
        inputOI = PrimitiveObjectInspectorFactory.javaBooleanObjectInspector;
        break;

      case LONG:
        inputOI = PrimitiveObjectInspectorFactory.javaLongObjectInspector;
        break;

      case DOUBLE:
        inputOI = PrimitiveObjectInspectorFactory.javaDoubleObjectInspector;
        break;

      case STRING:
      case NULL: // here to handle case where there's a null in a list or a struct
        inputOI = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
        break;

      default:
        throw new RuntimeException("Programming error, unexpected type " + json.getType().name());
    }
    objectInspectorCache.put(cacheKey, inputOI);
    return inputOI;
  }

  // Translate the output object inspector to the right type.  This is necessary to get rid of constant
  // OIs.
  private ObjectInspector translateOutputOI(ObjectInspector outputOI) {
    switch (outputOI.getCategory()) {
      case STRUCT:
        List<String> names = new ArrayList<>();
        List<ObjectInspector> inspectors = new ArrayList<>();
        StructObjectInspector soi = (StructObjectInspector)outputOI;
        for (StructField sf : soi.getAllStructFieldRefs()) {
          names.add(sf.getFieldName());
          inspectors.add(translateOutputOI(sf.getFieldObjectInspector()));
        }
        return ObjectInspectorFactory.getStandardStructObjectInspector(names, inspectors);

      case LIST:
        return ObjectInspectorFactory.getStandardListObjectInspector(translateOutputOI(((ListObjectInspector)outputOI).getListElementObjectInspector()));

      case PRIMITIVE:
        PrimitiveObjectInspector poi = (PrimitiveObjectInspector)outputOI;
        return PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(poi.getPrimitiveCategory());

      default:
        throw new RuntimeException("Programming error, unexpected category " + outputOI.getCategory().name());
    }
  }

  private String buildCacheKey(JsonSequence json, ObjectInspector outputOI) {
    return buildJsonTypeName(json) + outputOI.getClass().getName();
  }

  // This does not build a human readable type name.  The point here is to build a unique string that can be used
  // as a key in a map
  private String buildJsonTypeName(JsonSequence json) {
    switch (json.getType()) {
      case OBJECT:
        StringBuilder buf = new StringBuilder("O{");
        for (JsonSequence seq : json.asObject().values()) {
          buf.append(buildJsonTypeName(seq));
        }
        buf.append("}");
        return buf.toString();

      case LIST:
        return "L";

      case BOOL:
        return "b";

      case LONG:
        return "l";

      case DOUBLE:
        return "d";

      case NULL:
      case EMPTY_RESULT:
      case STRING:
        return "s";

      default:
        throw new RuntimeException("Programming error, unexpected type " + json.getType());
    }
  }
}
