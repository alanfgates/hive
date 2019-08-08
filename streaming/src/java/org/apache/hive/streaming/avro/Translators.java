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
package org.apache.hive.streaming.avro;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.hive.serde2.avro.AvroSerdeUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardUnionObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

class Translators {
  private static final Logger LOG = LoggerFactory.getLogger(Translators.class);

  static TranslatorInfo buildColTranslatorInfo(Schema schema) {
    // Look through the nullable type union, as Hive assumes everything is nullable.

    switch (schema.getType()) {
      case ARRAY:
        return buildListTranslatorInfo(schema);

      case BOOLEAN:
        return new TranslatorInfo(
            PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(PrimitiveObjectInspector.PrimitiveCategory.BOOLEAN),
            avroVal -> new DeserializerOutput(1, avroVal));

      case BYTES:
        return new TranslatorInfo(
            PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(PrimitiveObjectInspector.PrimitiveCategory.BINARY),
            avroVal -> {
              ByteBuffer buf = (ByteBuffer)avroVal;
              buf.rewind();
              byte[] result = new byte[buf.limit()];
              buf.get(result);
              return new DeserializerOutput(result.length, result);
            });

      case DOUBLE:
        return new TranslatorInfo(
            PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(PrimitiveObjectInspector.PrimitiveCategory.DOUBLE),
            eightByteNumericDeserializer);

      case ENUM:
        // Copied this from AvroDeserializer, not 100% sure it works.
        return new TranslatorInfo(
            PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(PrimitiveObjectInspector.PrimitiveCategory.STRING),
            stringDeserializer);

      case FIXED:
        return new TranslatorInfo(
            PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(PrimitiveObjectInspector.PrimitiveCategory.BINARY),
            avroVal -> {
              GenericData.Fixed fixed = (GenericData.Fixed)avroVal;
              return new DeserializerOutput(fixed.bytes().length, fixed.bytes());
            });

      case FLOAT:
        return new TranslatorInfo(
            PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(PrimitiveObjectInspector.PrimitiveCategory.FLOAT),
            fourByteNumericDeserializer);

      case INT:
        return new TranslatorInfo(
            PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(PrimitiveObjectInspector.PrimitiveCategory.INT),
            fourByteNumericDeserializer);

      case LONG:
        return new TranslatorInfo(
            PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(PrimitiveObjectInspector.PrimitiveCategory.LONG),
            eightByteNumericDeserializer);

      case MAP:
        return buildMapTranslatorInfo(schema);

      case NULL:
        throw new UnsupportedOperationException("Null type only supported as part of nullable struct");

      case RECORD:
        return buildRecordTranslatorInfo(schema);

      case STRING:
        return new TranslatorInfo(PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(
            PrimitiveObjectInspector.PrimitiveCategory.STRING), stringDeserializer);

      case UNION:
        return buildUnionTranslatorInfo(schema);

      default:      throw new RuntimeException("Unknown Avro type: " + schema.getName());
    }
  }

  static TranslatorInfo buildListTranslatorInfo(final Schema schema) {
    TranslatorInfo elementTranslatorInfo =
        buildColTranslatorInfo(schema.getElementType());
    Function<Object, DeserializerOutput> listDeserializer = (o) -> {
      List<Object> avroList = (List)o;
      List<Object> hiveList = new ArrayList<>(avroList.size());
      long length = 0;
      for (Object avroVal : avroList) {
        DeserializerOutput output = elementTranslatorInfo.getDeserializer().apply(avroVal);
        hiveList.add(output.getDeserialized());
        length += output.getAddedLength();
      }
      return new DeserializerOutput(length, hiveList);
    };
    return new TranslatorInfo(
        ObjectInspectorFactory.getStandardListObjectInspector(elementTranslatorInfo.getObjectInspector()), listDeserializer);

  }

  static TranslatorInfo buildMapTranslatorInfo(final Schema schema) {
    final TranslatorInfo valTranslatorInfo = buildColTranslatorInfo(schema.getValueType());
    ObjectInspector oi = ObjectInspectorFactory.getStandardMapObjectInspector(
        PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(PrimitiveObjectInspector.PrimitiveCategory.STRING),
        valTranslatorInfo.getObjectInspector());
    Function<Object, DeserializerOutput> deserializer = (o) -> {
      Map<CharSequence, Object> avroMap = (Map)o;
      Map<Object, Object> hiveMap = new HashMap<>(avroMap.size());
      long length = 0;
      for (Map.Entry<CharSequence, Object> e : avroMap.entrySet()) {
        DeserializerOutput key = stringDeserializer.apply(e.getKey().toString());
        DeserializerOutput value = valTranslatorInfo.getDeserializer().apply(e.getValue());
        hiveMap.put(key.getDeserialized(), value.getDeserialized());
        length += key.getAddedLength() + value.getAddedLength();
      }
      return new DeserializerOutput(length, hiveMap);
    };
    return new TranslatorInfo(oi, deserializer);
  }

  static TranslatorInfo buildRecordTranslatorInfo(final Schema schema) {
    final int size = schema.getFields().size();
    List<String> colNames = new ArrayList<>(size);
    List<ObjectInspector> ois = new ArrayList<>(size);
    final List<Function<Object, DeserializerOutput>> deserializers = new ArrayList<>(size);
    for (Schema.Field field : schema.getFields()) {
      colNames.add(field.name());
      TranslatorInfo tInfo = buildColTranslatorInfo(field.schema());
      ois.add(tInfo.getObjectInspector());
      deserializers.add(tInfo.getDeserializer());
    }
    return new TranslatorInfo(ObjectInspectorFactory.getStandardStructObjectInspector(colNames, ois),
        (o) -> {
          GenericData.Record record = (GenericData.Record)o;
          assert record != null;
          List<Object> row = new ArrayList<>(size);
          long length = 0;
          for (int i = 0; i < size; i++) {
            DeserializerOutput output = deserializers.get(i).apply(record.get(i));
            length += output.getAddedLength();
            row.add(output.getDeserialized());
          }
          return new DeserializerOutput(length, row);
        });
  }

  static TranslatorInfo buildUnionTranslatorInfo(final Schema schema) {
    // If this is just a union to make a type nullable, don't model it as a union, model it as the type.
    if (AvroSerdeUtils.isNullableType(schema)) {
      LOG.debug("Encountered union of just nullable type, treating it as the type");
      return buildColTranslatorInfo(AvroSerdeUtils.getOtherTypeFromNullableType(schema));
    }
    List<ObjectInspector> ois = new ArrayList<>(schema.getTypes().size());
    List<Function<Object, DeserializerOutput>> deserializers = new ArrayList<>(schema.getTypes().size());
    for (Schema type : schema.getTypes()) {
      TranslatorInfo tInfo = buildColTranslatorInfo(type);
      ois.add(tInfo.getObjectInspector());
      deserializers.add(tInfo.getDeserializer());

    }
    return new TranslatorInfo(ObjectInspectorFactory.getStandardUnionObjectInspector(ois), (union) -> {
      int offset = GenericData.get().resolveUnion(schema, union);
      DeserializerOutput output = deserializers.get(offset).apply(union);
      return new DeserializerOutput(output.getAddedLength(),
          new StandardUnionObjectInspector.StandardUnion((byte)offset, output.getDeserialized()));
    });
  }

  private static Function<Object, DeserializerOutput> stringDeserializer = avroVal -> {
    String s = avroVal.toString();
    return new DeserializerOutput(s.length(), s);
  };

  private static Function<Object, DeserializerOutput> fourByteNumericDeserializer = avroVal -> new DeserializerOutput(4, avroVal);

  private static Function<Object, DeserializerOutput> eightByteNumericDeserializer = avroVal -> new DeserializerOutput(8, avroVal);
}
