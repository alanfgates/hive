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
package org.apache.hive.streaming;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.avro.AvroSerdeUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardUnionObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Writable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.function.Function;

public class AvroWriter extends AbstractRecordWriter<GenericRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(AvroWriter.class);

  private final Schema avroSchema;    // Avro schema
  // I cannot use AvroSerDe because it requires AvroGenericRecordWritable.  And I can't use
  // AvroGenericRecordWritable because it depends on the schema being encoded with every record, which is
  // silly.  And in general records we pull off Kafka won't have the schema in every record.
  private ReadOnlyAvroSerDe serde;
  private Object encoded;
  private long length;

  private AvroWriter(Builder builder) {
    avroSchema = builder.avroSchema == null ? new Schema.Parser().parse(builder.avroSchemaStr) : builder.avroSchema;
    serde = new ReadOnlyAvroSerDe();
  }

  public static class Builder {
    private Schema avroSchema;    // Avro schema
    private String avroSchemaStr; // Avro schema as a string

    /**
     * Build with an Avro schema object.  Call this or {@link #withSchema(String)} but not both.
     * @param schema Avro schema
     * @return this pointer
     */
    public Builder withSchema(Schema schema) {
      this.avroSchema = schema;
      return this;
    }

    /**
     * Build with an Avro schema as a String.  Useful if you are reading the value from a property or a file.
     * Call this or {@link #withSchema(Schema)} but not both.
     * @param schema
     * @return
     */
    public Builder withSchema(String schema) {
      this.avroSchemaStr = schema;
      return this;
    }

    public AvroWriter build() {
      if (avroSchemaStr == null && avroSchema == null) {
        throw new IllegalStateException("You must provide an Avro schema, either as a string or an object");
      }

      if (avroSchemaStr != null && avroSchema != null) {
        throw new IllegalStateException("You have provided two schemas, provide either an object or a string");
      }

      return new AvroWriter(this);
    }
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  @Override
  public AbstractSerDe createSerde() throws SerializationError {
    return serde;
  }

  @Override
  protected void encode(GenericRecord record) throws SerializationError {
    encoded = serde.deserialize(record);
  }

  @Override
  protected Object getEncoded() {
    return encoded;
  }

  @Override
  protected long getRecordLength() {
    return length;
  }

  /**
   * This rather specialized SerDe builds a decoder to handle every row.  It does make the assumption that
   * every row passed to it has the same schema.
   */
  private class ReadOnlyAvroSerDe extends AbstractSerDe {
    private final ObjectInspectorAndDeserializer objectInspectorAndDeserializer = buildRecordObjectInspectorAndDeserializer(avroSchema);

    @Override
    public void initialize(@Nullable Configuration conf, Properties tbl) {
      // Not necessary, as only the outer class will be constructing it.
      throw new UnsupportedOperationException();
    }

    @Override
    public Class<? extends Writable> getSerializedClass() {
      // Not necessary, as this SerDe won't serialize
      throw new UnsupportedOperationException();
    }

    @Override
    public Writable serialize(Object obj, ObjectInspector objInspector) {
      // Not necessary, this SerDe is for deserialization only
      throw new UnsupportedOperationException();
    }

    @Override
    public SerDeStats getSerDeStats() {
      // Don't think this is necessary.
      throw new UnsupportedOperationException();
    }

    @Override
    public Object deserialize(Writable blob) {
      // Use deserialize(GenericRecord) instead
      throw new UnsupportedOperationException();
    }

    @Override
    public ObjectInspector getObjectInspector() {
      return objectInspectorAndDeserializer.oi;
    }

    private ObjectInspectorAndDeserializer buildColObjectInspectorAndDeserializer(Schema schema) {
      // Look through the nullable type union, as Hive assumes everything is nullable.

      switch (schema.getType()) {
        case ARRAY:
          return buildListObjectInspectorAndDeserializer(schema);

        case BOOLEAN:
          return new ObjectInspectorAndDeserializer(
              PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(PrimitiveObjectInspector.PrimitiveCategory.BOOLEAN),
              avroVal -> {
                length += 1;
                return avroVal;
              });

        case BYTES:
          return new ObjectInspectorAndDeserializer(
              PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(PrimitiveObjectInspector.PrimitiveCategory.BINARY),
              avroVal -> {
                ByteBuffer buf = (ByteBuffer)avroVal;
                buf.rewind();
                byte[] result = new byte[buf.limit()];
                buf.get(result);
                length += result.length;
                return result;
              });

        case DOUBLE:
          return new ObjectInspectorAndDeserializer(
              PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(PrimitiveObjectInspector.PrimitiveCategory.DOUBLE),
              eightByteNumericDeserializer);

        case ENUM:
          // Copied this from AvroDeserializer, not 100% sure it works.
          return new ObjectInspectorAndDeserializer(
              PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(PrimitiveObjectInspector.PrimitiveCategory.STRING),
              stringDeserializer);

        case FIXED:
          return new ObjectInspectorAndDeserializer(
              PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(PrimitiveObjectInspector.PrimitiveCategory.BINARY),
              avroVal -> {
                GenericData.Fixed fixed = (GenericData.Fixed)avroVal;
                length += fixed.bytes().length;
                return fixed.bytes();
              });

        case FLOAT:
          return new ObjectInspectorAndDeserializer(
              PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(PrimitiveObjectInspector.PrimitiveCategory.FLOAT),
              fourByteNumericDeserializer);

        case INT:
          return new ObjectInspectorAndDeserializer(
              PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(PrimitiveObjectInspector.PrimitiveCategory.INT),
              fourByteNumericDeserializer);

        case LONG:
          return new ObjectInspectorAndDeserializer(
              PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(PrimitiveObjectInspector.PrimitiveCategory.LONG),
              eightByteNumericDeserializer);

        case MAP:
          return buildMapObjectInspectorAndDeserializer(schema);

        case NULL:
          throw new UnsupportedOperationException("Null type only supported as part of nullable struct");

        case RECORD:
          return buildRecordObjectInspectorAndDeserializer(schema);

        case STRING:
          return new ObjectInspectorAndDeserializer(PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(
              PrimitiveObjectInspector.PrimitiveCategory.STRING), stringDeserializer);

        case UNION:
          return buildUnionObjectInspectorAndDeserializer(schema);

        default:      throw new RuntimeException("Unknown Avro type: " + schema.getName());
      }
    }

    private ObjectInspectorAndDeserializer buildListObjectInspectorAndDeserializer(final Schema schema) {
      ObjectInspectorAndDeserializer elementObjectInspectorAndDeserializer =
          buildColObjectInspectorAndDeserializer(schema.getElementType());
      Function<Object, Object> listDeserializer = (o) -> {
        List<Object> avroList = (List)o;
        List<Object> hiveList = new ArrayList<>(avroList.size());
        for (Object avroVal : avroList) {
          hiveList.add(elementObjectInspectorAndDeserializer.deserializer.apply(avroVal));
        }
        return hiveList;
      };
      return new ObjectInspectorAndDeserializer(
          ObjectInspectorFactory.getStandardListObjectInspector(elementObjectInspectorAndDeserializer.oi), listDeserializer);

    }

    private ObjectInspectorAndDeserializer buildMapObjectInspectorAndDeserializer(final Schema schema) {
      final ObjectInspectorAndDeserializer valObjectInspectorAndDeserializer = buildColObjectInspectorAndDeserializer(schema.getValueType());
      ObjectInspector oi = ObjectInspectorFactory.getStandardMapObjectInspector(
          PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(PrimitiveObjectInspector.PrimitiveCategory.STRING),
          valObjectInspectorAndDeserializer.oi);
      Function<Object, Object> deserializer = (o) -> {
        Map<CharSequence, Object> avroMap = (Map)o;
        Map<Object, Object> hiveMap = new HashMap<>(avroMap.size());
        for (Map.Entry<CharSequence, Object> e : avroMap.entrySet()) {
          hiveMap.put(stringDeserializer.apply(e.getKey().toString()),
              valObjectInspectorAndDeserializer.deserializer.apply(e.getValue()));
        }
        return hiveMap;
      };
      return new ObjectInspectorAndDeserializer(oi, deserializer);
    }

    private ObjectInspectorAndDeserializer buildRecordObjectInspectorAndDeserializer(final Schema schema) {
      final int size = schema.getFields().size();
      List<String> colNames = new ArrayList<>(size);
      List<ObjectInspector> ois = new ArrayList<>(size);
      final List<Function<Object, Object>> deserializers = new ArrayList<>(size);
      for (Schema.Field field : schema.getFields()) {
        colNames.add(field.name());
        ObjectInspectorAndDeserializer objectInspectorAndDeserializer = buildColObjectInspectorAndDeserializer(field.schema());
        ois.add(objectInspectorAndDeserializer.oi);
        deserializers.add(objectInspectorAndDeserializer.deserializer);
      }
      return new ObjectInspectorAndDeserializer(ObjectInspectorFactory.getStandardStructObjectInspector(colNames, ois),
          (o) -> {
            GenericData.Record record = (GenericData.Record)o;
            assert record != null;
            List<Object> row = new ArrayList<>(size);
            for (int i = 0; i < size; i++) {
              row.add(deserializers.get(i).apply(record.get(i)));
            }
            return row;
          });
    }

    private ObjectInspectorAndDeserializer buildUnionObjectInspectorAndDeserializer(final Schema schema) {
      // If this is just a union to make a type nullable, don't model it as a union, model it as the type.
      if (AvroSerdeUtils.isNullableType(schema)) {
        LOG.debug("Encountered union of just nullable type, treating it as the type");
        return buildColObjectInspectorAndDeserializer(AvroSerdeUtils.getOtherTypeFromNullableType(schema));
      }
      List<ObjectInspector> ois = new ArrayList<>(schema.getTypes().size());
      List<Function<Object, Object>> deserializers = new ArrayList<>(schema.getTypes().size());
      for (Schema type : schema.getTypes()) {
        ObjectInspectorAndDeserializer objectInspectorAndDeserializer = buildColObjectInspectorAndDeserializer(type);
        ois.add(objectInspectorAndDeserializer.oi);
        deserializers.add(objectInspectorAndDeserializer.deserializer);

      }
      return new ObjectInspectorAndDeserializer(ObjectInspectorFactory.getStandardUnionObjectInspector(ois), (union) -> {
        int offset = GenericData.get().resolveUnion(schema, union);
        return new StandardUnionObjectInspector.StandardUnion((byte)offset,
            deserializers.get(offset).apply(union));
      });
    }

    private Object deserialize(GenericRecord record) throws SerializationError {
      length = 0;
      return objectInspectorAndDeserializer.deserializer.apply(record);
    }

  }

  private Function<Object, Object> stringDeserializer = new Function<Object, Object>() {
    @Override
    public Object apply(Object avroVal) {
      String s = avroVal.toString();
      length += s.length();
      return s;
    }
  };

  private Function<Object, Object> fourByteNumericDeserializer = new Function<Object, Object>() {
    @Override
    public Object apply(Object avroVal) {
      length += 4;
      return avroVal;
    }
  };

  private Function<Object, Object> eightByteNumericDeserializer = new Function<Object, Object>() {
    @Override
    public Object apply(Object avroVal) {
      length += 8;
      return avroVal;
    }
  };

  private static class ObjectInspectorAndDeserializer {
    final ObjectInspector oi;
    final Function<Object, Object> deserializer;

    public ObjectInspectorAndDeserializer(ObjectInspector oi, Function<Object, Object> deserializer) {
      this.oi = oi;
      this.deserializer = deserializer;
    }
  }
}
