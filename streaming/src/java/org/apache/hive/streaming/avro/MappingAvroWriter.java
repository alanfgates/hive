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
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hive.streaming.AbstractRecordWriter;
import org.apache.hive.streaming.SerializationError;
import org.apache.hive.streaming.StreamingConnection;
import org.apache.hive.streaming.StreamingException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * A writer for Avro that accepts a map describing how Avro columns should be translated to Hive columns.
 * This allows ingesting Avro records into Hive tables that do not exactly match the Avro schema.  It is
 * assumed that every Avro record has all the columns referenced in the mapping.
 *
 * <p>If you are writing into a partitioned table, you must provide columns for each partition column
 * for each record.</p>
 *
 * <p>The access into Avro can be into elements of a complex type.  For example, an Avro record that had
 * a field <i>a</i> could be mapped to a Hive top level column <i>a</i> if the rest of the record is not
 * of interest.  For unions created for nullable types, there is no need to specify the union in the map,
 * this will "see through it" to the non-nullable type.  The syntax for specifying elements of complex
 * types is:
 * <ul>
 *   <li>maps: <i>colname.mapkey</i> </li>
 *   <li>records:  <i>colname.subcolname</i></li>
 *   <li>lists:  <i>colname.element_number</i></li>
 *   <li>unions: <i>colname.offset_number</i></li>
 * </ul>
 * <p>At this time writing in to non-top level columns in Hive (ie, into a field of a record) is not supported.</p>
 */
public class MappingAvroWriter extends AbstractRecordWriter<GenericRecord> {
  private final AvroWriterAvroSerde serde;
  private final Map<String, String> schemaMapping;
  private Object encoded;
  private long length;
  private List<FieldSchema> hiveSchema;

  private MappingAvroWriter(Builder builder) {
    Schema avroSchema = builder.avroSchema == null ? new Schema.Parser().parse(builder.avroSchemaStr) : builder.avroSchema;
    serde = new MappingAvroWriterAvroSerDe(avroSchema);
    schemaMapping = builder.schemaMapping;
  }

  public static class Builder {
    private Schema avroSchema;    // Avro schema
    private String avroSchemaStr; // Avro schema as a string
    private Map<String, String> schemaMapping;

    /**
     * Build with an Avro schema object.  Call this or {@link #withAvroSchema(String)} but not both.
     * @param schema Avro schema
     * @return this pointer
     */
    public Builder withAvroSchema(Schema schema) {
      this.avroSchema = schema;
      return this;
    }

    /**
     * Build with an Avro schema as a String.  Useful if you are reading the value from a property or a file.
     * Call this or {@link #withAvroSchema(Schema)} but not both.
     * @param schema Avro schema as a string
     * @return this pointer
     */
    public Builder withAvroSchema(String schema) {
      this.avroSchemaStr = schema;
      return this;
    }

    /**
     * Set the mapping of the Hive schema to the Avro schema.  The keys are Hive column names.
     * The values are Avro column names (or a path to an element of a complex type).
     * See class level comments on syntax for accessing elements of Avro complex types.
     * @param mapping schema mapping
     * @return this pointer
     */
    public Builder withSchemaMapping(Map<String, String> mapping) {
      if (schemaMapping != null) schemaMapping = new HashMap<>();
      for (Map.Entry<String, String> mapEntry : mapping.entrySet()) {
        schemaMapping.put(mapEntry.getKey().toLowerCase(), mapEntry.getValue());
      }
      return this;
    }

    /**
     * Add a column to the mapping of the Avro schema to the Hive schema.
     * @param hiveCol Hive column name.
     * @param avroCol Avro column name, or path to element of Avro complex type
     * @return
     */
    public Builder addMappingColumn(String hiveCol, String avroCol) {
      if (schemaMapping == null) schemaMapping = new HashMap<>();
      schemaMapping.put(hiveCol.toLowerCase(), avroCol);
      return this;
    }

    public MappingAvroWriter build() {
      if (avroSchemaStr == null && avroSchema == null) {
        throw new IllegalStateException("You must provide an Avro schema, either as a string or an object");
      }

      if (avroSchemaStr != null && avroSchema != null) {
        throw new IllegalStateException("You have provided two schemas, provide either an object or a string");
      }

      return new MappingAvroWriter(this);
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
  public void init(StreamingConnection conn, long minWriteId, long maxWriteId, int statementId) throws StreamingException {
    // Override this so we can set up our translator properly, as it needs the Hive schema, which is only available once
    // the RecordWriter has been embedded in a StreamingConection.
    super.init(conn, minWriteId, maxWriteId, statementId);
    hiveSchema = conn.getTable().getAllCols();
    serde.buildTranslator();
  }

  @Override
  protected void encode(GenericRecord record) throws SerializationError {
    try {
      encoded = serde.deserialize(record);
    } catch (ArrayIndexOutOfBoundsException e) {
      // This generally means the schema is screwed up
      throw new SerializationError("Unable to serialize record, likely due to record schema not matching schema " +
          "passed to writer", e);
    }
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
  private class MappingAvroWriterAvroSerDe extends AvroWriterAvroSerde {

    public MappingAvroWriterAvroSerDe(Schema avroSchema) {
      super(avroSchema);
      // Can't build the translator here, because it needs the table data, and the table info won't be
      // set until initialize has been called.
    }



    @Override
    protected void buildTranslator() throws SerializationError {
      //return Translators.buildRecordTranslatorInfo(schema);
      final int size = schemaMapping.size();
      List<String> colNames = new ArrayList<>(size);
      List<ObjectInspector> ois = new ArrayList<>(size);
      // a map of Hive field name to (avro field name, deserializer)
      final Map<String, MappableTranslatorInfo> deserializers = new HashMap<>(size);
      for (FieldSchema fs : hiveSchema) {
        colNames.add(fs.getName());
        String avroCol = schemaMapping.get(fs.getName().toLowerCase());
        if (avroCol == null) {
          // This wasn't in the map, so just set it to null
          ObjectInspector oi = PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(PrimitiveObjectInspector.PrimitiveCategory.STRING);
          ois.add(oi);
          deserializers.put(fs.getName(), new MappableTranslatorInfo(new TranslatorInfo(oi, o -> null), fs.getName()));
          // TODO should I be setting this to a default value?
        }
        MappableTranslatorInfo mtInfo = mapAvroColumn(avroCol, avroSchema);
        ois.add(mtInfo.getObjectInspector());
        deserializers.put(fs.getName(), mtInfo);
      }

      tInfo = new TranslatorInfo(ObjectInspectorFactory.getStandardStructObjectInspector(colNames, ois),
          o -> {
            GenericData.Record record = (GenericData.Record)o;
            assert record != null;
            List<Object> row = new ArrayList<>(size);
            long length = 0;
            for (FieldSchema fs : hiveSchema) {
              MappableTranslatorInfo mtInfo = deserializers.get(fs.getName());
              DeserializerOutput output = mtInfo.getDeserializer().apply(record.get(mtInfo.getTopLevelColumn()));
              length += output.getAddedLength();
              row.add(output.getDeserialized());
            }
            return new DeserializerOutput(length, row);
          });
    }

    protected Object deserialize(GenericRecord record) throws SerializationError {
      assert tInfo != null;
      DeserializerOutput output = tInfo.getDeserializer().apply(record);
      length = output.getAddedLength();
      return output.getDeserialized();
    }

    private MappableTranslatorInfo mapAvroColumn(String avroCol, Schema schema) throws SerializationError {
      // TODO - this is all wrong, I need to write a path parser for this.
      Schema.Field field = schema.getField(avroCol);
      if (field == null) throw new SerializationError("Mapping to non-existent avro column " + avroCol);
      /*
      int dotAt = avroCol.indexOf('.');
      // Handle the map case
      if (dotAt > 0) {
        if (field.schema().getType() != Schema.Type.RECORD) {
          throw new SerializationError("Attempt to dereference sub-column in non-record type: " + avroCol);
        }
        String topLevelCol = avroCol.substring(0, dotAt);
        String remainder = avroCol.substring(dotAt + 1);
        MappableTranslatorInfo remainderTInfo;
        Function<Object, DeserializerOutput> deserializer;
        switch (field.schema().getType()) {
          case RECORD:
            remainderTInfo = mapAvroColumn(remainder, field.schema());
            deserializer = obj -> {
              GenericData.Record record = (GenericData.Record)obj;
              return remainderTInfo.getDeserializer().apply(record.get(remainderTInfo.getTopLevelColumn()));
            };
            break;

          case MAP:
            remainderTInfo = mapAvroColumn()

          case ARRAY:

          case UNION:


          default:
            throw new SerializationError("Attempt to use index on non-array/map/union type " + avroCol);
        }
        return new MappableTranslatorInfo(new TranslatorInfo(remainderTInfo.getObjectInspector(), deserializer), topLevelCol);
      } else {
      */
        return new MappableTranslatorInfo(Translators.buildColTranslatorInfo(field.schema()), avroCol);
      //}
    }
  }

  private static class MappableTranslatorInfo {
    private final String topLevelColumn;
    private final TranslatorInfo tInfo;

    MappableTranslatorInfo(TranslatorInfo tInfo, String topLevelColumn) {
      this.topLevelColumn = topLevelColumn;
      this.tInfo = tInfo;
    }

    String getTopLevelColumn() {
      return topLevelColumn;
    }

    ObjectInspector getObjectInspector() {
      return tInfo.getObjectInspector();
    }

    Function<Object, DeserializerOutput> getDeserializer() {
      return tInfo.getDeserializer();
    }
  }

}
