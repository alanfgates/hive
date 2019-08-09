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
import org.apache.commons.lang3.StringUtils;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
 * types is a basic path type expression:
 * <ul>
 *   <li>maps: <i>colname[mapkey]</i> </li>
 *   <li>records:  <i>colname.subcolname</i></li>
 *   <li>lists:  <i>colname[element_number]</i></li>
 *   <li>unions: <i>colname[offset_number]</i></li>
 * </ul>
 * <p>This can be multiple level, so if there is a table T with column <i>a</i> which is a union where the 1st element
 *    is a record with a column <i>b</i> which is a map with value type integer, then you could map the the
 *    <i>total</i> key of that map to an integer column with <i>a[1].b[total]</i></p>
 *
 * <p>At this time writing in to non-top level columns in Hive (ie, into a field of a record) is not supported.</p>
 */
public class MappingAvroWriter extends AbstractRecordWriter<GenericRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(MappingAvroWriter.class);

  private final MappingAvroWriterAvroSerDe serde;
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
      if (schemaMapping == null) schemaMapping = new HashMap<>();
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

      if (schemaMapping == null) {
        throw new IllegalStateException("You have not provided a schema mapping");
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
    // the RecordWriter has been embedded in a StreamingConection.  However, this leads to an extremely intricate dance.
    // We have to build the translator before super.init is called because it fetches the ObjectInspector.
    // But if we get an exception while building the translator we still need to go ahead and call super.init so it
    // can set itself up properly, even though we're just going to tear it all down.  Otherwise it fails during the
    // connection close.
    StreamingException cachedException = null;
    try {
      hiveSchema = conn.getTable().getAllCols();
      serde.buildTranslator();
    } catch (SerializationError e) {
      cachedException = e;
      // This is a hack and a half, but I can't think of a better way around it atm.
      // Build a quick fake object inspector so the following init call doesn't blow up.  All the fields can be
      // string as this object inspector shouldn't be used for anything other than fetching the names of
      // partition columns
      List<String> colNames = new ArrayList<>(hiveSchema.size());
      List<ObjectInspector> ois = new ArrayList<>(hiveSchema.size());
      for (FieldSchema fs : hiveSchema) {
        colNames.add(fs.getName());
        ois.add(PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(PrimitiveObjectInspector.PrimitiveCategory.STRING));
      }
      serde.setFakeObjectInspector(ObjectInspectorFactory.getStandardStructObjectInspector(colNames, ois));
    }
    super.init(conn, minWriteId, maxWriteId, statementId);
    if (cachedException != null) throw cachedException;
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
      // Do a quick sanity to check to assure that all the Hive columns they specified actually exist
      // Have to make a copy of the key set to avoid screwing up the map.
        Set<String> hiveColsFromSchemaMapping = new HashSet<>(schemaMapping.keySet());
        for (FieldSchema fs : hiveSchema) hiveColsFromSchemaMapping.remove(fs.getName());
        if (hiveColsFromSchemaMapping.size() > 0) {
          throw new SerializationError("Unknown Hive columns " + StringUtils.join(hiveColsFromSchemaMapping, ", ") +
              " referenced in schema mapping");
        }
        final int size = schemaMapping.size();
      List<String> colNames = new ArrayList<>(size);
      List<ObjectInspector> ois = new ArrayList<>(size);
      // a map of Hive field name to (avro field name, deserializer)
      final Map<String, MappingTranslatorInfo> deserializers = new HashMap<>(size);
      for (FieldSchema fs : hiveSchema) {
        colNames.add(fs.getName());
        String avroCol = schemaMapping.get(fs.getName().toLowerCase());
        if (avroCol == null) {
          // This wasn't in the map, so just set it to null
          ObjectInspector oi = PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(PrimitiveObjectInspector.PrimitiveCategory.STRING);

          ois.add(oi);
          deserializers.put(fs.getName(), null);
          // TODO should I be setting this to a default value?
        } else {
          String topLevelColName = findTopLevelColName(avroCol);
          Schema.Field field = avroSchema.getField(topLevelColName);
          // Make sure the mapped to Avro column exists
          if (field == null) throw new SerializationError("Mapping to non-existent avro column " + topLevelColName);
          MappingTranslatorInfo mtInfo = mapAvroColumn(avroCol, field.schema(), topLevelColName);
          ois.add(mtInfo.getObjectInspector());
          deserializers.put(fs.getName(), mtInfo);
        }
      }

      tInfo = new TranslatorInfo(ObjectInspectorFactory.getStandardStructObjectInspector(colNames, ois),
          o -> {
            GenericData.Record record = (GenericData.Record) o;
            assert record != null;
            List<Object> row = new ArrayList<>(size);
            long length = 0;
            for (FieldSchema fs : hiveSchema) {
              MappingTranslatorInfo mtInfo = deserializers.get(fs.getName());
              if (mtInfo == null) {
                row.add(null);
              } else {
                DeserializerOutput output = mtInfo.getDeserializer().apply(record.get(mtInfo.getTopLevelColumn()));
                if (output == null) {
                  row.add(null);
                } else {
                  length += output.getAddedLength();
                  row.add(output.getDeserialized());
                }
              }
            }
            return new DeserializerOutput(length, row);
          });
    }

    protected Object deserialize(GenericRecord record) {
      assert tInfo != null;
      DeserializerOutput output = tInfo.getDeserializer().apply(record);
      length = output.getAddedLength();
      return output.getDeserialized();
    }

    void setFakeObjectInspector(ObjectInspector oi) {
      tInfo = new TranslatorInfo(oi, null);
    }

    private String findTopLevelColName(String avroCol) {
      return avroCol.split("[\\[.]", 2)[0];
    }

    private String findKey(String avroCol) throws SerializationError {
      assert avroCol.charAt(0) == '[';
      int closeBracketAt = avroCol.indexOf(']');
      if (closeBracketAt < 0) throw new SerializationError("Unmatched [ in " + avroCol);
      return avroCol.substring(1, closeBracketAt);
    }

    private String findIndexRemainder(String avroCol) {
      int closeBracketAt = avroCol.indexOf(']');
      assert closeBracketAt > 0; // Since this should always be called after findKey we should be guaranteed
                                 // that we've already checked for the close bracket.
      return closeBracketAt < avroCol.length() - 1 ? avroCol.substring(closeBracketAt + 1) : null;

    }

    private String findRemainder(String avroCol) {
      for (int i = 0; i < avroCol.length(); i++) {
        if (avroCol.charAt(i) == '.' || avroCol.charAt(i) == '[') return avroCol.substring(i);
      }
      return null;
    }

    private MappingTranslatorInfo mapAvroColumn(String avroCol, Schema schema, String topLevelColName) throws SerializationError {
      return new MappingTranslatorInfo(parseAvroColumn(avroCol, schema), topLevelColName);
    }

    private TranslatorInfo parseAvroColumn(String avroCol, Schema schema) throws SerializationError {
      if (avroCol.charAt(0) == '.') {
        // its a record
        if (schema.getType() != Schema.Type.RECORD) {
          throw new SerializationError("Attempt to dereference '" + avroCol + "' when containing column is not a record");
        }
        String fieldName = findTopLevelColName(avroCol.substring(1));
        Schema.Field innerField = schema.getField(fieldName);
        if (innerField == null) {
          throw new SerializationError("Attempt to reference non-existent record field '" + fieldName + "'");
        }
        final TranslatorInfo subInfo = getSubTranslatorInfo(findRemainder(avroCol.substring(1)), innerField.schema());
        return new TranslatorInfo(subInfo.getObjectInspector(), o -> {
          GenericData.Record record = (GenericData.Record)o;
          Object element = record.get(fieldName);
          return element == null ? null : subInfo.getDeserializer().apply(element);
        });
      } else if (avroCol.charAt(0) == '[') {
        // it's a array, map, or union
        String key = findKey(avroCol);
        String remainder = findIndexRemainder(avroCol);
        switch (schema.getType()) {
          case ARRAY:
            int index;
            try {
              index = Integer.valueOf(key);
            } catch (NumberFormatException e) {
              throw new SerializationError("Attempt to dereference array with non-number '" + key + "'", e);
            }
            final TranslatorInfo arraySubInfo = getSubTranslatorInfo(remainder, schema.getElementType());
            return new TranslatorInfo(arraySubInfo.getObjectInspector(), o -> {
              List<Object> avroList = (List)o;
              Object element = (index >= avroList.size()) ? null : avroList.get(index);
              return element == null ? null : arraySubInfo.getDeserializer().apply(element);
            });

          case MAP:
            final TranslatorInfo mapSubInfo = getSubTranslatorInfo(remainder, schema.getValueType());
            return new TranslatorInfo(mapSubInfo.getObjectInspector(), o -> {
              Map<CharSequence, Object> avroMap = (Map)o;
              Object val = avroMap.get(key);
              return val == null ? null : mapSubInfo.getDeserializer().apply(val);
            });

          case UNION:
            int unionTag;
            try {
              unionTag = Integer.valueOf(key);
            } catch (NumberFormatException e) {
              throw new SerializationError("Attempt to dereference union with non-number '" + key + "'", e);
            }
            if (unionTag >= schema.getTypes().size()) {
              throw new SerializationError("Attempt to read union element " + unionTag + " in union with only " +
                  schema.getTypes().size() + " elements");
            }
            final TranslatorInfo unionSubInfo = getSubTranslatorInfo(remainder, schema.getTypes().get(unionTag));
            return new TranslatorInfo(unionSubInfo.getObjectInspector(), o -> {
              int offset = GenericData.get().resolveUnion(schema, o);
              return offset == unionTag ?
                  unionSubInfo.getDeserializer().apply(o) :
                  null; // When the unionTag and offset don't match, it means this union has a different element in it.
            });

          default:
            throw new SerializationError("Attempt to deference '" + avroCol +
                "' when containing column is not an array, map, or union");
        }
      } else {
        // it's a column name
        // If this column name is followed by a dereference symbol (. or [) than we need to parse it.  Otherwise take
        // this column as is and place it in the map, whether it's simple or complex.
        String remainder = findRemainder(avroCol);
        return (remainder == null) ?
            Translators.buildColTranslatorInfo(schema) :
            parseAvroColumn(remainder, schema);
      }
    }

    private TranslatorInfo getSubTranslatorInfo(String remainder, Schema subSchema) throws SerializationError {
      // if split col name has only one element (that is, the name wasn't really split), then just return
      // a translator info for that field.  If it has a subelement, then parse further down.
      return remainder == null ?
          Translators.buildColTranslatorInfo(subSchema) :
          parseAvroColumn(remainder, subSchema);
    }
  }

  private static class MappingTranslatorInfo {
    private final String topLevelColumn;
    private final TranslatorInfo tInfo;

    MappingTranslatorInfo(TranslatorInfo tInfo, String topLevelColumn) {
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
