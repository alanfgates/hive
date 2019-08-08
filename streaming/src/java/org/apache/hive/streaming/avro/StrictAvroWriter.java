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
package org.apache.hive.streaming.avro;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hive.streaming.AbstractRecordWriter;
import org.apache.hive.streaming.SerializationError;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A writer for Avro records that assumes the Avro schema and Hive schema match.  Matching
 * means that they have the same number of columns and compatible types.  It is assumed that
 * every Avro record has the same schema, with the Hive partition columns at the end of the row.
 * <p>The Avro to Hive type compatibility is:</p>
 * <table>
 *   <tr>     <th>Avro</th>        <th>Hive</th>          </tr>
 *   <tr>     <td>array</td>       <td>list</td>          </tr>
 *   <tr>     <td>boolean</td>     <td>boolean</td>       </tr>
 *   <tr>     <td>bytes</td>       <td>binary</td>        </tr>
 *   <tr>     <td>double</td>      <td>double</td>        </tr>
 *   <tr>     <td>enum</td>        <td>string</td>        </tr>
 *   <tr>     <td>fixed</td>       <td>binary</td>        </tr>
 *   <tr>     <td>float</td>       <td>float</td>         </tr>
 *   <tr>     <td>int</td>         <td>int</td>           </tr>
 *   <tr>     <td>long</td>        <td>bigint</td>        </tr>
 *   <tr>     <td>map</td>         <td>map</td>           </tr>
 *   <tr>     <td>null</td>        <td>(see below)</td>   </tr>
 *   <tr>     <td>record</td>      <td>struct</td>        </tr>
 *   <tr>     <td>string</td>      <td>string</td>        </tr>
 *   <tr>     <td>union</td>       <td>union*</td>        </tr>
 * </table>
 * <p> *Notes on null and union: </p>
 * <p> Hive does not have a null type or the concept of a nullable type, instead all types are nullable.
 *   When this writer encounters a union of null and a type, it will "look through" the union to the non-null type
 *   and declare that to be the type of the column.  If it encounters a union or two or more non-nullable types,
 *   it will map it as a Hive union.  So, if it encounters a type that is union(long, null) it will expect to
 *   find a hive type of bigint for that column.  But if it encounters a union(long, double) it will expect to
 *   find a hive type of union(bigint, double) for that column. </p>
 *   <p>Null type is not directly supported, that is a null type cannot appear outside a union.</p>
 */
public class StrictAvroWriter extends AbstractRecordWriter<GenericRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(StrictAvroWriter.class);

  // I cannot use AvroSerDe because it requires AvroGenericRecordWritable.  And I can't use
  // AvroGenericRecordWritable because it depends on the schema being encoded with every record, which is
  // silly.  And in general records we pull off Kafka won't have the schema in every record.
  private final AvroWriterAvroSerde serde;
  private Object encoded;
  private long length;

  private StrictAvroWriter(Builder builder) {
    Schema avroSchema = builder.avroSchema == null ? new Schema.Parser().parse(builder.avroSchemaStr) : builder.avroSchema;
    serde = new StrictAvroWriterAvroSerDe(avroSchema);
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

    public StrictAvroWriter build() {
      if (avroSchemaStr == null && avroSchema == null) {
        throw new IllegalStateException("You must provide an Avro schema, either as a string or an object");
      }

      if (avroSchemaStr != null && avroSchema != null) {
        throw new IllegalStateException("You have provided two schemas, provide either an object or a string");
      }

      return new StrictAvroWriter(this);
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
  private class StrictAvroWriterAvroSerDe extends AvroWriterAvroSerde {

    public StrictAvroWriterAvroSerDe(Schema avroSchema) {
      super(avroSchema);
      // Build the translator, since we have all the info we need up front.
      buildTranslator();
    }

    @Override
    protected void buildTranslator() {
      tInfo = Translators.buildRecordTranslatorInfo(avroSchema);
    }

    protected Object deserialize(GenericRecord record) throws SerializationError {
      DeserializerOutput output = tInfo.getDeserializer().apply(record);
      length = output.getAddedLength();
      return output.getDeserialized();
    }

  }

}
