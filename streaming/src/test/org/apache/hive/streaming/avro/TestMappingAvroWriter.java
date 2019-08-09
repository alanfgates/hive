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
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.hive.streaming.HiveStreamingConnection;
import org.apache.hive.streaming.RecordWriter;
import org.apache.hive.streaming.SerializationError;
import org.apache.hive.streaming.StreamingException;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestMappingAvroWriter extends AvroTestBase {
  public TestMappingAvroWriter() throws Exception {
  }

  @Test
  public void basic() throws StreamingException, IOException {
    Schema schema = SchemaBuilder.record("basic")
        .namespace("org.apache.hive.streaming")
        .fields()
        .requiredString("field1")
        .requiredInt("field2")
        .endRecord();
    List<GenericRecord> records = new ArrayList<>();
    GenericRecordBuilder recordBuilder = new GenericRecordBuilder(schema);
    for (int i = 0; i < 10; i++) {
      recordBuilder.set("field1", Integer.toString(i + 10));
      recordBuilder.set("field2", i);
      records.add(recordBuilder.build());
    }

    String tableName = "b";
    String fullTableName = dbName + "." + tableName;
    dropAndCreateTable(fullTableName, "f1 int, f2 string");

    RecordWriter writer = MappingAvroWriter.newBuilder()
        .withAvroSchema(schema)
        .addMappingColumn("f1", "field2")
        .addMappingColumn("f2", "field1")
        .build();

    HiveStreamingConnection conn = HiveStreamingConnection.newBuilder()
        .withDatabase(dbName)
        .withTable(tableName)
        .withTransactionBatchSize(5)
        .withRecordWriter(writer)
        .withHiveConf(conf)
        .connect();

    conn.beginTransaction();
    for (GenericRecord r : records) conn.write(r);
    conn.commitTransaction();
    conn.close();

    List<String> results = querySql("select f1, f2 from " + fullTableName);
    Assert.assertEquals(10, results.size());
    for (int i = 0; i < 10; i++) Assert.assertEquals(i + "\t" + (i + 10), results.get(i));
  }

  @Test
  public void hiveColumnMissing() throws StreamingException, IOException {
    Schema schema = SchemaBuilder.record("hiveColMissing")
        .namespace("org.apache.hive.streaming")
        .fields()
        .nullableBoolean("nullableBoolean", false)
        .requiredBytes("avroBytes")
        .endRecord();

    List<GenericRecord> records = new ArrayList<>();
    GenericRecordBuilder recordBuilder = new GenericRecordBuilder(schema);
    for (int i = 0; i < 10; i++) {
      if (i % 2 == 0) recordBuilder.set("nullableBoolean", true);
      else if (i % 7 == 0) recordBuilder.set("nullableBoolean", false);
      else recordBuilder.set("nullableBoolean", null);

      recordBuilder.set("avroBytes", ByteBuffer.wrap(Integer.toString(i).getBytes()));

      records.add(recordBuilder.build());
    }
    String tableName = "hiveColMissing";
    String fullTableName = dbName + "." + tableName;
    dropAndCreateTable(fullTableName, "hiveBoolean boolean, hiveMia string, hiveBinary binary");

    Map<String, String> schemaMap = new HashMap<>();
    schemaMap.put("hiveBoolean", "nullableBoolean");
    schemaMap.put("hiveBinary", "avroBytes");

    RecordWriter writer = MappingAvroWriter.newBuilder()
        .withAvroSchema(schema.toString())
        .withSchemaMapping(schemaMap)
        .build();

    HiveStreamingConnection conn = HiveStreamingConnection.newBuilder()
        .withDatabase(dbName)
        .withTable(tableName)
        .withTransactionBatchSize(5)
        .withRecordWriter(writer)
        .withHiveConf(conf)
        .connect();

    conn.beginTransaction();
    for (GenericRecord r : records) conn.write(r);
    conn.commitTransaction();
    conn.close();

    List<String> results = querySql("select hiveBoolean, hiveMia, hiveBinary " +
        " from " + fullTableName);
    Assert.assertEquals(10, results.size());
    for (int i = 0; i < 10; i++) {
      StringBuilder buf = new StringBuilder();

      if (i % 2 == 0) buf.append("true");
      else if (i % 7 == 0) buf.append("false");
      else buf.append("NULL");

      buf.append("\tNULL");

      buf.append("\t")
          .append(i);

      Assert.assertEquals(buf.toString(), results.get(i));
    }
  }

  @Test
  public void avroColMissing() throws StreamingException, IOException {
    Schema fixedSchema = SchemaBuilder.fixed("avroColMissing").size(10);
    Schema schema = SchemaBuilder.record("allTypes")
        .namespace("org.apache.hive.streaming")
        .fields()
        .requiredDouble("avroDouble")
        .name("avroEnum")
        .type().enumeration("enumType").symbols("apple", "orange", "banana").enumDefault("apple")
        .requiredInt("avroInt")
        .name("avroFixed")
        .type(fixedSchema).noDefault()
        .endRecord();

    List<GenericRecord> records = new ArrayList<>();
    GenericRecordBuilder recordBuilder = new GenericRecordBuilder(schema);
    for (int i = 0; i < 10; i++) {
      double d = i + (double)i * 0.1;
      recordBuilder.set("avroDouble", d);

      if (i % 2 == 0) recordBuilder.set("avroEnum", "apple");
      else if (i % 7 == 0) recordBuilder.set("avroEnum", "orange");
      else recordBuilder.set("avroEnum", "banana");

      recordBuilder.set("avroInt", i);

      StringBuilder buf = new StringBuilder();
      for (int j = 0; j < 10; j++) buf.append(i);
      GenericData.Fixed fixed = new GenericData.Fixed(fixedSchema, buf.toString().getBytes());
      recordBuilder.set("avroFixed", fixed);

      records.add(recordBuilder.build());
    }
    String tableName = "avroColMissing";
    String fullTableName = dbName + "." + tableName;
    dropAndCreateTable(fullTableName, "hiveDouble double, hiveEnum string, hiveFixed binary "
    );


    RecordWriter writer = MappingAvroWriter.newBuilder()
        .withAvroSchema(schema)
        .addMappingColumn("hiveDouble", "avroDouble")
        .addMappingColumn("hiveEnum", "avroEnum")
        .addMappingColumn("hiveFixed", "avroFixed")
        .build();

    HiveStreamingConnection conn = HiveStreamingConnection.newBuilder()
        .withDatabase(dbName)
        .withTable(tableName)
        .withTransactionBatchSize(5)
        .withRecordWriter(writer)
        .withHiveConf(conf)
        .connect();

    conn.beginTransaction();
    for (GenericRecord r : records) conn.write(r);
    conn.commitTransaction();
    conn.close();

    List<String> results = querySql("select hiveDouble, hiveEnum, hiveFixed from " + fullTableName);
    Assert.assertEquals(10, results.size());
    for (int i = 0; i < 10; i++) {
      StringBuilder buf = new StringBuilder();
      double d = i + (double)i * 0.1;
      buf.append(d);

      if (i % 2 == 0) buf.append("\tapple");
      else if (i % 7 == 0) buf.append("\torange");
      else buf.append("\tbanana");

      buf.append('\t');
      for (int j = 0; j < 10; j++) buf.append(i);

      Assert.assertEquals(buf.toString(), results.get(i));
    }
  }

  @Test
  public void recordDereference() throws StreamingException, IOException {
    Schema innerRecordSchema = SchemaBuilder.record("recordType")
        .fields()
        .requiredString("innerString")
        .requiredInt("innerInt")
        .endRecord();
    Schema schema = SchemaBuilder.record("recordDeref")
        .namespace("org.apache.hive.streaming")
        .fields()
        .requiredDouble("avroDouble")
        .name("recordField")
        .type(innerRecordSchema).noDefault()
        .endRecord();

    List<GenericRecord> records = new ArrayList<>();
    GenericRecordBuilder recordBuilder = new GenericRecordBuilder(schema);
    for (int i = 0; i < 10; i++) {
      double d = i + (double)i * 0.1;
      recordBuilder.set("avroDouble", d);

      GenericRecordBuilder innerBuilder = new GenericRecordBuilder(innerRecordSchema);
      innerBuilder.set("innerString", Integer.toString(i*100));
      innerBuilder.set("innerInt", i);
      recordBuilder.set("recordField", innerBuilder.build());

      records.add(recordBuilder.build());
    }
    String tableName = "recordderef";
    String fullTableName = dbName + "." + tableName;
    dropAndCreateTable(fullTableName, "hiveInt int ");


    RecordWriter writer = MappingAvroWriter.newBuilder()
        .withAvroSchema(schema)
        .addMappingColumn("hiveInt", "recordField.innerInt")
        .build();

    HiveStreamingConnection conn = HiveStreamingConnection.newBuilder()
        .withDatabase(dbName)
        .withTable(tableName)
        .withTransactionBatchSize(5)
        .withRecordWriter(writer)
        .withHiveConf(conf)
        .connect();

    conn.beginTransaction();
    for (GenericRecord r : records) conn.write(r);
    conn.commitTransaction();
    conn.close();

    List<String> results = querySql("select hiveInt from " + fullTableName);
    Assert.assertEquals(10, results.size());
    for (int i = 0; i < 10; i++) {
      Assert.assertEquals(Integer.toString(i), results.get(i));
    }
  }

  @Test
  public void arrayDereference() throws StreamingException, IOException {
    Schema schema = SchemaBuilder.record("arrayderef")
        .namespace("org.apache.hive.streaming")
        .fields()
        .name("avroArray")
        .type().array().items().intType().noDefault()
        .requiredDouble("avroDouble")
        .endRecord();

    List<GenericRecord> records = new ArrayList<>();
    GenericRecordBuilder recordBuilder = new GenericRecordBuilder(schema);
    for (int i = 0; i < 10; i++) {
      List<Integer> list = new ArrayList<>();
      for (int j = i; j < 10; j++) list.add(j);
      recordBuilder.set("avroArray", list);

      double d = i + (double)i * 0.1;
      recordBuilder.set("avroDouble", d);

      records.add(recordBuilder.build());
    }
    String tableName = "arrayderef";
    String fullTableName = dbName + "." + tableName;
    dropAndCreateTable(fullTableName, "f1 int, f2 int ");

    RecordWriter writer = MappingAvroWriter.newBuilder()
        .withAvroSchema(schema)
        .addMappingColumn("f1", "avroArray[0]")
        .addMappingColumn("f2", "avroArray[1]")
        .build();

    HiveStreamingConnection conn = HiveStreamingConnection.newBuilder()
        .withDatabase(dbName)
        .withTable(tableName)
        .withTransactionBatchSize(5)
        .withRecordWriter(writer)
        .withHiveConf(conf)
        .connect();

    conn.beginTransaction();
    for (GenericRecord r : records) conn.write(r);
    conn.commitTransaction();
    conn.close();

    List<String> results = querySql("select f1, f2 from " + fullTableName);
    Assert.assertEquals(10, results.size());
    for (int i = 0; i < 10; i++) {
      StringBuilder buf = new StringBuilder();
      buf.append(i)
          .append('\t');
      if (i < 9) buf.append(i + 1);
      else buf.append("NULL");

      Assert.assertEquals(buf.toString(), results.get(i));
    }
  }

  @Test
  public void mapDereference() throws StreamingException, IOException {
    Schema schema = SchemaBuilder.record("mapderef")
        .namespace("org.apache.hive.streaming")
        .fields()
        .requiredLong("avroLong")
        .name("avroMap")
        .type().map().values().intType().mapDefault(Collections.emptyMap())
        .endRecord();

    List<GenericRecord> records = new ArrayList<>();
    GenericRecordBuilder recordBuilder = new GenericRecordBuilder(schema);
    for (int i = 0; i < 10; i++) {
      recordBuilder.set("avroLong", new Long(i));

      Map<String, Integer> m = new HashMap<>();
      m.put("always", i);
      m.put("alsoAlways", i * 10);
      if (i % 2 == 0) m.put("sometimes", i * 100);
      recordBuilder.set("avroMap", m);

      records.add(recordBuilder.build());
    }
    String tableName = "mapderef";
    String fullTableName = dbName + "." + tableName;
    dropAndCreateTable(fullTableName, "f1 int, f2 int ");

    RecordWriter writer = MappingAvroWriter.newBuilder()
        .withAvroSchema(schema)
        .addMappingColumn("f1", "avroMap[always]")
        .addMappingColumn("f2", "avroMap[sometimes]")
        .build();

    HiveStreamingConnection conn = HiveStreamingConnection.newBuilder()
        .withDatabase(dbName)
        .withTable(tableName)
        .withTransactionBatchSize(5)
        .withRecordWriter(writer)
        .withHiveConf(conf)
        .connect();

    conn.beginTransaction();
    for (GenericRecord r : records) conn.write(r);
    conn.commitTransaction();
    conn.close();

    List<String> results = querySql("select f1, f2 from " + fullTableName);
    Assert.assertEquals(10, results.size());
    for (int i = 0; i < 10; i++) {
      StringBuilder buf = new StringBuilder();
      buf.append(i)
          .append('\t');
      if (i % 2 == 0) buf.append(i * 100);
      else buf.append("NULL");

      Assert.assertEquals(buf.toString(), results.get(i));
    }
  }

  @Test
  public void unionDereference() throws StreamingException, IOException {
    Schema schema = SchemaBuilder.record("unionDeref")
        .namespace("org.apache.hive.streaming")
        .fields()
        .requiredLong("avroLong")
        .name("avroUnion")
        .type().unionOf().booleanType().and().intType().endUnion().booleanDefault(true)
        .endRecord();

    List<GenericRecord> records = new ArrayList<>();
    GenericRecordBuilder recordBuilder = new GenericRecordBuilder(schema);
    for (int i = 0; i < 10; i++) {
      recordBuilder.set("avroLong", new Long(i));

      if (i % 2 == 0) recordBuilder.set("avroUnion", i);
      else recordBuilder.set("avroUnion", true);

      records.add(recordBuilder.build());
    }
    String tableName = "unionderef";
    String fullTableName = dbName + "." + tableName;
    dropAndCreateTable(fullTableName, "f1 int, f2 boolean ");

    RecordWriter writer = MappingAvroWriter.newBuilder()
        .withAvroSchema(schema)
        .addMappingColumn("f1", "avroUnion[1]")
        .addMappingColumn("f2", "avroUnion[0]")
        .build();

    HiveStreamingConnection conn = HiveStreamingConnection.newBuilder()
        .withDatabase(dbName)
        .withTable(tableName)
        .withTransactionBatchSize(5)
        .withRecordWriter(writer)
        .withHiveConf(conf)
        .connect();

    conn.beginTransaction();
    for (GenericRecord r : records) conn.write(r);
    conn.commitTransaction();
    conn.close();

    List<String> results = querySql("select f1, f2 from " + fullTableName);
    Assert.assertEquals(10, results.size());
    for (int i = 0; i < 10; i++) {
      StringBuilder buf = new StringBuilder();

      if (i % 2 == 0) buf.append(i).append("\tNULL");
      else buf.append("NULL\t").append(true);

      Assert.assertEquals(buf.toString(), results.get(i));
    }
  }

  /*
  @Test
  public void allAvroTypes() throws StreamingException, IOException {
    Schema fixedSchema = SchemaBuilder.fixed("fixedType").size(10);
    Schema innerRecordSchema = SchemaBuilder.record("recordType")
        .fields()
        .requiredString("innerString")
        .requiredInt("innerInt")
        .endRecord();
    Schema schema = SchemaBuilder.record("allTypes")
                                 .namespace("org.apache.hive.streaming")
                                 .fields()
                                   .name("avroArray")
                                     .type().array().items().intType().noDefault()
                                   .nullableBoolean("nullableBoolean", false)
                                   .requiredBytes("avroBytes")
                                   .requiredDouble("avroDouble")
                                   .name("avroEnum")
                                     .type().enumeration("enumType").symbols("apple", "orange", "banana").enumDefault("apple")
                                   .name("avroFixed")
                                     .type(fixedSchema).noDefault()
                                   .requiredFloat("avroFloat")
                                   .requiredLong("avroLong")
                                   .name("avroMap")
                                     .type().map().values().intType().mapDefault(Collections.emptyMap())
                                   .name("recordField")
                                     .type(innerRecordSchema).noDefault()
                                   .name("avroUnion")
                                     .type().unionOf().booleanType().and().intType().endUnion().booleanDefault(true)
                                 .endRecord();

    LOG.debug("Avro schema is " + schema.toString(true));
    List<GenericRecord> records = new ArrayList<>();
    GenericRecordBuilder recordBuilder = new GenericRecordBuilder(schema);
    for (int i = 0; i < 10; i++) {
      List<Integer> list = new ArrayList<>();
      for (int j = i; j < 10; j++) list.add(j);
      recordBuilder.set("avroArray", list);

      if (i % 2 == 0) recordBuilder.set("nullableBoolean", true);
      else if (i % 7 == 0) recordBuilder.set("nullableBoolean", false);
      else recordBuilder.set("nullableBoolean", null);

      recordBuilder.set("avroBytes", ByteBuffer.wrap(Integer.toString(i).getBytes()));

      double d = i + (double)i * 0.1;
      recordBuilder.set("avroDouble", d);

      if (i % 2 == 0) recordBuilder.set("avroEnum", "apple");
      else if (i % 7 == 0) recordBuilder.set("avroEnum", "orange");
      else recordBuilder.set("avroEnum", "banana");

      StringBuilder buf = new StringBuilder();
      for (int j = 0; j < 10; j++) buf.append(i);
      GenericData.Fixed fixed = new GenericData.Fixed(fixedSchema, buf.toString().getBytes());
      recordBuilder.set("avroFixed", fixed);

      float f = i + (float)i * 0.1f;
      recordBuilder.set("avroFloat", f);

      recordBuilder.set("avroLong", new Long(i));

      // More than one element in the map causes ordering issues when we
      // compare the results.
      Map<String, Integer> m = new HashMap<>();
      m.put(Integer.toString(i), i);
      recordBuilder.set("avroMap", m);

      GenericRecordBuilder innerBuilder = new GenericRecordBuilder(innerRecordSchema);
      innerBuilder.set("innerString", Integer.toString(i*100));
      innerBuilder.set("innerInt", i);
      recordBuilder.set("recordField", innerBuilder.build());

      if (i % 2 == 0) recordBuilder.set("avroUnion", i);
      else recordBuilder.set("avroUnion", true);

      records.add(recordBuilder.build());
    }
    String tableName = "alltypes";
    String fullTableName = dbName + "." + tableName;
    dropAndCreateTable(fullTableName, "hiveArray array<int>, hiveBoolean boolean, hiveBinary binary, " +
                                      "hiveDouble double, hiveEnum string, hiveFixed binary, hiveFloat float, " +
                                      "hiveLong bigint, hiveMap map<string, int>, " +
                                      "hiveRecord struct<innerstring:string, innerint:int>, " +
                                      "hiveUnion uniontype<boolean, int> "
    );


    RecordWriter writer = StrictAvroWriter.newBuilder()
        .withSchema(schema)
        .build();

    HiveStreamingConnection conn = HiveStreamingConnection.newBuilder()
        .withDatabase(dbName)
        .withTable(tableName)
        .withTransactionBatchSize(5)
        .withRecordWriter(writer)
        .withHiveConf(conf)
        .connect();

    conn.beginTransaction();
    for (GenericRecord r : records) conn.write(r);
    conn.commitTransaction();
    conn.close();

    List<String> results = querySql("select hiveArray, hiveBoolean, hiveBinary, hiveDouble, " +
        "hiveEnum, hiveFixed, hiveFloat, hiveLong, hiveMap,  hiveRecord, hiveUnion " +
        " from " + fullTableName);
    Assert.assertEquals(10, results.size());
    for (int i = 0; i < 10; i++) {
      StringBuilder buf = new StringBuilder();
      buf.append('[');
      boolean first = true;
      for (int j = i; j < 10; j++) {
        if (first) first = false;
        else buf.append(',');
        buf.append(j);
      }
      buf.append("]\t");

      if (i % 2 == 0) buf.append("true");
      else if (i % 7 == 0) buf.append("false");
      else buf.append("NULL");

      buf.append("\t")
          .append(i);

      double d = i + (double)i * 0.1;
      buf.append("\t")
          .append(d);

      if (i % 2 == 0) buf.append("\tapple");
      else if (i % 7 == 0) buf.append("\torange");
      else buf.append("\tbanana");

      buf.append('\t');
      for (int j = 0; j < 10; j++) buf.append(i);

      float f = i + (float)i * 0.1f;
      buf.append("\t")
          .append(f);

      buf.append("\t")
          .append(i);

      buf.append("\t{\"")
          .append(i)
          .append("\":")
          .append(i);
      buf.append('}');

      buf.append("\t{\"innerstring\":\"")
          .append(i*100)
          .append("\",\"innerint\":")
          .append(i)
          .append('}');

      buf.append("\t{");
      if (i % 2 == 0) buf.append(1).append(':').append(i);
      else buf.append(0).append(':').append(true);
      buf.append('}');

      Assert.assertEquals(buf.toString(), results.get(i));
    }
  }

   */

  @Test
  public void nonExistentHiveCol() throws StreamingException, IOException {
    Schema schema = SchemaBuilder.record("noHiveCol")
        .namespace("org.apache.hive.streaming")
        .fields()
        .requiredString("field1")
        .requiredInt("field2")
        .endRecord();

    String tableName = "nohivecol";
    String fullTableName = dbName + "." + tableName;
    dropAndCreateTable(fullTableName, "f1 int, f2 string");

    RecordWriter writer = MappingAvroWriter.newBuilder()
        .withAvroSchema(schema)
        .addMappingColumn("f1", "field2")
        .addMappingColumn("nosuch", "field1")
        .build();

    try {
      HiveStreamingConnection conn = HiveStreamingConnection.newBuilder()
          .withDatabase(dbName)
          .withTable(tableName)
          .withTransactionBatchSize(5)
          .withRecordWriter(writer)
          .withHiveConf(conf)
          .connect();
      conn.beginTransaction();
      Assert.fail();
    } catch (SerializationError e) {
      Assert.assertEquals("Unknown Hive columns nosuch referenced in schema mapping", e.getMessage());
    }

  }

  @Test
  public void nonExistentAvroCol() throws StreamingException, IOException {
    Schema schema = SchemaBuilder.record("noAvroCol")
        .namespace("org.apache.hive.streaming")
        .fields()
        .requiredString("field1")
        .requiredInt("field2")
        .endRecord();

    String tableName = "noavrocol";
    String fullTableName = dbName + "." + tableName;
    dropAndCreateTable(fullTableName, "f1 int, f2 string");

    RecordWriter writer = MappingAvroWriter.newBuilder()
        .withAvroSchema(schema)
        .addMappingColumn("f1", "field2")
        .addMappingColumn("f2", "nosuch")
        .build();

    try {
      HiveStreamingConnection conn = HiveStreamingConnection.newBuilder()
          .withDatabase(dbName)
          .withTable(tableName)
          .withTransactionBatchSize(5)
          .withRecordWriter(writer)
          .withHiveConf(conf)
          .connect();
      conn.beginTransaction();
      Assert.fail();
    } catch (SerializationError e) {
      Assert.assertEquals("Mapping to non-existent avro column nosuch", e.getMessage());
    }

  }

  @Test
  public void partitioned() throws StreamingException, IOException {
    Schema schema = SchemaBuilder.record("partitioned")
        .namespace("org.apache.hive.streaming")
        .fields()
        .requiredString("field1")
        .requiredInt("field2")
        .requiredString("field3")
        .endRecord();
    List<GenericRecord> records = new ArrayList<>();
    GenericRecordBuilder recordBuilder = new GenericRecordBuilder(schema);
    for (int i = 0; i < 10; i++) {
      recordBuilder.set("field1", Integer.toString(i));
      recordBuilder.set("field2", i);
      recordBuilder.set("field3", "a");
      records.add(recordBuilder.build());
    }
    for (int i = 0; i < 10; i++) {
      recordBuilder.set("field1", Integer.toString(i));
      recordBuilder.set("field2", i);
      recordBuilder.set("field3", "b");
      records.add(recordBuilder.build());
    }

    String tableName = "ptable";
    String fullTableName = dbName + "." + tableName;
    dropAndCreateTable(fullTableName, "f1 string, f2 int", "f3 string");

    RecordWriter writer = MappingAvroWriter.newBuilder()
        .withAvroSchema(schema)
        .addMappingColumn("f1", "field1")
        .addMappingColumn("f2", "field2")
        .addMappingColumn("f3", "field3")
        .build();

    HiveStreamingConnection conn = HiveStreamingConnection.newBuilder()
        .withDatabase(dbName)
        .withTable(tableName)
        .withTransactionBatchSize(5)
        .withRecordWriter(writer)
        .withHiveConf(conf)
        .connect();

    conn.beginTransaction();
    for (GenericRecord r : records) conn.write(r);
    conn.commitTransaction();
    conn.close();

    List<String> results = querySql("select f1, f2 from " + fullTableName + " where f3 = 'a'");
    Assert.assertEquals(10, results.size());
    for (int i = 0; i < 10; i++) Assert.assertEquals(i + "\t" + i, results.get(i));
    results = querySql("select f1, f2 from " + fullTableName + " where f3 = 'b'");
    Assert.assertEquals(10, results.size());
    for (int i = 0; i < 10; i++) Assert.assertEquals(i + "\t" + i, results.get(i));
  }

  @Test(expected = IllegalStateException.class)
  public void missingMapping() throws StreamingException, IOException {
    MappingAvroWriter.newBuilder()
        .withAvroSchema("abcdef")
        .build();
  }

  // TODO test union dereference
  // TODO test union dereference of out of range tag
  // TODO test record dereference of non-existent column
  // TODO test multi-level dereference
  // TODO test union deref with non-number
  // TODO test array deref with non-number
}
