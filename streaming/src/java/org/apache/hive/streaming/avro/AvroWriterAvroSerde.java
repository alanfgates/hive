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
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.io.Writable;
import org.apache.hive.streaming.SerializationError;

import javax.annotation.Nullable;
import java.util.Properties;

abstract class AvroWriterAvroSerde extends AbstractSerDe {

  protected TranslatorInfo tInfo;
  protected Schema avroSchema;

  protected AvroWriterAvroSerde(Schema avroSchema) {
    this.avroSchema = avroSchema;
  }

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
    return tInfo.getObjectInspector();
  }

  /**
   * This should write its result into tInfo.
   */
  abstract protected void buildTranslator() throws SerializationError;

  abstract protected Object deserialize(GenericRecord record) throws SerializationError;
}
