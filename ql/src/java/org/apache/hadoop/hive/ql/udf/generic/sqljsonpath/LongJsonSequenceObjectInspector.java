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

import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.LongWritable;

public class LongJsonSequenceObjectInspector extends AbstractJsonSequenceObjectInspector implements LongObjectInspector {
  @Override
  public long get(Object o) {
    Long l = asLong(o);
    return l == null ? 0 : l;
  }

  @Override
  public PrimitiveTypeInfo getTypeInfo() {
    return TypeInfoFactory.longTypeInfo;
  }

  @Override
  public PrimitiveCategory getPrimitiveCategory() {
    return PrimitiveCategory.LONG;
  }

  @Override
  public Class<?> getPrimitiveWritableClass() {
    return LongWritable.class;
  }

  @Override
  public Object getPrimitiveWritableObject(Object o) {
    Long l = asLong(o);
    return l == null ? null : new LongWritable(l);
  }

  @Override
  public Class<?> getJavaPrimitiveClass() {
    return Long.class;
  }

  @Override
  public Object getPrimitiveJavaObject(Object o) {
    return asLong(o);
  }

  @Override
  public boolean preferWritable() {
    return false;
  }

  @Override
  public int precision() {
    return 0;
  }

  @Override
  public int scale() {
    return 0;
  }

  @Override
  public String getTypeName() {
    return serdeConstants.BIGINT_TYPE_NAME;
  }

  @Override
  public Category getCategory() {
    return Category.PRIMITIVE;
  }

  private Long asLong(Object o) {
    return asType(JsonSequence::isLong, JsonSequence::castToLong, o, "long");
  }
}
