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
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BooleanObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.BooleanWritable;

public class BooleanJsonSequenceObjectInspector extends AbstractJsonSequenceObjectInspector implements BooleanObjectInspector {
  @Override
  public boolean get(Object o) {
    Boolean b = asBool(o);
    return b == null ? false : b;
  }

  @Override
  public PrimitiveTypeInfo getTypeInfo() {
    return TypeInfoFactory.booleanTypeInfo;
  }

  @Override
  public PrimitiveCategory getPrimitiveCategory() {
    return PrimitiveCategory.BOOLEAN;
  }

  @Override
  public Class<?> getPrimitiveWritableClass() {
    return BooleanWritable.class;
  }

  @Override
  public Object getPrimitiveWritableObject(Object o) {
    Boolean bool = (Boolean)getPrimitiveJavaObject(o);
    return bool == null ? null : new BooleanWritable(bool);
  }

  @Override
  public Class<?> getJavaPrimitiveClass() {
    return Boolean.class;
  }

  @Override
  public Object getPrimitiveJavaObject(Object o) {
    return asBool(o);
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
    return serdeConstants.BOOLEAN_TYPE_NAME;
  }

  @Override
  public Category getCategory() {
    return Category.PRIMITIVE;
  }

  private Boolean asBool(Object o) {
    return asType(JsonSequence::isBool, JsonSequence::castToBool, o, "boolean");
  }
}
