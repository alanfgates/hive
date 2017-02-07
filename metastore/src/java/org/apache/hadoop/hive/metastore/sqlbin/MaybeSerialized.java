/**
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
package org.apache.hadoop.hive.metastore.sqlbin;

import com.google.common.annotations.VisibleForTesting;
import org.apache.thrift.TBase;

import java.io.IOException;

/**
 * A container that can store an object as an object or serialized.  The object is assumed to be
 * immutable and thus when serialize is called, if we've already serialized it, it will not be
 * re-serialized.
 */
public class MaybeSerialized<T extends TBase> {

  private Class<T> clazz;
  private T obj;
  private byte[] serialized;

  /**
   * Create the object in serialized form.
   * @param clazz Class of the object when it is deserialized.
   * @param serialized serialized form
   */
  MaybeSerialized(Class<T> clazz, byte[] serialized) {
    this.clazz = clazz;
    this.serialized = serialized;
  }

  /**
   * Create the object in deserialized form.
   * @param obj deserialized object.
   */
  MaybeSerialized(T obj) {
    this.obj = obj;
  }

  /**
   * Get the object.  If it has not yet been deserialized, it will be now.
   * @return object
   */
  T get() {
    if (obj == null) {
      assert serialized != null;
      try {
        obj = clazz.newInstance();
        PostgresKeyValue.deserialize(obj, serialized);
        serialized = null;
      } catch (InstantiationException|IllegalAccessException e) {
        throw new RuntimeException(e);
      }
    }
    return obj;
  }

  /**
   * Serialize the object.  If it is already serialized or was never deserialized the serialized
   * form will be returned, which is equivalent to saying that this class assumes the contained
   * object is immutable.
   * @return serialized form of the object.
   */
  byte[] serialize() {
    if (serialized == null) {
      serialized = PostgresKeyValue.serialize(obj);
    }
    return serialized;
  }

  @VisibleForTesting
  boolean isSerialized() {
    return obj == null;
  }
}
