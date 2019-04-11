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

public interface JsonSequenceConverter {

  /**
   * Convert the JsonSequence to something that can pass on to the rest of Hive.  The conversion will be based on
   * the object inspector.
   * @param json JsonSequence to be converted to an object that can be read by outputOI.
   * @return an object, not necessary a writable (may be a list or a map), or null.
   * @throws JsonConversionException if a bad conversion is attempted.
   */
  Object convert(JsonSequence json) throws JsonConversionException;
}
