/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */ // For the most part char and varchar can be handled identically to Strings.  The one case
package org.apache.hive.test.capybara.data;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.HiveCharObjectInspector;

// where this isn't true is object inspectors.
class CharColumn extends StringColumn {
  public CharColumn(int colNum) {
    super(colNum);
  }

  @Override
  public void fromObject(ObjectInspector objectInspector, Object o) {
    if (o == null) val = null;
    else val = ((HiveCharObjectInspector)objectInspector).getPrimitiveJavaObject(o).toString();
  }
}
