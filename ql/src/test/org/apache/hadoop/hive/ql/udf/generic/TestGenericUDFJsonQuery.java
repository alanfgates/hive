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
package org.apache.hadoop.hive.ql.udf.generic;

import org.apache.hadoop.hive.common.ObjectPair;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;

public class TestGenericUDFJsonQuery extends BaseTestGenericUDFJson {

  @Test
  public void noReturnSpecified() throws HiveException {
    ObjectPair<ObjectInspector, Object[]> results = test(json, "$.name");
    Assert.assertEquals(4, results.getSecond().length);
    Assert.assertTrue(results.getFirst() instanceof StringObjectInspector);
    StringObjectInspector soi = (StringObjectInspector)results.getFirst();
    Assert.assertEquals("chris", soi.getPrimitiveJavaObject(results.getSecond()[0]));
    Assert.assertEquals("tracy", soi.getPrimitiveJavaObject(results.getSecond()[1]));
    Assert.assertNull(soi.getPrimitiveJavaObject(results.getSecond()[2]));
    Assert.assertNull(soi.getPrimitiveJavaObject(results.getSecond()[3]));
  }

  @Test
  public void strSpecified() throws HiveException {
    ObjectPair<ObjectInspector, Object[]> results = test(json, "$.name", wrapInList("a"));
    Assert.assertEquals(4, results.getSecond().length);
    Assert.assertTrue(results.getFirst() instanceof StringObjectInspector);
    StringObjectInspector soi = (StringObjectInspector)results.getFirst();
    Assert.assertEquals("chris", soi.getPrimitiveJavaObject(results.getSecond()[0]));
    Assert.assertEquals("tracy", soi.getPrimitiveJavaObject(results.getSecond()[1]));
    Assert.assertNull(soi.getPrimitiveJavaObject(results.getSecond()[2]));
    Assert.assertNull(soi.getPrimitiveJavaObject(results.getSecond()[3]));
  }

  @Test
  public void longBad() throws HiveException {
    try {
      test(json, "$.age", wrapInList(1L));
      Assert.fail();
    } catch (UDFArgumentException e) {
      Assert.assertEquals("org.apache.hadoop.hive.ql.udf.generic.GenericUDFJsonQuery only returns String, Char, or Varchar", e.getMessage());
    }
  }

  @Test
  public void listBad() throws HiveException {
    try {
      test(json, "$.sports", wrapInList(Collections.singletonList("a")));
      Assert.fail();
    } catch (UDFArgumentException e) {
      Assert.assertEquals("org.apache.hadoop.hive.ql.udf.generic.GenericUDFJsonQuery only returns String, Char, or Varchar", e.getMessage());
    }
  }

  @Test
  public void list() throws HiveException {
    ObjectPair<ObjectInspector, Object[]> results = test(json, "$.sports");
    Assert.assertEquals(4, results.getSecond().length);
    Assert.assertTrue(results.getFirst() instanceof StringObjectInspector);
    StringObjectInspector soi = (StringObjectInspector)results.getFirst();
    Assert.assertEquals("[\"baseball\",\"soccer\"]", soi.getPrimitiveJavaObject(results.getSecond()[0]));
    Assert.assertEquals("[\"basketball\"]", soi.getPrimitiveJavaObject(results.getSecond()[1]));
    Assert.assertNull(soi.getPrimitiveJavaObject(results.getSecond()[2]));
    Assert.assertNull(soi.getPrimitiveJavaObject(results.getSecond()[3]));
  }

  @Test
  public void nested() throws HiveException {
    ObjectPair<ObjectInspector, Object[]> results = test(json, "$.nested");
    Assert.assertEquals(4, results.getSecond().length);
    Assert.assertTrue(results.getFirst() instanceof StringObjectInspector);
    StringObjectInspector soi = (StringObjectInspector)results.getFirst();
    Assert.assertNull(soi.getPrimitiveJavaObject(results.getSecond()[0]));
    Assert.assertNull(soi.getPrimitiveJavaObject(results.getSecond()[1]));
    Assert.assertNull(soi.getPrimitiveJavaObject(results.getSecond()[2]));
    Assert.assertEquals("{\"nestedlist\":[{\"anothernest\":[10,100,1000]}]}", soi.getPrimitiveJavaObject(results.getSecond()[3]));
  }

  @Override
  protected GenericUDFJsonValue getUdf() {
    return new GenericUDFJsonQuery();
  }
}
