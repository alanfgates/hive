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
 */
package org.apache.hive.test.capybara.qconverted;

import org.apache.hive.test.capybara.IntegrationTest;
import org.apache.hive.test.capybara.iface.TestTable;
import org.apache.hive.test.capybara.infra.StaticDataGenerator;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class TestUnicode extends IntegrationTest {

  @Test
  public void unicodeData() throws Exception {
    TestTable uni = TestTable.getBuilder("unitable")
        .addCol("a", "varchar(20)")
        .addCol("b", "char(25)")
        .addCol("c", "string")
        .build();
    uni.create();

    List<String> data = Arrays.asList(
        "aáâàäbcçdeéêèf,ghiíìjklmnoóôòpqrs,tuúûùüvwxyz",
        "AÁÂÀÄBCÇDEÉÊÈF,GHIÍÌJKLMNOÓÔÒPQRS,TUÚÛÙÜVWXYZ",
        "αβγδεζηθ,ικλμνξοπρσ,τυφχψω",
        "ΑΒΓΔΕΖΗΘ,ΙΚΛΜΝΞΟΠΡΣ,ΤΥΦΧΨΩ");
    StaticDataGenerator generator = new StaticDataGenerator(data, ",");
    uni.populate(generator);

    runQuery("select * from unitable");
    sortAndCompare();

    // Force a job rather than just a read from HDFS
    runQuery("select a, b, c, count(*) from unitable group by a, b, c");
    sortAndCompare();
  }
}
