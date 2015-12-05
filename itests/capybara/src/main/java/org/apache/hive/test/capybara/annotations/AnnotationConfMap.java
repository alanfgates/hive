/**
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
package org.apache.hive.test.capybara.annotations;

import java.util.HashMap;
import java.util.Map;

/**
 * A helper class to map various annotations to the associated parameters.  Ideally this would be
 * done via a member in each annotation, but thanks to JDK-8013485 that doesn't work consistently.
 */
public class AnnotationConfMap {

  private static Map<String, Map<String, String>> settings;

  static {
    settings = new HashMap<>();

    // ACID properties
    settings.put("AcidOn", mapMaker(
        "hive.support.concurrency", "true",
        "hive.txn.manager", "org.apache.hadoop.hive.ql.lockmgr.DbTxnManager",
        "hive.enforce.bucketing", "true"));


    // SQL Standard Auth properties
    settings.put("SqlStdAuthOn", mapMaker(
        "hive.test.authz.sstd.hs2.mode", "true",
        "hive.security.authorization.manager",
            "org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactoryForTest",
        "hive.security.authenticator.manager",
            "org.apache.hadoop.hive.ql.security.SessionStateConfigUserAuthenticator"));

    // Vectorization properties
    settings.put("VectorOn", mapMaker(
        "hive.vectorized.execution.enabled", "true"));
  }

  private static Map<String, String> mapMaker(String... properties) {
    assert properties.length % 2 == 0;
    Map<String, String> map = new HashMap<>(properties.length / 2);
    for (int i = 0; i < properties.length; i += 2) {
      map.put(properties[i], properties[i + 1]);
    }
    return map;
  }
}
