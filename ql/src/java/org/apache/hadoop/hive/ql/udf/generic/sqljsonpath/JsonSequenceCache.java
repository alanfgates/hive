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


import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Cache parsed bits of JSON to avoid re-parsing a JSON string repeatedly.
 *
 * This cache is intended to address the situation where a user has a table with one JSON column and then runs a:
 * query like
 * "select json_value(json, "$.name") as name, json_value(json, "$.address.street") as street, ..."
 * where he or she is repeatedly pulling values out of the same JSON column.  We don't want to parse that same JSON
 * for every  invocation.  At the same time consider a case query like:
 * "select json_value(jsoncol1, "$.name"), json_value(jsoncol2, "$.name"), ..."
 * Since each json_value invocation is looking at a different column there is no value in caching the resulting
 * JsonSequences.
 *
 * The cache needs to be large enough that it can cache all the parse values in a vector batch, since the first
 * invocation of json_value will process all 1024 entries in the batch before the second invocation sees any of them.
 *
 * Ideally the cache would be smart about what values to cache and what not.  But it's hard to know how to size the
 * cache because each UDF can't see all the others and thus doesn't know who's sharing its input.  Also, multiple
 * queries maybe run at the same time, possibly accessing the same or other values.
 *
 * The compromise solution to this is to build an LRU cache with a very aggressive timeout.  Given that the goal is
 * to preserve the values for the duration of one vector batch even 1 second should be plenty of time in the cache.
 *
 * There is also a cleaner thread that wakes periodically and calls cleanup() to avoid having the cache for the last
 * vector batch in a query sit there unused and hogging memory until some other query calls json_value possibly far in
 * the future.
 */
class JsonSequenceCache {
  private static final Logger LOG = LoggerFactory.getLogger(JsonSequenceCache.class);

  private static final int MAX_CACHE_SIZE = VectorizedRowBatch.DEFAULT_SIZE * 100;
  private static final int INITIAL_CACHE_SIZE = VectorizedRowBatch.DEFAULT_SIZE;
  private static final long MILLISECONDS_TO_LIVE = 1000;
  private static final long CLEAN_INTERVAL = 1000 * 10;

  private static JsonSequenceCache singleton = null;

  static JsonSequenceCache get() {
    if (singleton == null) {
      synchronized (JsonSequenceCache.class) {
        if (singleton == null) {
          singleton = new JsonSequenceCache();
        }
      }
    }
    return singleton;
  }

  private final Cache<ByteArrayWrapper, JsonSequence> cache;
  private final MessageDigest md5;

  private JsonSequenceCache() {
    LOG.debug("Cleaning");
    cache = CacheBuilder
        .newBuilder()
        .expireAfterAccess(MILLISECONDS_TO_LIVE, TimeUnit.MILLISECONDS)
        .initialCapacity(INITIAL_CACHE_SIZE)
        .maximumSize(MAX_CACHE_SIZE)
        .build();

    try {
      md5 = MessageDigest.getInstance("MD5");
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException(e);
    }

    Runnable cleaner = () -> {
      try {
        while (true) {
          Thread.sleep(CLEAN_INTERVAL);
          cache.cleanUp();
        }
      } catch (InterruptedException e) {
        LOG.info("JsonSequenceCache cleaner thread received interrupt, shutting down.");
      }
    };
    new Thread(cleaner).start();
  }

  JsonSequence parse(String jsonStr, final JsonValueParser parser) throws IOException, JsonPathException {
    // I don't use cache.get() because it does the loading (which in this case is the parsing) under the lock,
    // which is bad.  It's better to allow a string to be parsed multiple times then to serialize access to the cache.
    ByteArrayWrapper hash = hash(jsonStr);
    JsonSequence seq = cache.getIfPresent(hash);
    if (seq == null) {
      seq = parser.doParse(jsonStr);
      cache.put(hash, seq);
    }
    return seq;
  }

  private synchronized ByteArrayWrapper hash(String jsonStr) {
    md5.reset();
    return new ByteArrayWrapper(md5.digest(jsonStr.getBytes()));

  }

  // byte[] only matches as a key in the cache if it's the same byte array, so make it actually look at the contents
  // of the array.
  private static class ByteArrayWrapper {
    private final byte[] array;

    ByteArrayWrapper(byte[] array) {
      this.array = array;
    }

    @Override
    public boolean equals(Object o) {
      // Fast and dangerous
      assert o instanceof ByteArrayWrapper;
      return Arrays.equals(array, ((ByteArrayWrapper)o).array);
    }

    @Override
    public int hashCode() {
      int hash = 0;
      switch (array.length) {
        // All fall throughs here intentional.
        default:
        case 4: hash = array[3] << 12;
        case 3: hash += array[2] << 8;
        case 2: hash += array[1] << 4;
        case 1: hash += array[0];
      }
      return hash;
    }
  }

}
