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
package org.apache.hive.test.capybara.infra;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hive.test.capybara.IntegrationTest;
import org.junit.runners.BlockJUnit4ClassRunner;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.InitializationError;
import org.junit.Assert;

import java.lang.annotation.Annotation;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Runner for integration tests.  This extends the standard junit4 runner, but pays attention to
 * our annotations to weed out some tests.
 */
public class IntegrationRunner extends BlockJUnit4ClassRunner {
  static final private Logger LOG = LoggerFactory.getLogger(IntegrationRunner.class);

  private Map<String, List<Annotation>> testVisibleAnnotations;

  public IntegrationRunner(Class<?> klass) throws InitializationError {
    super(klass);
  }

  @Override
  protected List<FrameworkMethod> computeTestMethods() {
    if (testVisibleAnnotations == null) testVisibleAnnotations = new HashMap<>();

    // Get the list from our parent, then weed out any that don't match what we're currently
    // running.
    List<FrameworkMethod> toReturn = new ArrayList<>();
    List<FrameworkMethod> methods = super.computeTestMethods();
    for (FrameworkMethod method : methods) {
      boolean skipIt = false;
      List<Annotation> annotations = new ArrayList<>();
      annotations.addAll(Arrays.asList(getTestClass().getAnnotations()));
      annotations.addAll(Arrays.asList(method.getAnnotations()));
      for (Annotation annotation : annotations) {
        LOG.trace("Considering annotation " + annotation.annotationType().getSimpleName());
        if ("NoCli".equals(annotation.annotationType().getSimpleName()) &&
                TestConf.access().equals("cli") ||
            "NoJdbc".equals(annotation.annotationType().getSimpleName()) &&
                TestConf.access().equals("jdbc") ||
            "NoNonSecure".equals(annotation.annotationType().getSimpleName()) &&
                TestConf.security().equals("nonsecure") ||
            "NoSecure".equals(annotation.annotationType().getSimpleName()) &&
                TestConf.security().equals("secure") ||
            "NoOrc".equals(annotation.annotationType().getSimpleName()) &&
                TestConf.fileFormat().equals("orc") ||
            "NoParquet".equals(annotation.annotationType().getSimpleName()) &&
                TestConf.fileFormat().equals("parquet") ||
            "NoRcFile".equals(annotation.annotationType().getSimpleName()) &&
                TestConf.fileFormat().equals("rc") ||
            "NoTextFile".equals(annotation.annotationType().getSimpleName()) &&
                TestConf.fileFormat().equals("text") ||
            "NoSpark".equals(annotation.annotationType().getSimpleName()) &&
                TestConf.engine().equals("spark") ||
            "NoTez".equals(annotation.annotationType().getSimpleName()) &&
                TestConf.engine().equals("tez") ||
            "NoRdbmsMetastore".equals(annotation.annotationType().getSimpleName()) &&
                TestConf.metastore().equals("metastore") ||
            "NoHBaseMetastore".equals(annotation.annotationType().getSimpleName()) &&
                TestConf.metastore().equals("hbase") ||
            "RequireCluster".equals(annotation.annotationType().getSimpleName()) &&
                !TestConf.onCluster())  {
          skipIt = true;
          LOG.debug("Skipping test " + method.getName() + " because it is annotated with " +
            annotation.annotationType().getSimpleName());
          break;
        } else {
          LOG.trace("Not skipping test " + method.getName());
          if (annotation.annotationType().getSimpleName().matches(".*On$") ||
              annotation.annotationType().getSimpleName().equals("MetadataOnly")) {
            List<Annotation> a = testVisibleAnnotations.get(method.getName());
            if (a == null) {
              a = new ArrayList<>();
              testVisibleAnnotations.put(method.getName(), a);
            }
            a.add(annotation);
          }
        }
      }
      if (!skipIt) toReturn.add(method);
    }
    if (toReturn.size() == 0) {
      // JUnit complains when we give it no tests.  To avoid this put in a phantom test that does
      // nothing
      try {
        toReturn.add(new FrameworkMethod(IntegrationTest.class.getMethod("phantomTest")));
      } catch (NoSuchMethodException e) {
        throw new RuntimeException("Couldn't find phantomTest!", e);
      }
    }
    return toReturn;
  }

  @Override
  protected Object createTest() throws Exception {
    Object o = super.createTest();
    if (!(o instanceof IntegrationTest)) {
      throw new RuntimeException("Unexpected test type " + o.getClass().getName());
    }
    Assert.assertNotNull(testVisibleAnnotations);
    ((IntegrationTest)o).setAnnotations(testVisibleAnnotations);
    return o;
  }


}
