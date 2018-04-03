/*
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
package org.apache.hive.testutils.dtest;

import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class TestDockerTest {

  private static boolean imageBuilt, hadTimeouts, runSucceeded;
  private static int succeeded;
  private static List<String> failures;
  private static List<String> errors;

  static class MySuccessfulClientFactory extends ContainerClientFactory {
    @Override
    public ContainerClient getClient(int buildNum) {
      return new ContainerClient() {
        @Override
        public void buildImage(String dir, long toWait, TimeUnit unit) throws IOException {
          imageBuilt = true;
        }

        @Override
        public ContainerResult runContainer(long toWait, TimeUnit unit, ContainerCommand cmd) throws
            IOException {
          String logs = "Ran: " + StringUtils.join(cmd.shellCommand(), " ") +
              TestSimpleResultAnalyzer.LOG1;
          return new ContainerResult(cmd.containerName(), 0, logs);
        }
      };
    }
  }

  static class MySuccessfulWithFailingTestsClientFactory extends ContainerClientFactory {
    @Override
    public ContainerClient getClient(int buildNum) {
      return new ContainerClient() {
        @Override
        public void buildImage(String dir, long toWait, TimeUnit unit) throws IOException {
          imageBuilt = true;
        }

        @Override
        public ContainerResult runContainer(long toWait, TimeUnit unit, ContainerCommand cmd) throws
            IOException {
          String logs = "Ran: " + StringUtils.join(cmd.shellCommand(), " ") +
              TestSimpleResultAnalyzer.LOG2;
          return new ContainerResult(cmd.containerName(), 0, logs);
        }
      };
    }
  }

  static class MyTimingOutClientFactory extends ContainerClientFactory {
    @Override
    public ContainerClient getClient(int buildNum) {
      return new ContainerClient() {
        @Override
        public void buildImage(String dir, long toWait, TimeUnit unit) throws IOException {
          imageBuilt = true;
        }

        @Override
        public ContainerResult runContainer(long toWait, TimeUnit unit, ContainerCommand cmd) throws
            IOException {
          String logs = "Ran: " + StringUtils.join(cmd.shellCommand(), " ") +
              TestSimpleResultAnalyzer.LOG3;
          return new ContainerResult(cmd.containerName(), 0, logs);
        }
      };
    }
  }

  static class MyFailingClientFactory extends ContainerClientFactory {
    @Override
    public ContainerClient getClient(int buildNum) {
      return new ContainerClient() {
        @Override
        public void buildImage(String dir, long toWait, TimeUnit unit) throws IOException {
          imageBuilt = true;
        }

        @Override
        public ContainerResult runContainer(long toWait, TimeUnit unit, ContainerCommand cmd) throws
            IOException {
          String logs = "Ran: " + StringUtils.join(cmd.shellCommand(), " ") +
              TestSimpleResultAnalyzer.LOG1;
          return new ContainerResult(cmd.containerName(), 130, logs);
        }
      };
    }
  }


  static class MyCommandFactory extends ContainerCommandFactory {
    @Override
    public List<ContainerCommand> getContainerCommands(String baseDir) throws IOException {
      return Collections.singletonList(new ContainerCommand() {
        @Override
        public String containerName() {
          return "friendly";
        }

        @Override
        public String[] shellCommand() {
          return new String[] {"echo", "hello", "world"};
        }
      });
    }
  }

  static class MyItestCommandFactory extends ContainerCommandFactory {
    @Override
    public List<ContainerCommand> getContainerCommands(String baseDir) throws IOException {
      return Collections.singletonList(new ContainerCommand() {
        @Override
        public String containerName() {
          return "friendly-itests-qtest";
        }

        @Override
        public String[] shellCommand() {
          return new String[] {"echo", "hello", "world"};
        }
      });
    }
  }

  static class MyResultAnalyzerFactory extends ResultAnalyzerFactory {
    @Override
    public ResultAnalyzer getAnalyzer() {
      final SimpleResultAnalyzer contained = new SimpleResultAnalyzer();
      return new ResultAnalyzer() {
        @Override
        public void analyzeLog(ContainerResult result) {
          contained.analyzeLog(result);
        }

        @Override
        public int getSucceeded() {
          succeeded = contained.getSucceeded();
          return succeeded;
        }

        @Override
        public List<String> getFailed() {
          failures = contained.getFailed();
          return failures;
        }

        @Override
        public List<String> getErrors() {
          errors = contained.getErrors();
          return errors;
        }

        @Override
        public boolean hadTimeouts() {
          hadTimeouts = contained.hadTimeouts();
          return hadTimeouts;
        }

        @Override
        public boolean runSucceeded() {
          runSucceeded = contained.runSucceeded();
          return runSucceeded;
        }
      };
    }
  }

  @Before
  public void setup() {
    imageBuilt = false;
    succeeded = 0;
    failures = new ArrayList<>();
    errors = new ArrayList<>();
  }

  @Test
  public void successfulRunAllTestsPass() {
    DockerTest test = new DockerTest();
    test.run(new String[] {"-b", "fakebranch", "-C", MyCommandFactory.class.getName(), "-d",
                           "/tmp", "-F", MySuccessfulClientFactory.class.getName(), "-n", "1", "-R",
                           MyResultAnalyzerFactory.class.getName(), "-r", "fakerepo"});
    Assert.assertTrue(imageBuilt);
    Assert.assertEquals(1, errors.size());
    Assert.assertEquals("TestAcidOnTez.testGetSplitsLocks", errors.get(0));
    Assert.assertEquals(1, failures.size());
    Assert.assertEquals("TestActivePassiveHA.testManualFailover", failures.get(0));
    Assert.assertEquals(32, succeeded);
    Assert.assertFalse(hadTimeouts);
    Assert.assertTrue(runSucceeded);
  }

  @Test
  public void successfulRunSomeTestsFail() {
    DockerTest test = new DockerTest();
    test.run(new String[] {"-b", "fakebranch", "-C", MyItestCommandFactory.class.getName(), "-d",
                           "/tmp", "-F", MySuccessfulWithFailingTestsClientFactory.class.getName(),
                           "-n", "1", "-R", MyResultAnalyzerFactory.class.getName(), "-r", "fakerepo"});
    Assert.assertTrue(imageBuilt);
    Assert.assertEquals(1, errors.size());
    Assert.assertEquals("TestNegativeCliDriver.alter_notnull_constraint_violation", errors.get(0));
    Assert.assertEquals(1, failures.size());
    Assert.assertEquals("TestNegativeCliDriver.alter_table_constraint_duplicate_pk", failures.get(0));
    Assert.assertEquals(72, succeeded);
    Assert.assertFalse(hadTimeouts);
    Assert.assertTrue(runSucceeded);
  }

  @Test
  public void timeout() {
    DockerTest test = new DockerTest();
    test.run(new String[] {"-b", "fakebranch", "-C", MyCommandFactory.class.getName(), "-d",
                           "/tmp", "-F", MyTimingOutClientFactory.class.getName(), "-n", "1", "-R",
                           MyResultAnalyzerFactory.class.getName(), "-r", "fakerepo"});
    Assert.assertTrue(imageBuilt);
    Assert.assertTrue(hadTimeouts);
    Assert.assertTrue(runSucceeded);
  }

  @Test
  public void failedRun() {
    DockerTest test = new DockerTest();
    test.run(new String[] {"-b", "fakebranch", "-C", MyCommandFactory.class.getName(), "-d",
                           "/tmp", "-F", MyFailingClientFactory.class.getName(), "-n", "1", "-R",
                           MyResultAnalyzerFactory.class.getName(), "-r", "fakerepo"});
    Assert.assertTrue(imageBuilt);
    Assert.assertFalse(hadTimeouts);
    Assert.assertFalse(runSucceeded);
  }

}
