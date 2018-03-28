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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

class DockerClient {
  private static final Logger LOG = LoggerFactory.getLogger(DockerTest.class);
  private static final String CONTAINER_BASE = "hive-dtest-";
  private static final String IMAGE_BASE = "hive-dtest-image-";

  private final int runNumber;

  DockerClient(int runNumber) {
    this.runNumber = runNumber;
  }

  /**
   * Build an image
   * @param dir directory to build in, must either be absolute path or relative to CWD of the
   *            process.
   * @param toWait how long to wait for this command
   * @param unit toWait is measured in
   * @throws IOException if the image fails to build
   */
  void buildImage(String dir, long toWait, TimeUnit unit) throws IOException {
    long seconds = TimeUnit.SECONDS.convert(toWait, unit);
    LOG.info("=====================================================\n");
    LOG.info("Building image");
    ProcessResults res = runProcess(seconds, "docker", "build", "--tag", imageName(), dir);
    LOG.info("Stdout from image build: <\n" + res.stdout + "\n>");
    LOG.info("Stderr from image build: <\n" + res.stderr + "\n>");
    if (res.rc != 0) {
      throw new RuntimeException("Failed to build image, see logs for error message: " + res.stderr);
    }
  }

  /**
   * Run a container and return a string containing the logs
   * @param toWait how long to wait for this run
   * @param unit toWait is measured in
   * @param cmd command to run along with any arguments
   * @return results from the container
   * @throws IOException if the container fails to run
   */
  ContainerResult runContainer(long toWait, TimeUnit unit, MvnCommand cmd) throws IOException {
    List<String> runCmd = new ArrayList<>();
    String containerName = createContainerName(cmd.uniqueName());
    runCmd.addAll(Arrays.asList("docker", "run", "--name", containerName, imageName()));
    runCmd.addAll(Arrays.asList(cmd.buildCommand()));
    long seconds = TimeUnit.SECONDS.convert(toWait, unit);
    ProcessResults res = runProcess(seconds, runCmd.toArray(new String[runCmd.size()]));
    return new ContainerResult(containerName, res.rc, res.stdout);
  }

  private String imageName() {
    return IMAGE_BASE + runNumber;
  }

  private String createContainerName(String name) {
    return CONTAINER_BASE + runNumber + "_" + name;
  }

  private static class ProcessResults {
    final String stdout;
    final String stderr;
    final int rc;

    public ProcessResults(String stdout, String stderr, int rc) {
      this.stdout = stdout;
      this.stderr = stderr;
      this.rc = rc;
    }
  }

  private ProcessResults runProcess(long secondsToWait, String... cmd) throws IOException {
    LOG.info("Going to run: " + StringUtils.join(cmd, " ") + "\n");
    Process proc = Runtime.getRuntime().exec(cmd);
    AtomicBoolean running = new AtomicBoolean(true);
    Pumper stdout = new Pumper(running, proc.getInputStream());
    Pumper stderr = new Pumper(running, proc.getErrorStream());
    new Thread(stdout).start();
    new Thread(stderr).start();
    try {
      if (!proc.waitFor(secondsToWait, TimeUnit.SECONDS)) {
        throw new RuntimeException("Process " + cmd[0] + " failed to run in " + secondsToWait +
            " seconds");
      }
    } catch (InterruptedException e) {
      throw new IOException(e);
    } finally {
      running.set(false);
    }
    return new ProcessResults(stdout.getOutput(), stderr.getOutput(), proc.exitValue());
  }

  private static class Pumper implements Runnable {
    private final AtomicBoolean keepGoing;
    private final BufferedReader reader;
    private final StringBuilder buffer;

    public Pumper(AtomicBoolean keepGoing, InputStream input) {
      this.keepGoing = keepGoing;
      reader = new BufferedReader(new InputStreamReader(input));
      buffer = new StringBuilder();
    }

    public String getOutput() {
      return buffer.toString();
    }

    @Override
    public void run() {
      try {
        while (keepGoing.get()) {
          if (reader.ready()) {
            String s = reader.readLine();
            LOG.info(s);
            buffer.append(s).append('\n');
          } else {
            Thread.sleep(1000);
          }
        }
      } catch (Exception e) {
        LOG.error("Caught exception while puming stream", e);
      }

    }
  }
}
