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

import java.io.BufferedReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

class DockerClient {

  private static final String CONTAINER_BASE = "hive-dtest-";
  private static final String IMAGE_BASE = "hive-dtest-image-";

  private final int runNumber;
  private final FileWriter log;

  DockerClient(int runNumber, FileWriter log) {
    this.runNumber = runNumber;
    this.log = log;
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
    long seconds = unit.convert(toWait, TimeUnit.SECONDS);
    log.write("=====================================================\n");
    log.write("Building image");
    ProcessResults res = runProcess(seconds, "docker", "build", "--tag", imageName(), dir);
    log.write(res.stdout);
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
    long seconds = unit.convert(toWait, TimeUnit.SECONDS);
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
    log.write("Going to run: " + StringUtils.join(cmd, " ") + "\n");
    Process proc = Runtime.getRuntime().exec(cmd);
    try {
      if (!proc.waitFor(secondsToWait, TimeUnit.SECONDS)) {
        throw new RuntimeException("Process " + cmd[0] + " failed to run in " + secondsToWait +
            " seconds");
      }
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
    BufferedReader reader = new BufferedReader(new InputStreamReader(proc.getInputStream()));
    final StringBuilder lines = new StringBuilder();
    reader.lines().forEach(s -> lines.append(s).append('\n'));

    reader = new BufferedReader(new InputStreamReader(proc.getErrorStream()));
    final StringBuilder errLines = new StringBuilder();
    reader.lines().forEach(s -> errLines.append(s).append('\n'));
    return new ProcessResults(lines.toString(), errLines.toString(), proc.exitValue());
  }
}
