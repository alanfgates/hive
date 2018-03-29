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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

class DockerClient implements ContainerClient {
  private static final Logger LOG = LoggerFactory.getLogger(DockerClient.class);
  private static final String CONTAINER_BASE = "hive-dtest-";
  private static final String IMAGE_BASE = "hive-dtest-image-";

  private final int runNumber;

  DockerClient(int runNumber) {
    this.runNumber = runNumber;
  }

  @Override
  public void buildImage(String dir, long toWait, TimeUnit unit) throws IOException {
    long seconds = TimeUnit.SECONDS.convert(toWait, unit);
    LOG.info("=====================================================\n");
    LOG.info("Building image");
    ProcessResults res = Utils.runProcess(seconds, "docker", "build", "--tag", imageName(), dir);
    LOG.info("Stdout from image build: <\n" + res.stdout + "\n>");
    LOG.info("Stderr from image build: <\n" + res.stderr + "\n>");
    if (res.rc != 0) {
      throw new RuntimeException("Failed to build image, see logs for error message: " + res.stderr);
    }
  }

  @Override
  public ContainerResult runContainer(long toWait, TimeUnit unit, ContainerCommand cmd)
      throws IOException {
    List<String> runCmd = new ArrayList<>();
    String containerName = createContainerName(cmd.containerName());
    runCmd.addAll(Arrays.asList("docker", "run", "--name", containerName, imageName()));
    runCmd.addAll(Arrays.asList(cmd.shellCommand()));
    long seconds = TimeUnit.SECONDS.convert(toWait, unit);
    ProcessResults res = Utils.runProcess(seconds, runCmd.toArray(new String[runCmd.size()]));
    return new ContainerResult(containerName, res.rc, res.stdout);
  }

  private String imageName() {
    return IMAGE_BASE + runNumber;
  }

  private String createContainerName(String name) {
    return CONTAINER_BASE + runNumber + "_" + name;
  }

}
