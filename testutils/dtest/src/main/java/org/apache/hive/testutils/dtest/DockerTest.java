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

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class DockerTest {
  private DockerClient docker;
  private FileWriter log;

  private void run(String[] args) {
    CommandLineParser parser = new GnuParser();

    Options opts = new Options();
    opts.addOption(OptionBuilder
        .withArgName("b")
        .withLongOpt("branch")
        .withDescription("git branch to use")
        .isRequired()
        .hasArg()
        .create());

    opts.addOption(OptionBuilder
        .withArgName("c")
        .withLongOpt("num-containers")
        .withDescription("number of simultaneous containers to run, defaults to 1")
        .hasArg()
        .create());

    opts.addOption(OptionBuilder
        .withArgName("d")
        .withLongOpt("target-directory")
        .withDescription("directory to build dockerfile in")
        .isRequired()
        .hasArg()
        .create());

    opts.addOption(OptionBuilder
        .withArgName("n")
        .withLongOpt("build-number")
        .withDescription("build number, changing this will force a new container to be built")
        .isRequired()
        .hasArg()
        .create());

    opts.addOption(OptionBuilder
        .withArgName("r")
        .withLongOpt("repo")
        .withDescription("git repository to use")
        .isRequired()
        .hasArg()
        .create());

    CommandLine cmd;
    try {
      cmd = parser.parse(opts, args);
    } catch (ParseException e) {
      System.err.println("Failed to parse command line: " + e.getMessage());
      usage(opts);
      return;
    }

    int numContainers = cmd.hasOption("c") ? Integer.parseInt(cmd.getOptionValue("c")) : 1;
    String branch = cmd.getOptionValue("b");
    String dir = cmd.getOptionValue("d");
    String repo = cmd.getOptionValue("r");
    int buildNum = Integer.parseInt(cmd.getOptionValue("n"));

    try {
      log = new FileWriter(dir + File.pathSeparator + "test.log");
    } catch (IOException e) {
      System.err.println("Unable to open the log file, are you sure you gave me a reasonable " +
          "target directory?  " + e.getMessage());
    }

    docker = new DockerClient(buildNum, log);
    try {
      buildDockerImage(dir, repo, branch);
    } catch (IOException e) {
      System.err.println("Failed to build docker image, might mean your code doesn't compile: " +
          e.getMessage());
    }
    try {
      runContainers(dir, numContainers);
    } catch (IOException e) {
      System.err.println("Failed to run one or more of the containers: " + e.getMessage());
    }
  }

  private void usage(Options opts) {
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp("docker-test", opts);
  }

  private void buildDockerImage(String dir, String repo, String branch) throws IOException {
    DockerBuilder.createDockerFile(dir, repo, branch);
    docker.buildImage(dir, 30, TimeUnit.MINUTES);
  }

  private void runContainers(String dir, int numContainers) throws IOException {
    List<MvnCommand> taskCmds = DockerBuilder.testCommands("/root/hive");

    List <Future<ContainerResult>> tasks = new ArrayList<>(taskCmds.size());
    ExecutorService executor = Executors.newFixedThreadPool(numContainers);
    for (MvnCommand taskCmd : taskCmds) {
      tasks.add(executor.submit(() -> docker.runContainer(3, TimeUnit.HOURS, taskCmd)));
    }

    for (Future<ContainerResult> task : tasks) {
      try {
        ContainerResult result = task.get();
        System.out.println("Task " + result.name + ((result.rc == 0) ? " Succeeded" : " Failed"));
        log.write("=====================================================================\n");
        log.write("Logs from " + result.name + "\n");
        log.write(result.logs);
      } catch (InterruptedException e) {
        System.out.println("Interrupted while waiting for containers to finish, assuming I was" +
            " told to quit.");
        return;
      } catch (ExecutionException e) {
        System.err.println("Got an exception while running container, that's generally bad: " + e
            .getMessage());
      }
    }
  }

  public static void main(String[] args) {
    DockerTest test = new DockerTest();
    test.run(args);
  }
}
