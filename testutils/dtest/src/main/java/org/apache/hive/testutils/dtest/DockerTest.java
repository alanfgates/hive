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

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
  private static final Logger LOG = LoggerFactory.getLogger(DockerTest.class);

  private ContainerClient docker;
  private ContainerCommandFactory commandFactory;
  private ResultAnalyzerFactory analyzerFactory;

  @VisibleForTesting
  void run(String[] args) {
    CommandLineParser parser = new GnuParser();

    Options opts = new Options();
    opts.addOption(OptionBuilder
        .withLongOpt("branch")
        .withDescription("git branch to use")
        .isRequired()
        .hasArg()
        .create("b"));

    opts.addOption(OptionBuilder
        .withLongOpt("command-factory")
        .withDescription("Class to build ContainerCommands, defaults to MvnCommandFactory")
        .hasArg()
        .create("C"));

    opts.addOption(OptionBuilder
        .withLongOpt("num-containers")
        .withDescription("number of simultaneous containers to run, defaults to 1")
        .hasArg()
        .create("c"));

    opts.addOption(OptionBuilder
        .withLongOpt("target-directory")
        .withDescription("directory to build dockerfile in")
        .isRequired()
        .hasArg()
        .create("d"));

    opts.addOption(OptionBuilder
        .withLongOpt("container-factory")
        .withDescription("Class to build ContainerClients, defaults to ContainerClientFactory")
        .hasArg()
        .create("F"));

    opts.addOption(OptionBuilder
        .withLongOpt("build-number")
        .withDescription("build number, changing this will force a new container to be built")
        .isRequired()
        .hasArg()
        .create("n"));

    opts.addOption(OptionBuilder
        .withLongOpt("result-analyzer-factory")
        .withDescription("Class to build ResultAnalyzer, default to SimpleResultAnalyzer")
        .hasArg()
        .create("R"));

    opts.addOption(OptionBuilder
        .withLongOpt("repo")
        .withDescription("git repository to use")
        .isRequired()
        .hasArg()
        .create("r"));

    CommandLine cmd;
    try {
      cmd = parser.parse(opts, args);
    } catch (ParseException e) {
      LOG.error("Failed to parse command line: ", e);
      usage(opts);
      return;
    }

    int numContainers = cmd.hasOption("c") ? Integer.parseInt(cmd.getOptionValue("c")) : 1;
    String branch = cmd.getOptionValue("b");
    String dir = cmd.getOptionValue("d");
    String repo = cmd.getOptionValue("r");
    int buildNum = Integer.parseInt(cmd.getOptionValue("n"));

    try {
      ContainerClientFactory containerClientFactory =
          ContainerClientFactory.get(cmd.getOptionValue("F"));
      docker = containerClientFactory.getClient(buildNum);
      commandFactory = ContainerCommandFactory.get(cmd.getOptionValue("C"));
      analyzerFactory = ResultAnalyzerFactory.get(cmd.getOptionValue("R"));
    } catch (IOException e) {
      LOG.error("Failed to instantiate one of the factories", e);
      return;
    }
    try {
      buildDockerImage(dir, repo, branch, buildNum);
    } catch (IOException e) {
      LOG.error("Failed to build docker image, might mean your code doesn't compile", e);
      return;
    }
    try {
      runContainers(dir, numContainers);
    } catch (IOException e) {
      LOG.error("Failed to run one or more of the containers", e);
    }
  }

  private void usage(Options opts) {
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp("docker-test", opts);
  }

  private void buildDockerImage(String dir, String repo, String branch, int buildNumber)
      throws IOException {
    DockerBuilder.createDockerFile(dir, repo, branch, buildNumber);
    docker.buildImage(dir, 30, TimeUnit.MINUTES);
  }

  private void runContainers(String dir, int numContainers) throws IOException {
    List<ContainerCommand> taskCmds = commandFactory.getContainerCommands("/root/hive");

    ResultAnalyzer analyzer = analyzerFactory.getAnalyzer();
    List <Future<ContainerResult>> tasks = new ArrayList<>(taskCmds.size());
    ExecutorService executor = Executors.newFixedThreadPool(numContainers);
    for (ContainerCommand taskCmd : taskCmds) {
      tasks.add(executor.submit(() -> docker.runContainer(3, TimeUnit.HOURS, taskCmd)));
    }

    for (Future<ContainerResult> task : tasks) {
      try {
        ContainerResult result = task.get();
        FileWriter writer = new FileWriter(dir + File.separator + result.name);
        String statusMsg = "Task " + result.name + ((result.rc == 0) ? " Succeeded" : " Failed");
        LOG.info(statusMsg);
        writer.write(statusMsg);
        writer.write(result.logs);
        writer.close();
        analyzer.analyzeLog(result.name, result.logs);
      } catch (InterruptedException e) {
        LOG.error("Interrupted while waiting for containers to finish, assuming I was" +
            " told to quit.", e);
        return;
      } catch (ExecutionException e) {
        LOG.error("Got an exception while running container, that's generally bad", e);
      }
    }
    executor.shutdown();
    String msg = "Final counts: Succeeded: " + analyzer.getSucceeded() + ", Errors: " +
        analyzer.getErrors().size() + ", Failures: " + analyzer.getFailed().size();
    LOG.info("============================================");
    if (analyzer.getFailed().size() > 0) {
      LOG.info("All Failures:");
      for (String failure : analyzer.getFailed()) {
        LOG.info(failure);
      }
    }
    if (analyzer.getErrors().size() > 0) {
      LOG.info("All Errors:");
      for (String error : analyzer.getErrors()) {
        LOG.info(error);
      }
    }
    LOG.info(msg);
    System.out.println(msg);
  }

  public static void main(String[] args) {
    DockerTest test = new DockerTest();
    test.run(args);
  }
}
