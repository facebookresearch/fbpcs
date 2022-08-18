/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

package com.facebook.business.cloudbridge.pl.server.command;

import com.facebook.business.cloudbridge.pl.server.DeployController;
import java.io.*;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ShellCommandHandler implements ShellCommandRunner {
  private final Logger logger = LoggerFactory.getLogger(DeployController.class);
  private final Integer PROCESS_SLEEP_TIME_IN_MS = 500;
  private String name;
  private BufferedWriter logStreamer;

  public ShellCommandHandler(final String name) {
    this.name = name;
  }

  private String formatLog(final String log) {
    return this.name + " : " + log;
  }

  private String readOutput(BufferedReader stdout) {
    final StringBuilder sb = new StringBuilder();
    try {
      String logLine = null;
      while (stdout.ready() && (logLine = stdout.readLine()) != null) {
        sb.append(logLine);
        sb.append('\n');
        logger.info(logLine);
      }
    } catch (final IOException e) {
      logger.debug(formatLog("Problem reading deployment process logs: ") + e.getMessage());
    }
    return sb.toString();
  }

  private void logOutput(String output) {
    if (logStreamer != null) {
      try {
        logStreamer.write(output);
        logStreamer.flush();
      } catch (final Exception e) {
        logger.error(formatLog("Failed to write to logStream: ") + e.getMessage());
      }
    }
  }

  private String createFilePath(
      final String commandOutputDirectoryPath, final String commandOutputFileName) {
    return commandOutputDirectoryPath + "/" + commandOutputFileName;
  }

  private void ensureFileOpen(final String outputFilePath) throws IOException {
    try {
      logStreamer = new BufferedWriter(new FileWriter(outputFilePath, true));
    } catch (final Exception e) {
      logger.error(formatLog("An exception happened: ") + e.getMessage());
      throw e;
    }
  }

  @Override
  public CommandRunnerResult run(
      final List<String> commandToRun,
      final Map<String, String> commandEnvironment,
      final String commandOutputDirectoryPath,
      final String commandOutputFileName) {

    try {
      ensureFileOpen(createFilePath(commandOutputDirectoryPath, commandOutputFileName));
      final ProcessBuilder processBuilder = new ProcessBuilder(commandToRun);
      processBuilder.environment().putAll(commandEnvironment);
      processBuilder.directory(new File(commandOutputDirectoryPath));

      processBuilder.redirectErrorStream(true);
      final Process provisioningProcess = processBuilder.start();

      final BufferedReader stdout =
          new BufferedReader(new InputStreamReader(provisioningProcess.getInputStream()));

      while (provisioningProcess.isAlive()) {
        logOutput(readOutput(stdout));
        try {
          Thread.sleep(PROCESS_SLEEP_TIME_IN_MS);
        } catch (final InterruptedException e) {
          logger.error(formatLog(" exited with exception: ") + e.getMessage());
        }
      }
      logger.info(formatLog(" exited with value: ") + provisioningProcess.exitValue());
      return new CommandRunnerResult(provisioningProcess.exitValue());
    } catch (final Exception e) {
      logger.error(formatLog(" exited with exception: ") + e.getMessage());
    }

    return new CommandRunnerResult(1);
  }
}
