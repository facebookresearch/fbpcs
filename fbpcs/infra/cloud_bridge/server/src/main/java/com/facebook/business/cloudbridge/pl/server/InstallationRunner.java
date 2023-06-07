/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

package com.facebook.business.cloudbridge.pl.server;

import com.fasterxml.jackson.annotation.JsonValue;
import java.io.*;
import java.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InstallationRunner extends Thread {
  private Logger logger = LoggerFactory.getLogger(DeployController.class);
  private List<String> installationCommand;
  private Map<String, String> environmentVariables;

  private Runnable installationFinishedCallback;
  private Process provisioningProcess;
  private BufferedWriter installationLogFile;
  private int exitValue;

  public int getExitValue() {
    return exitValue;
  }

  private Object processOutputMutex = new Object();
  private String processOutput = new String();

  public String getOutput() {
    String output;
    synchronized (processOutputMutex) {
      output = processOutput;
      processOutput = new String();
    }

    if (installationState == InstallationState.STATE_FINISHED) {
      halt();
    }

    return output;
  }

  enum InstallationState {
    STATE_NOT_STARTED("not started"),
    STATE_RUNNING("running"),
    STATE_FINISHED("finished"),
    STATE_HALTED("halted");

    private String state;

    private InstallationState(String state) {
      this.state = state;
    }

    @JsonValue
    public String toString() {
      return state;
    }
  };

  private InstallationState installationState;

  public InstallationState getInstallationState() {
    return installationState;
  }

  public InstallationRunner(
      boolean shouldInstall,
      ToolkitInstallationParams installationParams,
      Runnable installationFinishedCallback) {

    this.installationState = InstallationState.STATE_NOT_STARTED;
    this.installationFinishedCallback = installationFinishedCallback;

    buildInstallationCommand(shouldInstall, installationParams);
    buildEnvironmentVariables(installationParams);

    try {
      installationLogFile = new BufferedWriter(new FileWriter("/tmp/deploy.log", true));
    } catch (IOException e) {
      logger.error("An exception happened: ", e.getMessage());
    }
  }

  private void buildInstallationCommand(
      boolean shouldInstall, ToolkitInstallationParams installationParams) {
    installationCommand = new ArrayList<String>();
    installationCommand.add("/bin/bash");
    installationCommand.add("/terraform_deployment/deploy_pc_infra.sh");
    if (shouldInstall) {
      installationCommand.add("deploy");
    } else {
      installationCommand.add("undeploy");
    }
    installationCommand.add("-r");
    installationCommand.add(installationParams.region);
    installationCommand.add("-a");
    installationCommand.add(installationParams.awsAccountId);
    installationCommand.add("-t");
    installationCommand.add(installationParams.tag);
    // always enable semi-automated data ingestion
    installationCommand.add("-b");
    logger.info("  PC toolkit installation command built: " + installationCommand);
  }

  private void buildEnvironmentVariables(ToolkitInstallationParams installationParams) {
    environmentVariables = new HashMap<String, String>();
    environmentVariables.put("AWS_ACCESS_KEY_ID", installationParams.awsAccessKeyId);
    environmentVariables.put("AWS_SECRET_ACCESS_KEY", installationParams.awsSecretAccessKey);
    if (!installationParams.awsSessionToken.isEmpty()) {
      environmentVariables.put("AWS_SESSION_TOKEN", installationParams.awsSessionToken);
    }

    environmentVariables.put("TF_LOG_STREAMING", Constants.DEPLOYMENT_STREAMING_LOG_FILE);
    environmentVariables.put("TF_RESOURCE_OUTPUT", Constants.DEPLOYMENT_RESOURCE_OUTPUT_FILE);
    if (installationParams.logLevel != LogLevel.DISABLED) {
      if (installationParams.logLevel == null) {
        installationParams.logLevel = LogLevel.DEBUG;
      }
      environmentVariables.put("TF_LOG", installationParams.logLevel.getLevel());
      environmentVariables.put("TF_LOG_PATH", "/tmp/terraform.log");
    }
  }

  private String readOutput(BufferedReader stdout) {
    StringBuilder sb = new StringBuilder();
    try {
      String s;
      while (stdout.ready() && (s = stdout.readLine()) != null) {
        sb.append(s);
        sb.append('\n');
      }
    } catch (IOException e) {
      logger.debug("  Problem reading installation process logs: " + e.getMessage());
    }
    logger.trace("  Read " + sb.length() + " chars from installation process logs");

    return sb.toString();
  }

  private void logOutput(String output) {
    synchronized (processOutputMutex) {
      processOutput += output;
    }
    if (installationLogFile != null) {
      try {
        installationLogFile.write(output);
        installationLogFile.flush();
      } catch (IOException e) {
        logger.error("Failed to log to Logger File");
      }
    }
  }

  private void halt() {
    installationState = InstallationState.STATE_HALTED;
    try {
      installationLogFile.close();
    } catch (IOException e) {
      logger.error("Failed to close Logger File");
    }
    logger.info("  Installation finished");
  }

  public void run() {
    installationState = InstallationState.STATE_RUNNING;
    BufferedReader stdout = null;

    try {
      ProcessBuilder pb = new ProcessBuilder(installationCommand);

      Map<String, String> env = pb.environment();
      env.putAll(environmentVariables);

      pb.redirectErrorStream(true);
      pb.directory(new File("/terraform_deployment"));
      provisioningProcess = pb.start();
      logger.info("  Creating installation process");

      stdout = new BufferedReader(new InputStreamReader(provisioningProcess.getInputStream()));

      while (provisioningProcess.isAlive()) {
        logOutput(readOutput(stdout));

        try {
          Thread.sleep(500);
        } catch (InterruptedException e) {
        }
      }

      exitValue = provisioningProcess.exitValue();
      logger.info("  Installation process exited with value: " + exitValue);

    } catch (IOException e) {
      logger.error("  Installation could not be started. Message: " + e.getMessage());
      throw new InstallationException("Installation could not be started");
    } finally {
      if (stdout != null) {
        logOutput(readOutput(stdout));
      }

      installationFinishedCallback.run();
      installationState = InstallationState.STATE_FINISHED;
    }
  }
}
