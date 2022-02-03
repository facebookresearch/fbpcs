/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

package com.facebook.business.cloudbridge.pl.server;

import com.fasterxml.jackson.annotation.JsonValue;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DeploymentRunner extends Thread {
  private Logger logger = LoggerFactory.getLogger(DeployController.class);
  private List<String> deployCommand;
  private Map<String, String> environmentVariables;

  private Runnable deploymentFinishedCallback;
  private Process provisioningProcess;
  private BufferedWriter deployLogFile;

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

    if (deploymentState == DeploymentState.STATE_FINISHED) {
      halt();
    }

    return output;
  }

  enum DeploymentState {
    STATE_NOT_STARTED("not started"),
    STATE_RUNNING("running"),
    STATE_FINISHED("finished"),
    STATE_HALTED("halted");

    private String state;

    private DeploymentState(String state) {
      this.state = state;
    }

    @JsonValue
    public String toString() {
      return state;
    }
  };

  private DeploymentState deploymentState;

  public DeploymentState getDeploymentState() {
    return deploymentState;
  }

  public DeploymentRunner(
      boolean shouldDeploy, DeploymentParams deployment, Runnable deploymentFinishedCallback) {

    this.deploymentState = DeploymentState.STATE_NOT_STARTED;
    this.deploymentFinishedCallback = deploymentFinishedCallback;

    buildDeployCommand(shouldDeploy, deployment);
    buildEnvironmentVariables(deployment);

    try {
      deployLogFile = new BufferedWriter(new FileWriter("/tmp/deploy.log", true));
    } catch (IOException e) {
      logger.error("An exception happened preventing the logger to log to file: /tmp/deploy.log");
    }
  }

  private void buildDeployCommand(boolean shouldDeploy, DeploymentParams deployment) {
    deployCommand = new ArrayList<String>();
    deployCommand.add("/bin/bash");
    deployCommand.add("/terraform_deployment/deploy.sh");
    if (shouldDeploy) {
      deployCommand.add("deploy");
    } else {
      deployCommand.add("undeploy");
    }
    deployCommand.add("-r");
    deployCommand.add(deployment.region);
    deployCommand.add("-a");
    deployCommand.add(deployment.accountId);
    deployCommand.add("-p");
    deployCommand.add(deployment.pubAccountId);
    deployCommand.add("-v");
    deployCommand.add(deployment.vpcId);
    deployCommand.add("-t");
    deployCommand.add(deployment.tag);
    if (deployment.configStorage != null) {
      deployCommand.add("-s");
      deployCommand.add(deployment.configStorage);
    }
    if (deployment.dataStorage != null) {
      deployCommand.add("-d");
      deployCommand.add(deployment.dataStorage);
    }
    if (deployment.enableSemiAutomatedDataIngestion) {
      deployCommand.add("-b");
    }
    logger.info("  Deploy command built: " + deployCommand);
  }

  private void buildEnvironmentVariables(DeploymentParams deployment) {
    environmentVariables = new HashMap<String, String>();
    environmentVariables.put("AWS_ACCESS_KEY_ID", deployment.awsAccessKeyId);
    environmentVariables.put("AWS_SECRET_ACCESS_KEY", deployment.awsSecretAccessKey);

    if (deployment.logLevel != DeploymentParams.LogLevel.DISABLED) {
      if (deployment.logLevel == null) deployment.logLevel = DeploymentParams.LogLevel.DEBUG;
      environmentVariables.put("TF_LOG", deployment.logLevel.getLevel());
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
      logger.debug("  Problem reading deployment process logs: " + e.getMessage());
    }
    logger.trace("  Read " + sb.length() + " chars from deployment process logs");

    return sb.toString();
  }

  private void logOutput(String output) {
    synchronized (processOutputMutex) {
      processOutput += output;
    }
    if (deployLogFile != null) {
      try {
        deployLogFile.write(output);
        deployLogFile.flush();
      } catch (IOException e) {
        logger.error("Failed to log to Logger File");
      }
    }
  }

  private void halt() {
    deploymentState = DeploymentState.STATE_HALTED;
    try {
      deployLogFile.close();
    } catch (IOException e) {
      logger.error("Failed to close Logger File");
    }
    logger.info("  Deployment finished");
  }

  public void run() {
    deploymentState = DeploymentState.STATE_RUNNING;
    BufferedReader stdout = null;

    try {
      ProcessBuilder pb = new ProcessBuilder(deployCommand);

      Map<String, String> env = pb.environment();
      env.putAll(environmentVariables);

      pb.redirectErrorStream(true);
      pb.directory(new File("/terraform_deployment"));
      provisioningProcess = pb.start();
      logger.info("  Creating deployment process");

      stdout = new BufferedReader(new InputStreamReader(provisioningProcess.getInputStream()));

      while (provisioningProcess.isAlive()) {
        logOutput(readOutput(stdout));

        try {
          Thread.sleep(500);
        } catch (InterruptedException e) {
        }
      }

      exitValue = provisioningProcess.exitValue();
      logger.info("  Deployment process exited with value: " + exitValue);

    } catch (IOException e) {
      logger.error("  Deployment could not be started. Message: " + e.getMessage());
      throw new DeploymentException("Deployment could not be started");
    } finally {
      if (stdout != null) {
        logOutput(readOutput(stdout));
      }

      deploymentFinishedCallback.run();
      deploymentState = DeploymentState.STATE_FINISHED;
    }
  }
}
