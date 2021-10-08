/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

package com.facebook.business.cloudbridge.pl.server;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class DeployController {
  private Lock singleProvisioningLock = new ReentrantLock();
  private Process provisioningProcess;
  private final String NO_UPDATES_MESSAGE = "No updates";

  private final Logger logger = LoggerFactory.getLogger(DeployController.class);

  enum DeploymentStatusRunning {
    DEPLOYMENT_NOT_STARTED,
    DEPLOYMENT_RUNNING,
    DEPLOYMENT_FINISHED
  };

  class DeploymentStatus {
    DeploymentStatus(DeploymentStatusRunning status, String message) {
      this.status = status;
      this.message = message;
    }

    DeploymentStatus() {
      this.status = DeploymentStatusRunning.DEPLOYMENT_NOT_STARTED;
      this.message = null;
    }

    public DeploymentStatusRunning status;
    public String message;
  }

  private Object singleUpdateMutex = new Object();
  private DeploymentStatus deploymentStatus = new DeploymentStatus();

  enum APIReturnStatus {
    STATUS_SUCCESS,
    STATUS_FAIL,
    STATUS_ERROR
  }

  class DeploymentAPIReturn {
    public APIReturnStatus status;
    public String message;
    public Object data;

    public DeploymentAPIReturn(APIReturnStatus status, String message) {
      this.status = status;
      this.message = message;
      this.data = null;
    }

    public DeploymentAPIReturn(APIReturnStatus status, String message, Object data) {
      this.status = status;
      this.message = message;
      this.data = data;
    }
  }

  @PostMapping(
      path = "/v1/deployment",
      consumes = "application/json",
      produces = "application/json")
  public DeploymentAPIReturn deploymentCreate(@RequestBody DeploymentParams deployment) {
    logger.info("Received deployment request: " + deployment.toString());

    try {
      deployment.validate();
    } catch (InvalidDeploymentArgumentException ex) {
      return new DeploymentAPIReturn(APIReturnStatus.STATUS_FAIL, ex.getMessage());
    }
    logger.info("  Validated input");

    if (singleProvisioningLock.tryLock()) {
      logger.info("  No deployment conflicts found");
      deploymentStatus.status = DeploymentStatusRunning.DEPLOYMENT_RUNNING;
      deploymentStatus.message = null;

      List<String> deployCommand = new ArrayList<String>();
      deployCommand.add("/bin/sh");
      deployCommand.add("/terraform_deployment/deploy.sh");
      deployCommand.add("-r");
      deployCommand.add(deployment.region);
      deployCommand.add("-a");
      deployCommand.add(deployment.accountId);
      deployCommand.add("-p");
      deployCommand.add(deployment.pubAccountId);
      deployCommand.add("-v");
      deployCommand.add(deployment.vpcId);
      deployCommand.add("-s");
      deployCommand.add(deployment.storage);
      deployCommand.add("-d");
      deployCommand.add(deployment.ingestionOutput);
      if (deployment.tag != null && !deployment.tag.isEmpty()) {
        deployCommand.add("-t");
        deployCommand.add(deployment.tag);
      }
      logger.info("  Deploy command built: " + deployCommand);

      try {
        ProcessBuilder pb = new ProcessBuilder(deployCommand);

        Map<String, String> env = pb.environment();
        env.put("AWS_ACCESS_KEY_ID", deployment.awsAccessKeyId);
        env.put("AWS_SECRET_ACCESS_KEY", deployment.awsSecretAccessKey);
        if (deployment.logLevel != DeploymentParams.LogLevel.DISABLED) {
          if (deployment.logLevel == null) deployment.logLevel = DeploymentParams.LogLevel.DEBUG;
          env.put("TF_LOG", deployment.logLevel.getLevel());
          env.put("TF_LOG_PATH", "/tmp/deploy.log");
        }

        pb.redirectErrorStream(true);
        pb.directory(new File("/terraform_deployment"));
        provisioningProcess = pb.start();
        logger.info("  Creating deployment process");
        try {
          provisioningProcess.waitFor(30, TimeUnit.MINUTES);
          logger.info("  Deployment process finished");

          synchronized (singleUpdateMutex) {
            deploymentStatus.status = DeploymentStatusRunning.DEPLOYMENT_FINISHED;
          }
          int exitCode = provisioningProcess.exitValue();
          if (exitCode == 0) {
            logger.info("  Deployment finished successfully");
            return new DeploymentAPIReturn(APIReturnStatus.STATUS_SUCCESS, "Deployed successfully");
          } else {
            logger.error("  Deployment failed with exit code: " + String.valueOf(exitCode));
            return new DeploymentAPIReturn(
                APIReturnStatus.STATUS_ERROR,
                "Deployment failed with exit code: " + String.valueOf(exitCode));
          }
        } catch (InterruptedException e) {
          logger.error("  Deployment timed out. Message: " + e.getMessage());
          return new DeploymentAPIReturn(APIReturnStatus.STATUS_ERROR, "Deployment timed out");
        }
      } catch (IOException e) {
        logger.error("  Deployment could not be started. Message: " + e.getMessage());
        return new DeploymentAPIReturn(APIReturnStatus.STATUS_FAIL, "Could not start deployment");
      } finally {
        if (provisioningProcess != null && provisioningProcess.isAlive())
          provisioningProcess.destroy();
        singleProvisioningLock.unlock();
      }
    } else {
      logger.error("  Another deployment is in progress");
      return new DeploymentAPIReturn(
          APIReturnStatus.STATUS_ERROR, "Another deployment is in progress");
    }
  }

  @GetMapping(path = "/v1/deployment", produces = "application/json")
  public DeploymentStatus deploymentStatus() {
    logger.info("Received status command");
    if (deploymentStatus.status == DeploymentStatusRunning.DEPLOYMENT_NOT_STARTED) {
      logger.info("  No deployment is running");
      return new DeploymentStatus(
          DeploymentStatusRunning.DEPLOYMENT_NOT_STARTED, NO_UPDATES_MESSAGE);
    }

    if (provisioningProcess == null) {
      logger.warn("  Deployment process unavailable, with status: " + deploymentStatus.status);
      deploymentStatus = new DeploymentStatus();
      return deploymentStatus;
    }

    synchronized (singleUpdateMutex) {
      StringBuilder sb = new StringBuilder();
      BufferedReader stdout =
          new BufferedReader(new InputStreamReader(provisioningProcess.getInputStream()));

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

      if (deploymentStatus.status == DeploymentStatusRunning.DEPLOYMENT_RUNNING) {
        deploymentStatus.message = sb.length() == 0 ? NO_UPDATES_MESSAGE : sb.toString();
        return deploymentStatus;
      } else {
        logger.info("  Deployment finished");
        deploymentStatus = new DeploymentStatus();
        provisioningProcess = null;
        return new DeploymentStatus(DeploymentStatusRunning.DEPLOYMENT_FINISHED, sb.toString());
      }
    }
  }
}
