/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

package com.facebook.business.cloudbridge.pl.server.deployment.controller;

import com.facebook.business.cloudbridge.pl.server.DeploymentParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class DeploymentController {
  private final Logger logger =
      LoggerFactory.getLogger(com.facebook.business.cloudbridge.pl.server.DeployController.class);

  // get

  @GetMapping(path = "/v1/pl/status", produces = "application/json")
  public void deploymentStatus() {}

  @GetMapping(path = "/v1/pl/healthCheck", produces = "application/json")
  public void checkHealth() {
    logger.info("checkHealth: Received status request");
  }

  @GetMapping(path = "/v1/pl/logs")
  public void downloadDeploymentLogs() {
    //
  }

  // post

  @PostMapping(path = "/v1/pl/deploy", consumes = "application/json", produces = "application/json")
  public void deploy(@RequestBody DeploymentParams deployment) {
    logger.info("Received deployment request: " + deployment.toString());
  }

  // delete
  @DeleteMapping(
      path = "/v1/pl/undeploy",
      consumes = "application/json",
      produces = "application/json")
  public void undeploy(@RequestBody DeploymentParams deployment) {
    logger.info("Received un-deployment request: " + deployment.toString());
  }
}
