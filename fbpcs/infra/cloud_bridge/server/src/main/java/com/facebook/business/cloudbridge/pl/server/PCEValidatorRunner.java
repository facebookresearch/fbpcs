/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

package com.facebook.business.cloudbridge.pl.server;

import com.facebook.business.cloudbridge.pl.server.command.ShellCommandHandler;
import com.facebook.business.cloudbridge.pl.server.command.ShellCommandRunner;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.lang.NonNull;

public class PCEValidatorRunner {
  private Logger logger = LoggerFactory.getLogger(DeployController.class);
  private ShellCommandRunner shellCommandRunner = new ShellCommandHandler("pceValidator");

  private List<String> buildPCEValidateCommand(final @NonNull DeploymentParams deployment) {
    final Stream.Builder<String> commandBuilder = Stream.builder();
    final Stream<String> commandStream =
        commandBuilder
            .add("/bin/bash")
            .add("/terraform_deployment/pceValidator.sh")
            .add("validate_pce_2")
            .add(deployment.region)
            .add(deployment.tag)
            .build();

    return commandStream.collect(Collectors.toList());
  }

  private Map<String, String> buildEnvironmentVariables(
      final @NonNull DeploymentParams deployment) {
    final Map<String, String> environmentVariables = new HashMap<String, String>();
    environmentVariables.put("AWS_ACCESS_KEY_ID", deployment.awsAccessKeyId);
    environmentVariables.put("AWS_SECRET_ACCESS_KEY", deployment.awsSecretAccessKey);
    if (!deployment.awsSessionToken.isEmpty()) {
      environmentVariables.put("AWS_SESSION_TOKEN", deployment.awsSessionToken);
    }
    environmentVariables.put("PCE_VALIDATOR_LOG_FILE", Constants.PCE_VALIDATOR_LOG_STREAMING);
    return environmentVariables;
  }

  public Integer start(final @NonNull DeploymentParams deployment) {
    final ShellCommandRunner.CommandRunnerResult result =
        shellCommandRunner.run(
            buildPCEValidateCommand(deployment),
            buildEnvironmentVariables(deployment),
            "/terraform_deployment",
            "pceValidator.log");
    return result.getExitCode();
  }
}
