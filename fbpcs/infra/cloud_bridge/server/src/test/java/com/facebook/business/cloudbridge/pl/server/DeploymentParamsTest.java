/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

package com.facebook.business.cloudbridge.pl.server;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import org.junit.Before;
import org.junit.Test;

public class DeploymentParamsTest {
  private DeploymentParams deploymentParams = null;

  @Before
  public void setUp() {
    DeploymentParams deploymentParams = new DeploymentParams();
    deploymentParams.region = "us-west-2";
    deploymentParams.accountId = "123456789012";
    deploymentParams.pubAccountId = "012345678901";
    deploymentParams.vpcId = "vpc-0123456789abcdefg";
    deploymentParams.configStorage = "test-bucket-1";
    deploymentParams.dataStorage = "test-bucket-2";
    deploymentParams.tag = "tag-postfix-123";
    deploymentParams.enableSemiAutomatedDataIngestion = true;
    deploymentParams.logLevel = LogLevel.DEBUG;
    deploymentParams.awsAccessKeyId = "awsAccessKeyId1";
    deploymentParams.awsSecretAccessKey = "awsSecretAccessKey2";
    deploymentParams.awsSessionToken = "awsSessionToken3";

    this.deploymentParams = deploymentParams;
  }

  @Test
  public void testLogLevel() {
    String logLevelString = "DEBUG";
    LogLevel expectedLogLevel = LogLevel.DEBUG;

    LogLevel actualLogLevel = LogLevel.getLogLevelFromName(logLevelString);

    assertEquals(actualLogLevel, expectedLogLevel);
  }

  @Test
  public void testValidateWhenAllFieldsAreValid() {
    assertDoesNotThrow(deploymentParams::validate);
  }

  @Test
  public void testValidateWhenValidationErrors() {
    String region = deploymentParams.region;
    deploymentParams.region = "";
    assertThrows(InvalidDeploymentArgumentException.class, deploymentParams::validate);
    deploymentParams.region = region;

    String accountId = deploymentParams.accountId;
    deploymentParams.accountId = "";
    assertThrows(InvalidDeploymentArgumentException.class, deploymentParams::validate);
    deploymentParams.accountId = accountId;

    String pubAccountId = deploymentParams.pubAccountId;
    deploymentParams.pubAccountId = "";
    assertThrows(InvalidDeploymentArgumentException.class, deploymentParams::validate);
    deploymentParams.pubAccountId = pubAccountId;

    String vpcId = deploymentParams.vpcId;
    deploymentParams.vpcId = "";
    assertThrows(InvalidDeploymentArgumentException.class, deploymentParams::validate);
    deploymentParams.vpcId = vpcId;

    String configStorage = deploymentParams.configStorage;
    deploymentParams.configStorage = "";
    assertThrows(InvalidDeploymentArgumentException.class, deploymentParams::validate);
    deploymentParams.configStorage = configStorage;

    String dataStorage = deploymentParams.dataStorage;
    deploymentParams.dataStorage = "";
    assertThrows(InvalidDeploymentArgumentException.class, deploymentParams::validate);
    deploymentParams.dataStorage = dataStorage;

    String tag = deploymentParams.tag;
    deploymentParams.tag = "";
    assertThrows(InvalidDeploymentArgumentException.class, deploymentParams::validate);
    deploymentParams.tag = tag;

    String awsAccessKeyId = deploymentParams.awsAccessKeyId;
    deploymentParams.awsAccessKeyId = "";
    assertThrows(InvalidDeploymentArgumentException.class, deploymentParams::validate);
    deploymentParams.awsAccessKeyId = awsAccessKeyId;

    String awsSecretAccessKey = deploymentParams.awsSecretAccessKey;
    deploymentParams.awsSecretAccessKey = "";
    assertThrows(InvalidDeploymentArgumentException.class, deploymentParams::validate);
    deploymentParams.awsSecretAccessKey = awsSecretAccessKey;
  }

  @Test
  public void testToString() {
    boolean enabledSemiAutoValue = deploymentParams.enableSemiAutomatedDataIngestion;
    String regexTemplate = ".*%s.*";
    String stringValue = deploymentParams.toString();

    assertTrue(stringValue.matches(String.format(regexTemplate, deploymentParams.region)));
    assertTrue(stringValue.matches(String.format(regexTemplate, deploymentParams.accountId)));
    assertTrue(stringValue.matches(String.format(regexTemplate, deploymentParams.pubAccountId)));
    assertTrue(stringValue.matches(String.format(regexTemplate, deploymentParams.vpcId)));
    assertTrue(stringValue.matches(String.format(regexTemplate, deploymentParams.configStorage)));
    assertTrue(stringValue.matches(String.format(regexTemplate, deploymentParams.dataStorage)));
    assertTrue(stringValue.matches(String.format(regexTemplate, deploymentParams.tag)));
    assertTrue(stringValue.matches(String.format(regexTemplate, enabledSemiAutoValue)));
    assertTrue(stringValue.matches(String.format(regexTemplate, deploymentParams.logLevel)));
  }
}
