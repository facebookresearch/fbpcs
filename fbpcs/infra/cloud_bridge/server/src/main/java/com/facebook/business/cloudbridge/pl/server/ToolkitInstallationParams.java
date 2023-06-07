/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

package com.facebook.business.cloudbridge.pl.server;

import com.amazonaws.regions.Regions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ToolkitInstallationParams {
  public String region;
  public String awsAccountId;
  public String configStorage;
  public String dataStorage;
  public String awsAccessKeyId;
  public String awsSecretAccessKey;
  public String awsSessionToken;
  public String tag;
  public LogLevel logLevel;

  private final Logger logger = LoggerFactory.getLogger(DeploymentParams.class);

  public boolean validRegion() {
    try {
      Regions.fromName(region);
    } catch (IllegalArgumentException e) {
      return false;
    }
    return true;
  }

  // Amazon account identifier format can be found in
  // https://docs.aws.amazon.com/general/latest/gr/acct-identifiers.html
  public boolean validAwsAccountID() {
    return awsAccountId != null && awsAccountId.matches("^\\d{12}$");
  }

  public boolean validConfigStorage() {
    return configStorage == null || validBucketID(configStorage);
  }

  public boolean validDataStorage() {
    return dataStorage == null || validBucketID(dataStorage);
  }

  // Amazon S3 Buck identifier format can be found in
  // https://docs.aws.amazon.com/AmazonS3/latest/userguide/bucketnamingrules.html
  // This is not a perfect validation, as it does not test for invalid S3 names formatted as IP
  // addresses. But it's not a hard security issue, rather than a usability one, and theoretically
  // the user would not be able to create a bucket with such a name anyway.
  public boolean validBucketID(String id) {
    return id != null && !id.isEmpty() && id.matches("^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$");
  }

  // Amazon tag identifier format can be found in
  // https://docs.aws.amazon.com/general/latest/gr/aws_tagging.html
  public boolean validTagPostfix() {
    return tag.matches("^([a-z0-9-][a-z0-9-]{1,18}[a-z0-9])$");
  }

  public boolean validAccessKeyID() {
    return awsAccessKeyId != null && !awsAccessKeyId.isEmpty();
  }

  public boolean validSecretAccessKey() {
    return awsSecretAccessKey != null && !awsSecretAccessKey.isEmpty();
  }

  private void logAndThrow(String message) throws InvalidDeploymentArgumentException {
    logger.error("  " + message);
    throw new InvalidDeploymentArgumentException(message);
  }

  public void validate() throws InvalidDeploymentArgumentException {
    if (!validRegion()) {
      logAndThrow("Invalid Region: " + region);
    }
    if (!validAwsAccountID()) {
      logAndThrow("Invalid AWS Account ID: " + awsAccountId);
    }
    if (!validTagPostfix()) {
      logAndThrow(
          "Invalid Tag Postfix: "
              + tag
              + "\nMake sure the tag length is less than 20 characters, and using lowercase letters, numbers and dash only.");
    }
    if (!validConfigStorage()) {
      logAndThrow("Invalid Configuration Storage: " + configStorage);
    }
    if (!validDataStorage()) {
      logAndThrow("Invalid Data Storage: " + dataStorage);
    }
    if (!validAccessKeyID()) {
      logAndThrow("Invalid AWS Access Key ID");
    }
    if (!validSecretAccessKey()) {
      logAndThrow("Invalid AWS Secret Access Key");
    }
  }

  public String toString() {
    StringBuilder sb =
        new StringBuilder()
            .append("{Region: ")
            .append(region)
            .append(", AWS Account ID: ")
            .append(awsAccountId)
            .append(", Configuration Storage: ")
            .append(configStorage)
            .append(", Data Storage: ")
            .append(dataStorage)
            .append(", Tag: ")
            .append(tag);
    if (logLevel != null) {
      sb.append(", Terraform Log Level: ").append(logLevel);
    }
    sb.append("}");
    return sb.toString();
  }

  public DeploymentParams toDeploymentParams() {
    DeploymentParams params = new DeploymentParams();
    params.region = region;
    params.accountId = awsAccountId;
    params.configStorage = configStorage;
    params.dataStorage = dataStorage;
    params.tag = tag;
    params.logLevel = logLevel;
    params.awsAccessKeyId = awsAccessKeyId;
    params.awsSecretAccessKey = awsSecretAccessKey;
    params.awsSessionToken = awsSessionToken;
    return params;
  }
}
