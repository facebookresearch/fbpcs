/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

package com.facebook.business.cloudbridge.pl.server;

import com.amazonaws.regions.Regions;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DeploymentParams {
  public String region;
  public String accountId;
  public String pubAccountId;
  public String vpcId;
  public String configStorage;
  public String dataStorage;
  public String tag;
  public boolean enableSemiAutomatedDataIngestion;
  public LogLevel logLevel;

  public String awsAccessKeyId;
  public String awsSecretAccessKey;

  private final Logger logger = LoggerFactory.getLogger(DeploymentParams.class);

  enum LogLevel {
    DISABLED("Disabled"),
    ERROR("ERROR"),
    WARNING("WARN"),
    INFORMATION("INFO"),
    DEBUG("DEBUG"),
    TRACE("TRACE");

    private final String level;

    LogLevel() {
      this.level = "DEBUG";
    }

    LogLevel(final String level) {
      this.level = level;
    }

    public String getLevel() {
      return this.level;
    }

    @JsonCreator(mode = JsonCreator.Mode.DELEGATING)
    public static LogLevel getLogLevelFromName(@JsonProperty("logLevel") String level) {
      for (LogLevel l : LogLevel.values()) {
        if (l.getLevel().equals(level)) {
          return l;
        }
      }
      return DEBUG;
    }
  }

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
  private boolean validAccountID(String id) {
    return id != null && id.matches("^\\d{12}$");
  }

  public boolean validPubAccountID() {
    return validAccountID(pubAccountId);
  }

  public boolean validAccountID() {
    return validAccountID(accountId);
  }

  // Amazon VPC ID identifier format can be found in
  // https://aws.amazon.com/about-aws/whats-new/2018/02/longer-format-resource-ids-are-now-available-in-amazon-ec2/
  public boolean validVpcID() {
    return vpcId != null && !vpcId.isEmpty() && vpcId.matches("^vpc-[a-z0-9]{17}$");
  }

  public boolean validConfigStorage() {
    return validBucketID(configStorage);
  }

  public boolean validDataStorage() {
    return validBucketID(dataStorage);
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
    if (!validAccountID()) {
      logAndThrow("Invalid Account ID: " + accountId);
    }
    if (!validPubAccountID()) {
      logAndThrow("Invalid Publisher Account ID: " + pubAccountId);
    }
    if (!validVpcID()) {
      logAndThrow("Invalid VPC ID: " + vpcId);
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
            .append(", Account ID: ")
            .append(accountId)
            .append(", Publisher Account ID: ")
            .append(pubAccountId)
            .append(", VPC ID: ")
            .append(vpcId)
            .append(", Configuration Storage: ")
            .append(configStorage)
            .append(", Data Storage: ")
            .append(dataStorage)
            .append(", Tag: ")
            .append(tag)
            .append(", Enable Semi-Automated Data Ingestion: ")
            .append(String.valueOf(enableSemiAutomatedDataIngestion));
    if (logLevel != null) {
      sb.append(", Terraform Log Level: ").append(logLevel);
    }
    sb.append("}");
    return sb.toString();
  }
}
