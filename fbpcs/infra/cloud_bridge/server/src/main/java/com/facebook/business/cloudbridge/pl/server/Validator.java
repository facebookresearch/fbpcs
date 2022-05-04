/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

package com.facebook.business.cloudbridge.pl.server;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.DescribeVpcsResult;
import com.amazonaws.services.securitytoken.AWSSecurityTokenService;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder;
import com.amazonaws.services.securitytoken.model.GetCallerIdentityRequest;
import com.amazonaws.services.securitytoken.model.GetCallerIdentityResult;
import com.amazonaws.services.servicequotas.AWSServiceQuotas;
import com.amazonaws.services.servicequotas.AWSServiceQuotasClient;
import com.amazonaws.services.servicequotas.model.GetServiceQuotaRequest;
import com.amazonaws.services.servicequotas.model.ServiceQuota;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Validator {
  private final Logger logger = LoggerFactory.getLogger(Validator.class);
  private static final String VPC_QUOTA_CODE = "L-F678F1CE";
  private static final String VPC_SERVICE_CODE = "vpc";

  public class ValidatorResult {
    public boolean isSuccessful;
    public String message;

    ValidatorResult(boolean isSuccessful, String message) {
      this.isSuccessful = isSuccessful;
      this.message = message;
    }
  }

  private AWSStaticCredentialsProvider getCredentials(DeploymentParams deploymentParams) {
    return new AWSStaticCredentialsProvider(
        new BasicAWSCredentials(
            deploymentParams.awsAccessKeyId, deploymentParams.awsSecretAccessKey));
  }

  private Integer countCurrentVPCInRegion(DeploymentParams deploymentParams) {
    AmazonEC2 ec2Client =
        AmazonEC2Client.builder()
            .withRegion(deploymentParams.region)
            .withCredentials(getCredentials(deploymentParams))
            .build();
    DescribeVpcsResult result = ec2Client.describeVpcs();
    return result.getVpcs().size();
  }

  private ValidatorResult validateVpcQuotaPerRegion(DeploymentParams deploymentParams) {
    final AWSServiceQuotas serviceQuotaClient =
        AWSServiceQuotasClient.builder()
            .withRegion(deploymentParams.region)
            .withCredentials(getCredentials(deploymentParams))
            .build();

    final ServiceQuota quota =
        serviceQuotaClient
            .getServiceQuota(
                new GetServiceQuotaRequest()
                    .withQuotaCode(VPC_QUOTA_CODE)
                    .withServiceCode(VPC_SERVICE_CODE))
            .getQuota();
    final Integer currentVPCsInThisRegion = countCurrentVPCInRegion(deploymentParams);
    logger.info(
        "current vpc count : "
            + currentVPCsInThisRegion
            + " , max VPC allowed: "
            + quota.getValue());

    if (currentVPCsInThisRegion >= quota.getValue()) {
      return new ValidatorResult(
          false,
          "VPC limit is at peek in this region, please choose another region or delete some unused VPC to continue deploy");
    }

    return new ValidatorResult(true, "VPC limit validation successful");
  }

  private String getAccountIDUsingAccessKey(DeploymentParams deploymentParams) {
    AWSSecurityTokenService stsService =
        AWSSecurityTokenServiceClientBuilder.standard()
            .withCredentials(getCredentials(deploymentParams))
            .withRegion(deploymentParams.region)
            .build();
    try {
      GetCallerIdentityResult callerIdentity =
          stsService.getCallerIdentity(new GetCallerIdentityRequest());
      logger.info("account info: " + callerIdentity.getAccount());
      return callerIdentity.getAccount();
    } catch (final Exception e) {
      logger.error("Got exception while verifying credentials " + e.getMessage());
      return null;
    }
  }

  private ValidatorResult validateCredentials(DeploymentParams deploymentParams) {
    final String accountIdFromAws = getAccountIDUsingAccessKey(deploymentParams);
    if (accountIdFromAws == null) {
      logger.error("Invalid credentials received");
      return new ValidatorResult(false, "The AWS access key or secret key provided is invalid");
    }

    if (!accountIdFromAws.equals(deploymentParams.accountId)) {
      logger.error(
          "Invalid credentials received vs provided, received from AWS: "
              + accountIdFromAws
              + " provided by user: "
              + deploymentParams.accountId);
      return new ValidatorResult(
          false, "The AWS account id provided doesn't match with the AWS credentials provided");
    }
    return new ValidatorResult(true, "credentials provides are valid");
  }

  public ValidatorResult validate(DeploymentParams deploymentParams) {
    final ValidatorResult credentialsValidationResult = validateCredentials(deploymentParams);
    if (!credentialsValidationResult.isSuccessful) {
      return credentialsValidationResult;
    }

    final ValidatorResult vpcValidationResult = validateVpcQuotaPerRegion(deploymentParams);
    if (!vpcValidationResult.isSuccessful) {
      return vpcValidationResult;
    }
    // TODO S3 bucket limit validation T119080329
    return new ValidatorResult(true, "No pre validation issues found so far!");
  }
}
