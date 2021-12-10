# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

from __future__ import print_function

# Set up logging
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Import Boto 3 for AWS Glue
import boto3

client = boto3.client("glue")

# Variables for the job:
# same name as the aws_glue_job resource in glue.tf
glueJobName = "TO_BE_UPDATED_DURING_DEPLOYMENT"

# Define Lambda function
def lambda_handler(event, context):

    ### should be one single upload (it should be larger than 1 by default)
    if len(event["Records"]) >= 2:
        logger.info("multiple csv uploaded. please upload only one csv at a time")
    elif len(event["Records"]) == 0:
        logger.info("no csv uploaded to S3. wrong trigger of lambda")
    else:
        record = event["Records"][0]

        ### extract s3 bucket and path
        s3_info = record["s3"]
        logger.info("s3_info: ")
        logger.info(s3_info)
        s3_bucket = s3_info["bucket"]["name"]
        s3_object_key = s3_info["object"]["key"]
        s3_read_path = s3_bucket + "/" + s3_object_key
        s3_write_path = "TO_BE_UPDATED_DURING_DEPLOYMENT"
        logger.info("s3_read_path: " + s3_read_path)
        response = client.start_job_run(
            JobName=glueJobName,
            Arguments={
                "--s3_read_path": s3_read_path,
                "--s3_write_path": s3_write_path,
            },
        )
        logger.info("## STARTED GLUE JOB: " + glueJobName)
        logger.info("## GLUE JOB RUN ID: " + response["JobRunId"])
        return response
