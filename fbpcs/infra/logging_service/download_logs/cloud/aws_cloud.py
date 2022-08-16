# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

import logging
import os
from typing import Any, Dict, List, Optional

import boto3
import botocore
from botocore.exceptions import ClientError, NoCredentialsError, NoRegionError

from fbpcs.infra.logging_service.download_logs.cloud.cloud_baseclass import (
    CloudBaseClass,
)
from tqdm import tqdm

# TODO: Convert this to factory
class AwsCloud(CloudBaseClass):
    """
    Class AwsCloud verifies the credentials needed to call the boto3 APIs
    """

    DEFAULT_RETRIES_LIMIT = 3

    def __init__(
        self,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        aws_region: Optional[str] = None,
        logger_name: Optional[str] = None,
    ) -> None:

        aws_access_key_id = aws_access_key_id or os.environ.get("AWS_ACCESS_KEY_ID")
        aws_secret_access_key = aws_secret_access_key or os.environ.get(
            "AWS_SECRET_ACCESS_KEY"
        )
        aws_session_token = aws_session_token or os.environ.get("AWS_SESSION_TOKEN")
        aws_region = aws_region or os.environ.get("AWS_REGION")
        self.log: logging.Logger = logging.getLogger(logger_name or __name__)

        try:
            sts = boto3.client(
                "sts",
                aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key,
                aws_session_token=aws_session_token,
            )
            self.cloudwatch_client: botocore.client.BaseClient = boto3.client(
                "logs",
                aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key,
                aws_session_token=aws_session_token,
                region_name=aws_region,
            )
            self.s3_client: botocore.client.BaseClient = boto3.client(
                "s3",
                aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key,
                aws_session_token=aws_session_token,
                region_name=aws_region,
            )

        except NoCredentialsError as error:
            self.log.error(
                f"Error occurred in validating access and secret keys of the aws account.\n"
                "Please verify if the correct access and secret key of root user are provided.\n"
                "Access and secret key can be passed using:\n"
                "1. Passing as variable to class object\n"
                "2. Placing keys in ~/.aws/config\n"
                "3. Placing keys in ~/.aws/credentials\n"
                "4. As environment variables\n"
                "\n"
                "Please refer to: https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html\n"
                "\n"
                "Following is the error:\n"
                f"{error}"
            )
        except NoRegionError as error:
            self.log.error(f"Couldn't find region in AWS config." f"{error}")

        try:
            self.log.info("Verifying AWS credentials.")
            sts.get_caller_identity()
        except NoCredentialsError as error:
            self.log.error(f"Couldn't validate the AWS credentials." f"{error}")

    def get_cloudwatch_logs(
        self,
        log_group_name: str,
        log_stream_name: str,
        container_arn: Optional[str] = None,
    ) -> List[str]:
        """
        Fetches cloudwatch logs from the AWS account for a given log group and log stream
        Args:
            log_group_name (string): Name of the log group
            log_stream_name (string): Name of the log stream
            container_arn (string): Container arn to get log group and log stream names
        Returns:
            List[string]
        """

        messages = []
        message_events = []

        try:
            self.log.info(
                f"Getting logs from cloudwatch for log group {log_group_name} and stream name {log_stream_name}"
            )

            response = self.cloudwatch_client.get_log_events(
                logGroupName=log_group_name,
                logStreamName=log_stream_name,
                startFromHead=True,
            )
            message_events = response["events"]

            # Loop through to get the all the logs

            while True:
                prev_token = response["nextForwardToken"]
                response = self.cloudwatch_client.get_log_events(
                    logGroupName=log_group_name,
                    logStreamName=log_stream_name,
                    nextToken=prev_token,
                )
                # same token then break
                if response["nextForwardToken"] == prev_token:
                    break
                message_events.extend(response["events"])

            messages = self._parse_log_events(message_events)

        except ClientError as error:
            error_code = error.response.get("Error", {}).get("Code")
            if error_code == "InvalidParameterException":
                error_message = (
                    f"Couldn't fetch the log events for log group {log_group_name} and log stream {log_stream_name}.\n"
                    f"Please check if the container arn {container_arn} is correct.\n"
                    f"{error}\n"
                )
            elif error_code == "ResourceNotFoundException":
                error_message = (
                    f"Couldn't find log group name {log_group_name} and log stream {log_stream_name} in AWS account.\n"
                    f"Please check if the container arn {container_arn} is correct.\n"
                    f"{error}\n"
                )
            else:
                error_message = (
                    f"Unexpected error occured in fetching the log event log group {log_group_name} and log stream {log_stream_name}\n"
                    f"{error}\n"
                )
            # TODO T122315363: Raise more specific exception
            raise Exception(f"{error_message}")

        return messages

    def create_s3_folder(self, bucket_name: str, folder_name: str) -> None:
        """
        Creates a folder (Key in boto3 terms) inside the s3 bucket
        Args:
            bucket_name (string): Name of the s3 bucket where logs will be stored
            folder_name (string): Name of folder for which is to be created
        Returns:
            None
        """

        response = self.s3_client.put_object(Bucket=bucket_name, Key=folder_name)

        if response["ResponseMetadata"]["HTTPStatusCode"] == 200:
            self.log.info(
                f"Successfully created folder {folder_name} in S3 bucket {bucket_name}"
            )
        else:
            error_message = (
                f"Failed to create folder {folder_name} in S3 bucket {bucket_name}\n"
            )
            # TODO T122315363: Raise more specific exception
            raise Exception(f"{error_message}")

    def _parse_log_events(self, log_events: List[Dict[str, Any]]) -> List[str]:
        """
        AWS returns events metadata with other fields like logStreamName, timestamp etc.
        Following is the sample events returned:
        {'logStreamName': 'ecs/fake-container/123456789abcdef',
        'timestamp': 123456789,
        'message': 'INFO:This is a fake message',
        'ingestionTime': 123456789,
        'eventId': '12345678901234567890'}

        Args:
            log_events (list): List of dict contains the log messages

        Returns: list
        """

        return [event["message"] for event in log_events]

    def get_s3_folder_contents(
        self,
        bucket_name: str,
        folder_name: str,
        next_continuation_token: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Fetches folders in a given S3 bucket and folders information

        Args:
            bucket_name (string): Name of the s3 bucket where logs will be stored
            folder_name (string): Name of folder for fetching the contents
            NextContinuationToken (string): Token to get all the logs in case of pagination
        Returns:
            Dict
        """

        response = {}
        kwargs = {}

        if next_continuation_token == "":
            next_continuation_token = None

        if next_continuation_token:
            kwargs = {"ContinuationToken": next_continuation_token}

        try:
            response = self.s3_client.list_objects_v2(
                Bucket=bucket_name, Prefix=folder_name, **kwargs
            )
        except ClientError as error:
            error_message = f"Couldn't find folder. Please check if S3 bucket name {bucket_name} and folder name {folder_name} are correct"
            if error.response.get("Error", {}).get("Code") == "NoSuchBucket":
                error_message = f"Couldn't find folder {folder_name} in S3 bucket {bucket_name}\n{error}"
            # TODO T122315363: Raise more specific exception
            raise Exception({error_message})

        return response

    def upload_file_to_s3(
        self,
        s3_bucket_name: str,
        s3_file_path: str,
        file_name: str,
        retries: int = DEFAULT_RETRIES_LIMIT,
    ) -> None:
        """
        Function to upload a file to S3 bucket
        Args:
            s3_bucket_name (str): Name of the s3 bucket where logs will be uploaded
            s3_file_path (str): Name of folder in S3 bucket where logs will be uploaded
            file_name (str): Full path of the file location Eg: /tmp/xyz.txt
        Returns:
            None
        """

        while True:
            try:
                self.log.info("Uploading log folder to AWS S3")
                file_size = os.stat(file_name).st_size
                with tqdm(
                    total=file_size, unit="B", unit_scale=True, desc=file_name
                ) as pbar:
                    self.s3_client.upload_file(
                        Filename=file_name,
                        Bucket=s3_bucket_name,
                        Key=s3_file_path,
                        Callback=lambda bytes_transferred: pbar.update(
                            bytes_transferred
                        ),
                    )
                self.log.info("Uploaded log folder to AWS S3")
                break
            except ClientError as error:
                retries -= 1
                if retries <= 0:
                    # TODO T122315363: Raise more specific exception
                    raise Exception(
                        f"Couldn't upload file {file_name} to bucket {s3_bucket_name}."
                        f"Please check if right S3 bucket name and file path in S3 bucket {s3_file_path}."
                        f"{error}"
                    )

    def _verify_log_group(self, log_group_name: str) -> bool:
        """
        Verifies if the log group is present in the AWS account
        Args:
            log_group_name (String): Log group name that needs to be checked

        Returns: Boolean
        """
        response = {}

        try:
            self.log.info("Checking for log group name in the AWS account")
            response = self.cloudwatch_client.describe_log_groups(
                logGroupNamePrefix=log_group_name
            )
        except ClientError as error:
            error_code = error.response.get("Error", {}).get("Code")
            if error_code == "InvalidParameterException":
                error_message = (
                    f"Wrong parameters passed to the API. Please check container arn.\n"
                    f"Couldn't find log group {log_group_name}\n"
                    f"{error}\n"
                )
            elif error_code == "ResourceNotFoundException":
                error_message = (
                    f"Couldn't find log group name {log_group_name} in AWS account.\n"
                    f"{error}\n"
                )
            else:
                error_message = (
                    f"Unexpected error occurred in fetching log group name {log_group_name}.\n"
                    f"{error}\n"
                )
            # TODO T122315363: Raise more specific exception
            raise Exception(f"{error_message}")

        return len(response.get("logGroups", [])) == 1

    def _verify_log_stream(self, log_group_name: str, log_stream_name: str) -> bool:
        """
        Verifies log stream name in AWS account.

        Args:
            log_group_name (string): Log group name in the AWS account
            log_stream_name (string): Log stream name in the AWS account

        Returns: Boolean
        """
        response = {}

        try:
            self.log.info("Checking for log stream name in the AWS account")
            response = self.cloudwatch_client.describe_log_streams(
                logGroupName=log_group_name, logStreamNamePrefix=log_stream_name
            )
        except ClientError as error:
            error_code = error.response.get("Error", {}).get("Code")
            if error_code == "InvalidParameterException":
                error_message = (
                    f"Wrong parameters passed to the API. Please check container arn.\n"
                    f"Couldn't find log stream name {log_stream_name} in log group {log_group_name}\n"
                    f"{error}\n"
                )
            elif error_code == "ResourceNotFoundException":
                error_message = (
                    f"Couldn't find log group name {log_group_name} or log stream {log_stream_name} in AWS account\n"
                    f"{error}\n"
                )
            else:
                error_message = (
                    f"Unexpected error occurred in finding log stream name {log_stream_name} in log grpup {log_group_name}\n"
                    f"{error}\n"
                )
            # TODO T122315363: Raise more specific exception
            raise Exception(f"{error_message}")

        return len(response.get("logStreams", [])) == 1
