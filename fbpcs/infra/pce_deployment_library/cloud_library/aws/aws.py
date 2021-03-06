# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict
import logging
import os
from typing import Optional

import boto3

import botocore

from botocore.exceptions import ClientError, NoCredentialsError
from fbpcs.infra.pce_deployment_library.cloud_library.cloud_base.cloud_base import (
    CloudBase,
)
from fbpcs.infra.pce_deployment_library.cloud_library.defaults import CloudPlatforms
from fbpcs.infra.pce_deployment_library.errors_library.aws_errors import (
    AccessDeniedError,
    S3BucketCreationError,
    S3BucketDeleteError,
    S3BucketDoesntExist,
    S3BucketVersioningFailedError,
)


class AWS(CloudBase):
    def __init__(
        self,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        aws_region: Optional[str] = None,
    ) -> None:

        aws_access_key_id = aws_access_key_id or os.environ.get("AWS_ACCESS_KEY_ID")
        aws_secret_access_key = aws_secret_access_key or os.environ.get(
            "AWS_SECRET_ACCESS_KEY"
        )
        aws_session_token = aws_session_token or os.environ.get("AWS_SESSION_TOKEN")
        self.aws_region: Optional[str] = aws_region or os.environ.get("AWS_REGION")

        self.log: logging.Logger = logging.getLogger(__name__)
        self.__account_id: Optional[str] = None

        try:
            self.sts: botocore.client.BaseClient = boto3.client(
                "sts",
                aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key,
                aws_session_token=aws_session_token,
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
        try:
            self.log.info("Verifying AWS credentials.")
            response = self.sts.get_caller_identity()

            # fetching account ID for the given credentials
            self.__account_id = response.get("Account", None)
        except NoCredentialsError as error:
            self.log.error(f"Couldn't validate the AWS credentials." f"{error}")

    @classmethod
    def cloud_type(cls) -> CloudPlatforms:
        return CloudPlatforms.AWS

    def check_s3_buckets_exists(
        self, s3_bucket_name: str, bucket_version: bool = True
    ) -> None:
        """
        Checks for the S3 bucket. If not found creates one.
        """
        try:
            self.log.info(f"Checking if S3 bucket {s3_bucket_name} exists.")
            self.s3_client.head_bucket(Bucket=s3_bucket_name)
            self.log.info(
                f"S3 bucket {s3_bucket_name} already exists in the AWS account."
            )
        except ClientError as error:
            if error.response["Error"]["Code"] == "404":
                # Error reponse was 404 which means bucket doesn't exist.
                # In this case creates a new bucket
                self.log.info(
                    f"S3 bucket {s3_bucket_name} deosn't exists in the AWS account."
                )
                self.create_s3_bucket(
                    s3_bucket_name=s3_bucket_name, bucket_version=bucket_version
                )
            elif error.response["Error"]["Code"] == "403":
                # Error reponse was 403 which means user doesn't have access to this bucket
                raise AccessDeniedError("Access denied") from error
            else:
                raise S3BucketCreationError(
                    f"Couldn't create bucket {s3_bucket_name}"
                ) from error

    def create_s3_bucket(
        self, s3_bucket_name: str, bucket_version: bool = True
    ) -> None:
        bucket_configuration = {"LocationConstraint": self.aws_region}

        try:
            self.log.info(f"Creating new S3 bucket {s3_bucket_name}")
            self.s3_client.create_bucket(
                Bucket=s3_bucket_name,
                CreateBucketConfiguration=bucket_configuration,
            )
            self.log.info(
                f"Create S3 bucket {s3_bucket_name} operation was successful."
            )
        except ClientError as error:
            error_code = error.response.get("Error", {}).get("Code", None)
            raise S3BucketCreationError(
                f"Failed to create S3 bucket with error code {error_code}"
            ) from error

        if bucket_version:
            self.update_bucket_versioning(s3_bucket_name=s3_bucket_name)

    def update_bucket_versioning(
        self, s3_bucket_name: str, versioning_status: Optional[str] = "Enabled"
    ) -> None:
        versioning_configuration = {"Status": versioning_status}
        try:
            self.log.info("Creating bucket versioning.")
            self.s3_client.put_bucket_versioning(
                Bucket=s3_bucket_name, VersioningConfiguration=versioning_configuration
            )
            self.log.info(f"Bucket {s3_bucket_name} is enabled with versioning.")
        except ClientError as error:
            if error.response["Error"]["Code"] == "404":
                raise S3BucketDoesntExist(
                    f"S3 bucket {s3_bucket_name} doesn't exist"
                ) from error
            elif error.response["Error"]["Code"] == "403":
                raise AccessDeniedError("Access denied") from error
            else:
                raise S3BucketVersioningFailedError(
                    f"Error in versioning S3 bucket {s3_bucket_name}"
                ) from error

    def delete_s3_bucket(self, s3_bucket_name: str) -> None:
        try:
            self.log.info(f"Deleting S3 bucket {s3_bucket_name}")
            self.s3_client.delete_bucket(Bucket=s3_bucket_name)
            self.log.info(
                f"Delete S3 bucket {s3_bucket_name} operation was successful."
            )
        except ClientError as error:
            raise S3BucketDeleteError(
                f"Error in deleting bucket {s3_bucket_name}"
            ) from error

    def check_s3_object_exists(
        self, s3_bucket_name: str, key_name: str, account_id: Optional[str] = ""
    ) -> bool:
        account_id = account_id or self.__account_id
        try:
            self.log.info(f"Checking for file {key_name} in bucket {s3_bucket_name}")
            self.s3_client.head_object(
                Bucket=s3_bucket_name, Key=key_name, ExpectedBucketOwner=account_id
            )
            self.log.info(f"File {key_name} exists.")
            return True
        except ClientError as error:
            if error.response["Error"]["Code"] == "404":
                self.log.error(
                    f"Couldn't find file {key_name} in bucket {s3_bucket_name}"
                )
            elif error.response["Error"]["Code"] == "403":
                self.log.error(
                    f"Access denied: failed to access bucket {s3_bucket_name}"
                )
            else:
                self.log.error(
                    f"Failed to find file {key_name} in bucket {s3_bucket_name}"
                )
            self.log.info(f"File {key_name} doesn't exist.")
            return False
