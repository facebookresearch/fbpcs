# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict


from typing import Any, Dict, List, Optional

from botocore.exceptions import ClientError

from fbpcs.infra.logging_service.download_logs.cloud.aws_cloud import AwsCloud
from fbpcs.infra.logging_service.download_logs.utils.utils import Utils


class AwsContainerLogs(AwsCloud):
    """
    Fetches container logs from the cloudwatch
    """

    LOG_GROUP = "/{}/{}"
    LOG_STREAM = "{}/{}/{}"
    ARN_PARSE_LENGTH = 6
    S3_LOGGING_FOLDER = "logging"
    DEFAULT_DOWNLOAD_LOCATION = "/tmp"

    def __init__(
        self,
        tag_name: str,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_region: Optional[str] = None,
    ) -> None:
        super().__init__(
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            aws_region=aws_region,
        )
        self.tag_name = tag_name
        self.utils = Utils()

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
            # TODO: Raise more specific exception
            raise Exception(f"{error_message}")

        return messages

    def _parse_container_arn(self, container_arn: Optional[str]) -> List[str]:
        """
        Parses container arn to get the container name and id needed to derive log name and log stream
        Example ARN looks like:
        arn:aws:ecs:fake-region:123456789:task/fake-container-name/1234abcdef56789
        Args:
            container_arn (String): Container ARN
        Returns: String, String, String
        """
        service_name, container_name, container_id = None, None, None

        if container_arn is None:
            # TODO: Raise more specific exception
            raise Exception(
                "Container arn is missing. Please check the arn of the container"
            )

        container_arn_list = container_arn.split(":")

        if len(container_arn_list) < self.ARN_PARSE_LENGTH:
            self.log.error("Container ARN is not in the right format.")

        try:
            self.log.info("Getting service name and task ID from container arn")
            service_name = container_arn_list[2]
            task_id = container_arn_list[5]
            container_name, container_id = self._get_container_name_id(task_id=task_id)
        except IndexError as error:
            # TODO: Raise more specific exception
            raise Exception(f"Error in getting service name and task ID: {error}")

        # TODO: Return dataclass object instead of list
        return [service_name, container_name, container_id]

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

    def _get_container_name_id(self, task_id: str) -> List[str]:
        """
        Fetches container name and container ID from the task ID
        Args:
            task_id (String): task of the container extracted from contianer arn.
        Return: String, String
        """
        container_name, container_id = None, None

        task_id_list = task_id.split("/")

        if len(task_id_list) < 3:
            self.log.error("Task ID is not in the right format.")

        try:
            self.log.info("Getting container name from the task ID")
            container_name = task_id_list[1].replace("-cluster", "-container")
            container_id = task_id_list[2]
        except IndexError as error:
            # TODO: Raise more specific exception
            raise Exception(
                f"Error in getting container name and container ID: {error}"
            )

        # TODO: Return dataclass object instead of list
        return [container_name, container_id]

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
            # TODO: Raise more specific exception
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
            # TODO: Raise more specific exception
            raise Exception(f"{error_message}")

        return len(response.get("logStreams", [])) == 1

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
            # TODO: Raise more specific exception
            raise Exception(f"{error_message}")

    def ensure_folder_exists(self, bucket_name: str, folder_name: str) -> bool:
        """
        Verify if the folder is present in s3 bucket
        Args:
            bucket_name (string): Name of the s3 bucket where logs will be stored
            folder_name (string): Name of folder for which verification is needed
        Returns:
            Boolean
        """

        response = self.get_s3_folder_contents(
            bucket_name=bucket_name, folder_name=folder_name
        )

        return "Contents" in response

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
            # TODO: Raise more specific exception
            raise Exception({error_message})

        return response

    def _get_s3_folder_path(self, tag_name: str, container_id: str) -> str:
        """
        Return path of S3 folder
        Args:
            tag_name (str): Tag name passed to download the logs
            container_id (str): Download logs for container ID
        Returns:
            str
        """
        return f"{self.S3_LOGGING_FOLDER}/{tag_name}/{container_id}"

    def _get_files_to_download_logs(
        self, s3_bucket_name: str, folder_to_download: str
    ) -> List[str]:
        """
        Returns all the files to be downloaded in a folder in S3 bucket
        Args:
            s3_bucket_name (str): Name of the S3 bucket in AWS account
            folder_to_download (str): Name of folder from which files should be downloaded from
        Returns:
            List
        """

        files_to_download = []
        next_continuation_token = ""

        # Loop over the contents in case of pagination
        while next_continuation_token is not None:
            response = self.get_s3_folder_contents(
                bucket_name=s3_bucket_name,
                folder_name=folder_to_download,
                next_continuation_token=next_continuation_token,
            )
            contents = response.get("Contents", [])

            # ignore the directory in lsit of files to be downloaded
            for content in contents:
                key = content.get("Key")
                if key[-1] != "/":
                    files_to_download.append(key)
            next_continuation_token = response.get("NextContinuationToken")
        return files_to_download

    def download_logs(
        self,
        s3_bucket_name: str,
        tag_name: str,
        local_download_dir: Optional[str] = None,
    ) -> None:
        """
        Download logs from S3 to local
        Args:
            s3_bucket_name (str): Name of the S3 bucket in AWS account
            tag_name (str): Unique name for downloaded logs
            local_download_dir (str): Path where logs should be downloaded locally
        Returns:
            None
        """

        # If the local download directory is not set then set to default
        if local_download_dir is None:
            local_download_dir = self.DEFAULT_DOWNLOAD_LOCATION

        s3_folders_contents = self.ensure_folder_exists(
            bucket_name=s3_bucket_name,
            folder_name=f"{self.S3_LOGGING_FOLDER}/{tag_name}",
        )
        if not s3_folders_contents:
            # TODO: Raise more specific exception
            raise Exception(
                f"Folder name {self.S3_LOGGING_FOLDER}/{tag_name} not found in bucket {s3_bucket_name}."
                f"Please check if tag name {tag_name} and S3 bucket name {s3_bucket_name} are passed set correctly."
            )

        files_to_download = self._get_files_to_download_logs(
            s3_bucket_name=s3_bucket_name,
            folder_to_download=f"{self.S3_LOGGING_FOLDER}/{tag_name}",
        )

        # check for download folder in local
        # TODO: Use pathlib instead of creating paths ourselves
        local_path = f"{local_download_dir}/{tag_name}"

        self.utils.create_folder(folder_location=local_path)

        for file_to_download in files_to_download:
            file_name = file_to_download.replace(
                f"{self.S3_LOGGING_FOLDER}/{tag_name}/", ""
            ).rstrip()
            self.s3_client.download_file(
                Bucket=s3_bucket_name,
                Key=file_to_download,
                Filename=f"{local_path}/{file_name}",
            )

        # compress downloaded logs
        self.utils.compress_downloaded_logs(folder_location=local_path)

    def upload_logs_to_s3_from_cloudwatch(
        self, s3_bucket_name: str, container_arn: str
    ) -> None:
        """
        Umbrella function to call other functions to upload the logs from cloudwatch to S3

        Folder structure

        [S3 Bucket]
            -> [folder name "logging"]
                -> [folder name in format {tag_name}]
                    -> [exported logs from each container]

        Args:
            s3_bucket_name (string): Name of the s3 bucket where logs will be stored
            container_arn (string): Container arn to get log group and log stream names
        Returns:
            None
        """

        # verify s3 bucket
        try:
            self.s3_client.head_bucket(Bucket=s3_bucket_name)
        except ClientError as error:
            if error.response.get("Error", {}).get("Code") == "NoSuchBucket":
                error_message = f"Couldn't find bucket in the AWS account.\n{error}\n"
            else:
                error_message = "Couldn't find the S3 bucket in AWS account. Please use the right AWS S3 bucket name\n"
            raise Exception(f"{error_message}")

        # create logging folder
        if not self.ensure_folder_exists(
            bucket_name=s3_bucket_name, folder_name=f"{self.S3_LOGGING_FOLDER}/"
        ):
            self.create_s3_folder(
                bucket_name=s3_bucket_name,
                folder_name=f"{self.S3_LOGGING_FOLDER}/",
            )

        # create folder with tag_name passed
        if not self.ensure_folder_exists(
            bucket_name=s3_bucket_name,
            folder_name=f"{self.S3_LOGGING_FOLDER}/{self.tag_name}/",
        ):
            self.create_s3_folder(
                bucket_name=s3_bucket_name,
                folder_name=f"{self.S3_LOGGING_FOLDER}/{self.tag_name}/",
            )

        # fetch logs
        self.log.info(
            "Getting service name, container name and container ID from continer arn"
        )
        service_name, container_name, container_id = self._parse_container_arn(
            container_arn=container_arn
        )
        log_group_name = self.LOG_GROUP.format(service_name, container_name)
        log_stream_name = self.LOG_STREAM.format(
            service_name, container_name, container_id
        )
        s3_folder_path = self._get_s3_folder_path(self.tag_name, container_id)

        if not self._verify_log_group(log_group_name=log_group_name):
            raise Exception(f"Couldn't find log group {log_group_name} in AWS account.")

        if not self._verify_log_stream(
            log_group_name=log_group_name, log_stream_name=log_stream_name
        ):
            # TODO: Raise more specific exception
            raise Exception(
                f"Couldn't find log stream {log_stream_name} in log group {log_group_name}"
            )

        message_events = self.get_cloudwatch_logs(
            log_group_name=log_group_name,
            log_stream_name=log_stream_name,
            container_arn=container_arn,
        )

        self.s3_client.put_object(
            Body="\n".join(str(event) for event in message_events).encode("utf-8"),
            Bucket=s3_bucket_name,
            Key=s3_folder_path,
        )
