# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict


from typing import List, Dict

from botocore.exceptions import ClientError
from cloud.aws_cloud import AwsCloud


class AWSContainerLogs(AwsCloud):
    """
    Fetches container logs from the cloudwatch
    """

    LOG_GROUP = "/{}/{}"
    LOG_STREAM = "{}/{}/{}"
    MAX_LOG_EVENT_LIMIT = 9999
    ARN_PARSE_LENGTH = 6

    def __init__(
        self,
        aws_access_key_id: str = None,
        aws_secret_access_key: str = None,
        aws_region: str = None,
    ):
        super().__init__(
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            aws_region=aws_region,
        )

    def get_cloudwatch_logs(self, container_arn: str) -> List[str]:
        """ """

        messages = []
        message_events = []

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

        if not self._verify_log_group(log_group_name=log_group_name):
            self.log.error(f"Couldn't find log group {log_group_name} in AWS account.")

        if not self._verify_log_stream(
            log_group_name=log_group_name, log_stream_name=log_stream_name
        ):
            self.log.error(
                f"Couldn't find log stream {log_stream_name} in log group {log_group_name}"
            )

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
                self.log.error(
                    f"Couldn't fetch the log events for log group {log_group_name} and log stream {log_stream_name}."
                    f"Please check if the container arn {container_arn} is correct."
                    f"{error}"
                )
            elif error_code == "ResourceNotFoundException":
                self.log.error(
                    f"Couldn't find log group name {log_group_name} and log stream {log_stream_name} in AWS account."
                    f"Please check if the container arn {container_arn} is correct."
                    f"{error}"
                )
            else:
                self.log.error(
                    f"Unexpected error occured in fetching the log event log group {log_group_name} and log stream {log_stream_name}"
                    f"{error}"
                )
            raise Exception("Please resolve the error.")

        return messages

    def _parse_container_arn(self, container_arn: str) -> List[str]:
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
            raise Exception(f"Error in getting service name and task ID: {error}")

        return [service_name, container_name, container_id]

    def _parse_log_events(self, log_events: List[Dict]) -> List[str]:
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

        return [event.get("message") for event in log_events]

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
            raise Exception(
                f"Error in getting container name and container ID: {error}"
            )

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
                self.log.error(
                    f"Wrong parameters passed to the API. Please check container arn."
                    f"Couldn't find log group {log_group_name}"
                    f"{error}"
                )
            elif error_code == "ResourceNotFoundException":
                self.log.error(
                    f"Couldn't find log group name {log_group_name} in AWS account."
                    f"{error}"
                )
            else:
                self.log.error(
                    f"Unexpected error occurred in fetching log group name {log_group_name}."
                    f"{error}"
                )
            raise Exception("Please resolve the error.")

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
                self.log.error(
                    f"Wrong parameters passed to the API. Please check container arn."
                    f"Couldn't find log stream name {log_stream_name} in log group {log_group_name}"
                    f"{error}"
                )
            elif error_code == "ResourceNotFoundException":
                self.log.error(
                    f"Couldn't find log group name {log_group_name} or log stream {log_stream_name} in AWS account"
                    f"{error}"
                )
            else:
                self.log.error(
                    f"Unexpected error occurred in finding log stream name {log_stream_name} in log grpup {log_group_name}"
                    f"{error}"
                )
            raise Exception("Please resolve the error.")

        return len(response.get("logStreams", [])) == 1
