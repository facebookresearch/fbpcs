#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

import unittest
from unittest.mock import patch

from botocore.exceptions import ClientError

from fbpcs.infra.logging_service.download_logs.download_logs import AwsContainerLogs


class TestDownloadLogs(unittest.TestCase):
    @patch("fbpcs.infra.logging_service.download_logs.cloud.aws_cloud.boto3")
    def test_get_cloudwatch_logs(self, mock_boto3) -> None:
        aws_container_logs = AwsContainerLogs("my_tag")
        aws_container_logs.cloudwatch_client.get_log_events.side_effect = [
            {"events": [{"message": "123"}], "nextForwardToken": "1"},
            {"events": [{"message": "456"}], "nextForwardToken": "2"},
            {"events": [{"message": "789"}], "nextForwardToken": "3"},
            # Repeated event indicates no more data available
            {"events": [{"message": "789"}], "nextForwardToken": "3"},
        ]

        expected = ["123", "456", "789"]
        self.assertEqual(
            expected,
            aws_container_logs.get_cloudwatch_logs("foo", "bar"),
        )
        # NOTE: we don't want to get *too* specific with these asserts
        # because we want to allow the internal details to change and
        # still meet the API requirements
        aws_container_logs.cloudwatch_client.get_log_events.assert_called()

        ####################
        # Test error cases #
        ####################
        aws_container_logs.cloudwatch_client.get_log_events.reset_mock()
        aws_container_logs.cloudwatch_client.get_log_events.side_effect = ClientError(
            error_response={"Error": {"Code": "InvalidParameterException"}},
            operation_name="get_log_events",
        )
        with self.assertRaisesRegex(Exception, "Couldn't fetch.*"):
            aws_container_logs.get_cloudwatch_logs("foo", "bar")
            aws_container_logs.cloudwatch_client.get_log_events.assert_called()

        aws_container_logs.cloudwatch_client.get_log_events.reset_mock()
        aws_container_logs.cloudwatch_client.get_log_events.side_effect = ClientError(
            error_response={"Error": {"Code": "ResourceNotFoundException"}},
            operation_name="get_log_events",
        )
        with self.assertRaisesRegex(Exception, "Couldn't find.*"):
            aws_container_logs.get_cloudwatch_logs("foo", "bar")
            aws_container_logs.cloudwatch_client.get_log_events.assert_called()

        aws_container_logs.cloudwatch_client.get_log_events.reset_mock()
        aws_container_logs.cloudwatch_client.get_log_events.side_effect = ClientError(
            error_response={"Error": {"Code": "SomethingElseHappenedException"}},
            operation_name="get_log_events",
        )
        with self.assertRaisesRegex(Exception, "Unexpected error.*"):
            aws_container_logs.get_cloudwatch_logs("foo", "bar")
            aws_container_logs.cloudwatch_client.get_log_events.assert_called()

    @patch("fbpcs.infra.logging_service.download_logs.cloud.aws_cloud.boto3")
    def test_parse_container_arn(self, mock_boto3) -> None:
        aws_container_logs = AwsContainerLogs("my_tag")
        with self.assertRaisesRegex(Exception, "Container arn is missing.*"):
            aws_container_logs._parse_container_arn(None)

        bad_arn = "abc:123"
        with self.assertRaisesRegex(Exception, "Error in getting service name.*"):
            aws_container_logs._parse_container_arn(bad_arn)

        normal_arn = (
            "arn:aws:ecs:fake-region:123456789:task/fake-container-name/1234abcdef56789"
        )
        expected = ["ecs", "fake-container-name", "1234abcdef56789"]
        self.assertEqual(expected, aws_container_logs._parse_container_arn(normal_arn))

    @patch("fbpcs.infra.logging_service.download_logs.cloud.aws_cloud.boto3")
    def test_parse_log_events(self, mock_boto3) -> None:
        aws_container_logs = AwsContainerLogs("my_tag")
        events = [
            {"message": "hello", "code": 200, "other": "ignore"},
            {"message": "world", "code": 200, "other": "ignore"},
        ]
        expected = ["hello", "world"]
        self.assertEqual(expected, aws_container_logs._parse_log_events(events))

    @patch("fbpcs.infra.logging_service.download_logs.cloud.aws_cloud.boto3")
    def test_get_container_name_id(self, mock_boto3) -> None:
        pass

    @patch("fbpcs.infra.logging_service.download_logs.cloud.aws_cloud.boto3")
    def test_verify_log_group(self, mock_boto3) -> None:
        pass

    @patch("fbpcs.infra.logging_service.download_logs.cloud.aws_cloud.boto3")
    def test_verify_log_stream(self, mock_boto3) -> None:
        pass

    @patch("fbpcs.infra.logging_service.download_logs.cloud.aws_cloud.boto3")
    def test_create_s3_folder(self, mock_boto3) -> None:
        pass

    @patch("fbpcs.infra.logging_service.download_logs.cloud.aws_cloud.boto3")
    def test_ensure_folder_exists(self, mock_boto3) -> None:
        pass

    @patch("fbpcs.infra.logging_service.download_logs.cloud.aws_cloud.boto3")
    def test_get_s3_folder_contents(self, mock_boto3) -> None:
        pass

    @patch("fbpcs.infra.logging_service.download_logs.cloud.aws_cloud.boto3")
    def test_get_s3_folder_path(self, mock_boto3) -> None:
        pass

    @patch("fbpcs.infra.logging_service.download_logs.cloud.aws_cloud.boto3")
    def test_get_files_to_download_logs(self, mock_boto3) -> None:
        pass

    @patch("fbpcs.infra.logging_service.download_logs.cloud.aws_cloud.boto3")
    def test_download_logs(self, mock_boto3) -> None:
        pass

    @patch("fbpcs.infra.logging_service.download_logs.cloud.aws_cloud.boto3")
    def test_upload_logs_to_s3_from_cloudwatch(self, mock_boto3) -> None:
        pass
