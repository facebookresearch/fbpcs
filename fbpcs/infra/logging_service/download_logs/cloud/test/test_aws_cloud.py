#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

import os
import unittest
from pathlib import Path
from unittest.mock import patch

from botocore.exceptions import ClientError
from fbpcs.infra.logging_service.download_logs.cloud.aws_cloud import AwsCloud


class TestAwsCloud(unittest.TestCase):
    def setUp(self) -> None:
        self.test_dir = Path(os.path.dirname(__file__))
        self.tag = "my_tag"
        with patch(
            "fbpcs.infra.logging_service.download_logs.cloud.aws_cloud.boto3"
        ), patch("fbpcs.infra.logging_service.download_logs.download_logs.Utils"):
            self.aws_container_logs = AwsCloud(self.tag)

    ##############################
    # Tests for public interface #
    ##############################
    def test_get_cloudwatch_logs(self) -> None:
        self.aws_container_logs.cloudwatch_client.get_log_events.side_effect = [
            {"events": [{"message": "123"}], "nextForwardToken": "1"},
            {"events": [{"message": "456"}], "nextForwardToken": "2"},
            {"events": [{"message": "789"}], "nextForwardToken": "3"},
            # Repeated event indicates no more data available
            {"events": [{"message": "789"}], "nextForwardToken": "3"},
        ]

        expected = ["123", "456", "789"]

        with self.subTest("basic"):
            self.assertEqual(
                expected,
                self.aws_container_logs.get_cloudwatch_logs("foo", "bar"),
            )
            # NOTE: we don't want to get *too* specific with these asserts
            # because we want to allow the internal details to change and
            # still meet the API requirements
            self.aws_container_logs.cloudwatch_client.get_log_events.assert_called()

        ####################
        # Test error cases #
        ####################
        error_cases = [
            ("InvalidParameterException", "Couldn't fetch.*"),
            ("ResourceNotFoundException", "Couldn't find.*"),
            ("SomethingElseHappenedException", "Unexpected error.*"),
        ]
        for error_code, exc_regex in error_cases:
            with self.subTest(f"get_log_events.{error_code}"):
                self.aws_container_logs.cloudwatch_client.get_log_events.reset_mock()
                self.aws_container_logs.cloudwatch_client.get_log_events.side_effect = (
                    ClientError(
                        error_response={"Error": {"Code": error_code}},
                        operation_name="get_log_events",
                    )
                )
                with self.assertRaisesRegex(Exception, exc_regex):
                    self.aws_container_logs.get_cloudwatch_logs("foo", "bar")
                    self.aws_container_logs.cloudwatch_client.get_log_events.assert_called()

    def test_create_s3_folder(self) -> None:
        self.aws_container_logs.s3_client.put_object.return_value = {
            "ResponseMetadata": {"HTTPStatusCode": 200}
        }

        with self.subTest("basic"):
            self.assertIsNone(
                self.aws_container_logs.create_s3_folder("bucket", "folder")
            )
            self.aws_container_logs.s3_client.put_object.assert_called_once_with(
                Bucket="bucket", Key="folder"
            )

        with self.subTest("put_object.Http403"):
            self.aws_container_logs.s3_client.put_object.reset_mock()
            self.aws_container_logs.s3_client.put_object.return_value = {
                "ResponseMetadata": {"HTTPStatusCode": 403}
            }
            with self.assertRaisesRegex(Exception, "Failed to create.*"):
                self.aws_container_logs.create_s3_folder("bucket", "folder")

    def test_parse_log_events(self) -> None:
        events = [
            {"message": "hello", "code": 200, "other": "ignore"},
            {"message": "world", "code": 200, "other": "ignore"},
        ]
        expected = ["hello", "world"]

        with self.subTest("basic"):
            self.assertEqual(
                expected, self.aws_container_logs._parse_log_events(events)
            )

    def test_get_s3_folder_contents(self) -> None:
        expected = {"ContinuationToken": "abc123", "Contents": ["a", "b", "c"]}
        self.aws_container_logs.s3_client.list_objects_v2.return_value = expected

        with self.subTest("basic"):
            self.assertEqual(
                expected,
                self.aws_container_logs.get_s3_folder_contents("bucket", "folder"),
            )

        # Check that continuation token is set
        with self.subTest("with_continuation_token"):
            self.aws_container_logs.s3_client.list_objects_v2.reset_mock()
            self.aws_container_logs.s3_client.list_objects_v2.return_value = expected
            self.assertEqual(
                expected,
                self.aws_container_logs.get_s3_folder_contents(
                    "bucket", "folder", "def678"
                ),
            )
            self.aws_container_logs.s3_client.list_objects_v2.assert_called_once_with(
                Bucket="bucket",
                Prefix="folder",
                ContinuationToken="def678",
            )

        # check exception cases
        with self.subTest("list_objects_v2.InvalidParameterException"):
            self.aws_container_logs.s3_client.list_objects_v2.reset_mock()
            self.aws_container_logs.s3_client.list_objects_v2.side_effect = ClientError(
                error_response={"Error": {"Code": "InvalidParameterException"}},
                operation_name="list_objects_v2",
            )
            with self.assertRaisesRegex(Exception, "Couldn't find folder.*"):
                self.aws_container_logs.get_s3_folder_contents("bucket", "folder")

    def test_verify_log_group(self) -> None:
        self.aws_container_logs.cloudwatch_client.describe_log_groups.return_value = {
            "logGroups": ["my_log_group"]
        }

        with self.subTest("basic"):
            self.assertTrue(self.aws_container_logs._verify_log_group("my_log_group"))

        with self.subTest("describe_log_groups.InvalidParameterException"):
            self.aws_container_logs.cloudwatch_client.describe_log_groups.reset_mock()
            self.aws_container_logs.cloudwatch_client.describe_log_groups.side_effect = ClientError(
                error_response={"Error": {"Code": "InvalidParameterException"}},
                operation_name="describe_log_groups",
            )
            with self.assertRaisesRegex(Exception, "Wrong parameters.*"):
                self.aws_container_logs._verify_log_group("my_log_group")

        with self.subTest("describe_log_groups.ResourceNotFoundException"):
            self.aws_container_logs.cloudwatch_client.describe_log_groups.reset_mock()
            self.aws_container_logs.cloudwatch_client.describe_log_groups.side_effect = ClientError(
                error_response={"Error": {"Code": "ResourceNotFoundException"}},
                operation_name="describe_log_groups",
            )
            with self.assertRaisesRegex(Exception, "Couldn't find.*"):
                self.aws_container_logs._verify_log_group("my_log_group")

        with self.subTest("describe_log_groups.SomethingElseHappenedException"):
            self.aws_container_logs.cloudwatch_client.describe_log_groups.reset_mock()
            self.aws_container_logs.cloudwatch_client.describe_log_groups.side_effect = ClientError(
                error_response={"Error": {"Code": "SomethingElseHappenedException"}},
                operation_name="describe_log_groups",
            )
            with self.assertRaisesRegex(Exception, "Unexpected error.*"):
                self.aws_container_logs._verify_log_group("my_log_group")

    def test_verify_log_stream(self) -> None:
        self.aws_container_logs.cloudwatch_client.describe_log_streams.return_value = {
            "logStreams": ["my_log_stream"]
        }

        with self.subTest("basic"):
            self.assertTrue(
                self.aws_container_logs._verify_log_stream(
                    "my_log_group", "my_log_stream"
                )
            )

        with self.subTest("describe_log_streams.InvalidParameterException"):
            self.aws_container_logs.cloudwatch_client.describe_log_streams.reset_mock()
            self.aws_container_logs.cloudwatch_client.describe_log_streams.side_effect = ClientError(
                error_response={"Error": {"Code": "InvalidParameterException"}},
                operation_name="describe_log_streams",
            )
            with self.assertRaisesRegex(Exception, "Wrong parameters.*"):
                self.aws_container_logs._verify_log_stream(
                    "my_log_group", "my_log_stream"
                )

        with self.subTest("describe_log_streams.ResourceNotFoundException"):
            self.aws_container_logs.cloudwatch_client.describe_log_streams.reset_mock()
            self.aws_container_logs.cloudwatch_client.describe_log_streams.side_effect = ClientError(
                error_response={"Error": {"Code": "ResourceNotFoundException"}},
                operation_name="describe_log_streams",
            )
            with self.assertRaisesRegex(Exception, "Couldn't find.*"):
                self.aws_container_logs._verify_log_stream(
                    "my_log_group", "my_log_stream"
                )

        with self.subTest("describe_log_streams.SomethingElseHappenedException"):
            self.aws_container_logs.cloudwatch_client.describe_log_streams.reset_mock()
            self.aws_container_logs.cloudwatch_client.describe_log_streams.side_effect = ClientError(
                error_response={"Error": {"Code": "SomethingElseHappenedException"}},
                operation_name="describe_log_streams",
            )
            with self.assertRaisesRegex(Exception, "Unexpected error.*"):
                self.aws_container_logs._verify_log_stream(
                    "my_log_group", "my_log_stream"
                )
