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

from fbpcs.infra.logging_service.download_logs.download_logs import AwsContainerLogs
from fbpcs.infra.logging_service.download_logs.download_logs_cli import DownloadLogsCli
from fbpcs.infra.logging_service.download_logs.utils.utils import ContainerDetails


class TestDownloadLogs(unittest.TestCase):
    def setUp(self) -> None:
        self.test_dir = Path(os.path.dirname(__file__))
        self.tag = "my_tag"
        with patch(
            "fbpcs.infra.logging_service.download_logs.cloud.aws_cloud.boto3"
        ), patch("fbpcs.infra.logging_service.download_logs.download_logs.Utils"):
            self.aws_container_logs = AwsContainerLogs(self.tag)

    def test_ensure_folder_exists(self) -> None:
        self.aws_container_logs.s3_client.list_objects_v2.return_value = {
            "Contents": ["a", "b", "c"]
        }

        with self.subTest("positive_case"):
            self.assertTrue(
                self.aws_container_logs.ensure_folder_exists("bucket", "folder")
            )

        with self.subTest("negative_case"):
            self.aws_container_logs.s3_client.list_objects_v2.reset_mock()
            self.aws_container_logs.s3_client.list_objects_v2.return_value = {}
            self.assertFalse(
                self.aws_container_logs.ensure_folder_exists("bucket", "folder")
            )

    def test_upload_logs_to_s3_from_cloudwatch(self) -> None:
        self.aws_container_logs.cloudwatch_client.get_log_events.side_effect = [
            {"events": [{"message": "123"}], "nextForwardToken": "1"},
            {"events": [{"message": "456"}], "nextForwardToken": "2"},
            {"events": [{"message": "789"}], "nextForwardToken": "3"},
            # Repeated event indicates no more data available
            {"events": [{"message": "789"}], "nextForwardToken": "3"},
        ]

        self.aws_container_logs.cloudwatch_client.describe_log_groups.return_value = {
            "logGroups": ["my_log_group"]
        }

        self.aws_container_logs.cloudwatch_client.describe_log_streams.return_value = {
            "logStreams": ["my_log_stream"]
        }

        self.aws_container_logs.s3_client.put_object.return_value = {
            "ResponseMetadata": {"HTTPStatusCode": 200}
        }

        arn = [
            "arn:aws:ecs:fake-region:123456789:task/fake-container-name/1234abcdef56789"
        ]

        # TODO: add changes to test temp dir changes

        ###############
        # Error cases #
        ###############
        error_cases = [
            ("head_bucket", "NoSuchBucket", "Couldn't find bucket.*"),
            ("head_bucket", "SomethingElseHappenedException", "Couldn't find the S3.*"),
        ]
        for s3_endpoint, error_code, exc_regex in error_cases:
            with self.subTest(f"{s3_endpoint}.{error_code}"):
                self.aws_container_logs.s3_client.head_bucket.reset_mock(
                    side_effect=True
                )
                self.aws_container_logs.cloudwatch_client.describe_log_groups.reset_mock(
                    side_effect=True
                )
                getattr(
                    self.aws_container_logs.s3_client, s3_endpoint
                ).side_effect = ClientError(
                    error_response={"Error": {"Code": error_code}},
                    operation_name=s3_endpoint,
                )
                with self.assertRaisesRegex(Exception, exc_regex):
                    self.aws_container_logs.upload_logs_to_s3_from_cloudwatch(
                        "bucket", arn
                    )
                    getattr(
                        self.aws_container_logs.s3_client, s3_endpoint
                    ).assert_called()

    def test_download_logs_cli(self) -> None:
        cli = DownloadLogsCli()
        exc_regex = "Unable to locate credentials"
        with self.assertRaisesRegex(Exception, exc_regex):
            cli.run(
                [
                    str(self._get_sample_log_path("container_ids.txt")),
                    "bucket-name",
                    "tag-name",
                    "--input_ids",
                ]
            )
        self.assertEqual("bucket-name", cli.s3_bucket)
        self.assertEqual("us-west-2", cli.aws_region)
        self.assertEqual(
            [
                "arn:aws:ecs:us-west-2:5592513842793:task/onedocker-container-comp-ui-e2e/00a04af576dd454784a4af543925c8de",
                "arn:aws:ecs:us-west-2:5592513842793:task/onedocker-container-comp-ui-e2e/03705975e3c04b4db8b7fe4667baac37",
            ],
            cli.container_ids,
        )
        self.assertIsNotNone(cli.aws_container_logs)

    #######################################
    # Tests for logically private methods #
    #######################################
    def test_parse_container_arn(self) -> None:
        with self.subTest("arn_missing"):
            with self.assertRaisesRegex(Exception, "Container arn is missing.*"):
                self.aws_container_logs._parse_container_arn(None)

        with self.subTest("bad_arn"):
            bad_arn = "abc:123"
            with self.assertRaisesRegex(Exception, "Error in getting service name.*"):
                self.aws_container_logs._parse_container_arn(bad_arn)

        with self.subTest("normal_arn"):
            normal_arn = "arn:aws:ecs:fake-region:123456789:task/fake-container-name/1234abcdef56789"
            expected = ContainerDetails(
                service_name="ecs",
                container_name="fake-container-name",
                container_id="1234abcdef56789",
            )
            self.assertEqual(
                expected, self.aws_container_logs._parse_container_arn(normal_arn)
            )

    def test_get_container_name_id(self) -> None:
        with self.subTest("bad_task_id"):
            bad_task_id = "abc/123"
            with self.assertRaisesRegex(Exception, "Error in getting container name.*"):
                self.aws_container_logs._get_container_name_id(bad_task_id)

        # Simple test
        with self.subTest("normal_task_id"):
            normal_task_id = "task/container-name/abc123"
            expected = ["container-name", "abc123"]
            self.assertEqual(
                expected, self.aws_container_logs._get_container_name_id(normal_task_id)
            )

        # Replace -cluster
        with self.subTest("replace_cluster"):
            cluster_task_id = "task/my-cluster/abc123"
            expected = ["my-container", "abc123"]
            self.assertEqual(
                expected,
                self.aws_container_logs._get_container_name_id(cluster_task_id),
            )

    def test_log_containers_without_logs(self) -> None:
        with self.subTest("Basic"):
            self.aws_container_logs.containers_without_logs = []
            with self.assertLogs() as captured:
                self.aws_container_logs.log_containers_without_logs()
                self.assertEqual(len(captured.records), 1)
                self.assertEqual(
                    captured.records[0].getMessage(),
                    "Found logs for all the containers.",
                )
        with self.subTest("ErrorLog"):
            self.aws_container_logs.containers_without_logs = ["a"]
            with self.assertLogs() as captured:
                self.aws_container_logs.log_containers_without_logs()
                self.assertEqual(len(captured.records), 2)
                self.assertEqual(
                    captured.records[0].getMessage(),
                    "Couldn't find logs for the following containers..",
                )
                self.assertEqual(
                    captured.records[1].getMessage(),
                    "Container ARN: a",
                )

    def test_log_containers_download_log_failed(self) -> None:
        # T124204521
        pass

    def test_run_threaded_download(self) -> None:
        # T124197675
        pass

    def test_copy_logs_for_debug(self) -> None:
        # T124216294
        pass

    def _get_sample_log_path(
        self,
        log_file: str,
    ) -> Path:
        return self.test_dir / "sample_log" / log_file
