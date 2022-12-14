#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

import os
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

import boto3

from botocore.exceptions import ClientError, NoCredentialsError, NoRegionError
from fbpcs.infra.logging_service.download_logs.cloud.aws_cloud import AwsCloud


class TestAwsCloud(unittest.TestCase):
    def setUp(self) -> None:
        self.test_dir = Path(os.path.dirname(__file__))
        self.tag = "my_tag"
        with patch(
            "fbpcs.infra.logging_service.download_logs.cloud.aws_cloud.boto3"
        ), patch("fbpcs.infra.logging_service.download_logs.download_logs.Utils"):
            self.aws_container_logs = AwsCloud(self.tag)

    def test_get_boto3_object(self) -> None:
        boto3.client = MagicMock()
        boto3.client.return_value = "something"

        with self.subTest("basic"):
            expected = "something"
            self.assertEqual(
                expected,
                self.aws_container_logs.get_boto3_object(service_name="aws_service"),
            )
        with self.subTest("NoCredentialsError"):
            expected = r"^Error occurred in validating access and secret keys of the aws account.*"
            boto3.client.reset_mock()
            boto3.client.side_effect = NoCredentialsError
            with self.assertLogs() as captured:
                self.aws_container_logs.get_boto3_object("aws_service")
                self.assertEqual(len(captured.records), 1)
                self.assertRegex(captured.records[0].getMessage(), expected)

        with self.subTest("NoRegionError"):
            expected = r"^Couldn't find region in AWS config.*"
            boto3.client.reset_mock()
            boto3.client.side_effect = NoRegionError
            with self.assertLogs() as captured:
                self.aws_container_logs.get_boto3_object("aws_service")
                self.assertEqual(len(captured.records), 1)
                self.assertRegex(captured.records[0].getMessage(), expected)

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
                    bucket_name="bucket",
                    folder_name="folder",
                    next_continuation_token="def678",
                ),
            )
            self.aws_container_logs.s3_client.list_objects_v2.assert_called_once_with(
                Bucket="bucket",
                Prefix="folder",
                MaxKeys=1,
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

    def test_verify_s3_bucket(self) -> None:
        self.aws_container_logs.s3_client.head_bucket.return_value = None

        with self.subTest("basic"):
            self.assertIsNone(self.aws_container_logs.verify_s3_bucket("test_bucket"))

        with self.subTest("ExcpetionCase"):
            self.aws_container_logs.s3_client.head_bucket.reset_mock()
            self.aws_container_logs.s3_client.head_bucket.side_effect = ClientError(
                error_response={"Error": {"Code": "BucketNotFound"}},
                operation_name="head_bucket",
            )
            with self.assertRaisesRegex(Exception, "Failed to fetch S3.*"):
                self.aws_container_logs.verify_s3_bucket("test_bucket")

    def test_ensure_folder_exists(self) -> None:
        bucket_name = "test_bucket"
        folder_name = "test_folder"
        mock_return = {"Contents": bucket_name}
        self.aws_container_logs.s3_client.list_objects_v2.return_value = mock_return

        with self.subTest("PositiveCase"):
            self.assertTrue(
                self.aws_container_logs.ensure_folder_exists(
                    bucket_name=bucket_name, folder_name=folder_name
                )
            )

        with self.subTest("NegativeCase"):
            mock_return = {}
            self.aws_container_logs.s3_client.list_objects_v2.reset_mock()
            self.aws_container_logs.s3_client.list_objects_v2.return_value = mock_return

            self.assertFalse(
                self.aws_container_logs.ensure_folder_exists(
                    bucket_name=bucket_name, folder_name=folder_name
                )
            )

    def test_get_kinesis_firehose_streams(self) -> None:

        kinesis_firehose_stream_name = "test_stream"
        mock_return = {"stream_name": kinesis_firehose_stream_name}
        self.aws_container_logs.kinesis_client.describe_delivery_stream.return_value = (
            mock_return
        )

        with self.subTest("basic"):
            expected = {"stream_name": "test_stream"}
            self.assertEqual(
                expected,
                self.aws_container_logs.get_kinesis_firehose_streams(
                    kinesis_firehose_stream_name=kinesis_firehose_stream_name
                ),
            )

        with self.subTest("ExceptionCase"):
            self.aws_container_logs.kinesis_client.describe_delivery_stream.reset_mock()
            self.aws_container_logs.kinesis_client.describe_delivery_stream.side_effect = ClientError(
                error_response={"Error": {"Code": "StreamNotFound"}},
                operation_name="describe_delivery_stream",
            )
            with self.assertRaisesRegex(
                Exception, "Failed to get Kinesis firehose stream.*"
            ):
                self.aws_container_logs.get_kinesis_firehose_streams(
                    kinesis_firehose_stream_name=kinesis_firehose_stream_name
                )

    def test_get_kinesis_firehose_config(self) -> None:
        mock_response = {
            "DeliveryStreamDescription": {
                "Destinations": [
                    {
                        "S3DestinationDescription": {
                            "CloudWatchLoggingOptions": {"Enabled": True}
                        }
                    }
                ]
            }
        }
        with self.subTest("basic"):
            expected = {"Enabled": True}
            self.assertEqual(
                expected,
                self.aws_container_logs.get_kinesis_firehose_config(
                    response=mock_response
                ),
            )

        with self.subTest("NegativeCase"):
            expected = {"Enabled": False}
            mock_response = {
                "DeliveryStreamDescription": {
                    "Destinations": [{"S3DestinationDescription": {}}]
                }
            }
            self.assertEqual(
                expected,
                self.aws_container_logs.get_kinesis_firehose_config(
                    response=mock_response
                ),
            )

    def test_get_latest_cloudwatch_log(self) -> None:
        mock_streams = [
            {"logStreamName": "test_stream_1"},
            {"logStreamName": "test_stream_2"},
        ]
        mock_response = {"logStreams": mock_streams}
        self.aws_container_logs.cloudwatch_client.describe_log_streams.return_value = (
            mock_response
        )

        with self.subTest("basic"):
            self.assertEqual(
                mock_streams[0]["logStreamName"],
                self.aws_container_logs.get_latest_cloudwatch_log(
                    log_group_name="test_group"
                ),
            )

        with self.subTest("EmptyStreamsList"):
            mock_streams = []
            mock_response = {"logStreams": mock_streams}
            self.aws_container_logs.cloudwatch_client.describe_log_streams.reset_mock()
            self.aws_container_logs.cloudwatch_client.describe_log_streams.return_value = (
                mock_response
            )
            expected = ""
            self.assertEqual(
                expected,
                self.aws_container_logs.get_latest_cloudwatch_log(
                    log_group_name="test_group"
                ),
            )

        with self.subTest("EmptyResponse"):
            mock_response = {}
            self.aws_container_logs.cloudwatch_client.describe_log_streams.reset_mock()
            self.aws_container_logs.cloudwatch_client.describe_log_streams.return_value = (
                mock_response
            )
            expected = ""
            self.assertEqual(
                expected,
                self.aws_container_logs.get_latest_cloudwatch_log(
                    log_group_name="test_group"
                ),
            )

        with self.subTest("ExceptionCase"):
            mock_response = {}
            self.aws_container_logs.cloudwatch_client.describe_log_streams.reset_mock()
            self.aws_container_logs.cloudwatch_client.describe_log_streams.side_effect = ClientError(
                error_response={"Error": {"Code": "cloudwatcherro"}},
                operation_name="describe_log_streams",
            )
            expected = r"^Couldn't fetch log streams.*"
            with self.assertLogs() as captured:
                self.aws_container_logs.get_latest_cloudwatch_log(
                    log_group_name="test_group"
                )
                self.assertEqual(len(captured.records), 2)
                self.assertRegex(captured.records[1].getMessage(), expected)

    def test_get_glue_crawler_config(self) -> None:
        mock_response = {"glue_crawler": "test_config"}
        expected = {"glue_crawler": "test_config"}
        self.aws_container_logs.glue_client.get_crawler.return_value = mock_response

        with self.subTest("basic"):
            self.assertEqual(
                expected,
                self.aws_container_logs.get_glue_crawler_config(
                    glue_crawler_name="test_crawler_name"
                ),
            )
        with self.subTest("CrawlerNameEmpty"):
            expected = {}
            self.assertEqual(
                expected,
                self.aws_container_logs.get_glue_crawler_config(glue_crawler_name=""),
            )

        with self.subTest("CrawlerNameNone"):
            expected = {}
            self.assertEqual(
                expected,
                self.aws_container_logs.get_glue_crawler_config(glue_crawler_name=None),
            )

        with self.subTest("ExceptionCase"):
            crawler_name = "test_crawler_name"
            crawler_exception = "crawlerException"
            expected_error = f"Couldn't fetch glue crawler {crawler_name}: An error occurred ({crawler_exception}) when calling the get_crawler operation: Unknown"
            expected = {"Get_Crawler_Error": expected_error}
            self.aws_container_logs.glue_client.get_crawler.reset_mock()
            self.aws_container_logs.glue_client.get_crawler.side_effect = ClientError(
                error_response={"Error": {"Code": crawler_exception}},
                operation_name="get_crawler",
            )
            self.assertEqual(
                expected,
                self.aws_container_logs.get_glue_crawler_config(
                    glue_crawler_name=crawler_name
                ),
            )

    def test_get_glue_crawler_metrics(self) -> None:
        mock_response = {"glue_crawler_metric": "test_metric"}
        expected = {"glue_crawler_metric": "test_metric"}
        self.aws_container_logs.glue_client.get_crawler_metrics.return_value = (
            mock_response
        )

        with self.subTest("basic"):
            self.assertEqual(
                expected,
                self.aws_container_logs.get_glue_crawler_metrics(
                    glue_crawler_name="test_crawler_name"
                ),
            )

        with self.subTest("CrawlerNameEmpty"):
            expected = {}
            self.assertEqual(
                expected,
                self.aws_container_logs.get_glue_crawler_metrics(glue_crawler_name=""),
            )

        with self.subTest("CrawlerNameNone"):
            expected = {}
            self.assertEqual(
                expected,
                self.aws_container_logs.get_glue_crawler_metrics(
                    glue_crawler_name=None
                ),
            )

        with self.subTest("ExceptionCase"):
            crawler_name = "test_crawler_name"
            crawler_exception = "crawlerMetricException"
            expected_error = f"Couldn't fetch glue crawler metrics {crawler_name}: An error occurred ({crawler_exception}) when calling the get_crawler_metrics operation: Unknown"
            expected = {"Get_Crawler_Metrics_Error": expected_error}
            self.aws_container_logs.glue_client.get_crawler_metrics.reset_mock()
            self.aws_container_logs.glue_client.get_crawler_metrics.side_effect = (
                ClientError(
                    error_response={"Error": {"Code": crawler_exception}},
                    operation_name="get_crawler_metrics",
                )
            )
            self.assertEqual(
                expected,
                self.aws_container_logs.get_glue_crawler_metrics(
                    glue_crawler_name=crawler_name
                ),
            )

    def test_get_glue_etl_job_details(self) -> None:
        mock_response = {"glue_etl": "test_etl_name"}
        expected = {"glue_etl": "test_etl_name"}
        self.aws_container_logs.glue_client.get_job.return_value = mock_response

        with self.subTest("basic"):
            self.assertEqual(
                expected,
                self.aws_container_logs.get_glue_etl_job_details(
                    glue_etl_name="test_etl_name"
                ),
            )

        with self.subTest("EtlNameEmpty"):
            expected = {}
            self.assertEqual(
                expected,
                self.aws_container_logs.get_glue_etl_job_details(glue_etl_name=""),
            )

        with self.subTest("EtlNameNone"):
            expected = {}
            self.assertEqual(
                expected,
                self.aws_container_logs.get_glue_etl_job_details(glue_etl_name=None),
            )

        with self.subTest("ExceptionCase"):
            etl_name = "test_etl_name"
            etl_exception = "getJobException"
            expected_error = f"Couldn't fetch glue ETL job {etl_name}: An error occurred ({etl_exception}) when calling the get_job operation: Unknown"
            expected = {"Get_Job_Error": expected_error}
            self.aws_container_logs.glue_client.get_job.reset_mock()
            self.aws_container_logs.glue_client.get_job.side_effect = ClientError(
                error_response={"Error": {"Code": etl_exception}},
                operation_name="get_job",
            )
            self.assertEqual(
                expected,
                self.aws_container_logs.get_glue_etl_job_details(
                    glue_etl_name=etl_name
                ),
            )

    def test_get_glue_etl_job_run_details(self) -> None:
        mock_response = {"glue_etl": "test_etl_name"}
        expected = {"glue_etl": "test_etl_name"}
        self.aws_container_logs.glue_client.get_job_runs.return_value = mock_response

        with self.subTest("basic"):
            self.assertEqual(
                expected,
                self.aws_container_logs.get_glue_etl_job_run_details(
                    glue_etl_name="test_etl_name"
                ),
            )

        with self.subTest("EtlNameEmpty"):
            expected = {}
            self.assertEqual(
                expected,
                self.aws_container_logs.get_glue_etl_job_run_details(glue_etl_name=""),
            )

        with self.subTest("EtlNameNone"):
            expected = {}
            self.assertEqual(
                expected,
                self.aws_container_logs.get_glue_etl_job_run_details(
                    glue_etl_name=None
                ),
            )

        with self.subTest("ExceptionCase"):
            etl_name = "test_etl_name"
            etl_exception = "getJobException"
            expected = {"glue_etl": "test_etl_name"}
            self.aws_container_logs.glue_client.get_job.reset_mock()
            self.aws_container_logs.glue_client.get_job.side_effect = ClientError(
                error_response={"Error": {"Code": etl_exception}},
                operation_name="get_job_runs",
            )
            ret = self.aws_container_logs.get_glue_etl_job_run_details(
                glue_etl_name=etl_name
            )
            self.assertEqual(
                expected,
                ret,
            )

    def test_get_athena_database_list(self) -> None:
        data_catalog_name = "test_data_catalog_name"
        data_catalog_exception = "getJobException"
        mock_response = {"athena_catalog": "test_athena_catalog"}

        expected = {"athena_catalog": "test_athena_catalog"}
        self.aws_container_logs.athena_client.list_databases.return_value = (
            mock_response
        )

        with self.subTest("basic"):
            self.assertEqual(
                expected,
                self.aws_container_logs.get_athena_database_list(
                    data_catalog_name=data_catalog_name
                ),
            )

        with self.subTest("DataCatalogNameEmpty"):
            expected = {}
            self.assertEqual(
                expected,
                self.aws_container_logs.get_athena_database_list(data_catalog_name=""),
            )

        with self.subTest("DataCatalogNameNone"):
            expected = {}
            self.assertEqual(
                expected,
                self.aws_container_logs.get_athena_database_list(
                    data_catalog_name=None
                ),
            )

        with self.subTest("ExceptionCase"):
            data_catalog_name_exception = "getJobException"
            expected_error = f"Couldn't fetch databases for data catalog {data_catalog_name}: An error occurred ({data_catalog_exception}) when calling the list_databases operation: Unknown"
            expected = {"List_Databases_Error": expected_error}
            self.aws_container_logs.athena_client.list_databases.reset_mock()
            self.aws_container_logs.athena_client.list_databases.side_effect = (
                ClientError(
                    error_response={"Error": {"Code": data_catalog_name_exception}},
                    operation_name="list_databases",
                )
            )
            ret = self.aws_container_logs.get_athena_database_list(
                data_catalog_name=data_catalog_name
            )
            self.assertEqual(
                expected,
                ret,
            )
