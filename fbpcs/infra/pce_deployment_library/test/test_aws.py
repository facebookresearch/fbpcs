#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

import unittest
from unittest.mock import create_autospec

from botocore.exceptions import ClientError

from fbpcs.infra.pce_deployment_library.cloud_library.aws.aws import AWS
from fbpcs.infra.pce_deployment_library.errors_library.aws_errors import (
    AccessDeniedError,
    S3BucketCreationError,
    S3BucketDeleteError,
    S3BucketDoesntExist,
    S3BucketVersioningFailedError,
)


class TestAws(unittest.TestCase):
    def setUp(self) -> None:
        self.aws = AWS()
        self.aws.sts.get_caller_identity = create_autospec(
            self.aws.sts.get_caller_identity
        )

    def test_check_s3_buckets_exists(self) -> None:
        s3_bucket_name = "fake_bucket"
        self.aws.s3_client.head_bucket = create_autospec(self.aws.s3_client.head_bucket)

        with self.subTest("basic"):
            with self.assertLogs() as captured:
                self.aws.check_s3_buckets_exists(
                    s3_bucket_name=s3_bucket_name, bucket_version=False
                )
                self.assertEqual(len(captured.records), 2)
                self.assertEqual(
                    captured.records[1].getMessage(),
                    f"S3 bucket {s3_bucket_name} already exists in the AWS account.",
                )

        with self.subTest("BucketNotFound"):
            self.aws.s3_client.create_bucket = create_autospec(
                self.aws.s3_client.create_bucket
            )
            self.aws.s3_client.put_bucket_versioning = create_autospec(
                self.aws.s3_client.put_bucket_versioning
            )
            self.aws.s3_client.head_bucket.side_effect = ClientError(
                error_response={"Error": {"Code": "404"}},
                operation_name="head_bucket",
            )
            with self.assertLogs() as captured:
                self.aws.check_s3_buckets_exists(
                    s3_bucket_name=s3_bucket_name, bucket_version=False
                )
                self.assertEqual(len(captured.records), 4)
                self.assertEqual(
                    captured.records[2].getMessage(),
                    f"Creating new S3 bucket {s3_bucket_name}",
                )
                self.assertEqual(
                    captured.records[3].getMessage(),
                    f"Create S3 bucket {s3_bucket_name} operation was successful.",
                )

        with self.subTest("AccessDenied"):
            self.aws.s3_client.head_bucket.side_effect = ClientError(
                error_response={"Error": {"Code": "403"}},
                operation_name="head_bucket",
            )
            with self.assertRaisesRegex(AccessDeniedError, "Access denied*"):
                self.aws.check_s3_buckets_exists(
                    s3_bucket_name=s3_bucket_name, bucket_version=False
                )

        with self.subTest("CatchAllError"):
            self.aws.s3_client.head_bucket.side_effect = ClientError(
                error_response={"Error": {"Code": None}},
                operation_name="head_bucket",
            )
            with self.assertRaisesRegex(
                S3BucketCreationError, "Couldn't create bucket*"
            ):
                self.aws.check_s3_buckets_exists(
                    s3_bucket_name=s3_bucket_name, bucket_version=False
                )

    def test_create_s3_bucket(self) -> None:
        self.aws.s3_client.create_bucket = create_autospec(
            self.aws.s3_client.create_bucket
        )
        s3_bucket_name = "fake_bucket"

        with self.subTest("Basic"):
            with self.assertLogs() as captured:
                self.aws.create_s3_bucket(
                    s3_bucket_name=s3_bucket_name, bucket_version=False
                )
                self.assertEqual(len(captured.records), 2)
                self.assertEqual(
                    captured.records[1].getMessage(),
                    f"Create S3 bucket {s3_bucket_name} operation was successful.",
                )

        with self.subTest("CreateBucketException"):
            self.aws.s3_client.create_bucket.side_effect = ClientError(
                error_response={"Error": {"Code": None}},
                operation_name="create_bucket",
            )

            with self.assertRaisesRegex(
                S3BucketCreationError, "Failed to create S3 bucket*"
            ):
                self.aws.create_s3_bucket(
                    s3_bucket_name=s3_bucket_name, bucket_version=False
                )

    def test_update_bucket_versioning(self) -> None:
        s3_bucket_name = "fake_bucket"
        self.aws.s3_client.put_bucket_versioning = create_autospec(
            self.aws.s3_client.put_bucket_versioning
        )

        with self.subTest("Basic"):
            with self.assertLogs() as captured:
                self.aws.update_bucket_versioning(s3_bucket_name=s3_bucket_name)
                self.assertEqual(len(captured.records), 2)
                self.assertEqual(
                    captured.records[1].getMessage(),
                    f"Bucket {s3_bucket_name} is enabled with versioning.",
                )

        with self.subTest("S3BucketDoesntExist"):
            self.aws.s3_client.put_bucket_versioning.side_effect = ClientError(
                error_response={"Error": {"Code": "404"}},
                operation_name="put_bucket_versioning",
            )
            self.assertRaises(
                S3BucketDoesntExist,
                lambda: self.aws.update_bucket_versioning(
                    s3_bucket_name=s3_bucket_name
                ),
            )

        with self.subTest("AccessDeniedException"):
            self.aws.s3_client.put_bucket_versioning.side_effect = ClientError(
                error_response={"Error": {"Code": "403"}},
                operation_name="put_bucket_versioning",
            )
            self.assertRaises(
                AccessDeniedError,
                lambda: self.aws.update_bucket_versioning(
                    s3_bucket_name=s3_bucket_name
                ),
            )

        with self.subTest("BucketVersioningFailed"):
            self.aws.s3_client.put_bucket_versioning.side_effect = ClientError(
                error_response={"Error": {"Code": None}},
                operation_name="put_bucket_versioning",
            )
            self.assertRaises(
                S3BucketVersioningFailedError,
                lambda: self.aws.update_bucket_versioning(
                    s3_bucket_name=s3_bucket_name
                ),
            )
            with self.assertRaisesRegex(
                S3BucketVersioningFailedError, "Error in versioning S3 bucket*"
            ):
                self.aws.update_bucket_versioning(s3_bucket_name=s3_bucket_name)

    def test_delete_s3_bucket(self) -> None:
        s3_bucket_name = "fake_bucket"
        self.aws.s3_client.delete_bucket = create_autospec(
            self.aws.s3_client.delete_bucket
        )

        with self.subTest("Basic"):
            with self.assertLogs() as captured:
                self.aws.delete_s3_bucket(s3_bucket_name=s3_bucket_name)
                self.assertEqual(len(captured.records), 2)
                self.assertEqual(
                    captured.records[1].getMessage(),
                    f"Delete S3 bucket {s3_bucket_name} operation was successful.",
                )

        with self.subTest("BucketDeleteFailed"):
            self.aws.s3_client.delete_bucket.side_effect = ClientError(
                error_response={"Error": {"Code": None}},
                operation_name="delete_bucket",
            )
            self.assertRaises(
                S3BucketDeleteError,
                lambda: self.aws.delete_s3_bucket(s3_bucket_name=s3_bucket_name),
            )
