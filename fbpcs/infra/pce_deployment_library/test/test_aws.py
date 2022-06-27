#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

import unittest
from unittest.mock import MagicMock

from botocore.exceptions import ClientError

from fbpcs.infra.pce_deployment_library.cloud_library.aws.aws import AWS
from fbpcs.infra.pce_deployment_library.errors_library.aws_errors import (
    AccessDeniedError,
    S3BucketCreationError,
)


class TestAws(unittest.TestCase):
    def setUp(self) -> None:
        self.aws = AWS()
        self.aws.sts.get_caller_identity = MagicMock(return_value=None)

    def test_check_s3_buckets_exists(self) -> None:
        s3_bucket_name = "fake_bucket"
        self.aws.s3_client.head_bucket = MagicMock(return_value=None)

        with self.subTest("basic"):
            self.assertIsNone(
                self.aws.check_s3_buckets_exists(s3_bucket_name=s3_bucket_name)
            )

        with self.subTest("BucketNotFound"):
            self.aws.s3_client.create_bucket = MagicMock(return_value=None)
            self.aws.s3_client.put_bucket_versionin = MagicMock(return_value=None)
            self.aws.s3_client.head_bucket.side_effect = ClientError(
                error_response={"Error": {"Code": "404"}},
                operation_name="head_bucket",
            )
            self.assertIsNone(
                self.aws.check_s3_buckets_exists(
                    s3_bucket_name=s3_bucket_name, bucket_version=False
                )
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
