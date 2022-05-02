# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

from unittest import TestCase

from fbpcs.pc_pre_validation.binary_path import (
    LocalBinaryPath,
    BinaryInfo,
    S3BinaryPath,
)

TEST_REPO = "https://test-bucket.us-west-2.amazonaws.com/"
TEST_EXEC_FOLDER = "/root/test-path/"


class TestBinaryPath(TestCase):
    def test_s3_package_path(self) -> None:
        test_cases = [
            {
                "binary_info": BinaryInfo("data_processing/attribution_id_combiner"),
                "version": "latest",
                "expected": f"{TEST_REPO}data_processing/attribution_id_combiner/latest/attribution_id_combiner",
            },
            {
                "binary_info": BinaryInfo("pcf2_aggregation", "pcf2_aggregation"),
                "version": "latest",
                "expected": f"{TEST_REPO}pcf2_aggregation/latest/pcf2_aggregation",
            },
            {
                "binary_info": BinaryInfo("data_processing/attribution_id_combiner"),
                "version": "canary",
                "expected": f"{TEST_REPO}data_processing/attribution_id_combiner/canary/attribution_id_combiner",
            },
        ]

        for case in test_cases:
            # pyre-ignore
            s3_path = S3BinaryPath(TEST_REPO, case["binary_info"], case["version"])
            self.assertEqual(case["expected"], str(s3_path))

    def test_local_package_path(self) -> None:
        test_cases = [
            {
                "binary_info": BinaryInfo("data_processing/attribution_id_combiner"),
                "expected": f"{TEST_EXEC_FOLDER}attribution_id_combiner",
            },
            {
                "binary_info": BinaryInfo("pcf2_aggregation", "pcf2_aggregation"),
                "expected": f"{TEST_EXEC_FOLDER}pcf2_aggregation",
            },
        ]

        for case in test_cases:
            # pyre-ignore
            local_path = LocalBinaryPath(TEST_EXEC_FOLDER, case["binary_info"])
            self.assertEqual(case["expected"], str(local_path))
