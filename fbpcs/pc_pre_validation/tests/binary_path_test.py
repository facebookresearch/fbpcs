# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

from unittest import TestCase

from fbpcs.pc_pre_validation.binary_path import BinaryInfo, S3BinaryPath

TEST_REPO = "https://test-bucket.us-west-2.amazonaws.com/"


class TestBinaryPath(TestCase):
    def test_s3_package_path(self) -> None:
        test_cases = [
            {
                "binary_info": BinaryInfo("data_processing/attribution_id_combiner"),
                "version": "latest",
                "expected": f"{TEST_REPO}data_processing/attribution_id_combiner/latest/attribution_id_combiner",
            },
            {
                "binary_info": BinaryInfo("pid/private-id-client", "cross-psi-client"),
                "version": "latest",
                "expected": f"{TEST_REPO}pid/private-id-client/latest/cross-psi-client",
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
            self.assertEquals(case["expected"], str(s3_path))
