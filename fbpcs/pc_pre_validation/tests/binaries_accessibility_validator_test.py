# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict
from unittest import TestCase
from unittest.mock import patch, Mock

from fbpcp.error.pcp import PcpError
from fbpcs.pc_pre_validation.binaries_accessibility_validator import (
    BinariesAccessibilityValidator,
)
from fbpcs.pc_pre_validation.constants import BINARIES_ACCESSIBILITY_VALIDATOR_NAME
from fbpcs.pc_pre_validation.enums import ValidationResult
from fbpcs.pc_pre_validation.validation_report import ValidationReport

TEST_REGION = "us-west-2"
TEST_BINARY_REPO = "https://test.s3.us-west-2.amazonaws.com"
TEST_BINARY_PATHS = ["path/to/binary/1", "path/to_binary_2", "path/to_binary_3"]


class TestBinariesAccessibilityValidator(TestCase):
    @patch("fbpcs.pc_pre_validation.binaries_accessibility_validator.S3StorageService")
    def test_run_validations_success(self, storage_service_mock: Mock) -> None:
        expected_report = ValidationReport(
            validation_result=ValidationResult.SUCCESS,
            validator_name=BINARIES_ACCESSIBILITY_VALIDATOR_NAME,
            message="Completed binary accessibility validation successfuly",
        )
        storage_service_mock.__init__(return_value=storage_service_mock)
        storage_service_mock.file_exists.return_value = True

        validator = BinariesAccessibilityValidator(
            TEST_REGION, TEST_BINARY_REPO, TEST_BINARY_PATHS
        )
        report = validator.validate()

        self.assertEqual(report, expected_report)
        self.assertEqual(
            storage_service_mock.file_exists.call_count, len(TEST_BINARY_PATHS)
        )

    @patch("fbpcs.pc_pre_validation.binaries_accessibility_validator.S3StorageService")
    def test_run_validations_binary_not_exist(self, storage_service_mock: Mock) -> None:
        expected_report = ValidationReport(
            validation_result=ValidationResult.FAILED,
            validator_name=BINARIES_ACCESSIBILITY_VALIDATOR_NAME,
            message="You don't have permission to access some private computation softwares. Please contact your representative at Meta",
            details={
                f"{TEST_BINARY_REPO}/{TEST_BINARY_PATHS[0]}": "binary does not exist"
            },
        )
        storage_service_mock.__init__(return_value=storage_service_mock)
        storage_service_mock.file_exists.side_effect = [False, True, True]

        validator = BinariesAccessibilityValidator(
            TEST_REGION, TEST_BINARY_REPO, TEST_BINARY_PATHS
        )
        report = validator.validate()

        self.assertEqual(report, expected_report)
        self.assertEqual(
            storage_service_mock.file_exists.call_count, len(TEST_BINARY_PATHS)
        )

    @patch("fbpcs.pc_pre_validation.binaries_accessibility_validator.S3StorageService")
    def test_run_validations_binary_access_denied(
        self, storage_service_mock: Mock
    ) -> None:
        expected_report = ValidationReport(
            validation_result=ValidationResult.FAILED,
            validator_name=BINARIES_ACCESSIBILITY_VALIDATOR_NAME,
            message="You don't have permission to access some private computation softwares. Please contact your representative at Meta",
            details={
                f"{TEST_BINARY_REPO}/{TEST_BINARY_PATHS[2]}": "An error occurred (403) when calling the HeadObject operation: Forbidden"
            },
        )
        storage_service_mock.__init__(return_value=storage_service_mock)
        storage_service_mock.file_exists.side_effect = [
            True,
            True,
            PcpError(
                Exception(
                    "An error occurred (403) when calling the HeadObject operation: Forbidden"
                )
            ),
        ]
        validator = BinariesAccessibilityValidator(
            TEST_REGION, TEST_BINARY_REPO, TEST_BINARY_PATHS
        )
        report = validator.validate()

        self.assertEqual(report, expected_report)
        self.assertEqual(
            storage_service_mock.file_exists.call_count, len(TEST_BINARY_PATHS)
        )

    @patch("fbpcs.pc_pre_validation.binaries_accessibility_validator.S3StorageService")
    def test_run_validations_unexpected_error(self, storage_service_mock: Mock) -> None:
        expected_report = ValidationReport(
            validation_result=ValidationResult.SUCCESS,
            validator_name=BINARIES_ACCESSIBILITY_VALIDATOR_NAME,
            message=f"WARNING: {BINARIES_ACCESSIBILITY_VALIDATOR_NAME} throws an unexpected error: An internal error occurred (500)",
        )
        storage_service_mock.__init__(return_value=storage_service_mock)
        storage_service_mock.file_exists.side_effect = PcpError(
            Exception("An internal error occurred (500)")
        )
        validator = BinariesAccessibilityValidator(
            TEST_REGION, TEST_BINARY_REPO, TEST_BINARY_PATHS
        )
        report = validator.validate()

        self.assertEqual(report, expected_report)
        self.assertEqual(storage_service_mock.file_exists.call_count, 1)
