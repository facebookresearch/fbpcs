# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict
import os
from unittest import TestCase
from unittest.mock import patch, call, Mock

from fbpcp.error.pcp import PcpError
from fbpcs.pc_pre_validation.binary_file_validator import BinaryFileValidator
from fbpcs.pc_pre_validation.binary_path import BinaryInfo
from fbpcs.pc_pre_validation.constants import (
    BINARY_FILE_VALIDATOR_NAME,
    DEFAULT_BINARY_REPOSITORY,
    DEFAULT_BINARY_VERSION,
    DEFAULT_EXE_FOLDER,
    ONEDOCKER_REPOSITORY_PATH,
    ONEDOCKER_EXE_PATH,
)
from fbpcs.pc_pre_validation.enums import ValidationResult
from fbpcs.pc_pre_validation.validation_report import ValidationReport

TEST_REGION = "us-west-2"
TEST_BINARY_INFOS = [
    BinaryInfo("package/1"),
    BinaryInfo("package/2"),
    BinaryInfo("package/3", "binary"),
]


class TestBinaryFileValidator(TestCase):
    @patch("fbpcs.pc_pre_validation.binary_file_validator.S3StorageService")
    def test_run_validations_success(self, storage_service_mock: Mock) -> None:
        expected_report = ValidationReport(
            validation_result=ValidationResult.SUCCESS,
            validator_name=BINARY_FILE_VALIDATOR_NAME,
            message=f"Completed binary accessibility validation successfully (Repo path: {DEFAULT_BINARY_REPOSITORY}, software_version: {DEFAULT_BINARY_VERSION}).",
        )
        storage_service_mock.__init__(return_value=storage_service_mock)
        storage_service_mock.file_exists.return_value = True

        validator = BinaryFileValidator(TEST_REGION, TEST_BINARY_INFOS)
        report = validator.validate()

        self.assertEqual(report, expected_report)
        self.assertEqual(
            storage_service_mock.file_exists.call_count, len(TEST_BINARY_INFOS)
        )
        storage_service_mock.file_exists.assert_has_calls(
            [
                call(f"{DEFAULT_BINARY_REPOSITORY}package/1/latest/1"),
                call(f"{DEFAULT_BINARY_REPOSITORY}package/2/latest/2"),
                call(f"{DEFAULT_BINARY_REPOSITORY}package/3/latest/binary"),
            ]
        )

    @patch("fbpcs.pc_pre_validation.binary_file_validator.S3StorageService")
    def test_run_validations_binary_not_exist(self, storage_service_mock: Mock) -> None:
        expected_report = ValidationReport(
            validation_result=ValidationResult.FAILED,
            validator_name=BINARY_FILE_VALIDATOR_NAME,
            message=f"You don't have permission to access some private computation software (Repo path: {DEFAULT_BINARY_REPOSITORY}, software_version: {DEFAULT_BINARY_VERSION}). Please contact your representative at Meta",
            details={
                f"{DEFAULT_BINARY_REPOSITORY}package/1/latest/1": "binary does not exist"
            },
        )
        storage_service_mock.__init__(return_value=storage_service_mock)
        storage_service_mock.file_exists.side_effect = [False, True, True]

        validator = BinaryFileValidator(TEST_REGION, TEST_BINARY_INFOS)
        report = validator.validate()

        self.assertEqual(report, expected_report)
        self.assertEqual(
            storage_service_mock.file_exists.call_count, len(TEST_BINARY_INFOS)
        )

    @patch("fbpcs.pc_pre_validation.binary_file_validator.S3StorageService")
    def test_run_validations_binary_access_denied(
        self, storage_service_mock: Mock
    ) -> None:
        expected_report = ValidationReport(
            validation_result=ValidationResult.FAILED,
            validator_name=BINARY_FILE_VALIDATOR_NAME,
            message=f"You don't have permission to access some private computation software (Repo path: {DEFAULT_BINARY_REPOSITORY}, software_version: {DEFAULT_BINARY_VERSION}). Please contact your representative at Meta",
            details={
                f"{DEFAULT_BINARY_REPOSITORY}package/3/latest/binary": "An error occurred (403) when calling the HeadObject operation: Forbidden"
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
        validator = BinaryFileValidator(TEST_REGION, TEST_BINARY_INFOS)
        report = validator.validate()

        self.assertEqual(report, expected_report)
        self.assertEqual(
            storage_service_mock.file_exists.call_count, len(TEST_BINARY_INFOS)
        )

    @patch("fbpcs.pc_pre_validation.binary_file_validator.S3StorageService")
    def test_run_validations_unexpected_error(self, storage_service_mock: Mock) -> None:
        expected_report = ValidationReport(
            validation_result=ValidationResult.SUCCESS,
            validator_name=BINARY_FILE_VALIDATOR_NAME,
            message=f"WARNING: {BINARY_FILE_VALIDATOR_NAME} threw an unexpected error: An internal error occurred (500)",
        )
        storage_service_mock.__init__(return_value=storage_service_mock)
        storage_service_mock.file_exists.side_effect = PcpError(
            Exception("An internal error occurred (500)")
        )
        validator = BinaryFileValidator(TEST_REGION, TEST_BINARY_INFOS)
        report = validator.validate()

        self.assertEqual(report, expected_report)
        self.assertEqual(storage_service_mock.file_exists.call_count, 1)

    @patch("fbpcs.pc_pre_validation.binary_file_validator.S3StorageService")
    @patch.dict(os.environ, {ONEDOCKER_REPOSITORY_PATH: "LOCAL"}, clear=True)
    def test_run_validations_if_repo_envvar_is_local(
        self, storage_service_mock: Mock
    ) -> None:
        expected_report = ValidationReport(
            validation_result=ValidationResult.SUCCESS,
            validator_name=BINARY_FILE_VALIDATOR_NAME,
            message="Completed binary accessibility validation successfully (Repo path: LOCAL, software_version: latest).",
        )
        storage_service_mock.__init__(return_value=storage_service_mock)
        storage_service_mock.file_exists.return_value = True

        validator = BinaryFileValidator(TEST_REGION, TEST_BINARY_INFOS)
        report = validator.validate()

        self.assertEqual(report, expected_report)
        self.assertEqual(storage_service_mock.file_exists.call_count, 0)

    @patch("fbpcs.pc_pre_validation.binary_file_validator.S3StorageService")
    @patch.dict(
        os.environ, {ONEDOCKER_REPOSITORY_PATH: "https://test-repo.com/"}, clear=True
    )
    def test_run_validations_non_default_repo(self, storage_service_mock: Mock) -> None:
        expected_report = ValidationReport(
            validation_result=ValidationResult.SUCCESS,
            validator_name=BINARY_FILE_VALIDATOR_NAME,
            message=f"Completed binary accessibility validation successfully (Repo path: https://test-repo.com/, software_version: {DEFAULT_BINARY_VERSION}).",
        )
        storage_service_mock.__init__(return_value=storage_service_mock)
        storage_service_mock.file_exists.return_value = True

        validator = BinaryFileValidator(TEST_REGION, TEST_BINARY_INFOS)
        report = validator.validate()

        self.assertEqual(report, expected_report)
        self.assertEqual(
            storage_service_mock.file_exists.call_count, len(TEST_BINARY_INFOS)
        )
        storage_service_mock.file_exists.assert_has_calls(
            [
                call("https://test-repo.com/package/1/latest/1"),
                call("https://test-repo.com/package/2/latest/2"),
                call("https://test-repo.com/package/3/latest/binary"),
            ]
        )

    @patch("fbpcs.pc_pre_validation.binary_file_validator.S3StorageService")
    def test_run_validations_non_default_version_tag(
        self, storage_service_mock: Mock
    ) -> None:
        binary_version = "canary"
        expected_report = ValidationReport(
            validation_result=ValidationResult.SUCCESS,
            validator_name=BINARY_FILE_VALIDATOR_NAME,
            message=f"Completed binary accessibility validation successfully (Repo path: {DEFAULT_BINARY_REPOSITORY}, software_version: {binary_version}).",
        )
        storage_service_mock.__init__(return_value=storage_service_mock)
        storage_service_mock.file_exists.return_value = True

        validator = BinaryFileValidator(TEST_REGION, TEST_BINARY_INFOS, binary_version)
        report = validator.validate()

        self.assertEqual(report, expected_report)
        self.assertEqual(
            storage_service_mock.file_exists.call_count, len(TEST_BINARY_INFOS)
        )
        storage_service_mock.file_exists.assert_has_calls(
            [
                call(f"{DEFAULT_BINARY_REPOSITORY}package/1/canary/1"),
                call(f"{DEFAULT_BINARY_REPOSITORY}package/2/canary/2"),
                call(f"{DEFAULT_BINARY_REPOSITORY}package/3/canary/binary"),
            ]
        )

    @patch("fbpcs.pc_pre_validation.binary_file_validator.S3StorageService")
    def test_get_binary_repo_default(self, storage_service_mock: Mock) -> None:
        validator = BinaryFileValidator(TEST_REGION, TEST_BINARY_INFOS)
        self.assertEqual(DEFAULT_BINARY_REPOSITORY, validator._get_repo_path())

    @patch("fbpcs.pc_pre_validation.binary_file_validator.S3StorageService")
    @patch.dict(os.environ, {ONEDOCKER_REPOSITORY_PATH: "non-default"}, clear=True)
    def test_get_binary_repo_non_default(self, storage_service_mock: Mock) -> None:
        validator = BinaryFileValidator(TEST_REGION, TEST_BINARY_INFOS)
        self.assertEqual("non-default", validator._get_repo_path())

    @patch("fbpcs.pc_pre_validation.binary_file_validator.S3StorageService")
    def test_get_exe_folder_default(self, storage_service_mock: Mock) -> None:
        validator = BinaryFileValidator(TEST_REGION, TEST_BINARY_INFOS)
        self.assertEqual(DEFAULT_EXE_FOLDER, validator._get_exe_folder())

    @patch("fbpcs.pc_pre_validation.binary_file_validator.S3StorageService")
    @patch.dict(os.environ, {ONEDOCKER_EXE_PATH: "/non-default/folder/"}, clear=True)
    def test_get_exe_folder_non_default(self, storage_service_mock: Mock) -> None:
        validator = BinaryFileValidator(TEST_REGION, TEST_BINARY_INFOS)
        self.assertEqual("/non-default/folder/", validator._get_exe_folder())
