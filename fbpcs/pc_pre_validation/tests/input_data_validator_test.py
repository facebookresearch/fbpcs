# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

import json
import os
import random
import time
from typing import Dict, Iterable, List
from unittest import TestCase
from unittest.mock import patch, MagicMock, Mock

from fbpcs.pc_pre_validation.constants import (
    INPUT_DATA_VALIDATOR_NAME,
    INPUT_DATA_TMP_FILE_PATH,
    PA_FIELDS,
    PL_FIELDS,
    DEFAULT_VALID_THRESHOLDS,
)
from fbpcs.pc_pre_validation.enums import PCRole, ValidationResult
from fbpcs.pc_pre_validation.input_data_validator import InputDataValidator
from fbpcs.pc_pre_validation.validation_report import ValidationReport
from fbpcs.private_computation.entity.cloud_provider import CloudProvider

# Name the file randomly in order to avoid failures when the tests run concurrently
TEST_FILENAME = f"test-input-data-validation-{random.randint(0, 1000000)}.csv"
TEST_CLOUD_PROVIDER: CloudProvider = CloudProvider.AWS
TEST_INPUT_FILE_PATH = f"s3://test-bucket/{TEST_FILENAME}"
TEST_REGION = "us-west-2"
TEST_PC_ROLE: PCRole = PCRole.PARTNER
TEST_TIMESTAMP: float = time.time()
TEST_TEMP_FILEPATH = f"{INPUT_DATA_TMP_FILE_PATH}/{TEST_FILENAME}-{TEST_TIMESTAMP}"
TEST_THRESHOLD_OVERRIDES: Dict[str, float] = {
    "id_": 0.5,
    "value": 0.76,
}
TEST_THRESHOLD_OVERRIDES_STR: str = json.dumps(TEST_THRESHOLD_OVERRIDES)
SKIP_THRESHOLD_VALIDATION_STR: str = json.dumps(
    {
        "id_": 0,
        "value": 0,
        "conversion_value": 0,
        "event_timestamp": 0,
        "conversion_timestamp": 0,
    }
)


class TestInputDataValidator(TestCase):
    def setUp(self) -> None:
        with open(TEST_TEMP_FILEPATH, "a") as file:
            file.write("")

    def tearDown(self) -> None:
        os.remove(TEST_TEMP_FILEPATH)

    def write_lines_to_file(self, lines: Iterable[bytes]) -> None:
        with open(TEST_TEMP_FILEPATH, "wb") as tmp_csv_file:
            tmp_csv_file.writelines(lines)

    @patch("fbpcs.pc_pre_validation.input_data_validator.S3StorageService")
    def test_initializing_the_validation_runner_fields(
        self, mock_storage_service: Mock
    ) -> None:
        access_key_id = "id1"
        access_key_data = "data2"
        constructed_storage_service = MagicMock()
        mock_storage_service.__init__(return_value=constructed_storage_service)

        validator = InputDataValidator(
            TEST_INPUT_FILE_PATH,
            TEST_CLOUD_PROVIDER,
            TEST_REGION,
            TEST_PC_ROLE,
            access_key_id,
            access_key_data,
        )

        mock_storage_service.assert_called_with(
            TEST_REGION, access_key_id, access_key_data
        )
        self.assertEqual(validator._storage_service, constructed_storage_service)
        self.assertEqual(validator._input_file_path, TEST_INPUT_FILE_PATH)
        self.assertEqual(validator._cloud_provider, TEST_CLOUD_PROVIDER)
        self.assertEqual(validator._pc_role, TEST_PC_ROLE)

    @patch("fbpcs.pc_pre_validation.input_data_validator.S3StorageService")
    def test_run_validations_copy_failure(self, storage_service_mock: Mock) -> None:
        exception_message = "failed to copy"
        expected_report = ValidationReport(
            validation_result=ValidationResult.FAILED,
            validator_name=INPUT_DATA_VALIDATOR_NAME,
            message=f"File: {TEST_INPUT_FILE_PATH} failed validation. Error: Failed to download the input file. Please check the file path and its permission.\n\t{exception_message}",
            details={
                "rows_processed_count": 0,
            },
        )
        storage_service_mock.__init__(return_value=storage_service_mock)
        storage_service_mock.copy.side_effect = Exception(exception_message)

        validator = InputDataValidator(
            TEST_INPUT_FILE_PATH, TEST_CLOUD_PROVIDER, TEST_REGION, TEST_PC_ROLE
        )
        report = validator.validate()

        self.assertEqual(report, expected_report)

    @patch("fbpcs.pc_pre_validation.input_data_validator.S3StorageService")
    @patch("fbpcs.pc_pre_validation.input_data_validator.time")
    def test_run_validations_reads_the_local_csv_rows(
        self, time_mock: Mock, _storage_service_mock: Mock
    ) -> None:
        time_mock.time.return_value = TEST_TIMESTAMP
        lines = [
            b"id_,value,event_timestamp\n",
            b"abcd/1234+WXYZ=,100,1645157987\n",
            b"abcd/1234+WXYZ=,100,1645157987\n",
            b"abcd/1234+WXYZ=,100,1645157987\n",
        ]
        self.write_lines_to_file(lines)
        expected_report = ValidationReport(
            validation_result=ValidationResult.SUCCESS,
            validator_name=INPUT_DATA_VALIDATOR_NAME,
            message=f"File: {TEST_INPUT_FILE_PATH} completed validation successfully",
            details={
                "rows_processed_count": 3,
            },
        )

        validator = InputDataValidator(
            TEST_INPUT_FILE_PATH, TEST_CLOUD_PROVIDER, TEST_REGION, TEST_PC_ROLE
        )
        report = validator.validate()
        self.assertEqual(report, expected_report)

    @patch("fbpcs.pc_pre_validation.input_data_validator.S3StorageService")
    @patch("fbpcs.pc_pre_validation.input_data_validator.time")
    def test_run_validations_errors_when_input_data_fields_not_found(
        self, time_mock: Mock, _storage_service_mock: Mock
    ) -> None:
        exception_message = f"Failed to parse the header row. The header row fields must be either: {PL_FIELDS} or: {PA_FIELDS}"
        time_mock.time.return_value = TEST_TIMESTAMP
        lines = [
            b"bad,header,row\n",
            b"1,2,3\n",
            b"4,5,6\n",
        ]
        self.write_lines_to_file(lines)
        expected_report = ValidationReport(
            validation_result=ValidationResult.FAILED,
            validator_name=INPUT_DATA_VALIDATOR_NAME,
            message=f"File: {TEST_INPUT_FILE_PATH} failed validation. Error: {exception_message}",
            details={
                "rows_processed_count": 0,
            },
        )

        validator = InputDataValidator(
            TEST_INPUT_FILE_PATH, TEST_CLOUD_PROVIDER, TEST_REGION, TEST_PC_ROLE
        )
        report = validator.validate()

        self.assertEqual(report, expected_report)

    @patch("fbpcs.pc_pre_validation.input_data_validator.S3StorageService")
    @patch("fbpcs.pc_pre_validation.input_data_validator.time")
    def test_run_validations_errors_when_there_is_no_header_row(
        self, time_mock: Mock, _storage_service_mock: Mock
    ) -> None:
        time_mock.time.return_value = TEST_TIMESTAMP
        expected_report = ValidationReport(
            validation_result=ValidationResult.FAILED,
            validator_name=INPUT_DATA_VALIDATOR_NAME,
            message=f"File: {TEST_INPUT_FILE_PATH} failed validation. Error: The header row was empty.",
            details={
                "rows_processed_count": 0,
            },
        )

        validator = InputDataValidator(
            TEST_INPUT_FILE_PATH, TEST_CLOUD_PROVIDER, TEST_REGION, TEST_PC_ROLE
        )
        report = validator.validate()

        self.assertEqual(report, expected_report)

    @patch("fbpcs.pc_pre_validation.input_data_validator.S3StorageService")
    @patch("fbpcs.pc_pre_validation.input_data_validator.time")
    def test_run_validations_errors_when_the_line_ending_is_unsupported(
        self, time_mock: Mock, _storage_service_mock: Mock
    ) -> None:
        exception_message = "Detected an unexpected line ending. The only supported line ending is '\\n'"
        time_mock.time.return_value = TEST_TIMESTAMP
        lines = [
            b"id_,value,event_timestamp\n",
            b"abcd/1234+WXYZ=,100,1645157987\r\n",
            b"abcd/1234+WXYZ=,100,1645157987\r\n",
        ]
        self.write_lines_to_file(lines)
        expected_report = ValidationReport(
            validation_result=ValidationResult.FAILED,
            validator_name=INPUT_DATA_VALIDATOR_NAME,
            message=f"File: {TEST_INPUT_FILE_PATH} failed validation. Error: {exception_message}",
            details={
                "rows_processed_count": 0,
            },
        )

        validator = InputDataValidator(
            TEST_INPUT_FILE_PATH, TEST_CLOUD_PROVIDER, TEST_REGION, TEST_PC_ROLE
        )
        report = validator.validate()

        self.assertEqual(report, expected_report)

    @patch("fbpcs.pc_pre_validation.input_data_validator.S3StorageService")
    @patch("fbpcs.pc_pre_validation.input_data_validator.time")
    def test_run_validations_reports_for_pl_when_row_values_are_empty(
        self, time_mock: Mock, _storage_service_mock: Mock
    ) -> None:
        time_mock.time.return_value = TEST_TIMESTAMP
        lines = [
            b"id_,value,event_timestamp\n",
            b",100,1645157987\n",
            b"abcd/1234+WXYZ=,,1645157987\n",
            b"abcd/1234+WXYZ=,100,\n",
            b"abcd/1234+WXYZ=,,\n",
            b"abcd/1234+WXYZ=,100,\n",
            b"abcd/1234+WXYZ=,100,\n",
        ]
        self.write_lines_to_file(lines)
        expected_report = ValidationReport(
            validation_result=ValidationResult.SUCCESS,
            validator_name=INPUT_DATA_VALIDATOR_NAME,
            message=f"File: {TEST_INPUT_FILE_PATH} completed validation successfully, with some errors.",
            details={
                "rows_processed_count": 6,
                "validation_errors": {
                    "id_": {
                        "empty": 1,
                    },
                    "value": {
                        "empty": 2,
                    },
                    "event_timestamp": {
                        "empty": 4,
                    },
                },
            },
        )

        validator = InputDataValidator(
            TEST_INPUT_FILE_PATH,
            TEST_CLOUD_PROVIDER,
            TEST_REGION,
            TEST_PC_ROLE,
            valid_threshold_override=SKIP_THRESHOLD_VALIDATION_STR,
        )
        report = validator.validate()

        self.assertEqual(report, expected_report)

    @patch("fbpcs.pc_pre_validation.input_data_validator.S3StorageService")
    @patch("fbpcs.pc_pre_validation.input_data_validator.time")
    def test_run_validations_reports_for_pa_when_row_values_are_empty(
        self, time_mock: Mock, _storage_service_mock: Mock
    ) -> None:
        time_mock.time.return_value = TEST_TIMESTAMP
        lines = [
            b"id_,conversion_value,conversion_timestamp,conversion_metadata\n",
            b"abcd/1234+WXYZ=,100,1645157987,\n",
            b"abcd/1234+WXYZ=,,1645157987,\n",
            b"abcd/1234+WXYZ=,100,,0\n",
            b"abcd/1234+WXYZ=,,,0\n",
            b"abcd/1234+WXYZ=,100,,0\n",
            b"abcd/1234+WXYZ=,100,,\n",
        ]
        self.write_lines_to_file(lines)
        expected_report = ValidationReport(
            validation_result=ValidationResult.SUCCESS,
            validator_name=INPUT_DATA_VALIDATOR_NAME,
            message=f"File: {TEST_INPUT_FILE_PATH} completed validation successfully, with some errors.",
            details={
                "rows_processed_count": 6,
                "validation_errors": {
                    "conversion_value": {
                        "empty": 2,
                    },
                    "conversion_timestamp": {
                        "empty": 4,
                    },
                    "conversion_metadata": {
                        "empty": 3,
                    },
                },
            },
        )

        validator = InputDataValidator(
            TEST_INPUT_FILE_PATH,
            TEST_CLOUD_PROVIDER,
            TEST_REGION,
            TEST_PC_ROLE,
            valid_threshold_override=SKIP_THRESHOLD_VALIDATION_STR,
        )
        report = validator.validate()

        self.assertEqual(report, expected_report)

    @patch("fbpcs.pc_pre_validation.input_data_validator.S3StorageService")
    @patch("fbpcs.pc_pre_validation.input_data_validator.time")
    def test_run_validations_reports_for_pl_when_row_values_are_not_valid(
        self, time_mock: Mock, _storage_service_mock: Mock
    ) -> None:
        time_mock.time.return_value = TEST_TIMESTAMP
        lines = [
            b"id_,value,event_timestamp\n",
            b"ab...,100,1645157987\n",
            b"abcd/1234+WXYZ=,test,ts2\n",
            b"abcd/1234+WXYZ=,100,1645157987\n",
            b"abcd/1234+WXYZ=,,*\n",
            b"abcd/1234+WXYZ=,,&\n",
        ]
        self.write_lines_to_file(lines)
        expected_report = ValidationReport(
            validation_result=ValidationResult.SUCCESS,
            validator_name=INPUT_DATA_VALIDATOR_NAME,
            message=f"File: {TEST_INPUT_FILE_PATH} completed validation successfully, with some errors.",
            details={
                "rows_processed_count": 5,
                "validation_errors": {
                    "id_": {
                        "bad_format": 1,
                    },
                    "value": {
                        "bad_format": 1,
                        "empty": 2,
                    },
                    "event_timestamp": {
                        "bad_format": 3,
                    },
                },
            },
        )

        validator = InputDataValidator(
            TEST_INPUT_FILE_PATH,
            TEST_CLOUD_PROVIDER,
            TEST_REGION,
            TEST_PC_ROLE,
            valid_threshold_override=SKIP_THRESHOLD_VALIDATION_STR,
        )
        report = validator.validate()

        self.assertEqual(report, expected_report)

    @patch("fbpcs.pc_pre_validation.input_data_validator.S3StorageService")
    @patch("fbpcs.pc_pre_validation.input_data_validator.time")
    def test_run_validations_reports_for_pa_when_row_values_are_not_valid(
        self, time_mock: Mock, _storage_service_mock: Mock
    ) -> None:
        time_mock.time.return_value = TEST_TIMESTAMP
        lines = [
            b"id_,conversion_value,conversion_timestamp,conversion_metadata\n",
            b"abcd/1234+WXYZ=,$100,1645157987,\n",
            b" ! ,100,1645157987,\n",
            b"_,100,...,0\n",
            b",100,...,data\n",
        ]
        self.write_lines_to_file(lines)
        expected_report = ValidationReport(
            validation_result=ValidationResult.SUCCESS,
            validator_name=INPUT_DATA_VALIDATOR_NAME,
            message=f"File: {TEST_INPUT_FILE_PATH} completed validation successfully, with some errors.",
            details={
                "rows_processed_count": 4,
                "validation_errors": {
                    "id_": {
                        "bad_format": 2,
                        "empty": 1,
                    },
                    "conversion_value": {
                        "bad_format": 1,
                    },
                    "conversion_timestamp": {
                        "bad_format": 2,
                    },
                    "conversion_metadata": {"bad_format": 1, "empty": 2},
                },
            },
        )

        validator = InputDataValidator(
            TEST_INPUT_FILE_PATH,
            TEST_CLOUD_PROVIDER,
            TEST_REGION,
            TEST_PC_ROLE,
            valid_threshold_override=SKIP_THRESHOLD_VALIDATION_STR,
        )
        report = validator.validate()
        self.assertEqual(report, expected_report)

    @patch("fbpcs.pc_pre_validation.input_data_validator.S3StorageService")
    @patch("fbpcs.pc_pre_validation.input_data_validator.time")
    def test_run_validations_it_fails_if_good_rows_count_falls_under_the_threshold(
        self, time_mock: Mock, _storage_service_mock: Mock
    ) -> None:
        time_mock.time.return_value = TEST_TIMESTAMP
        expected_actual_thresholds = {
            "id_": 0.4,
            "value": 0.5,
            "event_timestamp": 0.9,
        }
        all_thresholds = self._merge_default_threshold_into_override(
            TEST_THRESHOLD_OVERRIDES, list(expected_actual_thresholds.keys())
        )
        exception_message = "\n".join(
            [
                "Too many row values for 'id_,value' are unusable:",
                f"Required data quality: {all_thresholds}",
                f"Actual data quality: {expected_actual_thresholds}",
            ]
        )
        lines = [
            b"id_,value,event_timestamp\n",
            b"...,100,\n",
            b",$100,1645157987\n",
            b",$100,1645157987\n",
            b",$100,1645157987\n",
            b",100,1645157987\n",
            b",100,1645157987\n",
            b"abcd/1234+WXYZ=,,1645157987\n",
            b"abcd/1234+WXYZ=,,1645157987\n",
            b"abcd/1234+WXYZ=,,1645157987\n",
            b"abcd/1234+WXYZ=,,1645157987\n",
        ]
        self.write_lines_to_file(lines)
        expected_report = ValidationReport(
            validation_result=ValidationResult.FAILED,
            validator_name=INPUT_DATA_VALIDATOR_NAME,
            message=f"File: {TEST_INPUT_FILE_PATH} failed validation. Error: {exception_message}",
            details={
                "rows_processed_count": 10,
                "validation_errors": {
                    "id_": {
                        "bad_format": 1,
                        "empty": 5,
                    },
                    "value": {
                        "bad_format": 3,
                        "empty": 4,
                    },
                    "event_timestamp": {
                        "empty": 1,
                    },
                },
            },
        )

        validator = InputDataValidator(
            TEST_INPUT_FILE_PATH,
            TEST_CLOUD_PROVIDER,
            TEST_REGION,
            TEST_PC_ROLE,
            valid_threshold_override=TEST_THRESHOLD_OVERRIDES_STR,
        )
        report = validator.validate()

        self.assertEqual(report, expected_report)

    def _merge_default_threshold_into_override(
        self, override_thresholds: Dict[str, float], actual_threshold_keys: List[str]
    ) -> Dict[str, float]:
        merged_thresholds = override_thresholds.copy()
        for field, threshold in DEFAULT_VALID_THRESHOLDS.items():
            if field not in merged_thresholds and field in actual_threshold_keys:
                merged_thresholds[field] = threshold
        return merged_thresholds
