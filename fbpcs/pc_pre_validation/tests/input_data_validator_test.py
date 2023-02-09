# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict
import os
import random
import time
from typing import Iterable
from unittest import TestCase
from unittest.mock import MagicMock, Mock, patch

from fbpcs.pc_pre_validation.constants import (
    ID_FIELD_PREFIX,
    INPUT_DATA_MAX_FILE_SIZE_IN_BYTES,
    INPUT_DATA_TMP_FILE_PATH,
    INPUT_DATA_VALIDATOR_NAME,
    PA_FIELDS,
    PL_FIELDS,
    PRIVATE_ID_DFCA_FIELDS,
)
from fbpcs.pc_pre_validation.enums import ValidationResult
from fbpcs.pc_pre_validation.input_data_validator import InputDataValidator
from fbpcs.pc_pre_validation.validation_report import ValidationReport
from fbpcs.private_computation.entity.cloud_provider import CloudProvider

# Name the file randomly in order to avoid failures when the tests run concurrently
TEST_FILENAME = f"test-input-data-validation-{random.randint(0, 1000000)}.csv"
TEST_BUCKET = "test-bucket"
TEST_FILE_SIZE = 1234
TEST_CLOUD_PROVIDER: CloudProvider = CloudProvider.AWS
TEST_INPUT_FILE_PATH = (
    f"https://{TEST_BUCKET}.s3.us-west-2.amazonaws.com/{TEST_FILENAME}"
)
TEST_REGION = "us-west-2"
TEST_STREAM_FILE = False
TEST_TIMESTAMP: float = time.time()
TEST_TEMP_FILEPATH = f"{INPUT_DATA_TMP_FILE_PATH}/{TEST_FILENAME}-{TEST_TIMESTAMP}"


class TestInputDataValidator(TestCase):
    def setUp(self) -> None:
        patched_boto3_client = patch(
            "fbpcs.pc_pre_validation.input_data_validator.boto3.client"
        )
        patched_storage_service = patch(
            "fbpcs.pc_pre_validation.input_data_validator.S3StorageService"
        )
        self.addCleanup(patched_storage_service.stop)
        self.addCleanup(patched_boto3_client.stop)
        storage_service_mock = patched_storage_service.start()
        storage_service_mock.__init__(return_value=storage_service_mock)
        self.storage_service_mock = storage_service_mock
        storage_service_mock.get_file_size.return_value = TEST_FILE_SIZE
        with open(TEST_TEMP_FILEPATH, "a") as file:
            file.write("")
        boto3_client_mock = patched_boto3_client.start()
        boto3_client_mock.__init__(return_value=boto3_client_mock)
        self._boto3_client_mock = boto3_client_mock

    def tearDown(self) -> None:
        os.remove(TEST_TEMP_FILEPATH)

    def write_lines_to_file(self, lines: Iterable[bytes]) -> None:
        with open(TEST_TEMP_FILEPATH, "wb") as tmp_csv_file:
            tmp_csv_file.writelines(lines)

    def test_initializing_the_validation_runner_fields(self) -> None:
        access_key_id = "id1"
        access_key_data = "data2"
        constructed_storage_service = MagicMock()
        self.storage_service_mock.__init__(return_value=constructed_storage_service)

        validator = InputDataValidator(
            TEST_INPUT_FILE_PATH,
            TEST_CLOUD_PROVIDER,
            TEST_REGION,
            TEST_STREAM_FILE,
            access_key_id,
            access_key_data,
        )

        self.storage_service_mock.assert_called_with(
            TEST_REGION, access_key_id, access_key_data
        )
        self.assertEqual(validator._storage_service, constructed_storage_service)
        self.assertEqual(validator._input_file_path, TEST_INPUT_FILE_PATH)
        self.assertEqual(validator._cloud_provider, TEST_CLOUD_PROVIDER)

    def test_run_validations_copy_failure(self) -> None:
        exception_message = "failed to copy"
        expected_report = ValidationReport(
            validation_result=ValidationResult.FAILED,
            validator_name=INPUT_DATA_VALIDATOR_NAME,
            message=f"File: {TEST_INPUT_FILE_PATH} failed validation. Error: Failed to download the input file. Please check the file path and its permission.\n\t{exception_message}",
            details={
                "rows_processed_count": 0,
            },
        )
        self.storage_service_mock.copy.side_effect = Exception(exception_message)

        validator = InputDataValidator(
            TEST_INPUT_FILE_PATH, TEST_CLOUD_PROVIDER, TEST_REGION, TEST_STREAM_FILE
        )
        report = validator.validate()

        self.assertEqual(report, expected_report)

    @patch("fbpcs.pc_pre_validation.input_data_validator.time")
    def test_run_validations_success_for_pl_fields(self, time_mock: Mock) -> None:
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
            TEST_INPUT_FILE_PATH, TEST_CLOUD_PROVIDER, TEST_REGION, TEST_STREAM_FILE
        )
        report = validator.validate()

        self.assertEqual(report, expected_report)

    @patch("fbpcs.pc_pre_validation.input_data_validator.time")
    def test_run_validations_success_for_pl_fields_with_cohort_id(
        self, time_mock: Mock
    ) -> None:
        time_mock.time.return_value = TEST_TIMESTAMP
        lines = [
            b"id_,value,event_timestamp,cohort_id\n",
            b"abcd/1234+WXYZ=,100,1645157987,0\n",
            b"abcd/1234+WXYZ=,100,1645157987,1\n",
            b"abcd/1234+WXYZ=,100,1645157987,2\n",
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
            TEST_INPUT_FILE_PATH, TEST_CLOUD_PROVIDER, TEST_REGION, TEST_STREAM_FILE
        )
        report = validator.validate()

        self.assertEqual(report, expected_report)

    @patch("fbpcs.pc_pre_validation.input_data_validator.time")
    def test_run_validations_fail_for_cohort_id_not_starting_with_zero(
        self, time_mock: Mock
    ) -> None:
        time_mock.time.return_value = TEST_TIMESTAMP
        lines = [
            b"id_,value,event_timestamp,cohort_id\n",
            b"abcd/1234+WXYZ=,100,1645157987,23434\n",
            b"abcd/1234+WXYZ=,100,1645157987,23425\n",
            b"abcd/1234+WXYZ=,100,1645157987,23436\n",
        ]
        self.write_lines_to_file(lines)
        expected_report = ValidationReport(
            validation_result=ValidationResult.FAILED,
            validator_name=INPUT_DATA_VALIDATOR_NAME,
            message=f"File: {TEST_INPUT_FILE_PATH} failed validation. Error: Cohort Id Format is invalid. Cohort ID should start with 0 and increment by 1.",
            details={
                "rows_processed_count": 3,
            },
        )

        validator = InputDataValidator(
            TEST_INPUT_FILE_PATH, TEST_CLOUD_PROVIDER, TEST_REGION, TEST_STREAM_FILE
        )
        report = validator.validate()

        self.assertEqual(report, expected_report)

    @patch("fbpcs.pc_pre_validation.input_data_validator.time")
    def test_run_validations_fail_for_purchase_value_greater_than_int_max_for_pl(
        self, time_mock: Mock
    ) -> None:
        time_mock.time.return_value = TEST_TIMESTAMP
        lines = [
            b"id_,value,event_timestamp,cohort_id\n",
            b"abcd/1234+WXYZ=,100,1645157987,0\n",
            b"abcd/1234+WXYZ=,100,1645157987,1\n",
            b"abcd/1234+WXYZ=,2147483648,1645157987,2\n",
        ]
        self.write_lines_to_file(lines)
        expected_report = ValidationReport(
            validation_result=ValidationResult.FAILED,
            validator_name=INPUT_DATA_VALIDATOR_NAME,
            message=f"File: {TEST_INPUT_FILE_PATH} failed validation, with errors on 'value'.",
            details={
                "rows_processed_count": 3,
                "validation_errors": {
                    "value": {
                        "out_of_range_count": 1,
                    },
                    "error_messages": ["Purchase value should be less than 2147483647"],
                },
            },
        )

        validator = InputDataValidator(
            TEST_INPUT_FILE_PATH, TEST_CLOUD_PROVIDER, TEST_REGION, TEST_STREAM_FILE
        )
        report = validator.validate()

        self.assertEqual(report, expected_report)

    @patch("fbpcs.pc_pre_validation.input_data_validator.time")
    def test_run_validations_fail_for_purchase_value_greater_than_int_max_for_pa(
        self, time_mock: Mock
    ) -> None:
        time_mock.time.return_value = TEST_TIMESTAMP
        lines = [
            b"id_,conversion_value,conversion_timestamp,conversion_metadata,cohort_id\n",
            b"abcd/1234+WXYZ=,100,1645157987,0,0\n",
            b"abcd/1234+WXYZ=,100,1645157987,0,1\n",
            b"abcd/1234+WXYZ=,2147483648,1645157987,0,2\n",
        ]
        self.write_lines_to_file(lines)
        expected_report = ValidationReport(
            validation_result=ValidationResult.FAILED,
            validator_name=INPUT_DATA_VALIDATOR_NAME,
            message=f"File: {TEST_INPUT_FILE_PATH} failed validation, with errors on 'conversion_value'.",
            details={
                "rows_processed_count": 3,
                "validation_errors": {
                    "conversion_value": {
                        "out_of_range_count": 1,
                    },
                    "error_messages": ["Purchase value should be less than 2147483647"],
                },
            },
        )

        validator = InputDataValidator(
            TEST_INPUT_FILE_PATH, TEST_CLOUD_PROVIDER, TEST_REGION, TEST_STREAM_FILE
        )
        report = validator.validate()

        self.assertEqual(report, expected_report)

    @patch("fbpcs.pc_pre_validation.input_data_validator.time")
    def test_run_validations_fail_for_cohort_id_not_incremental_by_one(
        self, time_mock: Mock
    ) -> None:
        time_mock.time.return_value = TEST_TIMESTAMP
        lines = [
            b"id_,value,event_timestamp,cohort_id\n",
            b"abcd/1234+WXYZ=,100,1645157987,0\n",
            b"abcd/1234+WXYZ=,100,1645157987,1\n",
            b"abcd/1234+WXYZ=,100,1645157987,3\n",
        ]
        self.write_lines_to_file(lines)
        expected_report = ValidationReport(
            validation_result=ValidationResult.FAILED,
            validator_name=INPUT_DATA_VALIDATOR_NAME,
            message=f"File: {TEST_INPUT_FILE_PATH} failed validation. Error: Cohort Id Format is invalid. Cohort ID should start with 0 and increment by 1.",
            details={
                "rows_processed_count": 3,
            },
        )

        validator = InputDataValidator(
            TEST_INPUT_FILE_PATH, TEST_CLOUD_PROVIDER, TEST_REGION, TEST_STREAM_FILE
        )
        report = validator.validate()

        self.assertEqual(report, expected_report)

    @patch("fbpcs.pc_pre_validation.input_data_validator.time")
    def test_run_validations_fail_for_cohort_id_count_too_high(
        self, time_mock: Mock
    ) -> None:
        time_mock.time.return_value = TEST_TIMESTAMP
        lines = [
            b"id_,value,event_timestamp,cohort_id\n",
            b"abcd/1234+WXYZ=,100,1645157987,0\n",
            b"abcd/1234+WXYZ=,100,1645157987,1\n",
            b"abcd/1234+WXYZ=,100,1645157987,2\n",
            b"abcd/1234+WXYZ=,100,1645157987,3\n",
            b"abcd/1234+WXYZ=,100,1645157987,4\n",
            b"abcd/1234+WXYZ=,100,1645157987,5\n",
            b"abcd/1234+WXYZ=,100,1645157987,6\n",
            b"abcd/1234+WXYZ=,100,1645157987,7\n",
        ]
        self.write_lines_to_file(lines)
        expected_report = ValidationReport(
            validation_result=ValidationResult.FAILED,
            validator_name=INPUT_DATA_VALIDATOR_NAME,
            message=f"File: {TEST_INPUT_FILE_PATH} failed validation. Error: Number of cohorts is higher than currently supported.",
            details={
                "rows_processed_count": 8,
            },
        )

        validator = InputDataValidator(
            TEST_INPUT_FILE_PATH, TEST_CLOUD_PROVIDER, TEST_REGION, TEST_STREAM_FILE
        )
        report = validator.validate()

        self.assertEqual(report, expected_report)

    @patch("fbpcs.pc_pre_validation.input_data_validator.time")
    def test_run_validations_success_for_multikey_pl_fields(
        self, time_mock: Mock
    ) -> None:
        time_mock.time.return_value = TEST_TIMESTAMP
        lines = [
            b"id_madid,id_email,id_phone,value,event_timestamp\n",
            b"abcd/1234+WXYZ=,dabcd/1234+WXYZ=,4abcd/1234+WXYZ=,100,1645157987\n",
            b",abcd/1234+WXYZ=,abcd/1234+WXYZ=,100,1645157987\n",
            b"abcd/1234+WXYZ=,,,100,1645157987\n",
        ]
        self.write_lines_to_file(lines)
        warning_fields = "id_"
        expected_report = ValidationReport(
            validation_result=ValidationResult.SUCCESS,
            validator_name=INPUT_DATA_VALIDATOR_NAME,
            message=f"File: {TEST_INPUT_FILE_PATH} completed validation successfully, with warnings on '{warning_fields}'.",
            details={
                "rows_processed_count": 3,
                "validation_warnings": {
                    "id_": {
                        "empty_count": 3,
                    },
                },
            },
        )

        validator = InputDataValidator(
            TEST_INPUT_FILE_PATH, TEST_CLOUD_PROVIDER, TEST_REGION, TEST_STREAM_FILE
        )
        report = validator.validate()

        self.assertEqual(report, expected_report)

    @patch("fbpcs.pc_pre_validation.input_data_validator.time")
    def test_run_validations_success_for_pa_fields(self, time_mock: Mock) -> None:
        time_mock.time.return_value = TEST_TIMESTAMP
        cloud_provider = CloudProvider.AWS
        lines = [
            b"id_,conversion_value,conversion_timestamp,conversion_metadata\n",
            b"abcd/1234+WXYZ=,,1645157987,0\n",
            b"abcd/1234+WXYZ=,,1645157987,0\n",
            b"abcd/1234+WXYZ=,$20,1645157987,0\n",
        ]
        self.write_lines_to_file(lines)
        warning_fields = "conversion_value"
        expected_report = ValidationReport(
            validation_result=ValidationResult.SUCCESS,
            validator_name=INPUT_DATA_VALIDATOR_NAME,
            message=f"File: {TEST_INPUT_FILE_PATH} completed validation successfully, with warnings on '{warning_fields}'.",
            details={
                "rows_processed_count": 3,
                "validation_warnings": {
                    "conversion_value": {
                        "empty_count": 2,
                        "bad_format_count": 1,
                    },
                },
            },
        )

        validator = InputDataValidator(
            TEST_INPUT_FILE_PATH, cloud_provider, TEST_REGION, TEST_STREAM_FILE
        )
        report = validator.validate()

        self.assertEqual(report, expected_report)

    @patch("fbpcs.pc_pre_validation.input_data_validator.time")
    def test_run_validations_success_for_multikey_pa_fields(
        self, time_mock: Mock
    ) -> None:
        time_mock.time.return_value = TEST_TIMESTAMP
        cloud_provider = CloudProvider.AWS
        lines = [
            b"id_madid,id_email,id_phone,conversion_value,conversion_timestamp,conversion_metadata\n",
            b"abcd/1234+WXYZ=,abcd/1234+WXYZ=,abcd/1234+WXYZ=,,1645157987,0\n",
            b"abcd/1234+WXYZ=,,,,1645157987,0\n",
            b",abcd/1234+WXYZ=,abcd/1234+WXYZ=,$20,1645157987,0\n",
        ]
        self.write_lines_to_file(lines)
        warning_fields = "conversion_value, id_"
        expected_report = ValidationReport(
            validation_result=ValidationResult.SUCCESS,
            validator_name=INPUT_DATA_VALIDATOR_NAME,
            message=f"File: {TEST_INPUT_FILE_PATH} completed validation successfully, with warnings on '{warning_fields}'.",
            details={
                "rows_processed_count": 3,
                "validation_warnings": {
                    "id_": {
                        "empty_count": 3,
                    },
                    "conversion_value": {
                        "empty_count": 2,
                        "bad_format_count": 1,
                    },
                },
            },
        )

        validator = InputDataValidator(
            TEST_INPUT_FILE_PATH, cloud_provider, TEST_REGION, TEST_STREAM_FILE
        )
        report = validator.validate()

        self.assertEqual(report, expected_report)

    @patch("fbpcs.pc_pre_validation.input_data_validator.time")
    def test_run_validations_errors_when_pa_pl_data_fields_not_found(
        self, time_mock: Mock
    ) -> None:
        exception_message = f"Failed to parse the header row. The header row fields must have either: {PL_FIELDS} or: {PA_FIELDS} or: {PRIVATE_ID_DFCA_FIELDS}"
        time_mock.time.return_value = TEST_TIMESTAMP
        lines = [
            b"id_,header,row\n",
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
            TEST_INPUT_FILE_PATH, TEST_CLOUD_PROVIDER, TEST_REGION, TEST_STREAM_FILE
        )
        report = validator.validate()
        self.assertEqual(report, expected_report)

    @patch("fbpcs.pc_pre_validation.input_data_validator.time")
    def test_run_validations_errors_when_pid_data_fields_not_found(
        self, time_mock: Mock
    ) -> None:
        exception_message = f"Failed to parse the header row. The header row fields must have columns with prefix {ID_FIELD_PREFIX}"
        time_mock.time.return_value = TEST_TIMESTAMP
        lines = [
            b"noid_,conversion_value,conversion_timestamp,conversion_metadata\n",
            b"abcd/1234+WXYZ=,,1645157987,0\n",
            b"abcd/1234+WXYZ=,,1645157987,0\n",
            b"abcd/1234+WXYZ=,$20,1645157987,0\n",
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
            TEST_INPUT_FILE_PATH, TEST_CLOUD_PROVIDER, TEST_REGION, TEST_STREAM_FILE
        )
        report = validator.validate()
        self.assertEqual(report, expected_report)

    @patch("fbpcs.pc_pre_validation.input_data_validator.time")
    def test_run_validations_errors_when_there_is_no_header_row(
        self, time_mock: Mock
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
            TEST_INPUT_FILE_PATH, TEST_CLOUD_PROVIDER, TEST_REGION, TEST_STREAM_FILE
        )
        report = validator.validate()

        self.assertEqual(report, expected_report)

    @patch("fbpcs.pc_pre_validation.input_data_validator.time")
    def test_run_validations_errors_when_the_line_ending_is_unsupported(
        self, time_mock: Mock
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
            TEST_INPUT_FILE_PATH, TEST_CLOUD_PROVIDER, TEST_REGION, TEST_STREAM_FILE
        )
        report = validator.validate()

        self.assertEqual(report, expected_report)

    @patch("fbpcs.pc_pre_validation.input_data_validator.time")
    def test_run_validations_reports_for_pl_when_row_values_are_empty(
        self, time_mock: Mock
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
        error_fields = "event_timestamp"
        expected_report = ValidationReport(
            validation_result=ValidationResult.FAILED,
            validator_name=INPUT_DATA_VALIDATOR_NAME,
            message=f"File: {TEST_INPUT_FILE_PATH} failed validation, with errors on '{error_fields}'.",
            details={
                "rows_processed_count": 6,
                "validation_errors": {
                    "event_timestamp": {
                        "empty_count": 4,
                    },
                },
                "validation_warnings": {
                    "value": {
                        "empty_count": 2,
                    },
                    "id_": {
                        "empty_count": 1,
                    },
                },
            },
        )

        validator = InputDataValidator(
            TEST_INPUT_FILE_PATH, TEST_CLOUD_PROVIDER, TEST_REGION, TEST_STREAM_FILE
        )
        report = validator.validate()
        self.assertEqual(report, expected_report)

    @patch("fbpcs.pc_pre_validation.input_data_validator.time")
    def test_run_validations_reports_for_pa_when_row_values_are_empty(
        self, time_mock: Mock
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
        error_fields = "conversion_timestamp"
        expected_report = ValidationReport(
            validation_result=ValidationResult.FAILED,
            validator_name=INPUT_DATA_VALIDATOR_NAME,
            message=f"File: {TEST_INPUT_FILE_PATH} failed validation, with errors on '{error_fields}'.",
            details={
                "rows_processed_count": 6,
                "validation_errors": {
                    "conversion_timestamp": {
                        "empty_count": 4,
                    },
                },
                "validation_warnings": {
                    "conversion_value": {
                        "empty_count": 2,
                    },
                    "conversion_metadata": {
                        "empty_count": 3,
                    },
                },
            },
        )

        validator = InputDataValidator(
            TEST_INPUT_FILE_PATH, TEST_CLOUD_PROVIDER, TEST_REGION, TEST_STREAM_FILE
        )
        report = validator.validate()

        self.assertEqual(report, expected_report)

    @patch("fbpcs.pc_pre_validation.input_data_validator.time")
    def test_run_validations_reports_for_pl_when_row_values_are_not_valid(
        self, time_mock: Mock
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
        error_fields = "event_timestamp, id_"
        expected_report = ValidationReport(
            validation_result=ValidationResult.FAILED,
            validator_name=INPUT_DATA_VALIDATOR_NAME,
            message=f"File: {TEST_INPUT_FILE_PATH} failed validation, with errors on '{error_fields}'.",
            details={
                "rows_processed_count": 5,
                "validation_errors": {
                    "id_": {
                        "bad_format_count": 1,
                    },
                    "event_timestamp": {
                        "bad_format_count": 3,
                    },
                },
                "validation_warnings": {
                    "value": {
                        "bad_format_count": 1,
                        "empty_count": 2,
                    },
                },
            },
        )

        validator = InputDataValidator(
            TEST_INPUT_FILE_PATH, TEST_CLOUD_PROVIDER, TEST_REGION, TEST_STREAM_FILE
        )
        report = validator.validate()
        self.assertEqual(report, expected_report)

    @patch("fbpcs.pc_pre_validation.input_data_validator.time")
    def test_run_validations_reports_for_pl_when_no_ids(self, time_mock: Mock) -> None:
        time_mock.time.return_value = TEST_TIMESTAMP
        lines = [
            b"id_madid,id_email,value,event_timestamp\n",
            b",,100,1645157987\n",
            b",,100,1645157987\n",
        ]
        self.write_lines_to_file(lines)
        error_fields = "id_"
        expected_report = ValidationReport(
            validation_result=ValidationResult.FAILED,
            validator_name=INPUT_DATA_VALIDATOR_NAME,
            message=f"File: {TEST_INPUT_FILE_PATH} failed validation, with errors on '{error_fields}'.",
            details={
                "rows_processed_count": 2,
                "validation_errors": {
                    "id_": {
                        "empty_count": 4,
                    },
                },
            },
        )

        validator = InputDataValidator(
            TEST_INPUT_FILE_PATH, TEST_CLOUD_PROVIDER, TEST_REGION, TEST_STREAM_FILE
        )
        report = validator.validate()
        self.assertEqual(report, expected_report)

    @patch("fbpcs.pc_pre_validation.input_data_validator.time")
    def test_run_validations_reports_for_pa_when_row_values_are_not_valid(
        self, time_mock: Mock
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
        error_fields = "conversion_timestamp, id_"
        expected_report = ValidationReport(
            validation_result=ValidationResult.FAILED,
            validator_name=INPUT_DATA_VALIDATOR_NAME,
            message=f"File: {TEST_INPUT_FILE_PATH} failed validation, with errors on '{error_fields}'.",
            details={
                "rows_processed_count": 4,
                "validation_errors": {
                    "id_": {
                        "bad_format_count": 2,
                    },
                    "conversion_timestamp": {
                        "bad_format_count": 2,
                    },
                },
                "validation_warnings": {
                    "id_": {
                        "empty_count": 1,
                    },
                    "conversion_metadata": {"empty_count": 2, "bad_format_count": 1},
                    "conversion_value": {
                        "bad_format_count": 1,
                    },
                },
            },
        )

        validator = InputDataValidator(
            TEST_INPUT_FILE_PATH, TEST_CLOUD_PROVIDER, TEST_REGION, TEST_STREAM_FILE
        )
        report = validator.validate()
        self.assertEqual(report, expected_report)

    @patch(
        "fbpcs.pc_pre_validation.input_data_validator.InputDataValidationIssues.count_empty_field"
    )
    @patch("fbpcs.pc_pre_validation.input_data_validator.time")
    def test_run_validations_an_unhandled_exception_propagates_to_the_caller(
        self,
        time_mock: Mock,
        count_empty_field_mock: Mock,
    ) -> None:
        time_mock.time.return_value = TEST_TIMESTAMP
        expected_exception_message = "bug in the logic"
        lines = [
            b"id_,value,event_timestamp\n",
            b"abcd/1234+WXYZ=,,1645157987\n",
        ]
        self.write_lines_to_file(lines)
        count_empty_field_mock.side_effect = Exception(expected_exception_message)

        validator = InputDataValidator(
            TEST_INPUT_FILE_PATH, TEST_CLOUD_PROVIDER, TEST_REGION, TEST_STREAM_FILE
        )
        report = validator.validate()

        self.assertEqual(report.validation_result, ValidationResult.SUCCESS)
        self.assertRegex(
            report.message,
            f"WARNING: {INPUT_DATA_VALIDATOR_NAME} threw an unexpected error: {expected_exception_message}",
        )

    @patch("fbpcs.pc_pre_validation.input_data_validator.time")
    def test_run_validations_it_skips_input_data_processing_when_the_file_is_too_large(
        self, time_mock: Mock
    ) -> None:
        file_size = 3567123432
        time_mock.time.return_value = TEST_TIMESTAMP
        self.storage_service_mock.get_file_size.return_value = file_size
        expected_report = ValidationReport(
            validation_result=ValidationResult.SUCCESS,
            validator_name=INPUT_DATA_VALIDATOR_NAME,
            message=" ".join(
                [
                    f"WARNING: File: {TEST_INPUT_FILE_PATH} is too large to download.",
                    f"The maximum file size is {int(INPUT_DATA_MAX_FILE_SIZE_IN_BYTES / (1024 * 1024))} MB.",
                    "Skipped input_data validation. completed validation successfully",
                ]
            ),
            details={
                "rows_processed_count": 0,
            },
        )

        validator = InputDataValidator(
            TEST_INPUT_FILE_PATH, TEST_CLOUD_PROVIDER, TEST_REGION, TEST_STREAM_FILE
        )
        report = validator.validate()

        self.storage_service_mock.get_file_size.assert_called_with(TEST_INPUT_FILE_PATH)
        self.storage_service_mock.copy.assert_not_called()
        self.assertEqual(report, expected_report)

    @patch("fbpcs.pc_pre_validation.input_data_validator.time")
    def test_run_validations_validation_fails_when_fetching_the_file_size_errors(
        self, time_mock: Mock
    ) -> None:
        exception_message = "failed to get the file size"
        expected_report = ValidationReport(
            validation_result=ValidationResult.FAILED,
            validator_name=INPUT_DATA_VALIDATOR_NAME,
            message=f"File: {TEST_INPUT_FILE_PATH} failed validation. Error: Failed to get the input file size. Please check the file path and its permission.\n\t{exception_message}",
            details={
                "rows_processed_count": 0,
            },
        )
        self.storage_service_mock.get_file_size.side_effect = Exception(
            exception_message
        )

        validator = InputDataValidator(
            TEST_INPUT_FILE_PATH, TEST_CLOUD_PROVIDER, TEST_REGION, TEST_STREAM_FILE
        )
        report = validator.validate()

        self.assertEqual(report, expected_report)

    @patch("fbpcs.pc_pre_validation.input_data_validator.time")
    def test_run_validations_success_for_private_id_dfca_fields(
        self, time_mock: Mock
    ) -> None:
        time_mock.time.return_value = TEST_TIMESTAMP
        cloud_provider = CloudProvider.AWS
        lines = [
            b"id_,partner_user_id\n",
            b"abcd/1234+WXYZ=,\n",
            b"abcd/1234+WXYZ=,1\n",
            b"abcd/1234+WXYZ=,0\n",
        ]
        self.write_lines_to_file(lines)
        expected_report = ValidationReport(
            validation_result=ValidationResult.SUCCESS,
            validator_name=INPUT_DATA_VALIDATOR_NAME,
            message=f"File: {TEST_INPUT_FILE_PATH} completed validation successfully",
            details={"rows_processed_count": 3},
        )

        validator = InputDataValidator(
            TEST_INPUT_FILE_PATH, cloud_provider, TEST_REGION, TEST_STREAM_FILE
        )
        report = validator.validate()

        self.assertEqual(report, expected_report)

    @patch("fbpcs.pc_pre_validation.input_data_validator.time")
    def test_validator_it_does_not_set_incorrect_timestamps(
        self, time_mock: Mock
    ) -> None:
        validator = InputDataValidator(
            input_file_path=TEST_INPUT_FILE_PATH,
            cloud_provider=TEST_CLOUD_PROVIDER,
            region=TEST_REGION,
            stream_file=TEST_STREAM_FILE,
            start_timestamp="1650000000",
            end_timestamp="1640000000",
        )

        self.assertIsNone(validator._start_timestamp)
        self.assertIsNone(validator._end_timestamp)

    @patch("fbpcs.pc_pre_validation.input_data_validator.time")
    def test_validator_it_ignores_bad_timestamps(self, time_mock: Mock) -> None:
        validator = InputDataValidator(
            input_file_path=TEST_INPUT_FILE_PATH,
            cloud_provider=TEST_CLOUD_PROVIDER,
            region=TEST_REGION,
            stream_file=TEST_STREAM_FILE,
            start_timestamp="bad-timestamp",
            end_timestamp="",
        )

        self.assertIsNone(validator._start_timestamp)
        self.assertIsNone(validator._end_timestamp)

    @patch("fbpcs.pc_pre_validation.input_data_validator.time")
    def test_run_validations_reports_for_pa_when_timestamps_are_not_valid(
        self, time_mock: Mock
    ) -> None:
        time_mock.time.return_value = TEST_TIMESTAMP
        lines = [
            b"id_,conversion_value,conversion_timestamp,conversion_metadata\n",
            b"abcd/1234+WXYZ=,25,1645157987,0\n",
            b"abcd/1234+WXYZ=,25,1645157987,0\n",
            b"abcd/1234+WXYZ=,25,1639999999,0\n",
            b"abcd/1234+WXYZ=,25,1645157987,0\n",
            b"abcd/1234+WXYZ=,25,9999999999,0\n",
            b"abcd/1234+WXYZ=,25,1640000000,0\n",
            b"abcd/1234+WXYZ=,25,1650000000,0\n",
        ]
        self.write_lines_to_file(lines)
        error_fields = "conversion_timestamp"
        expected_report = ValidationReport(
            validation_result=ValidationResult.FAILED,
            validator_name=INPUT_DATA_VALIDATOR_NAME,
            message=f"File: {TEST_INPUT_FILE_PATH} failed validation, with errors on '{error_fields}'.",
            details={
                "rows_processed_count": 7,
                "validation_errors": {
                    "conversion_timestamp": {
                        "out_of_range_count": 2,
                    },
                },
            },
        )
        validator = InputDataValidator(
            input_file_path=TEST_INPUT_FILE_PATH,
            cloud_provider=TEST_CLOUD_PROVIDER,
            region=TEST_REGION,
            stream_file=TEST_STREAM_FILE,
            start_timestamp="1640000000",
            end_timestamp="1650000000",
        )

        report = validator.validate()

        self.assertEqual(report, expected_report)

    @patch("fbpcs.pc_pre_validation.input_data_validator.time")
    def test_run_validations_reports_for_pl_when_timestamps_are_not_valid(
        self, time_mock: Mock
    ) -> None:
        time_mock.time.return_value = TEST_TIMESTAMP
        lines = [
            b"id_,value,event_timestamp\n",
            b"abcd/1234+WXYZ=,25,1645157987\n",
            b"abcd/1234+WXYZ=,25,1645157987\n",
            b"abcd/1234+WXYZ=,25,1639999999\n",
            b"abcd/1234+WXYZ=,25,1645157987\n",
            b"abcd/1234+WXYZ=,25,9999999999\n",
            b"abcd/1234+WXYZ=,25,1640000000\n",
            b"abcd/1234+WXYZ=,25,1650000000\n",
        ]
        self.write_lines_to_file(lines)
        error_fields = "event_timestamp"
        expected_report = ValidationReport(
            validation_result=ValidationResult.FAILED,
            validator_name=INPUT_DATA_VALIDATOR_NAME,
            message=f"File: {TEST_INPUT_FILE_PATH} failed validation, with errors on '{error_fields}'.",
            details={
                "rows_processed_count": 7,
                "validation_errors": {
                    "event_timestamp": {
                        "out_of_range_count": 2,
                    },
                },
            },
        )
        validator = InputDataValidator(
            input_file_path=TEST_INPUT_FILE_PATH,
            cloud_provider=TEST_CLOUD_PROVIDER,
            region=TEST_REGION,
            stream_file=TEST_STREAM_FILE,
            start_timestamp="1640000000",
            end_timestamp="1650000000",
        )

        report = validator.validate()

        self.assertEqual(report, expected_report)

    @patch("fbpcs.pc_pre_validation.input_data_validator.time")
    def test_run_validations_reports_for_pa_when_most_timestamps_are_valid(
        self, time_mock: Mock
    ) -> None:
        time_mock.time.return_value = TEST_TIMESTAMP
        lines = [
            b"id_,conversion_value,conversion_timestamp,conversion_metadata\n",
            b"abcd/1234+WXYZ=,25,9999999999,0\n",
        ]
        lines.extend([b"abcd/1234+WXYZ=,25,1645157987,0\n"] * 19)
        self.write_lines_to_file(lines)
        warning_fields = "conversion_timestamp"
        expected_report = ValidationReport(
            validation_result=ValidationResult.SUCCESS,
            validator_name=INPUT_DATA_VALIDATOR_NAME,
            message=f"File: {TEST_INPUT_FILE_PATH} completed validation successfully, with warnings on '{warning_fields}'.",
            details={
                "rows_processed_count": 20,
                "validation_warnings": {
                    "conversion_timestamp": {
                        "out_of_range_count": 1,
                    },
                },
            },
        )
        validator = InputDataValidator(
            input_file_path=TEST_INPUT_FILE_PATH,
            cloud_provider=TEST_CLOUD_PROVIDER,
            region=TEST_REGION,
            stream_file=TEST_STREAM_FILE,
            start_timestamp="1640000000",
            end_timestamp="1650000000",
        )

        report = validator.validate()

        self.assertEqual(report, expected_report)

    @patch("fbpcs.pc_pre_validation.input_data_validator.time")
    def test_run_validations_reports_for_pl_when_most_timestamps_are_valid(
        self, time_mock: Mock
    ) -> None:
        time_mock.time.return_value = TEST_TIMESTAMP
        lines = [
            b"id_,value,event_timestamp\n",
            b"abcd/1234+WXYZ=,25,1639999999\n",
        ]
        lines.extend([b"abcd/1234+WXYZ=,25,1645157987\n"] * 10)
        self.write_lines_to_file(lines)
        warning_fields = "event_timestamp"
        expected_report = ValidationReport(
            validation_result=ValidationResult.SUCCESS,
            validator_name=INPUT_DATA_VALIDATOR_NAME,
            message=f"File: {TEST_INPUT_FILE_PATH} completed validation successfully, with warnings on '{warning_fields}'.",
            details={
                "rows_processed_count": 11,
                "validation_warnings": {
                    "event_timestamp": {
                        "out_of_range_count": 1,
                    },
                },
            },
        )
        validator = InputDataValidator(
            input_file_path=TEST_INPUT_FILE_PATH,
            cloud_provider=TEST_CLOUD_PROVIDER,
            region=TEST_REGION,
            stream_file=TEST_STREAM_FILE,
            start_timestamp="1640000000",
            end_timestamp="1650000000",
        )

        report = validator.validate()

        self.assertEqual(report, expected_report)

    def test_it_streams_the_file_when_streaming_is_enabled(self) -> None:
        lines = [
            b"id_,value,event_timestamp\n",
            b"abcd/1234+WXYZ=,25,1645157987\n",
            b"abcd/1234+WXYZ=,25,1645157987\n",
        ]
        expected_report = ValidationReport(
            validation_result=ValidationResult.SUCCESS,
            validator_name=INPUT_DATA_VALIDATOR_NAME,
            message=f"File: {TEST_INPUT_FILE_PATH} completed validation successfully",
            details={"rows_processed_count": 2},
        )
        stream_mock = MagicMock(name="stream_mock_obj")
        self._boto3_client_mock.get_object.return_value = {"Body": stream_mock}
        stream_mock.iter_lines.return_value = lines

        validator = InputDataValidator(
            input_file_path=TEST_INPUT_FILE_PATH,
            cloud_provider=TEST_CLOUD_PROVIDER,
            region=TEST_REGION,
            stream_file=True,
        )

        report = validator.validate()

        self._boto3_client_mock.get_object.assert_called_with(
            Bucket=TEST_BUCKET,
            Key=TEST_FILENAME,
        )
        self.assertEqual(report, expected_report)

    @patch("fbpcs.pc_pre_validation.input_data_validator.time")
    def test_streaming_preemptively_times_out_after_15_minutes(
        self, time_mock: Mock
    ) -> None:
        lines = [b"id_,value,event_timestamp\n"]
        lines.extend([b"abcd/1234+WXYZ=,25,1645157987\n"] * 100001)

        expected_warning = " ".join(
            [
                f"File: {TEST_INPUT_FILE_PATH} completed validation successfully,",
                "with some warnings.",
                "Warning: ran the validations on 100000 total rows,",
                "the rest of the rows were skipped to avoid container timeout. ",
            ]
        )
        expected_report = ValidationReport(
            validation_result=ValidationResult.SUCCESS,
            validator_name=INPUT_DATA_VALIDATOR_NAME,
            message=expected_warning,
            details={"rows_processed_count": 100000},
        )
        stream_mock = MagicMock(name="stream_mock_obj")
        self._boto3_client_mock.get_object.return_value = {"Body": stream_mock}
        stream_mock.iter_lines.return_value = lines
        start_time = time.time()
        time_mock.time.side_effect = [
            start_time,
            start_time,
            start_time,
            start_time + 1200,
        ]

        validator = InputDataValidator(
            input_file_path=TEST_INPUT_FILE_PATH,
            cloud_provider=TEST_CLOUD_PROVIDER,
            region=TEST_REGION,
            stream_file=True,
        )

        report = validator.validate()

        self.assertEqual(report, expected_report)
