# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

from unittest import TestCase
from unittest.mock import patch, MagicMock, Mock

from fbpcs.input_data_validation.constants import INPUT_DATA_TMP_FILE_PATH
from fbpcs.input_data_validation.enums import ValidationResult
from fbpcs.input_data_validation.validation_runner import ValidationRunner
from fbpcs.private_computation.entity.cloud_provider import CloudProvider

TEST_INPUT_FILE_PATH = "s3://test-bucket/data.csv"


class TestValidationRunner(TestCase):
    @patch("fbpcp.service.storage_s3.S3StorageService")
    def test_initializating_the_validation_runner_fields(self, _mock) -> None:
        cloud_provider = CloudProvider.AWS

        validation_runner = ValidationRunner(
            TEST_INPUT_FILE_PATH, cloud_provider, "us-west-2"
        )

        self.assertEqual(validation_runner._input_file_path, TEST_INPUT_FILE_PATH)
        self.assertEqual(validation_runner._cloud_provider, cloud_provider)

    @patch("fbpcs.input_data_validation.validation_runner.S3StorageService")
    def test_initializing_the_validation_runner_storage_service(
        self, mock_storage_service: Mock
    ) -> None:
        cloud_provider = CloudProvider.AWS
        access_key_id = "id1"
        access_key_data = "data2"
        region = "us-east-2"
        constructed_storage_service = MagicMock()
        mock_storage_service.__init__(return_value=constructed_storage_service)

        validation_runner = ValidationRunner(
            TEST_INPUT_FILE_PATH, cloud_provider, region, access_key_id, access_key_data
        )

        mock_storage_service.assert_called_with(region, access_key_id, access_key_data)
        self.assertEqual(
            validation_runner._storage_service, constructed_storage_service
        )

    @patch("fbpcs.input_data_validation.validation_runner.time")
    @patch("fbpcs.input_data_validation.validation_runner.S3StorageService")
    def test_run_validations_success(
        self, storage_service_mock: Mock, time_mock: Mock
    ) -> None:
        timestamp = 1644538741.077141
        time_mock.time.return_value = timestamp
        input_file_path = "s3://test-bucket/data.csv"
        cloud_provider = CloudProvider.AWS
        expected_temp_filepath = f"{INPUT_DATA_TMP_FILE_PATH}/data.csv-{timestamp}"
        expected_report = {
            "status": ValidationResult.SUCCESS.value,
            "message": f"File: {input_file_path} was validated successfully",
        }
        storage_service_mock.__init__(return_value=storage_service_mock)

        validation_runner = ValidationRunner(
            input_file_path, cloud_provider, "us-west-2"
        )
        report = validation_runner.run()

        self.assertEqual(validation_runner._local_file_path, expected_temp_filepath)
        self.assertEqual(report, expected_report)
        storage_service_mock.copy.assert_called_with(
            input_file_path, expected_temp_filepath
        )

    @patch("fbpcs.input_data_validation.validation_runner.S3StorageService")
    def test_run_validations_failure(self, storage_service_mock: Mock) -> None:
        exception_message = "failed to copy"
        input_file_path = "s3://test-bucket/data.csv"
        cloud_provider = CloudProvider.AWS
        expected_report = {
            "status": ValidationResult.FAILED.value,
            "message": f"File: {input_file_path} failed validation. Error: {exception_message}",
        }
        storage_service_mock.__init__(return_value=storage_service_mock)
        storage_service_mock.copy.side_effect = Exception(exception_message)

        validation_runner = ValidationRunner(
            input_file_path, cloud_provider, "us-west-2"
        )
        report = validation_runner.run()

        self.assertEqual(report, expected_report)
