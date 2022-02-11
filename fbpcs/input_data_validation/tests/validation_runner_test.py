# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

from unittest import TestCase
from unittest.mock import patch, MagicMock, Mock

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
