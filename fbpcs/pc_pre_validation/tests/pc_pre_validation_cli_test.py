# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

from unittest import TestCase
from unittest.mock import patch, Mock

from fbpcs.pc_pre_validation import (
    pc_pre_validation_cli as validation_cli,
)
from fbpcs.pc_pre_validation.enums import ValidationResult
from fbpcs.private_computation.entity.cloud_provider import CloudProvider


class TestPCPreValidationCLI(TestCase):
    @patch("fbpcs.pc_pre_validation.pc_pre_validation_cli.print")
    @patch("fbpcs.pc_pre_validation.pc_pre_validation_cli.InputDataValidator")
    @patch("fbpcs.pc_pre_validation.pc_pre_validation_cli.BinaryFileValidator")
    @patch("fbpcs.pc_pre_validation.pc_pre_validation_cli.run_validators")
    def test_parsing_required_args(
        self,
        run_validators_mock: Mock,
        binary_file_validator_mock: Mock,
        input_data_validator_mock: Mock,
        _print_mock: Mock,
    ) -> None:
        aggregated_result = ValidationResult.SUCCESS
        aggregated_report = "Aggregated report..."
        run_validators_mock.side_effect = [[aggregated_result, aggregated_report]]
        expected_input_file_path = "https://test/input-file-path0"
        cloud_provider_str = "AWS"
        expected_cloud_provider = CloudProvider.AWS
        expected_region = "region1"
        argv = [
            f"--input-file-path={expected_input_file_path}",
            f"--cloud-provider={cloud_provider_str}",
            f"--region={expected_region}",
        ]

        validation_cli.main(argv)

        input_data_validator_mock.assert_called_with(
            input_file_path=expected_input_file_path,
            cloud_provider=expected_cloud_provider,
            region=expected_region,
            start_timestamp=None,
            end_timestamp=None,
            access_key_id=None,
            access_key_data=None,
        )
        binary_file_validator_mock.assert_called_with(
            region=expected_region,
            access_key_id=None,
            access_key_data=None,
            binary_version=None,
        )
        run_validators_mock.assert_called_with(
            [input_data_validator_mock(), binary_file_validator_mock()]
        )

    @patch("fbpcs.pc_pre_validation.pc_pre_validation_cli.print")
    @patch("fbpcs.pc_pre_validation.pc_pre_validation_cli.InputDataValidator")
    @patch("fbpcs.pc_pre_validation.pc_pre_validation_cli.BinaryFileValidator")
    @patch("fbpcs.pc_pre_validation.pc_pre_validation_cli.run_validators")
    def test_parsing_all_args(
        self,
        run_validators_mock: Mock,
        binary_file_validator_mock: Mock,
        input_data_validator_mock: Mock,
        _print_mock: Mock,
    ) -> None:
        aggregated_result = ValidationResult.SUCCESS
        aggregated_report = "Aggregated report..."
        run_validators_mock.side_effect = [[aggregated_result, aggregated_report]]
        expected_input_file_path = "https://test/input-file-path0"
        cloud_provider_str = "AWS"
        expected_cloud_provider = CloudProvider.AWS
        expected_region = "region1"
        expected_start_timestamp = "1600000000"
        expected_end_timestamp = "1640000000"
        expected_access_key_id = "access_key_id2"
        expected_access_key_data = "access_key_data3"
        expected_binary_version = "binary_version"
        argv = [
            f"--input-file-path={expected_input_file_path}",
            f"--cloud-provider={cloud_provider_str}",
            f"--region={expected_region}",
            f"--start-timestamp={expected_start_timestamp}",
            f"--end-timestamp={expected_end_timestamp}",
            f"--access-key-id={expected_access_key_id}",
            f"--access-key-data={expected_access_key_data}",
            f"--binary-version={expected_binary_version}",
        ]

        validation_cli.main(argv)

        input_data_validator_mock.assert_called_with(
            input_file_path=expected_input_file_path,
            cloud_provider=expected_cloud_provider,
            region=expected_region,
            start_timestamp=expected_start_timestamp,
            end_timestamp=expected_end_timestamp,
            access_key_id=expected_access_key_id,
            access_key_data=expected_access_key_data,
        )
        binary_file_validator_mock.assert_called_with(
            region=expected_region,
            access_key_id=expected_access_key_id,
            access_key_data=expected_access_key_data,
            binary_version=expected_binary_version,
        )
        run_validators_mock.assert_called_with(
            [input_data_validator_mock(), binary_file_validator_mock()]
        )

    @patch("fbpcs.pc_pre_validation.pc_pre_validation_cli.print")
    @patch("fbpcs.pc_pre_validation.pc_pre_validation_cli.InputDataValidator")
    @patch("fbpcs.pc_pre_validation.pc_pre_validation_cli.BinaryFileValidator")
    @patch("fbpcs.pc_pre_validation.pc_pre_validation_cli.run_validators")
    def test_it_includes_the_overall_result_when_failed(
        self,
        run_validators_mock: Mock,
        binary_file_validator_mock: Mock,
        input_data_validator_mock: Mock,
        _print_mock: Mock,
    ) -> None:
        aggregated_result = ValidationResult.FAILED
        aggregated_report = "Aggregated report..."
        run_validators_mock.side_effect = [[aggregated_result, aggregated_report]]
        argv = [
            "--input-file-path=test-path",
            "--cloud-provider=AWS",
            "--region=test-region",
        ]
        expected_overall_result_str = (
            f"Overall Validation Result: {aggregated_result.value}"
        )

        with self.assertRaisesRegex(Exception, expected_overall_result_str):
            validation_cli.main(argv)

    @patch("fbpcs.pc_pre_validation.pc_pre_validation_cli.print")
    @patch("fbpcs.pc_pre_validation.pc_pre_validation_cli.InputDataValidator")
    @patch("fbpcs.pc_pre_validation.pc_pre_validation_cli.BinaryFileValidator")
    @patch("fbpcs.pc_pre_validation.pc_pre_validation_cli.run_validators")
    def test_it_includes_the_overall_result_when_success(
        self,
        run_validators_mock: Mock,
        binary_file_validator_mock: Mock,
        input_data_validator_mock: Mock,
        print_mock: Mock,
    ) -> None:
        aggregated_result = ValidationResult.SUCCESS
        aggregated_report = "Aggregated report..."
        run_validators_mock.side_effect = [[aggregated_result, aggregated_report]]
        argv = [
            "--input-file-path=test-path",
            "--cloud-provider=AWS",
            "--region=test-region",
        ]
        expected_overall_result_str = (
            f"Overall Validation Result: {aggregated_result.value}"
        )

        validation_cli.main(argv)

        print_str = str(print_mock.call_args[0])
        self.assertRegex(print_str, expected_overall_result_str)
