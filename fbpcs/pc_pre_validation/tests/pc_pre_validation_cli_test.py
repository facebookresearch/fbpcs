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
from fbpcs.pc_pre_validation.enums import PCRole, ValidationResult
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
        expected_pc_role = PCRole.PARTNER
        pc_role_str = "PARTNER"
        argv = [
            f"--input-file-path={expected_input_file_path}",
            f"--cloud-provider={cloud_provider_str}",
            f"--region={expected_region}",
            f"--pc-role={pc_role_str}",
        ]

        validation_cli.main(argv)

        input_data_validator_mock.assert_called_with(
            expected_input_file_path,
            expected_cloud_provider,
            expected_region,
            expected_pc_role,
            None,
            None,
        )
        binary_file_validator_mock.assert_called_with(
            region=expected_region, access_key_id=None, access_key_data=None
        )
        run_validators_mock.assert_called_with(
            [input_data_validator_mock(), binary_file_validator_mock()]
        )
