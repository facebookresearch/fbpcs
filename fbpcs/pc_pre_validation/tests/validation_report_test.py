# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

from unittest import TestCase

from fbpcs.pc_pre_validation.enums import ValidationResult
from fbpcs.pc_pre_validation.validation_report import ValidationReport


class TestValidationReport(TestCase):
    def test_get_str_for_report_with_details(self) -> None:
        expected_report_str = """Validation Report: test_validator_name
Result: success
Message: test_message
Details:
{
    "test_key_1": 5,
    "test_key_2": {
        "test_key_3": {
            "test_key_4": 1
        },
        "test_key_5": {
            "test_key_6": 1,
            "test_key_7": 2
        }
    }
}"""
        report = ValidationReport(
            validation_result=ValidationResult.SUCCESS,
            validator_name="test_validator_name",
            message="test_message",
            details={
                "test_key_1": 5,
                "test_key_2": {
                    "test_key_3": {
                        "test_key_4": 1,
                    },
                    "test_key_5": {
                        "test_key_6": 1,
                        "test_key_7": 2,
                    },
                },
            },
        )
        self.assertEqual(expected_report_str, str(report))

    def test_get_str_for_report_without_details(self) -> None:
        expected_report_str = """Validation Report: test_validator_name
Result: failed
Message: test_message"""
        report = ValidationReport(
            validation_result=ValidationResult.FAILED,
            validator_name="test_validator_name",
            message="test_message",
        )
        self.assertEqual(expected_report_str, str(report))
