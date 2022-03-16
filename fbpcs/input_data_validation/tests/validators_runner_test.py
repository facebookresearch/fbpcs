# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

from unittest import TestCase

from fbpcs.input_data_validation.enums import ValidationResult
from fbpcs.input_data_validation.validation_report import ValidationReport
from fbpcs.input_data_validation.validator import Validator
from fbpcs.input_data_validation.validators_runner import run_validators


class TestDummyValidator(Validator):
    def __init__(self, dummy_report: ValidationReport) -> None:
        self.dummy_report: ValidationReport = dummy_report

    @property
    def name(self) -> str:
        return self.dummy_report.validator_name

    def __validate__(self) -> ValidationReport:
        return self.dummy_report


class TestExceptionValidator(Validator):
    @property
    def name(self) -> str:
        return "TestExceptionValidator"

    def __validate__(self) -> ValidationReport:
        raise Exception("test error message")


TEST_SUCCESSFUL_REPORT_1 = ValidationReport(
    validation_result=ValidationResult.SUCCESS,
    validator_name="validator 1",
    message="message 1",
    details={
        "test_key_1": 5,
    },
)

TEST_SUCCESSFUL_REPORT_2 = ValidationReport(
    validation_result=ValidationResult.SUCCESS,
    validator_name="validator 2",
    message="message 2",
    details={
        "test_key_2": 5,
    },
)

TEST_FAILED_REPORT_1 = ValidationReport(
    validation_result=ValidationResult.FAILED,
    validator_name="validator 3",
    message="message 3",
    details={
        "test_key_3": 5,
    },
)


class TestValidationReport(TestCase):
    def test_all_validators_succeed(self) -> None:
        expected_aggregated_result = ValidationResult.SUCCESS
        expected_aggregated_report = (
            f"{TEST_SUCCESSFUL_REPORT_1}\n\n{TEST_SUCCESSFUL_REPORT_2}"
        )

        (actual_result, actual_report) = run_validators(
            [
                TestDummyValidator(TEST_SUCCESSFUL_REPORT_1),
                TestDummyValidator(TEST_SUCCESSFUL_REPORT_2),
            ]
        )

        self.assertEqual(expected_aggregated_result, actual_result)
        self.assertEqual(expected_aggregated_report, actual_report)

    def test_a_validator_fails(self) -> None:
        expected_aggregated_result = ValidationResult.FAILED
        expected_aggregated_report = (
            f"{TEST_SUCCESSFUL_REPORT_1}\n\n{TEST_FAILED_REPORT_1}"
        )

        (actual_result, actual_report) = run_validators(
            [
                TestDummyValidator(TEST_SUCCESSFUL_REPORT_1),
                TestDummyValidator(TEST_FAILED_REPORT_1),
            ]
        )

        self.assertEqual(expected_aggregated_result, actual_result)
        self.assertEqual(expected_aggregated_report, actual_report)

    def test_a_validator_throws_exception(self) -> None:
        expected_report_thrown_by_validator = ValidationReport(
            validation_result=ValidationResult.SUCCESS,
            validator_name="TestExceptionValidator",
            message="WARNING: TestExceptionValidator throws an unexpected error: test error message",
        )
        expected_aggregated_result = ValidationResult.SUCCESS
        expected_aggregated_report = (
            f"{TEST_SUCCESSFUL_REPORT_1}\n\n{expected_report_thrown_by_validator}"
        )

        (actual_result, actual_report) = run_validators(
            [
                TestDummyValidator(TEST_SUCCESSFUL_REPORT_1),
                TestExceptionValidator(),
            ]
        )

        self.assertEqual(expected_aggregated_result, actual_result)
        self.assertEqual(expected_aggregated_report, actual_report)
