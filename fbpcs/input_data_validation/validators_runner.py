#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

from typing import List, Tuple

from fbpcs.input_data_validation.enums import ValidationResult
from fbpcs.input_data_validation.validation_report import ValidationReport
from fbpcs.input_data_validation.validator import Validator


def run_validators(validators: List[Validator]) -> Tuple[ValidationResult, str]:
    # run each validator once
    validation_reports: List[ValidationReport] = [
        validator.validate() for validator in validators
    ]

    # aggregated result is SUCCESS only if all validators succeed.
    validator_results = [
        report.validation_result == ValidationResult.SUCCESS
        for report in validation_reports
    ]
    aggregated_result = (
        ValidationResult.SUCCESS if all(validator_results) else ValidationResult.FAILED
    )

    aggregated_report = "\n\n".join([str(report) for report in validation_reports])

    return (aggregated_result, aggregated_report)
