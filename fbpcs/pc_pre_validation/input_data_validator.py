# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict


"""
This is the main class that runs the input data validations.

This class handles the overall logic to:
* Copy the file to local storage
* Run the validations
* Generate a validation report

Error handling:
* If an unhandled error occurs, it will be returned in the report
"""

import csv
import json
import time
from typing import Dict, List, Sequence, Optional

from fbpcp.service.storage_s3 import S3StorageService
from fbpcs.pc_pre_validation.constants import (
    DEFAULT_VALID_THRESHOLDS,
    INPUT_DATA_TMP_FILE_PATH,
    INPUT_DATA_VALIDATOR_NAME,
    PA_FIELDS,
    PL_FIELDS,
    VALID_LINE_ENDING_REGEX,
    VALIDATION_REGEXES,
    VALUE_FIELDS,
)
from fbpcs.pc_pre_validation.enums import ValidationResult
from fbpcs.pc_pre_validation.input_data_validation_issues import (
    InputDataValidationIssues,
)
from fbpcs.pc_pre_validation.validation_report import ValidationReport
from fbpcs.pc_pre_validation.validator import Validator
from fbpcs.private_computation.entity.cloud_provider import CloudProvider


class InputDataValidator(Validator):
    def __init__(
        self,
        input_file_path: str,
        cloud_provider: CloudProvider,
        region: str,
        access_key_id: Optional[str] = None,
        access_key_data: Optional[str] = None,
        start_timestamp: Optional[str] = None,
        end_timestamp: Optional[str] = None,
        valid_threshold_override: Optional[str] = None,
    ) -> None:
        self._input_file_path = input_file_path
        self._local_file_path: str = self._get_local_filepath()
        self._cloud_provider = cloud_provider
        self._storage_service = S3StorageService(region, access_key_id, access_key_data)
        self._name: str = INPUT_DATA_VALIDATOR_NAME
        self._valid_thresholds: Dict[str, float] = self._get_valid_thresholds(
            valid_threshold_override
        )

    @property
    def name(self) -> str:
        return self._name

    def _get_local_filepath(self) -> str:
        now = time.time()
        filename = self._input_file_path.split("/")[-1]
        return f"{INPUT_DATA_TMP_FILE_PATH}/{filename}-{now}"

    def __validate__(self) -> ValidationReport:
        field_names = []
        rows_processed_count = 0
        validation_issues = InputDataValidationIssues()
        try:
            self._download_input_file()
            header_row = ""
            with open(self._local_file_path) as local_file:
                csv_reader = csv.DictReader(local_file)
                field_names = csv_reader.fieldnames or []
                header_row = ",".join(field_names)
                field_names = self._validate_header(field_names)

            with open(self._local_file_path, "rb") as local_file:
                header_line = local_file.readline().decode("utf-8")
                self._validate_line_ending(header_line)

                while raw_line := local_file.readline():
                    line = raw_line.decode("utf-8")
                    self._validate_line_ending(line)
                    csv_row_reader = csv.DictReader([header_row, line])
                    for row in csv_row_reader:
                        for field, value in row.items():
                            self._validate_row(validation_issues, field, value)
                    rows_processed_count += 1

        except Exception as e:
            return self._format_validation_report(
                ValidationResult.FAILED,
                f"File: {self._input_file_path} failed validation. Error: {e}",
                rows_processed_count,
                validation_issues,
            )

        self._check_validation_thresholds(
            field_names, validation_issues, rows_processed_count
        )

        if validation_issues.fields_under_threshold:
            fields_str = ",".join(validation_issues.fields_under_threshold)
            validation_thresholds_required = {
                key: value
                for key, value in self._valid_thresholds.items()
                if key in validation_issues.field_thresholds
            }
            error_message = "\n".join(
                [
                    f"Too many row values for '{fields_str}' are unusable:",
                    f"Required data quality: {validation_thresholds_required}",
                    f"Actual data quality: {validation_issues.field_thresholds}",
                ]
            )
            return self._format_validation_report(
                ValidationResult.FAILED,
                f"File: {self._input_file_path} failed validation. Error: {error_message}",
                rows_processed_count,
                validation_issues,
            )

        return self._format_validation_report(
            ValidationResult.SUCCESS,
            f"File: {self._input_file_path} completed validation successfully",
            rows_processed_count,
            validation_issues,
        )

    def _download_input_file(self) -> None:
        try:
            self._storage_service.copy(self._input_file_path, self._local_file_path)
        except Exception as e:
            raise Exception(
                f"Failed to download the input file. Please check the file path and its permission.\n\t{e}"
            )

    def _validate_header(self, header_row: Sequence[str]) -> List[str]:
        if not header_row:
            raise Exception("The header row was empty.")

        match_pa_fields = len(set(PA_FIELDS).intersection(set(header_row))) == len(
            PA_FIELDS
        )
        match_pl_fields = len(set(PL_FIELDS).intersection(set(header_row))) == len(
            PL_FIELDS
        )

        if match_pa_fields:
            return PA_FIELDS
        if match_pl_fields:
            return PL_FIELDS

        raise Exception(
            f"Failed to parse the header row. The header row fields must be either: {PL_FIELDS} or: {PA_FIELDS}"
        )

    def _validate_line_ending(self, line: str) -> None:
        if not VALID_LINE_ENDING_REGEX.match(line):
            raise Exception(
                "Detected an unexpected line ending. The only supported line ending is '\\n'"
            )

    def _validate_row(
        self, validation_issues: InputDataValidationIssues, field: str, value: str
    ) -> None:
        if value.strip() == "":
            validation_issues.count_empty_field(field)
            return

        validation_issues.count_not_empty_field(field)
        if field in VALIDATION_REGEXES and not VALIDATION_REGEXES[field].match(value):
            validation_issues.count_format_error_field(field)

    def _format_validation_report(
        self,
        result: ValidationResult,
        message: str,
        rows_processed_count: int,
        validation_issues: InputDataValidationIssues,
    ) -> ValidationReport:

        validation_errors = validation_issues.get_as_dict()

        if validation_errors:
            some_errors_str = (
                ", with some errors." if result == ValidationResult.SUCCESS else ""
            )
            return ValidationReport(
                validation_result=result,
                validator_name=INPUT_DATA_VALIDATOR_NAME,
                message=f"{message}{some_errors_str}",
                details={
                    "rows_processed_count": rows_processed_count,
                    "validation_errors": validation_errors,
                },
            )
        else:
            return ValidationReport(
                validation_result=result,
                validator_name=INPUT_DATA_VALIDATOR_NAME,
                message=message,
                details={
                    "rows_processed_count": rows_processed_count,
                },
            )

    def _get_valid_thresholds(
        self, threshold_override: Optional[str]
    ) -> Dict[str, float]:
        if not threshold_override:
            return DEFAULT_VALID_THRESHOLDS

        override_thresholds = json.loads(threshold_override)
        merged = override_thresholds.copy()
        for field, threshold in DEFAULT_VALID_THRESHOLDS.items():
            if field not in merged:
                merged[field] = threshold
        return merged

    def _check_validation_thresholds(
        self,
        field_names: List[str],
        validation_issues: InputDataValidationIssues,
        rows_processed_count: int,
    ) -> None:
        if rows_processed_count == 0:
            return
        for field in field_names:
            actual_ratio = 1.0
            empty_count = validation_issues.empty_counter[field]
            not_empty_count = validation_issues.not_empty_counter[field]
            format_error_count = validation_issues.format_error_counter[field]
            is_value_field = field in VALUE_FIELDS
            if is_value_field and not_empty_count == 0:
                # Skip if the value is empty for all rows
                continue
            if is_value_field:
                actual_ratio = (not_empty_count - format_error_count) / not_empty_count
            else:
                total_issues = format_error_count + empty_count
                actual_ratio = (
                    rows_processed_count - total_issues
                ) / rows_processed_count

            actual_ratio = round(actual_ratio, 2)
            validation_issues.set_field_threshold(field, actual_ratio)
            if (
                field in self._valid_thresholds
                and self._valid_thresholds[field] > 0.0
                and actual_ratio < self._valid_thresholds[field]
            ):
                validation_issues.add_field_under_threshold(field)
