# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict


"""
This is the main class that runs all of the validations.

This class handles the overall logic to:
* Copy the file to local storage
* Run the validations
* Generate a validation report

Error handling:
* If an unhandled error occurs, it will be returned in the report
"""

import csv
import time
from typing import Any, Dict, Optional
from typing import Sequence

from fbpcp.service.storage_s3 import S3StorageService
from fbpcs.input_data_validation.constants import INPUT_DATA_TMP_FILE_PATH
from fbpcs.input_data_validation.constants import (
    PA_FIELDS,
    PL_FIELDS,
    VALID_LINE_ENDING_REGEX,
    VALIDATION_REGEXES,
)
from fbpcs.input_data_validation.enums import ValidationResult
from fbpcs.input_data_validation.input_data_validation_issues import (
    InputDataValidationIssues,
)
from fbpcs.private_computation.entity.cloud_provider import CloudProvider


class InputDataValidator:
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

    def _get_local_filepath(self) -> str:
        now = time.time()
        filename = self._input_file_path.split("/")[-1]
        return f"{INPUT_DATA_TMP_FILE_PATH}/{filename}-{now}"

    def validate(self) -> Dict[str, str]:
        rows_processed_count = 0
        validation_issues = InputDataValidationIssues()
        try:
            self._storage_service.copy(self._input_file_path, self._local_file_path)
            header_row = ""
            with open(self._local_file_path) as local_file:
                csv_reader = csv.DictReader(local_file)
                field_names = csv_reader.fieldnames or []
                header_row = ",".join(field_names)
                self._validate_header(field_names)

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
            return self._format_validation_result(
                ValidationResult.FAILED,
                f"File: {self._input_file_path} failed validation. Error: {e}",
                rows_processed_count,
                validation_issues,
            )

        return self._format_validation_result(
            ValidationResult.SUCCESS,
            f"File: {self._input_file_path} completed validation successfully",
            rows_processed_count,
            validation_issues,
        )

    def _validate_header(self, header_row: Sequence[str]) -> None:
        if not header_row:
            raise Exception("The header row was empty.")

        match_pa_fields = len(set(PA_FIELDS).intersection(set(header_row))) == len(
            PA_FIELDS
        )
        match_pl_fields = len(set(PL_FIELDS).intersection(set(header_row))) == len(
            PL_FIELDS
        )

        if not (match_pa_fields or match_pl_fields):
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
        elif field in VALIDATION_REGEXES and not VALIDATION_REGEXES[field].match(value):
            validation_issues.count_format_error_field(field)

    def _format_validation_result(
        self,
        status: ValidationResult,
        message: str,
        rows_processed_count: int,
        validation_issues: InputDataValidationIssues,
    ) -> Dict[str, Any]:
        result = {
            "status": status.value,
            "message": message,
            "rows_processed_count": rows_processed_count,
        }

        validation_errors = validation_issues.get_as_dict()
        if validation_errors:
            result["validation_errors"] = validation_errors
            result["message"] += ", with some errors."

        return result
