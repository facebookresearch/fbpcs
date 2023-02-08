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
import time
from typing import Optional, Sequence, Set

import boto3
from botocore.client import BaseClient

from fbpcp.service.storage_s3 import S3StorageService
from fbpcp.util.s3path import S3Path
from fbpcs.pc_pre_validation.constants import (
    COHORT_ID_FIELD,
    ID_FIELD_PREFIX,
    INPUT_DATA_MAX_FILE_SIZE_IN_BYTES,
    INPUT_DATA_TMP_FILE_PATH,
    INPUT_DATA_VALIDATOR_NAME,
    INTEGER_MAX_VALUE,
    PA_FIELDS,
    PL_FIELDS,
    PRIVATE_ID_DFCA_FIELDS,
    STREAMING_DURATION_LIMIT_IN_SECONDS,
    TIMESTAMP,
    TIMESTAMP_REGEX,
    VALID_LINE_ENDING_REGEX,
    VALIDATION_REGEXES,
    VALUE_FIELD,
)
from fbpcs.pc_pre_validation.enums import ValidationResult
from fbpcs.pc_pre_validation.exceptions import InputDataValidationException
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
        stream_file: bool,
        access_key_id: Optional[str] = None,
        access_key_data: Optional[str] = None,
        start_timestamp: Optional[str] = None,
        end_timestamp: Optional[str] = None,
    ) -> None:
        self._input_file_path = input_file_path
        self._local_file_path: str = self._get_local_filepath()
        self._cloud_provider = cloud_provider
        self._storage_service = S3StorageService(region, access_key_id, access_key_data)
        self._name: str = INPUT_DATA_VALIDATOR_NAME
        self._num_id_columns = 0
        self._stream_file = stream_file

        s3_path = S3Path(input_file_path)
        self._bucket: str = s3_path.bucket
        self._key: str = s3_path.key

        if access_key_id and access_key_data:
            self._s3_client: BaseClient = boto3.client(
                "s3",
                region_name=region,
                aws_access_key_id=access_key_id,
                aws_secret_access_key=access_key_data,
            )
        else:
            self._s3_client = boto3.client("s3")

        start = None
        end = None
        if start_timestamp and TIMESTAMP_REGEX.match(start_timestamp):
            start = int(start_timestamp)

        if end_timestamp and TIMESTAMP_REGEX.match(end_timestamp):
            end = int(end_timestamp)

        # Skip setting the timestamps and log a warning if the range is not valid
        if start and end and start > end:
            print("Warning: the start_timestamp is after the end_timestamp")
            self._start_timestamp: Optional[int] = None
            self._end_timestamp: Optional[int] = None
        else:
            self._start_timestamp: Optional[int] = start
            self._end_timestamp: Optional[int] = end

    @property
    def name(self) -> str:
        return self._name

    def _get_local_filepath(self) -> str:
        now = time.time()
        filename = self._input_file_path.split("/")[-1]
        return f"{INPUT_DATA_TMP_FILE_PATH}/{filename}-{now}"

    def _validate_line(
        self,
        header_row: str,
        line: str,
        validation_issues: InputDataValidationIssues,
        cohort_id_set: Set[int],
    ) -> None:
        self._validate_line_ending(line)
        csv_row_reader = csv.DictReader([header_row, line])
        for row in csv_row_reader:
            for field, value in row.items():
                self._validate_row(validation_issues, field, value)
                if field.startswith(COHORT_ID_FIELD):
                    cohort_id_set.add(int(value))

    def _download_locally(
        self, validation_issues: InputDataValidationIssues, rows_processed_count: int
    ) -> Optional[ValidationReport]:
        file_size = self._get_file_size()
        if file_size > INPUT_DATA_MAX_FILE_SIZE_IN_BYTES:
            max_size_mb = int(INPUT_DATA_MAX_FILE_SIZE_IN_BYTES / (1024 * 1024))
            warning_message = " ".join(
                [
                    f"WARNING: File: {self._input_file_path} is too large to download.",
                    f"The maximum file size is {max_size_mb} MB.",
                    "Skipped input_data validation.",
                ]
            )
            return self._format_validation_report(
                warning_message,
                rows_processed_count,
                validation_issues,
            )

        self._download_input_file()

        return None

    def __validate__(self) -> ValidationReport:
        rows_processed_count = 0
        validation_issues = InputDataValidationIssues()
        keep_streaming_check = True

        try:
            if not self._stream_file:
                validation_report = self._download_locally(
                    validation_issues, rows_processed_count
                )
                if validation_report:
                    return validation_report

            field_names = []
            if self._stream_file:
                response = self._s3_client.get_object(
                    Bucket=self._bucket, Key=self._key
                )
                stream = response["Body"]
                for line in stream.iter_lines(keepends=True):
                    field_names = (
                        csv.DictReader([line.decode("utf-8")]).fieldnames or []
                    )
                    # Read just the first header row then stop streaming
                    break
            else:
                with open(self._local_file_path) as local_file:
                    csv_reader = csv.DictReader(local_file)
                    field_names = csv_reader.fieldnames or []

            header_row = ",".join(field_names)
            self._set_num_id_columns(field_names)
            self._validate_header(field_names)

            cohort_id_set = set()

            if self._stream_file:
                past_first_row = False
                response = self._s3_client.get_object(
                    Bucket=self._bucket, Key=self._key
                )
                stream = response["Body"]
                start_time = time.time()
                for line in stream.iter_lines(keepends=True):
                    if not past_first_row:
                        past_first_row = True
                        continue
                    decoded_line = line.decode("utf-8")
                    self._validate_line(
                        header_row, decoded_line, validation_issues, cohort_id_set
                    )
                    keep_streaming_check = self._keep_streaming_check(
                        start_time, rows_processed_count
                    )
                    if not keep_streaming_check:
                        break
                    rows_processed_count += 1
            else:
                with open(self._local_file_path, "rb") as local_file:
                    header_line = local_file.readline().decode("utf-8")
                    self._validate_line_ending(header_line)

                    while raw_line := local_file.readline():
                        line = raw_line.decode("utf-8")
                        self._validate_line(
                            header_row, line, validation_issues, cohort_id_set
                        )
                        rows_processed_count += 1

            self._validate_cohort_ids(cohort_id_set)

        except InputDataValidationException as e:
            return self._format_validation_report(
                f"File: {self._input_file_path} failed validation. Error: {e}",
                rows_processed_count,
                validation_issues,
                had_exception=True,
            )

        validation_issues.set_max_issue_count_til_error(
            {
                ID_FIELD_PREFIX: {
                    "empty_count": self._num_id_columns * rows_processed_count - 1,
                },
            }
        )

        return self._format_validation_report(
            f"File: {self._input_file_path}",
            rows_processed_count,
            validation_issues,
            streaming_timed_out=(not keep_streaming_check),
        )

    def _validate_cohort_ids(self, cohort_id_set: Set[int]) -> None:
        for i, cohort_id in enumerate(sorted(cohort_id_set)):
            if i != cohort_id:
                raise InputDataValidationException(
                    "Cohort Id Format is invalid. Cohort ID should start with 0 and increment by 1."
                )

        if len(cohort_id_set) > 7:
            raise InputDataValidationException(
                "Number of cohorts is higher than currently supported."
            )

    def _keep_streaming_check(
        self, start_time: float, rows_processed_count: int
    ) -> bool:
        if rows_processed_count % 100000 == 0:
            current_time = time.time()
            return (current_time - start_time) < STREAMING_DURATION_LIMIT_IN_SECONDS

        return True

    def _set_num_id_columns(self, header_row: Sequence[str]) -> None:
        if not header_row:
            raise InputDataValidationException("The header row was empty.")

        self._num_id_columns = len(
            [col for col in header_row if col.startswith(ID_FIELD_PREFIX)]
        )

    def _get_file_size(self) -> int:
        try:
            return self._storage_service.get_file_size(self._input_file_path)
        except Exception as e:
            raise InputDataValidationException(
                f"Failed to get the input file size. Please check the file path and its permission.\n\t{e}"
            )

    def _download_input_file(self) -> None:
        try:
            self._storage_service.copy(self._input_file_path, self._local_file_path)
        except Exception as e:
            raise InputDataValidationException(
                f"Failed to download the input file. Please check the file path and its permission.\n\t{e}"
            )

    def _validate_header(self, header_row: Sequence[str]) -> None:
        if not header_row:
            raise InputDataValidationException("The header row was empty.")

        match_id_fields = self._num_id_columns > 0

        match_pa_fields = len(set(PA_FIELDS).intersection(set(header_row))) == len(
            PA_FIELDS
        )
        match_pl_fields = len(set(PL_FIELDS).intersection(set(header_row))) == len(
            PL_FIELDS
        )
        match_private_id_dfca_fields = len(
            set(PRIVATE_ID_DFCA_FIELDS).intersection(set(header_row))
        ) == len(PRIVATE_ID_DFCA_FIELDS)

        if not match_id_fields:
            raise InputDataValidationException(
                f"Failed to parse the header row. The header row fields must have columns with prefix {ID_FIELD_PREFIX}"
            )

        if not (match_pa_fields or match_pl_fields or match_private_id_dfca_fields):
            raise InputDataValidationException(
                f"Failed to parse the header row. The header row fields must have either: {PL_FIELDS} or: {PA_FIELDS} or: {PRIVATE_ID_DFCA_FIELDS}"
            )

    def _validate_line_ending(self, line: str) -> None:
        if not VALID_LINE_ENDING_REGEX.match(line):
            raise InputDataValidationException(
                "Detected an unexpected line ending. The only supported line ending is '\\n'"
            )

    def _validate_row(
        self, validation_issues: InputDataValidationIssues, field: str, value: str
    ) -> None:
        if field.startswith(ID_FIELD_PREFIX):
            field = ID_FIELD_PREFIX

        if value.strip() == "":
            validation_issues.count_empty_field(field)
        elif field in VALIDATION_REGEXES and not VALIDATION_REGEXES[field].match(value):
            validation_issues.count_format_error_field(field)
        elif field.endswith(TIMESTAMP):
            # The timestamp is 10 digits, now we validate if it's in the expected time range when present
            self._validate_timestamp(validation_issues, field, value)
        elif field == VALUE_FIELD:
            # Validate that the purchase value is in valid range.
            self._validate_purchase_value(field, value)

    # This is the timestamp range that gets validated:
    # * timestamp >= start_timestamp
    # * timestamp <= end_timestamp
    def _validate_timestamp(
        self, validation_issues: InputDataValidationIssues, field: str, timestamp: str
    ) -> None:
        # When at least one of the timestamp requirements is specified it will run the validation
        if not (self._start_timestamp or self._end_timestamp):
            return
        timestamp_int = int(timestamp)
        start = self._start_timestamp
        end = self._end_timestamp

        if start and timestamp_int < start:
            validation_issues.count_format_out_of_range_field(field)

        if end and timestamp_int > end:
            validation_issues.count_format_out_of_range_field(field)

    # This is the purchase value range that gets validated:
    # * purchase_value < INTEGER_MAX_VALUE
    def _validate_purchase_value(self, field: str, value: str) -> None:
        # int in python is unbound, so we would not get any exception at this point for larger value.
        value_int = int(value)

        if value_int >= INTEGER_MAX_VALUE:
            raise InputDataValidationException(
                "Purchase value is invalid. Purchase value should be less than 2147483647."
            )

    def _format_validation_report(
        self,
        message: str,
        rows_processed_count: int,
        validation_issues: InputDataValidationIssues,
        had_exception: bool = False,
        streaming_timed_out: bool = False,
    ) -> ValidationReport:
        validation_errors = validation_issues.get_errors()
        validation_warnings = validation_issues.get_warnings()

        if had_exception:
            return ValidationReport(
                validation_result=ValidationResult.FAILED,
                validator_name=INPUT_DATA_VALIDATOR_NAME,
                message=message,
                details={
                    "rows_processed_count": rows_processed_count,
                },
            )

        timed_out_message = ""
        timed_out_warning_message = ""
        if streaming_timed_out:
            timed_out_message = " ".join(
                [
                    f" Warning: ran the validations on {rows_processed_count} total rows,",
                    "the rest of the rows were skipped to avoid container timeout. ",
                ]
            )
            timed_out_warning_message = ", with some warnings."

        if validation_errors:
            error_fields = ", ".join(sorted(validation_errors.keys()))
            details = {
                "rows_processed_count": rows_processed_count,
                "validation_errors": validation_errors,
            }
            if validation_warnings:
                details["validation_warnings"] = validation_warnings
            return ValidationReport(
                validation_result=ValidationResult.FAILED,
                validator_name=INPUT_DATA_VALIDATOR_NAME,
                message=f"{message} failed validation, with errors on '{error_fields}'.{timed_out_message}",
                details=details,
            )
        elif validation_warnings:
            return ValidationReport(
                validation_result=ValidationResult.SUCCESS,
                validator_name=INPUT_DATA_VALIDATOR_NAME,
                message=f"{message} completed validation successfully, with some warnings.{timed_out_message}",
                details={
                    "rows_processed_count": rows_processed_count,
                    "validation_warnings": validation_warnings,
                },
            )
        else:
            return ValidationReport(
                validation_result=ValidationResult.SUCCESS,
                validator_name=INPUT_DATA_VALIDATOR_NAME,
                message=f"{message} completed validation successfully{timed_out_warning_message}{timed_out_message}",
                details={
                    "rows_processed_count": rows_processed_count,
                },
            )
