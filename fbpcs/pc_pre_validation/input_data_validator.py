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
import sys
import time
from multiprocessing import Process, Queue
from typing import List, Optional, Sequence, Set

import boto3
from botocore.client import BaseClient
from botocore.exceptions import ClientError

from fbpcp.service.storage_s3 import S3StorageService
from fbpcp.util.s3path import S3Path
from fbpcs.pc_pre_validation.constants import (
    COHORT_ID_FIELD,
    CONVERSION_TIMESTAMP_FIELD,
    ERROR_MESSAGES,
    EVENT_TIMESTAMP_FIELD,
    ID_FIELD_PREFIX,
    INPUT_DATA_MAX_FILE_SIZE_IN_BYTES,
    INPUT_DATA_TMP_FILE_PATH,
    INPUT_DATA_VALIDATOR_NAME,
    INTEGER_MAX_VALUE,
    MAX_PARALLELISM,
    MIN_CHUNK_SIZE,
    PA_FIELDS,
    PA_PUBLISHER_FIELDS,
    PL_FIELDS,
    PL_PUBLISHER_FIELDS,
    PRIVATE_ID_DFCA_FIELDS,
    STREAMING_DURATION_LIMIT_IN_SECONDS,
    TIMESTAMP,
    TIMESTAMP_OUT_OF_RANGE_MAX_THRESHOLD,
    TIMESTAMP_REGEX,
    VALID_LINE_ENDING_REGEX,
    VALIDATION_REGEXES,
    VALUE_FIELDS,
)
from fbpcs.pc_pre_validation.enums import ValidationResult
from fbpcs.pc_pre_validation.exceptions import (
    InputDataValidationException,
    TimeoutException,
)
from fbpcs.pc_pre_validation.input_data_validation_issues import (
    InputDataValidationIssues,
)
from fbpcs.pc_pre_validation.validation_report import ValidationReport
from fbpcs.pc_pre_validation.validator import Validator
from fbpcs.private_computation.entity.cloud_provider import CloudProvider
from fbpcs.private_computation.entity.private_computation_instance import (
    PrivateComputationRole,
)


class InputDataValidator(Validator):
    def __init__(
        self,
        input_file_path: str,
        cloud_provider: CloudProvider,
        region: str,
        stream_file: bool,
        publisher_pc_pre_validation: bool,
        partner_pc_pre_validation: bool,
        private_computation_role: PrivateComputationRole,
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
        self._publisher_pc_pre_validation = publisher_pc_pre_validation
        self._partner_pc_pre_validation = partner_pc_pre_validation
        self._private_computation_role: PrivateComputationRole = (
            private_computation_role
        )
        self._parallelism: int = MAX_PARALLELISM
        self._file_size: int = 0

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

        self._start_timestamp_not_valid: bool = False
        self._end_timestamp_not_valid: bool = False
        self._timestamp_range_not_valid: bool = False

        self._start_timestamp: Optional[int] = None
        self._end_timestamp: Optional[int] = None

        start = None
        end = None

        if start_timestamp and TIMESTAMP_REGEX.match(start_timestamp):
            start = int(start_timestamp)
            self._start_timestamp = start
        elif start_timestamp and not TIMESTAMP_REGEX.match(start_timestamp):
            self._start_timestamp_not_valid = True

        if end_timestamp and TIMESTAMP_REGEX.match(end_timestamp):
            end = int(end_timestamp)
            self._end_timestamp = end
        elif end_timestamp and not TIMESTAMP_REGEX.match(end_timestamp):
            self._end_timestamp_not_valid = True

        if start and end and start > end:
            self._timestamp_range_not_valid = True
            self._start_timestamp = None
            self._end_timestamp = None

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
            # value_int = 0
            cohort_id = None
            for field, value in row.items():
                self._validate_row(validation_issues, field, value)
                if field.startswith(COHORT_ID_FIELD):
                    cohort_id = int(value)
                    cohort_id_set.add(cohort_id)
                # if field in VALUE_FIELDS:
                #     try:
                #         value_int = int(value)
                #     except ValueError:
                #         # Values with a bad format are counted already by _validate_row()
                #         pass
            # Temporarily disable the aggregated value check. TODO T147920505
            # if cohort_id is not None:
            # validation_issues.update_cohort_aggregate(cohort_id, value_int)

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

    def _get_chunk_path(self, base: str, idx: int) -> str:
        return base + "." + str(idx)

    # shard the file into multiple shards
    def _create_shards(self) -> None:
        shards = []
        for i in range(self._parallelism):
            shards.append(open(self._get_chunk_path(self._local_file_path, i), "wb"))

        shards_processed_count = 0
        with open(self._local_file_path, "rb") as local_file:
            header_line = local_file.readline().decode("utf-8")
            self._validate_line_ending(header_line)

            while lines := local_file.readlines(1024 * 1024):
                i = shards_processed_count % self._parallelism
                shards[i].write(b"".join(lines))
                shards_processed_count += 1

        for i in range(self._parallelism):
            shards[i].close()

    # worker process when reading from the local file
    def _validation_worker_local_download(
        self,
        s: int,
        header_row: str,
        validation_issues: InputDataValidationIssues,
        validation_issues_queue: Queue,
        cohort_id_set_queue: Queue,
        rows_processed_queue: Queue,
        exception_queue: Queue,
    ) -> None:
        rows_processed = 0
        cohort_id_set = set()
        try:
            with open(self._get_chunk_path(self._local_file_path, s), "rb") as f:
                while line := f.readline():
                    line = line.decode("utf-8")
                    self._validate_line(
                        header_row, line, validation_issues, cohort_id_set
                    )
                    rows_processed += 1
        except Exception as e:
            exception_queue.put(e)
            sys.exit(0)
        finally:
            validation_issues_queue.put(validation_issues)
            cohort_id_set_queue.put(cohort_id_set)
            rows_processed_queue.put(rows_processed)

    def _get_byte_range(self, s: int) -> str:
        file_size = self._file_size
        # num_bytes_per_worker is calculated by dividing the file size by the number of workers
        num_bytes_per_worker = int(file_size / self._parallelism)
        start = s * num_bytes_per_worker
        end = (s + 1) * num_bytes_per_worker - 1
        return "bytes={}-{}".format(start, end)

    # worker process when streaming
    def _validation_worker_streaming(
        self,
        s: int,
        header_row: str,
        validation_issues: InputDataValidationIssues,
        validation_issues_queue: Queue,
        cohort_id_set_queue: Queue,
        rows_processed_queue: Queue,
        exception_queue: Queue,
    ) -> None:
        range_header = self._get_byte_range(s)
        # Create client to read from S3 bucket with those ranges.
        response = self._s3_client.get_object(
            Bucket=self._bucket, Key=self._key, Range=range_header
        )
        start = time.time()
        rows_processed = 0
        cohort_id_set = set()
        stream = response["Body"]
        lines = stream.iter_lines(keepends=True)

        # Skip the first row
        line = next(lines, None)
        line = next(lines, None)
        next_line = next(lines, None)

        try:
            while line is not None:
                # Since we read byte ranges, it may not align with line endings. So skip the last row
                if next_line is None:
                    break
                line = line.decode("utf-8")
                self._validate_line(header_row, line, validation_issues, cohort_id_set)
                rows_processed += 1
                line = next_line
                next_line = next(lines, None)
                if not self._keep_streaming_check(start, rows_processed):
                    raise TimeoutException

        except Exception as e:
            exception_queue.put(e)
            sys.exit(0)
        finally:
            validation_issues_queue.put(validation_issues)
            cohort_id_set_queue.put(cohort_id_set)
            rows_processed_queue.put(rows_processed)

        return

    def _get_and_validate_header(
        self, validation_issues: InputDataValidationIssues
    ) -> str:
        field_names = []
        if self._stream_file:
            field_names = self._stream_field_names() or []
        else:
            with open(self._local_file_path) as local_file:
                csv_reader = csv.DictReader(local_file)
                field_names = csv_reader.fieldnames or []

        self._set_num_id_columns(field_names)
        self._validate_header(field_names)
        self._parse_value_field_name(field_names, validation_issues)

        return ",".join(field_names)

    def __validate__(self) -> ValidationReport:
        validation_issues = InputDataValidationIssues()

        try:
            self._file_size = self._get_file_size()
            # Add a worker only if each one is alredy going to process MIN_CHUNK_SIZE
            # but capped at MAX_PARALLELISM
            self._parallelism = min(
                int(self._file_size / MIN_CHUNK_SIZE) + 1, MAX_PARALLELISM
            )
            if not self._stream_file:
                validation_report = self._download_locally(
                    validation_issues, validation_issues.rows_processed_count
                )
                if validation_report:
                    return validation_report

            header_row = self._get_and_validate_header(validation_issues)

            if not self._stream_file:
                self._create_shards()

            self._run_workers(validation_issues, header_row)

            self._validate_cohort_ids(validation_issues.cohort_id_set)

        except InputDataValidationException as e:
            return self._format_validation_report(
                f"File: {self._input_file_path} failed validation. Error: {e}",
                validation_issues.rows_processed_count,
                validation_issues,
                had_exception=True,
            )

        rows_processed_count = validation_issues.rows_processed_count
        validation_issues.set_max_issue_count_til_error(
            {
                ID_FIELD_PREFIX: {
                    "empty_count": self._num_id_columns * rows_processed_count - 1,
                },
                EVENT_TIMESTAMP_FIELD: {
                    "out_of_range_count": int(
                        rows_processed_count * TIMESTAMP_OUT_OF_RANGE_MAX_THRESHOLD
                    ),
                },
                CONVERSION_TIMESTAMP_FIELD: {
                    "out_of_range_count": int(
                        rows_processed_count * TIMESTAMP_OUT_OF_RANGE_MAX_THRESHOLD
                    ),
                },
            }
        )
        return self._format_validation_report(
            f"File: {self._input_file_path}",
            rows_processed_count,
            validation_issues,
            streaming_timed_out=(validation_issues.streaming_timed_out),
        )

    def _run_workers(
        self, validation_issues: InputDataValidationIssues, header_row: str
    ) -> None:
        seed_validation_issues = InputDataValidationIssues()
        validation_issues_queue = Queue(self._parallelism)
        cohort_id_set_queue = Queue(self._parallelism)
        rows_processed_queue = Queue(self._parallelism)
        exception_queue = Queue(self._parallelism)

        workers = []
        for i in range(self._parallelism):
            w = Process(
                target=self._validation_worker_streaming
                if self._stream_file
                else self._validation_worker_local_download,
                args=(
                    i,
                    header_row,
                    seed_validation_issues,
                    validation_issues_queue,
                    cohort_id_set_queue,
                    rows_processed_queue,
                    exception_queue,
                ),
            )
            workers.append(w)
            w.start()

        for _, w in enumerate(workers):
            w.join()
            if not validation_issues_queue.empty():
                validation_issues.merge(validation_issues_queue.get())
            if not cohort_id_set_queue.empty():
                validation_issues.cohort_id_set |= cohort_id_set_queue.get()
            if not rows_processed_queue.empty():
                validation_issues.rows_processed_count += rows_processed_queue.get()

        for i, _ in enumerate(workers):
            if not exception_queue.empty():
                e = exception_queue.get()
                if type(e) == TimeoutException:
                    validation_issues.streaming_timed_out = True
                else:
                    raise e
            if w.exitcode != 0:
                raise InputDataValidationException(
                    f"Worker {i} failed with exit code {w.exitcode}"
                )

    def _stream_field_names(self) -> Sequence[str]:
        try:
            response = self._s3_client.get_object(
                Bucket=self._bucket, Key=self._key, Range=""
            )
            stream = response["Body"]
            for line in stream.iter_lines(keepends=True):
                # Read just the header row then stop streaming
                return csv.DictReader([line.decode("utf-8")]).fieldnames or []
        except ClientError as e:
            raise InputDataValidationException(
                f"Failed to stream the input file. Please check the file path and its permission.\n\t{e}"
            )
        return []

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

        match_pa_publisher_fields = len(
            set(PA_PUBLISHER_FIELDS).intersection(set(header_row))
        ) == len(PA_PUBLISHER_FIELDS)

        match_pl_fields = len(set(PL_FIELDS).intersection(set(header_row))) == len(
            PL_FIELDS
        )

        match_pl_publisher_fields = len(
            set(PL_PUBLISHER_FIELDS).intersection(set(header_row))
        ) == len(PL_PUBLISHER_FIELDS)

        match_private_id_dfca_fields = len(
            set(PRIVATE_ID_DFCA_FIELDS).intersection(set(header_row))
        ) == len(PRIVATE_ID_DFCA_FIELDS)

        run_publisher_pre_validation_check = (
            self._private_computation_role is PrivateComputationRole.PUBLISHER
            and self._publisher_pc_pre_validation
        )

        run_partner_pre_validation_check = (
            self._private_computation_role is PrivateComputationRole.PARTNER
            and self._partner_pc_pre_validation
        )
        if not match_id_fields:
            raise InputDataValidationException(
                f"Failed to parse the header row. The header row fields must have columns with prefix {ID_FIELD_PREFIX}"
            )

        partner_header_matches = sum(
            [match_pa_fields, match_pl_fields, match_private_id_dfca_fields]
        )
        if run_partner_pre_validation_check and partner_header_matches == 0:
            raise InputDataValidationException(
                f"Failed to parse the {self._private_computation_role} header row. The header row fields must have either: {PL_FIELDS} or: {PA_FIELDS} or: {PRIVATE_ID_DFCA_FIELDS}"
            )

        publisher_header_matches = sum(
            [
                match_private_id_dfca_fields,
                match_pl_publisher_fields,
                match_pa_publisher_fields,
            ]
        )
        if run_publisher_pre_validation_check and publisher_header_matches == 0:
            raise InputDataValidationException(
                f"Failed to parse the {self._private_computation_role} header row. The header row fields must have either: {PRIVATE_ID_DFCA_FIELDS} or: {PL_PUBLISHER_FIELDS} or: {PA_PUBLISHER_FIELDS}"
            )

        if run_partner_pre_validation_check and partner_header_matches > 1:
            raise InputDataValidationException(
                f"The {self._private_computation_role} header row fields must contain just one of the following: {PL_FIELDS} or: {PA_FIELDS} or: {PRIVATE_ID_DFCA_FIELDS}"
            )

        if publisher_header_matches > 1:
            raise InputDataValidationException(
                f"The {self._private_computation_role} header row fields must contain just one of the following: {PRIVATE_ID_DFCA_FIELDS} or: {PL_PUBLISHER_FIELDS} or: {PA_PUBLISHER_FIELDS}"
            )

    def _validate_line_ending(self, line: str) -> None:
        if not VALID_LINE_ENDING_REGEX.match(line):
            raise InputDataValidationException(
                "Detected an unexpected line ending. The only supported line ending is '\\n'"
            )

    def _validate_row(
        self, validation_issues: InputDataValidationIssues, field: str, value: str
    ) -> None:
        if value is None:
            raise InputDataValidationException(
                "CSV format error - line is missing expected value(s)."
            )
        if field is None:
            raise InputDataValidationException(
                "CSV format error - line has too many values."
            )
        if field.startswith(ID_FIELD_PREFIX):
            field = ID_FIELD_PREFIX

        if value.strip() == "":
            validation_issues.count_empty_field(field)
        elif field in VALIDATION_REGEXES and not VALIDATION_REGEXES[field].match(value):
            validation_issues.count_format_error_field(field)
        elif field.endswith(TIMESTAMP):
            # The timestamp is 10 digits, now we validate if it's in the expected time range when present
            self._validate_timestamp(validation_issues, field, value)
        elif field in VALUE_FIELDS:
            # Validate that the purchase value is in valid range.
            self._validate_purchase_value(validation_issues, field, value)

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
    def _validate_purchase_value(
        self, validation_issues: InputDataValidationIssues, field: str, value: str
    ) -> None:
        # int in python is unbound, so we would not get any exception at this point for larger value.
        value_int = int(value)

        if value_int >= INTEGER_MAX_VALUE:
            validation_issues.count_format_out_of_range_field(field)

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

        timestamp_warnings = ""
        if self._timestamp_range_not_valid:
            timestamp_warnings += " - Warning: the timestamp range is not valid"
        if self._start_timestamp_not_valid:
            timestamp_warnings += " - Warning: the start timestamp is not valid"
        if self._end_timestamp_not_valid:
            timestamp_warnings += " - Warning: the end timestamp is not valid"

        if validation_errors:
            error_fields = ", ".join(
                sorted(self._get_error_keys(list(validation_errors.keys())))
            )
            details = {
                "rows_processed_count": rows_processed_count,
                "validation_errors": validation_errors,
            }
            if validation_warnings:
                details["validation_warnings"] = validation_warnings
            fields_string = f", with errors on '{error_fields}'" if error_fields else ""
            return ValidationReport(
                validation_result=ValidationResult.FAILED,
                validator_name=INPUT_DATA_VALIDATOR_NAME,
                message=f"{message} failed validation{fields_string}.{timed_out_message}{timestamp_warnings}",
                details=details,
            )
        elif validation_warnings:
            warning_fields = ", ".join(sorted(validation_warnings.keys()))
            return ValidationReport(
                validation_result=ValidationResult.SUCCESS,
                validator_name=INPUT_DATA_VALIDATOR_NAME,
                message=f"{message} completed validation successfully, with warnings on '{warning_fields}'.{timestamp_warnings}",
                details={
                    "rows_processed_count": rows_processed_count,
                    "validation_warnings": validation_warnings,
                },
            )
        else:
            return ValidationReport(
                validation_result=ValidationResult.SUCCESS,
                validator_name=INPUT_DATA_VALIDATOR_NAME,
                message=f"{message} completed validation successfully{timed_out_warning_message}{timed_out_message}{timestamp_warnings}",
                details={
                    "rows_processed_count": rows_processed_count,
                },
            )

    def _get_error_keys(self, error_keys: List[str]) -> List[str]:
        return [key for key in error_keys if key != ERROR_MESSAGES]

    def _parse_value_field_name(
        self, field_names: Sequence[str], validation_issues: InputDataValidationIssues
    ) -> None:
        for field_name in field_names:
            if field_name in VALUE_FIELDS:
                validation_issues.set_value_field_name(field_name)
                # The header row should have either 'value' or 'conversion_value'
                break
