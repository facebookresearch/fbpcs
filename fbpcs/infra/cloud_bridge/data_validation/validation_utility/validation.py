# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

import csv
import re
from typing import Dict, List, Optional, Set

from botocore.response import StreamingBody
from expected_fields import (
    UNFILTERED_ALL_REQUIRED_FIELDS,
    UNFILTERED_ONE_OR_MORE_REQUIRED_FIELDS,
    UNFILTERED_FORMAT_VALIDATION_FOR_FIELD,
    PA_ALL_REQUIRED_FIELDS,
    PA_FORMAT_VALIDATION_FOR_FIELD,
    PL_ALL_REQUIRED_FIELDS,
    PL_FORMAT_VALIDATION_FOR_FIELD,
)

HEADER_ROW_OFFSET = 1
MAX_ERROR_LINES = 100
VALID_LINE_ENDING = re.compile(r".*(\S|\S\n)$")


class ValidationState:
    def __init__(self):
        self.total_rows = 0
        self.valid_rows = 0
        self.error_rows = 0
        self.validation_messages = []
        self.lines_missing_required_field = {}
        self.lines_missing_all_required_fields = []
        self.lines_incorrect_field_format = {}
        self.all_required_fields = set()
        self.one_or_more_required_fields = set()
        self.format_validation_for_field = {}


def any_required_header_fields_missing(
    header_fields: List[str], all_required_fields: Set[str]
) -> bool:
    fields_missing = all_required_fields.difference(set(header_fields))
    return len(fields_missing) > 0


def is_header_missing_all_identity_fields(
    header_fields: List[str], one_or_more_required_fields: Set[str]
) -> bool:
    intersection = one_or_more_required_fields.intersection(set(header_fields))
    return len(intersection) == 0


def field_value_is_valid(value: str, regex: re.Pattern) -> bool:
    return value.strip() == value and regex.match(value) is not None


def append_line_number_to_field(
    field: str, fields_lines: Dict[str, List[int]], current_line: int
) -> None:
    has_line_numbers = field in fields_lines
    if has_line_numbers and len(fields_lines[field]) <= MAX_ERROR_LINES:
        fields_lines[field].append(current_line)
    elif not has_line_numbers:
        fields_lines[field] = [current_line]


def validate_line(line: Dict[str, str], validation_state: ValidationState) -> None:
    missing_required_field = False
    missing_all_required_fields = len(validation_state.one_or_more_required_fields) > 0
    pattern_validation_failed = False
    current_line = validation_state.total_rows + HEADER_ROW_OFFSET
    for field in validation_state.all_required_fields:
        if field not in line or value_empty(line[field]):
            missing_required_field = True
            append_line_number_to_field(
                field, validation_state.lines_missing_required_field, current_line
            )
        else:
            field_is_valid = field_value_is_valid(
                line[field], validation_state.format_validation_for_field[field]
            )
            if not field_is_valid:
                pattern_validation_failed = True
                append_line_number_to_field(
                    field, validation_state.lines_incorrect_field_format, current_line
                )

    for field in validation_state.one_or_more_required_fields:
        if field in line and not value_empty(line[field]):
            missing_all_required_fields = False
            field_is_valid = field_value_is_valid(
                line[field], validation_state.format_validation_for_field[field]
            )
            if not field_is_valid:
                pattern_validation_failed = True
                append_line_number_to_field(
                    field, validation_state.lines_incorrect_field_format, current_line
                )

    if (
        missing_all_required_fields
        and len(validation_state.lines_missing_all_required_fields) <= MAX_ERROR_LINES
    ):
        validation_state.lines_missing_all_required_fields.append(current_line)

    if (
        missing_required_field
        or missing_all_required_fields
        or pattern_validation_failed
    ):
        validation_state.error_rows += 1
    else:
        validation_state.valid_rows += 1


def value_empty(value: Optional[str]) -> bool:
    return str(value).strip() == "" or value is None


def lines_missing_report(validation_state: ValidationState) -> List[str]:
    report = []
    max_error_lines_message = f" (First {MAX_ERROR_LINES} lines shown)"
    for field, lines in validation_state.lines_missing_required_field.items():
        max_lines = "" if len(lines) <= MAX_ERROR_LINES else max_error_lines_message
        error_lines = ",".join(map(str, lines[:MAX_ERROR_LINES]))
        report.append(f"Line numbers missing '{field}'{max_lines}: {error_lines}\n")
    if validation_state.lines_missing_all_required_fields:
        max_lines = (
            ""
            if len(validation_state.lines_missing_all_required_fields)
            <= MAX_ERROR_LINES
            else max_error_lines_message
        )
        sorted_fields = ",".join(sorted(validation_state.one_or_more_required_fields))
        error_lines = ",".join(
            map(
                str,
                validation_state.lines_missing_all_required_fields[:MAX_ERROR_LINES],
            )
        )
        report.append(
            f"Line numbers that are missing 1 or more of these required fields '{sorted_fields}'{max_lines}: {error_lines}"
        )
    return report


def lines_incorrect_format_report(validation_state: ValidationState) -> List[str]:
    report = []
    max_error_lines_message = f" (First {MAX_ERROR_LINES} lines shown)"
    for field, lines in validation_state.lines_incorrect_field_format.items():
        max_lines = "" if len(lines) <= MAX_ERROR_LINES else max_error_lines_message
        error_lines = ",".join(map(str, lines[:MAX_ERROR_LINES]))
        report.append(
            f"Line numbers with incorrect '{field}' format{max_lines}: {error_lines}\n"
        )
    return report


def is_header_row_valid(line_string: str, validation_state: ValidationState) -> bool:
    field_names = csv.DictReader([line_string]).fieldnames or []
    header_fields = []
    header_fields.extend(field_names)
    # First check if it is one of filtered formats
    missing_required_pa_fields = any_required_header_fields_missing(
        header_fields, PA_ALL_REQUIRED_FIELDS
    )
    if not missing_required_pa_fields:
        validation_state.format_validation_for_field = PA_FORMAT_VALIDATION_FOR_FIELD
        validation_state.all_required_fields = PA_ALL_REQUIRED_FIELDS
        return True

    missing_required_pl_fields = any_required_header_fields_missing(
        header_fields, PL_ALL_REQUIRED_FIELDS
    )
    if not missing_required_pl_fields:
        validation_state.format_validation_for_field = PL_FORMAT_VALIDATION_FOR_FIELD
        validation_state.all_required_fields = PL_ALL_REQUIRED_FIELDS
        return True

    # Otherwise check if it is in the unfiltered events format
    missing_any_unfiltered_required_fields = any_required_header_fields_missing(
        header_fields, UNFILTERED_ALL_REQUIRED_FIELDS
    )
    missing_all_identity_fields = is_header_missing_all_identity_fields(
        header_fields, UNFILTERED_ONE_OR_MORE_REQUIRED_FIELDS
    )
    if missing_any_unfiltered_required_fields or missing_all_identity_fields:
        return False

    validation_state.all_required_fields = UNFILTERED_ALL_REQUIRED_FIELDS
    validation_state.one_or_more_required_fields = (
        UNFILTERED_ONE_OR_MORE_REQUIRED_FIELDS
    )
    validation_state.format_validation_for_field = (
        UNFILTERED_FORMAT_VALIDATION_FOR_FIELD
    )
    return True


def is_line_ending_valid(line_string: str) -> bool:
    return VALID_LINE_ENDING.match(line_string) is not None


def generate_report(validation_state: ValidationState) -> str:
    report = ["Validation Summary:"]
    report.append(f"Total rows: {validation_state.total_rows}")
    report.append(f"Valid rows: {validation_state.valid_rows}")
    report.append(f"Rows with errors: {validation_state.error_rows}")
    report.extend(validation_state.validation_messages)
    report.extend(lines_missing_report(validation_state))
    report.extend(lines_incorrect_format_report(validation_state))
    return "\n".join(report) + "\n"


def generate_from_body(body: StreamingBody) -> str:
    validation_state = ValidationState()
    valid_header_row = None

    for line in body.iter_lines(keepends=True):
        line_string = line.decode("utf-8")
        valid_line_ending = is_line_ending_valid(line_string)
        if not valid_line_ending:
            skip_row_processing_offset = 1 if valid_header_row else 0
            line_number = (
                validation_state.total_rows +
                HEADER_ROW_OFFSET +
                skip_row_processing_offset
            )
            validation_state.validation_messages.extend(
                [
                    "ERROR - The CSV file is not valid.",
                    "Lines must end with a newline character: \\n",
                    f"The error was detected on line number: {line_number}",
                    "Validation processing stopped.",
                ]
            )
            break
        if valid_header_row:
            reader = csv.DictReader([valid_header_row, line_string])
            for parsed_line in reader:
                validation_state.total_rows += 1
                validate_line(parsed_line, validation_state)
        else:
            header_row_valid = is_header_row_valid(line_string, validation_state)
            if not header_row_valid:
                validation_state.validation_messages.extend(
                    [
                        "ERROR - The header row is not valid.",
                        "1 or more of the required fields is missing.",
                        "Validation processing stopped.",
                    ]
                )
                break
            valid_header_row = line_string

    return generate_report(validation_state)
