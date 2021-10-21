# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

import csv
from botocore.response import StreamingBody
from typing import Dict, List, Optional, Set
import re

ALL_REQUIRED_FIELDS: Set[str] = {
    'action_source',
    'conversion_value',
    'currency_type',
    'event_type',
    'timestamp',
}
ONE_OR_MORE_REQUIRED_FIELDS: Set[str] = {'email','device_id'}
FORMAT_VALIDATION_FOR_FIELD: Dict[str, re.Pattern] = {
    'email': re.compile(r"^[a-f0-9]{64}$"),
    'device_id': re.compile(r"^([a-f0-9]{32}|[a-f0-9-]{36})$"),
    'timestamp': re.compile(r"^[0-9]+$"),
    'currency_type': re.compile(r"^[a-z]+$"),
    'conversion_value': re.compile(r"^[0-9]+$"),
    'action_source': re.compile(r"^(email|website|phone_call|chat|physical_store|system_generated|other)$"),
    'event_type': re.compile(r"^.+$"),
}
HEADER_ROW_OFFSET = 1
MAX_ERROR_LINES = 100

class ValidationState:
    def __init__(self):
        self.total_rows = 0
        self.valid_rows = 0
        self.error_rows = 0
        self.header_validation_messages = []
        self.lines_missing_required_field = {}
        self.lines_missing_all_required_fields = []
        self.lines_incorrect_field_format = {}

def header_check_fields_missing(header_fields: List[str]) -> List[str]:
    fields_missing = ALL_REQUIRED_FIELDS.difference(set(header_fields))
    return sorted(fields_missing)

def header_contains_identity_fields(header_fields: List[str]) -> bool:
    intersection = ONE_OR_MORE_REQUIRED_FIELDS.intersection(set(header_fields))
    return len(intersection) > 0

def field_value_is_valid(value: str, regex: re.Pattern) -> bool:
    return value.strip() == value and regex.match(value) is not None

def append_line_number_to_field(
    field: str,
    fields_lines: Dict[str, List[int]],
    current_line: int
) -> None:
    has_line_numbers = field in fields_lines
    if has_line_numbers and len(fields_lines[field]) <= MAX_ERROR_LINES:
        fields_lines[field].append(current_line)
    elif not has_line_numbers:
        fields_lines[field] = [current_line]

def validate_line(line: Dict[str, str], validation_state: ValidationState) -> None:
    missing_required_field = False
    missing_all_required_fields = True
    pattern_validation_failed = False
    current_line = validation_state.total_rows + HEADER_ROW_OFFSET
    for field in ALL_REQUIRED_FIELDS:
        if field not in line or value_empty(line[field]):
            missing_required_field = True
            append_line_number_to_field(field, validation_state.lines_missing_required_field, current_line)
        else:
            field_is_valid = field_value_is_valid(line[field], FORMAT_VALIDATION_FOR_FIELD[field])
            if not field_is_valid:
                pattern_validation_failed = True
                append_line_number_to_field(field, validation_state.lines_incorrect_field_format, current_line)

    for field in ONE_OR_MORE_REQUIRED_FIELDS:
        if field in line and not value_empty(line[field]):
            missing_all_required_fields = False
            field_is_valid = field_value_is_valid(line[field], FORMAT_VALIDATION_FOR_FIELD[field])
            if not field_is_valid:
                pattern_validation_failed = True
                append_line_number_to_field(field, validation_state.lines_incorrect_field_format, current_line)

    if missing_all_required_fields and len(validation_state.lines_missing_all_required_fields) <= MAX_ERROR_LINES:
        validation_state.lines_missing_all_required_fields.append(current_line)

    if missing_required_field or missing_all_required_fields or pattern_validation_failed:
        validation_state.error_rows += 1
    else:
        validation_state.valid_rows += 1

def value_empty(value: Optional[str]) -> bool:
    return (
        str(value).strip() == '' or
        value is None
    )

def lines_missing_report(validation_state: ValidationState) -> List[str]:
    report = []
    max_error_lines_message = f' (First {MAX_ERROR_LINES} lines shown)'
    for field, lines in validation_state.lines_missing_required_field.items():
        max_lines = '' if len(lines) <= MAX_ERROR_LINES else max_error_lines_message
        error_lines = ','.join(map(str, lines[:MAX_ERROR_LINES]))
        report.append(f"Line numbers missing '{field}'{max_lines}: {error_lines}\n")
    if validation_state.lines_missing_all_required_fields:
        max_lines = '' if len(validation_state.lines_missing_all_required_fields) <= MAX_ERROR_LINES else max_error_lines_message
        sorted_fields = ','.join(sorted(ONE_OR_MORE_REQUIRED_FIELDS))
        error_lines = ','.join(map(str, validation_state.lines_missing_all_required_fields[:MAX_ERROR_LINES]))
        report.append(
            f"Line numbers that are missing 1 or more of these required fields '{sorted_fields}'{max_lines}: {error_lines}"
        )
    return report

def lines_incorrect_format_report(validation_state: ValidationState) -> List[str]:
    report = []
    max_error_lines_message = f' (First {MAX_ERROR_LINES} lines shown)'
    for field, lines in validation_state.lines_incorrect_field_format.items():
        max_lines = '' if len(lines) <= MAX_ERROR_LINES else max_error_lines_message
        error_lines = ','.join(map(str, lines[:MAX_ERROR_LINES]))
        report.append(f"Line numbers with incorrect '{field}' format{max_lines}: {error_lines}\n")
    return report

def is_header_row_valid(line_string: str, validation_state: ValidationState) -> bool:
    header_row_valid = True
    raw_field_names = csv.DictReader([line_string]).fieldnames
    header_fields = []
    if raw_field_names:
        for s in raw_field_names:
            header_fields.append(s)
    missing_fields = header_check_fields_missing(header_fields)
    if len(missing_fields) > 0:
        missing_fields_str = ','.join(missing_fields)
        validation_state.header_validation_messages.append(
            f'Header row not valid, missing `{missing_fields_str}` required fields.'
        )
        header_row_valid = False
    if not header_contains_identity_fields(header_fields):
        required_header_fields = ','.join(sorted(ONE_OR_MORE_REQUIRED_FIELDS))
        validation_state.header_validation_messages.append(
            f'Header row not valid, at least one of `{required_header_fields}` is required.'
        )
        header_row_valid = False
    return header_row_valid

def generate_report(validation_state: ValidationState) -> str:
    report = ['Validation Summary:']
    report.append(f'Total rows: {validation_state.total_rows}')
    report.append(f'Valid rows: {validation_state.valid_rows}')
    report.append(f'Rows with errors: {validation_state.error_rows}')
    report.extend(validation_state.header_validation_messages)
    report.extend(lines_missing_report(validation_state))
    report.extend(lines_incorrect_format_report(validation_state))
    return '\n'.join(report) + '\n'

def generate_from_body(body: StreamingBody) -> str:
    validation_state = ValidationState()
    valid_header_row = None

    for line in body.iter_lines():
        line_string = line.decode('utf-8')
        if valid_header_row:
            reader = csv.DictReader([valid_header_row, line_string])
            for parsed_line in reader:
                validation_state.total_rows += 1
                validate_line(parsed_line, validation_state)
        else:
            header_row_valid = is_header_row_valid(line_string, validation_state)
            if not header_row_valid:
                validation_state.header_validation_messages.append(
                    'Validation processing stopped.'
                )
                break
            valid_header_row = line_string

    return generate_report(validation_state)
