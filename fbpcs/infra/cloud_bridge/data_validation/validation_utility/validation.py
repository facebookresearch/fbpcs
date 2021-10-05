# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

import boto3
import csv
from botocore.response import StreamingBody
from typing import Dict, List, Optional, Set

ALL_REQUIRED_FIELDS: Set[str] = {
    'action_source',
    'conversion_value',
    'currency_type',
    'event_type',
    'timestamp',
}
ONE_OR_MORE_REQUIRED_FIELDS: Set[str] = {'email','device_id'}
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

def validate_and_generate_report(bucket: str, key: str) -> str:
    s3_client = boto3.client('s3')
    response = s3_client.get_object(Bucket=bucket, Key=key)
    body = response['Body']
    try:
        return generate_from_body(body)
    except BaseException as e:
        return f'Something went wrong while validating the data. Exception details if available:\n{e}'

def header_check_fields_missing(header_fields: List[str]) -> List[str]:
    fields_missing = ALL_REQUIRED_FIELDS.difference(set(header_fields))
    return sorted(fields_missing)

def header_contains_identity_fields(header_fields: List[str]) -> bool:
    intersection = ONE_OR_MORE_REQUIRED_FIELDS.intersection(set(header_fields))
    return len(intersection) > 0

def validate_line(line: Dict[str, str], validation_state: ValidationState) -> None:
    missing_required_field = False
    missing_all_required_fields = False
    current_line = validation_state.total_rows + HEADER_ROW_OFFSET
    for field in ALL_REQUIRED_FIELDS:
        if field not in line or value_empty(line[field]):
            missing_required_field = True
            has_line_numbers = field in validation_state.lines_missing_required_field
            if has_line_numbers and len(validation_state.lines_missing_required_field[field]) <= MAX_ERROR_LINES:
                validation_state.lines_missing_required_field[field].append(current_line)
            elif not has_line_numbers:
                validation_state.lines_missing_required_field[field] = [current_line]

    missing_all_required_fields = not any(
        field in line and not value_empty(line[field])
        for field in ONE_OR_MORE_REQUIRED_FIELDS
    )

    if missing_all_required_fields and len(validation_state.lines_missing_all_required_fields) <= MAX_ERROR_LINES:
        validation_state.lines_missing_all_required_fields.append(current_line)

    if missing_required_field or missing_all_required_fields:
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
            if not header_row_valid:
                validation_state.header_validation_messages.append(
                    'Validation processing stopped.'
                )
                break
            valid_header_row = line_string

    report = ['Validation Summary:']
    report.append(f'Total rows: {validation_state.total_rows}')
    report.append(f'Valid rows: {validation_state.valid_rows}')
    report.append(f'Rows with errors: {validation_state.error_rows}')
    report.extend(validation_state.header_validation_messages)
    report.extend(lines_missing_report(validation_state))
    return '\n'.join(report) + '\n'
