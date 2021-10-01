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

def is_line_valid(line: Dict[str, str]) -> bool:
    for field in ALL_REQUIRED_FIELDS:
        if field not in line or value_empty(line[field]):
            return False

    return any(
        field in line and not value_empty(line[field])
        for field in ONE_OR_MORE_REQUIRED_FIELDS
    )

def value_empty(value: Optional[str]) -> bool:
    return (
        str(value).strip() == '' or
        value is None
    )

def generate_from_body(body: StreamingBody) -> str:
    validation_state = ValidationState()
    valid_header_row = None

    for line in body.iter_lines():
        line_string = line.decode('utf-8')
        if valid_header_row:
            reader = csv.DictReader([valid_header_row, line_string])
            for parsed_line in reader:
                if is_line_valid(parsed_line):
                    validation_state.valid_rows += 1
                else:
                    validation_state.error_rows += 1
                validation_state.total_rows += 1
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
    return '\n'.join(report) + '\n'

class ValidationState:
    def __init__(self):
        self.total_rows = 0
        self.valid_rows = 0
        self.error_rows = 0
        self.header_validation_messages = []
