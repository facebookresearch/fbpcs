# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict


from collections import Counter
from typing import Any, Dict

from fbpcs.input_data_validation.constants import (
    ID_FIELD,
    VALUE_FIELD,
    EVENT_TIMESTAMP_FIELD,
    CONVERSION_METADATA_FIELD,
    CONVERSION_VALUE_FIELD,
    CONVERSION_TIMESTAMP_FIELD,
)


class ValidationIssues:
    def __init__(self) -> None:
        self.empty_counter: Counter[str] = Counter()
        self.format_error_counter: Counter[str] = Counter()

    def get_as_dict(self) -> Dict[str, Any]:
        issues = {}
        fields = [
            ID_FIELD,
            VALUE_FIELD,
            EVENT_TIMESTAMP_FIELD,
            CONVERSION_METADATA_FIELD,
            CONVERSION_VALUE_FIELD,
            CONVERSION_TIMESTAMP_FIELD,
        ]
        for field in fields:
            self.set_issues_for_field(issues, field)

        return issues

    def count_empty_field(self, field: str) -> None:
        self.empty_counter[field] += 1

    def count_format_error_field(self, field: str) -> None:
        self.format_error_counter[field] += 1

    def set_issues_for_field(self, issues: Dict[str, Any], field: str) -> None:
        field_issues = {}
        empty_count = self.empty_counter[field]
        format_error_count = self.format_error_counter[field]
        if empty_count > 0:
            field_issues["empty"] = empty_count
        if format_error_count > 0:
            field_issues["bad_format"] = format_error_count
        if field_issues:
            issues[field] = field_issues
