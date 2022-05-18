# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

from collections import Counter
from typing import Any, Dict, Optional

from fbpcs.pc_pre_validation.constants import (
    ALL_FIELDS,
    FORMATTED_FIELDS,
    REQUIRED_FIELDS,
)


class InputDataValidationIssues:
    def __init__(self) -> None:
        self.empty_counter: Counter[str] = Counter()
        self.format_error_counter: Counter[str] = Counter()
        self.max_issue_count_til_error: Dict[str, Dict[str, int]] = {}

    def get_errors(self) -> Dict[str, Any]:
        errors = {}
        for field in ALL_FIELDS:
            if field in REQUIRED_FIELDS + list(self.max_issue_count_til_error.keys()):
                self.set_empty_count_for_field(
                    errors, field, False, self.max_issue_count_til_error.get(field)
                )
            if field in FORMATTED_FIELDS + list(self.max_issue_count_til_error.keys()):
                self.set_format_error_count_for_field(
                    errors, field, False, self.max_issue_count_til_error.get(field)
                )

        return errors

    def get_warnings(self) -> Dict[str, Any]:
        warnings = {}
        for field in ALL_FIELDS:
            if field not in REQUIRED_FIELDS:
                self.set_empty_count_for_field(
                    warnings, field, True, self.max_issue_count_til_error.get(field)
                )
            if field not in FORMATTED_FIELDS:
                self.set_format_error_count_for_field(
                    warnings, field, True, self.max_issue_count_til_error.get(field)
                )

        return warnings

    def count_empty_field(self, field: str) -> None:
        self.empty_counter[field] += 1

    def count_format_error_field(self, field: str) -> None:
        self.format_error_counter[field] += 1

    def set_max_issue_count_til_error(
        self, max_issue_count_til_error: Dict[str, Dict[str, int]]
    ) -> None:
        self.max_issue_count_til_error = max_issue_count_til_error

    def set_empty_count_for_field(
        self,
        fields_counts: Dict[str, Any],
        field: str,
        warning: bool,
        max_issue_count: Optional[Dict[str, int]],
    ) -> None:
        counts = {}
        empty_count = self.empty_counter[field]
        if max_issue_count and ("empty_count" in max_issue_count):
            if not warning and empty_count <= max_issue_count["empty_count"]:
                return
            if warning and empty_count > max_issue_count["empty_count"]:
                return
        if empty_count > 0:
            counts["empty_count"] = empty_count
        else:
            return

        if field in fields_counts:
            fields_counts[field].update(counts)
        else:
            fields_counts[field] = counts

    def set_format_error_count_for_field(
        self,
        fields_counts: Dict[str, Any],
        field: str,
        warning: bool,
        max_issue_count: Optional[Dict[str, int]],
    ) -> None:
        counts = {}
        format_error_count = self.format_error_counter[field]
        if max_issue_count and ("bad_format_count" in max_issue_count):
            if (
                not warning
                and format_error_count <= max_issue_count["bad_format_count"]
            ):
                return
            if warning and format_error_count > max_issue_count["bad_format_count"]:
                return
        if format_error_count > 0:
            counts["bad_format_count"] = format_error_count
        else:
            return

        if field in fields_counts:
            fields_counts[field].update(counts)
        else:
            fields_counts[field] = counts
