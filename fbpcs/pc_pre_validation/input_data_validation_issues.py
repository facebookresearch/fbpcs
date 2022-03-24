# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict


from collections import Counter
from typing import Any, Dict

from fbpcs.pc_pre_validation.constants import (
    ALL_FIELDS,
    REQUIRED_FIELDS,
)


class InputDataValidationIssues:
    def __init__(self) -> None:
        self.empty_counter: Counter[str] = Counter()
        self.format_error_counter: Counter[str] = Counter()

    def get_errors(self) -> Dict[str, Any]:
        errors = {}
        for field in ALL_FIELDS:
            if field in REQUIRED_FIELDS:
                self.set_for_field(errors, field)

        return errors

    def get_warnings(self) -> Dict[str, Any]:
        warnings = {}
        for field in ALL_FIELDS:
            if field not in REQUIRED_FIELDS:
                self.set_for_field(warnings, field)

        return warnings

    def count_empty_field(self, field: str) -> None:
        self.empty_counter[field] += 1

    def count_format_error_field(self, field: str) -> None:
        self.format_error_counter[field] += 1

    def set_for_field(self, fields_counts: Dict[str, Any], field: str) -> None:
        counts = {}
        empty_count = self.empty_counter[field]
        format_error_count = self.format_error_counter[field]
        if empty_count > 0:
            counts["empty"] = empty_count
        if format_error_count > 0:
            counts["bad_format"] = format_error_count

        if counts:
            fields_counts[field] = counts
