# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

from collections import Counter
from typing import Any, Dict, Optional

from fbpcs.pc_pre_validation.constants import (
    ALL_FIELDS,
    CONVERSION_VALUE_FIELD,
    ERROR_MESSAGES,
    FORMATTED_FIELDS,
    INTEGER_MAX_VALUE,
    OUT_OF_RANGE_COUNT,
    RANGE_FIELDS,
    REQUIRED_FIELDS,
    TIMESTAMP_RANGE_FIELDS,
    VALUE_FIELD,
)


class InputDataValidationIssues:
    def __init__(self) -> None:
        self.empty_counter: Counter[str] = Counter()
        self.format_error_counter: Counter[str] = Counter()
        self.range_error_counter: Counter[str] = Counter()
        self.max_issue_count_til_error: Dict[str, Dict[str, int]] = {}
        self.cohort_id_aggregates: Counter[int] = Counter()
        self.value_field_name: str = ""

    def get_errors(self) -> Dict[str, Any]:
        errors = {}
        error_messages = []
        for field in ALL_FIELDS:
            if field in REQUIRED_FIELDS + list(self.max_issue_count_til_error.keys()):
                self.set_empty_count_for_field(
                    errors, field, False, self.max_issue_count_til_error.get(field)
                )
            if field in FORMATTED_FIELDS + list(self.max_issue_count_til_error.keys()):
                self.set_format_error_count_for_field(
                    errors, field, False, self.max_issue_count_til_error.get(field)
                )
            if field in RANGE_FIELDS:
                self.set_range_error_count_for_field(
                    errors, field, False, self.max_issue_count_til_error.get(field)
                )

        value_field = errors.get(VALUE_FIELD, {}) or errors.get(
            CONVERSION_VALUE_FIELD, {}
        )

        # If any of the values were out of range, show a specific error_message
        if value_field.get(OUT_OF_RANGE_COUNT, None):
            error_messages.append(
                f"The data in '{self._get_value_field_name()}' should be less than {INTEGER_MAX_VALUE}"
            )

        # If any cohort_id's aggregated (sum of) values was out of range, show a specific error_message
        cohort_ids_overflowed_value = []
        for cohort_id in self.cohort_id_aggregates:
            if self.cohort_id_aggregates[cohort_id] >= INTEGER_MAX_VALUE:
                cohort_ids_overflowed_value.append(cohort_id)
        if cohort_ids_overflowed_value:
            value_field_name = self._get_value_field_name()
            for cohort_id in sorted(cohort_ids_overflowed_value):
                error_messages.append(
                    f"The total aggregate sum of '{value_field_name}' should be less than {INTEGER_MAX_VALUE} for cohort_id {cohort_id}"
                )

        if error_messages:
            errors[ERROR_MESSAGES] = error_messages

        return errors

    def _get_value_field_name(self) -> str:
        return self.value_field_name or VALUE_FIELD

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
            if field in TIMESTAMP_RANGE_FIELDS:
                self.set_range_error_count_for_field(
                    warnings, field, True, self.max_issue_count_til_error.get(field)
                )

        return warnings

    def count_empty_field(self, field: str) -> None:
        self.empty_counter[field] += 1

    def count_format_error_field(self, field: str) -> None:
        self.format_error_counter[field] += 1

    def count_format_out_of_range_field(self, field: str) -> None:
        self.range_error_counter[field] += 1

    def set_max_issue_count_til_error(
        self, max_issue_count_til_error: Dict[str, Dict[str, int]]
    ) -> None:
        self.max_issue_count_til_error = max_issue_count_til_error

    def update_cohort_aggregate(self, cohort_id: int, value: int) -> None:
        self.cohort_id_aggregates.update({cohort_id: value})

    def set_value_field_name(self, value_field_name: str) -> None:
        self.value_field_name = value_field_name

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

    """
    The max_issue_count is used to determine if the error count will be
    reported in the fields_counts which will cause the validation to fail.
    """

    def set_range_error_count_for_field(
        self,
        fields_counts: Dict[str, Dict[str, int]],
        field: str,
        warning: bool,
        max_issue_count: Optional[Dict[str, int]],
    ) -> None:
        counts = {}
        format_error_count = self.range_error_counter[field]
        if max_issue_count and (OUT_OF_RANGE_COUNT in max_issue_count):
            if (
                not warning
                and format_error_count <= max_issue_count[OUT_OF_RANGE_COUNT]
            ):
                return
            if warning and format_error_count > max_issue_count[OUT_OF_RANGE_COUNT]:
                return
        if format_error_count > 0:
            counts[OUT_OF_RANGE_COUNT] = format_error_count
        else:
            return

        if field in fields_counts:
            fields_counts[field].update(counts)
        else:
            fields_counts[field] = counts
