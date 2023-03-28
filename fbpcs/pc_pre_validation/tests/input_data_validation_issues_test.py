# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict
from unittest import TestCase

from fbpcs.pc_pre_validation.input_data_validation_issues import (
    InputDataValidationIssues,
)


class InputDataValidationIssuesTest(TestCase):
    def test_merge(self) -> None:
        issues = self._create_item()
        issues2 = self._create_item()

        issues2.count_empty_field("field2")
        issues2.count_format_error_field("field2")
        issues2.count_format_out_of_range_field("field2")
        issues2.update_cohort_aggregate(2, 1)
        issues.merge(issues2)

        self.assertEqual(dict(issues.empty_counter), {"field1": 2, "field2": 1})
        self.assertEqual(dict(issues.format_error_counter), {"field1": 2, "field2": 1})
        self.assertEqual(dict(issues.range_error_counter), {"field1": 2, "field2": 1})
        self.assertEqual(dict(issues.cohort_id_aggregates), {1: 2, 2: 1})

    def _create_item(self) -> InputDataValidationIssues:
        issues = InputDataValidationIssues()
        issues.empty_counter["field1"] += 1
        issues.format_error_counter["field1"] += 1
        issues.range_error_counter["field1"] += 1
        issues.cohort_id_aggregates[1] += 1

        return issues
