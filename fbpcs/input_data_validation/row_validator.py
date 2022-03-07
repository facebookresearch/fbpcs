# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict


from fbpcs.input_data_validation.validation_issues import ValidationIssues


class RowValidator:
    def __init__(self, validation_issues: ValidationIssues) -> None:
        self.validation_issues = validation_issues

    def validate(self, field: str, value: str) -> None:
        if value.strip() == "":
            self.validation_issues.count_empty_field(field)
