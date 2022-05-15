# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict
import json
from dataclasses import dataclass
from typing import Any, Dict, Optional

from dataclasses_json import dataclass_json
from fbpcs.pc_pre_validation.enums import ValidationResult


@dataclass_json
@dataclass
class ValidationReport:
    validation_result: ValidationResult
    validator_name: str
    message: str
    details: Optional[Dict[str, Any]] = None

    def __str__(self) -> str:
        if self.details:
            return (
                f"Validation Report: {self.validator_name}\n"
                f"Result: {self.validation_result.value}\n"
                f"Message: {self.message}\n"
                f"Details:\n{json.dumps(self.details, sort_keys=True, indent=4)}"
            )
        else:
            return (
                f"Validation Report: {self.validator_name}\n"
                f"Result: {self.validation_result.value}\n"
                f"Message: {self.message}"
            )
