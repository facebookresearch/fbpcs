#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict


from dataclasses import dataclass
from typing import Dict, Optional

from dataclasses_json import dataclass_json


@dataclass_json
@dataclass
class PCValidatorConfig:
    region: str
    # Temporarily disable the pre validator by default, until it
    # is ready to be always run
    pc_pre_validator_enabled: bool = False
    data_validation_threshold_overrides: Optional[Dict[str, float]] = None

    def __str__(self) -> str:
        # pyre-ignore
        return self.to_json()
