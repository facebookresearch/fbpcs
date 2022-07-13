# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

from dataclasses import dataclass
from enum import Enum
from typing import List, Optional


@dataclass
class RunCommandReturn:
    return_code: int
    output: Optional[str]
    error: Optional[str]


@dataclass
class TerraformCliOptions:
    state: str = "state"
    target: str = "target"
    var: str = "var"
    var_file: str = "var_file"
    parallelism: str = "parallelism"
    terraform_input: str = "input"
    backend_config: str = "backend_config"
    reconfigure: str = "reconfigure"


NOT_SUPPORTED_INIT_DEFAULT_OPTIONS: List[str] = [
    TerraformCliOptions.state,
    TerraformCliOptions.parallelism,
]


class TerraformCommands(str, Enum):
    INIT: str = "init"
    APPLY: str = "apply"
