# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

from dataclasses import dataclass
from enum import Enum
from typing import List, Optional


@dataclass
class RunCommandResult:
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


class TerraformCommand(str, Enum):
    INIT: str = "init"
    APPLY: str = "apply"
    DESTROY: str = "destroy"
    PLAN: str = "plan"


class TerraformOptionFlag:
    pass


class FlaggedOption(TerraformOptionFlag):
    """
    Used to set flag options, eg, `terraform init -reconfigure`
    `-reconfigure` is a flagged option here.

    This should not be confused with the options that accept bool values.
    In case of options that accept bool values, explicit bool value is passed.
    Eg of bool option: `terraform apply -input=false`

    Usage of FlaggedOption:
        t = TerraformDeployment()
        t.terraform_init(reconfigure=FlaggedOption)

        Results in : `terraform init -reconfigure`
    """

    pass


class NotFlaggedOption(TerraformOptionFlag):
    """
    Is opposite of the FlaggedOption and is used to unset flag options.

    Usage:
        t = TerraformDeployment()
        t.terraform_init(reconfigure=NotFlaggedOption)

        Results in : `terraform init`
    """

    pass
