#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict


from dataclasses import dataclass
from enum import Enum

from dataclasses_json import dataclass_json, DataClassJsonMixin


@dataclass_json
@dataclass
class PidRunConfigs:
    pidMrMultikeyJarPath: str


@dataclass_json
@dataclass
class PidWorkflowConfigs:
    stateMachineArn: str


@dataclass
class PidMrConfig(DataClassJsonMixin):
    runConfigs: PidRunConfigs
    workflowConfigs: PidWorkflowConfigs


class Protocol(Enum):
    PID_PROTOCOL = "PID"
    MR_PID_PROTOCOL = "MR_PID"
