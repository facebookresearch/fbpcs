#!/usr/bin/env python3
# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

from dataclasses import dataclass
from typing import TypeVar

from fbpcs.private_computation.entity.private_computation_status import (
    PrivateComputationInstanceStatus,
)
from fbpcs.stage_flow.stage_flow import StageFlow, StageFlowData

C = TypeVar("C", bound="PrivateComputationBaseStageFlow")


@dataclass(frozen=True)
class PrivateComputationStageFlowData(StageFlowData[PrivateComputationInstanceStatus]):
    is_joint_stage: bool


class PrivateComputationBaseStageFlow(StageFlow):
    def __init__(self, data: PrivateComputationStageFlowData) -> None:
        super().__init__()
        self.started_status: PrivateComputationInstanceStatus = data.started_status
        self.failed_status: PrivateComputationInstanceStatus = data.failed_status
        self.completed_status: PrivateComputationInstanceStatus = data.completed_status
        self.is_joint_stage: bool = data.is_joint_stage
