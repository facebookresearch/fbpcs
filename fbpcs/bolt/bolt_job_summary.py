#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict
from dataclasses import dataclass, field
from enum import Enum
from typing import List, Optional

from fbpcs.private_computation.entity.infra_config import PrivateComputationRole

from fbpcs.private_computation.stage_flows.private_computation_base_stage_flow import (
    PrivateComputationBaseStageFlow,
)


class BoltMetricType(Enum):
    JOB_QUEUE_TIME = "JOB_QUEUE_TIME"
    JOB_RUN_TIME = "JOB_RUN_TIME"
    STAGE_START_UP_TIME = "STAGE_START_UP_TIME"
    STAGE_WAIT_FOR_COMPLETED = "STAGE_WAIT_FOR_COMPLETED"
    STAGE_TOTAL_RUNTIME = "STAGE_TOTAL_RUNTIME"
    PLAYER_STAGE_START_UP_TIME = "PLAYER_STAGE_START_UP_TIME"


@dataclass
class BoltMetric:
    metric_type: BoltMetricType
    value: float
    stage: Optional[PrivateComputationBaseStageFlow] = None
    role: Optional[PrivateComputationRole] = None

    def __repr__(self) -> str:
        stage_name = self.stage.name if self.stage else None
        role_name = self.role.name if self.role else None
        return f"{self.metric_type.name}: ({self.value}, {stage_name}, {role_name})"


@dataclass
class BoltJobSummary:
    job_name: str
    publisher_instance_id: str
    partner_instance_id: str
    is_success: bool
    bolt_metrics: List[BoltMetric] = field(default_factory=list)

    @property
    def job_metrics(self) -> List[BoltMetric]:
        return [s for s in self.bolt_metrics if s.stage is None]

    @property
    def stage_metrics(self) -> List[BoltMetric]:
        return [s for s in self.bolt_metrics if s.stage is not None]

    @property
    def partner_metrics(self) -> List[BoltMetric]:
        return [
            s for s in self.bolt_metrics if s.role is PrivateComputationRole.PARTNER
        ]

    @property
    def publisher_metrics(self) -> List[BoltMetric]:
        return [
            s for s in self.bolt_metrics if s.role is PrivateComputationRole.PUBLISHER
        ]

    def get_stage_metrics(
        self, stage: PrivateComputationBaseStageFlow
    ) -> List[BoltMetric]:
        return [s for s in self.bolt_metrics if s.stage is stage]
