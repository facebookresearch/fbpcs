#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

from enum import auto, Enum

from fbpcs.private_computation.stage_flows.private_computation_base_stage_flow import (
    PrivateComputationBaseStageFlow,
    PrivateComputationStageFlowData,
)

from fbpcs.stage_flow.stage_flow import StageFlowData


class DummyStageFlowStatus(Enum):
    STAGE_1_STARTED = auto()
    STAGE_1_COMPLETED = auto()
    STAGE_1_FAILED = auto()
    STAGE_2_STARTED = auto()
    STAGE_2_COMPLETED = auto()
    STAGE_2_FAILED = auto()
    STAGE_3_STARTED = auto()
    STAGE_3_COMPLETED = auto()
    STAGE_3_FAILED = auto()


DummyStageFlowData = StageFlowData[DummyStageFlowStatus]


class DummyStageFlow(PrivateComputationBaseStageFlow):
    STAGE_1 = PrivateComputationStageFlowData(
        DummyStageFlowStatus.STAGE_1_STARTED,
        DummyStageFlowStatus.STAGE_1_COMPLETED,
        DummyStageFlowStatus.STAGE_1_FAILED,
        False,
    )
    STAGE_2 = PrivateComputationStageFlowData(
        DummyStageFlowStatus.STAGE_2_STARTED,
        DummyStageFlowStatus.STAGE_2_COMPLETED,
        DummyStageFlowStatus.STAGE_2_FAILED,
        True,
    )
    STAGE_3 = PrivateComputationStageFlowData(
        DummyStageFlowStatus.STAGE_3_STARTED,
        DummyStageFlowStatus.STAGE_3_COMPLETED,
        DummyStageFlowStatus.STAGE_3_FAILED,
        False,
    )
