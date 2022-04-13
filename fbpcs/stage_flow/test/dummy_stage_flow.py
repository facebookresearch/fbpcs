#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

from enum import auto, Enum

from fbpcs.stage_flow.stage_flow import StageFlow, StageFlowData


class DummyStageFlowStatus(Enum):
    # pyre-fixme[20]: Argument `value` expected.
    STAGE_1_STARTED = auto()
    # pyre-fixme[20]: Argument `value` expected.
    STAGE_1_COMPLETED = auto()
    # pyre-fixme[20]: Argument `value` expected.
    STAGE_1_FAILED = auto()
    # pyre-fixme[20]: Argument `value` expected.
    STAGE_2_STARTED = auto()
    # pyre-fixme[20]: Argument `value` expected.
    STAGE_2_COMPLETED = auto()
    # pyre-fixme[20]: Argument `value` expected.
    STAGE_2_FAILED = auto()
    # pyre-fixme[20]: Argument `value` expected.
    STAGE_3_STARTED = auto()
    # pyre-fixme[20]: Argument `value` expected.
    STAGE_3_COMPLETED = auto()
    # pyre-fixme[20]: Argument `value` expected.
    STAGE_3_FAILED = auto()


DummyStageFlowData = StageFlowData[DummyStageFlowStatus]


class DummyStageFlow(StageFlow):
    STAGE_1 = DummyStageFlowData(
        DummyStageFlowStatus.STAGE_1_STARTED,
        DummyStageFlowStatus.STAGE_1_COMPLETED,
        DummyStageFlowStatus.STAGE_1_FAILED,
    )
    STAGE_2 = DummyStageFlowData(
        DummyStageFlowStatus.STAGE_2_STARTED,
        DummyStageFlowStatus.STAGE_2_COMPLETED,
        DummyStageFlowStatus.STAGE_2_FAILED,
    )
    STAGE_3 = DummyStageFlowData(
        DummyStageFlowStatus.STAGE_3_STARTED,
        DummyStageFlowStatus.STAGE_3_COMPLETED,
        DummyStageFlowStatus.STAGE_3_FAILED,
    )
