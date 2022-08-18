#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

from enum import Enum
from typing import Dict


class UnionPIDStage(Enum):
    PUBLISHER_SHARD = "PUBLISHER_SHARD"
    PUBLISHER_PREPARE = "PUBLISHER_PREPARE"
    PUBLISHER_RUN_PID = "PUBLISHER_RUN_PID"
    PUBLISHER_RUN_MR_PID = "PUBLISHER_RUN_MR_PID"
    ADV_SHARD = "ADV_SHARD"
    ADV_PREPARE = "ADV_PREPARE"
    ADV_RUN_PID = "ADV_RUN_PID"
    ADV_RUN_MR_PID = "ADV_RUN_MR_PID"


class PIDStageFailureError(RuntimeError):
    """Custom exception thrown when a PID stage fails unexpectedly"""


class PIDFlowUnsupportedError(RuntimeError):
    """Custom exception thrown when a PID protocol + role combination is undefined"""


STAGE_TO_FILE_FORMAT_MAP: Dict[UnionPIDStage, str] = {
    UnionPIDStage.PUBLISHER_SHARD: "_publisher_sharded",
    UnionPIDStage.PUBLISHER_PREPARE: "_publisher_prepared",
    UnionPIDStage.PUBLISHER_RUN_PID: "_publisher_pid_matched",
    UnionPIDStage.PUBLISHER_RUN_MR_PID: "_publisher_mr_pid_matched",
    UnionPIDStage.ADV_SHARD: "_advertiser_sharded",
    UnionPIDStage.ADV_PREPARE: "_advertiser_prepared",
    UnionPIDStage.ADV_RUN_PID: "_advertiser_pid_matched",
    UnionPIDStage.ADV_RUN_MR_PID: "_advertiser_mr_pid_matched",
}
