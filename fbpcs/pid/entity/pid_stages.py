#!/usr/bin/env python3
# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

from enum import Enum


class UnionPIDStage(Enum):
    PUBLISHER_SHARD = "PUBLISHER_SHARD"
    PUBLISHER_PREPARE = "PUBLISHER_PREPARE"
    PUBLISHER_RUN_PID = "PUBLISHER_RUN_PID"
    ADV_SHARD = "ADV_SHARD"
    ADV_PREPARE = "ADV_PREPARE"
    ADV_RUN_PID = "ADV_RUN_PID"


class PIDStageFailureError(RuntimeError):
    """Custom exception thrown when a PID stage fails unexpectedly"""


class PIDFlowUnsupportedError(RuntimeError):
    """Custom exception thrown when a PID protocol + role combination is undefined"""
