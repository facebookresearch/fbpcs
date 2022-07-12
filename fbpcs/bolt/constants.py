#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

from fbpcs.private_computation.stage_flows.private_computation_pcf2_stage_flow import (
    PrivateComputationPCF2StageFlow,
)
from fbpcs.private_computation.stage_flows.private_computation_stage_flow import (
    PrivateComputationStageFlow,
)

DEFAULT_POLL_INTERVAL_SEC = 5
DEFAULT_ATTRIBUTION_STAGE_FLOW = PrivateComputationPCF2StageFlow
DEFAULT_LIFT_STAGE_FLOW = PrivateComputationStageFlow
DEFAULT_MAX_PARALLEL_RUNS = 10
DEFAULT_NUM_TRIES = 2
TIMEOUT_SEC = 1200
RETRY_INTERVAL = 60
