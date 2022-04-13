#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict


from typing import List

from fbpcs.private_computation.entity.private_computation_status import (
    PrivateComputationInstanceStatus,
)

INVALID_STATUS_LIST: List[PrivateComputationInstanceStatus] = [
    PrivateComputationInstanceStatus.UNKNOWN,
    PrivateComputationInstanceStatus.PROCESSING_REQUEST,
    PrivateComputationInstanceStatus.TIMEOUT,
]

POLL_INTERVAL = 60
WAIT_VALID_STATUS_TIMEOUT = 600
WAIT_VALID_STAGE_TIMEOUT = 300
OPERATION_REQUEST_TIMEOUT = 1200
CANCEL_STAGE_TIMEOUT: int = POLL_INTERVAL * 5

MIN_TRIES = 1
MAX_TRIES = 2
RETRY_INTERVAL = 60

MIN_NUM_INSTANCES = 1
MAX_NUM_INSTANCES = 5
PROCESS_WAIT = 1  # interval between starting processes.
INSTANCE_SLA = 57600  # 8 hr instance sla, 2 tries per stage, total 16 hrs.

FBPCS_GRAPH_API_TOKEN = "FBPCS_GRAPH_API_TOKEN"
