#!/usr/bin/env python3
# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

from enum import Enum


class OneDockerBinaryNames(Enum):
    ATTRIBUTION_ID_SPINE_COMBINER = "data_processing/attribution_id_combiner"
    LIFT_ID_SPINE_COMBINER        = "data_processing/lift_id_combiner"
    SHARDER                       = "data_processing/sharder"
    SHARDER_HASHED_FOR_PID        = "data_processing/sharder_hashed_for_pid"
    UNION_PID_PREPARER            = "data_processing/pid_preparer"

    DECOUPLED_ATTRIBUTION = "private_attribution/decoupled_attribution"
    ATTRIBUTION_COMPUTE = "private_attribution/compute"
    SHARD_AGGREGATOR    = "private_attribution/shard-aggregator"

    PID_CLIENT = "pid/private-id-client"
    PID_SERVER = "pid/private-id-server"

    LIFT_COMPUTE = "private_lift/lift"
