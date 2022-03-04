#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

from enum import Enum


class OneDockerBinaryNames(Enum):
    ATTRIBUTION_ID_SPINE_COMBINER = "data_processing/attribution_id_combiner"
    LIFT_ID_SPINE_COMBINER = "data_processing/lift_id_combiner"
    SHARDER = "data_processing/sharder"
    SHARDER_HASHED_FOR_PID = "data_processing/sharder_hashed_for_pid"
    UNION_PID_PREPARER = "data_processing/pid_preparer"

    DECOUPLED_ATTRIBUTION = "private_attribution/decoupled_attribution"
    DECOUPLED_AGGREGATION = "private_attribution/decoupled_aggregation"
    PCF2_ATTRIBUTION = "private_attribution/pcf2_attribution"
    PCF2_AGGREGATION = "private_attribution/pcf2_aggregation"
    SHARD_AGGREGATOR = "private_attribution/shard-aggregator"

    PID_CLIENT = "pid/private-id-client"
    PID_SERVER = "pid/private-id-server"
    CROSS_PSI_SERVER = "pid/cross-psi-server"
    CROSS_PSI_CLIENT = "pid/cross-psi-client"
    CROSS_PSI_COR_SERVER = "pid/cross-psi-xor-server"
    CROSS_PSI_COR_CLIENT = "pid/cross-psi-xor-client"

    LIFT_COMPUTE = "private_lift/lift"
