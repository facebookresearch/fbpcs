#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

from enum import Enum


class PrivateComputationInstanceStatus(Enum):
    # NOTE: If you add any more statuses, you should regenerate the test resources
    # That file should be named gen_status_resources.py
    UNKNOWN = "UNKNOWN"
    CREATION_INITIALIZED = "CREATION_INITIALIZED"
    CREATION_STARTED = "CREATION_STARTED"
    CREATED = "CREATED"
    CREATION_FAILED = "CREATION_FAILED"
    INPUT_DATA_VALIDATION_STARTED = "INPUT_DATA_VALIDATION_STARTED"  # DEPRECATED TODO: T131443509 delete deprecated INPUT_DATA_VALIDATION status
    INPUT_DATA_VALIDATION_COMPLETED = "INPUT_DATA_VALIDATION_COMPLETED"  # DEPRECATED TODO: T131443509 delete deprecated INPUT_DATA_VALIDATION status
    INPUT_DATA_VALIDATION_FAILED = "INPUT_DATA_VALIDATION_FAILED"  # DEPRECATED TODO: T131443509 delete deprecated INPUT_DATA_VALIDATION status
    PC_PRE_VALIDATION_INITIALIZED = "PC_PRE_VALIDATION_INITIALIZED"
    PC_PRE_VALIDATION_STARTED = "PC_PRE_VALIDATION_STARTED"
    PC_PRE_VALIDATION_COMPLETED = "PC_PRE_VALIDATION_COMPLETED"
    PC_PRE_VALIDATION_FAILED = "PC_PRE_VALIDATION_FAILED"
    PID_MR_INITIALIZED = "PID_MR_INITIALIZED"
    PID_MR_STARTED = "PID_MR_STARTED"
    PID_MR_COMPLETED = "PID_MR_COMPLETED"
    PID_MR_FAILED = "PID_MR_FAILED"
    PID_SHARD_INITIALIZED = "PID_SHARD_INITIALIZED"
    PID_SHARD_STARTED = "PID_SHARD_STARTED"
    PID_SHARD_COMPLETED = "PID_SHARD_COMPLETED"
    PID_SHARD_FAILED = "PID_SHARD_FAILED"
    PID_PREPARE_INITIALIZED = "PID_PREPARE_INITIALIZED"
    PID_PREPARE_STARTED = "PID_PREPARE_STARTED"
    PID_PREPARE_COMPLETED = "PID_PREPARE_COMPLETED"
    PID_PREPARE_FAILED = "PID_PREPARE_FAILED"
    ID_MATCHING_INITIALIZED = "ID_MATCHING_INITIALIZED"
    ID_MATCHING_STARTED = "ID_MATCHING_STARTED"
    ID_MATCHING_COMPLETED = "ID_MATCHING_COMPLETED"
    ID_MATCHING_FAILED = "ID_MATCHING_FAILED"
    ID_MATCHING_POST_PROCESS_INITIALIZED = "ID_MATCHING_POST_PROCESS_INITIALIZED"
    ID_MATCHING_POST_PROCESS_STARTED = "ID_MATCHING_POST_PROCESS_STARTED"
    ID_MATCHING_POST_PROCESS_COMPLETED = "ID_MATCHING_POST_PROCESS_COMPLETED"
    ID_MATCHING_POST_PROCESS_FAILED = "ID_MATCHING_POST_PROCESS_FAILED"
    PREPARE_DATA_STARTED = "PREPARE_DATA_STARTED"  # DEPRECATED
    PREPARE_DATA_COMPLETED = "PREPARE_DATA_COMPLETED"  # DEPRECATED
    PREPARE_DATA_FAILED = "PREPARE_DATA_FAILED"  # DEPRECATED
    ID_SPINE_COMBINER_INITIALIZED = "ID_SPINE_COMBINER_INITIALIZED"
    ID_SPINE_COMBINER_STARTED = "ID_SPINE_COMBINER_STARTED"
    ID_SPINE_COMBINER_COMPLETED = "ID_SPINE_COMBINER_COMPLETED"
    ID_SPINE_COMBINER_FAILED = "ID_SPINE_COMBINER_FAILED"
    RESHARD_INITIALIZED = "RESHARD_INITIALIZED"
    RESHARD_STARTED = "RESHARD_STARTED"
    RESHARD_COMPLETED = "RESHARD_COMPLETED"
    RESHARD_FAILED = "RESHARD_FAILED"
    COMPUTATION_INITIALIZED = "COMPUTATION_INITIALIZED"
    COMPUTATION_STARTED = "COMPUTATION_STARTED"
    COMPUTATION_COMPLETED = "COMPUTATION_COMPLETED"
    COMPUTATION_FAILED = "COMPUTATION_FAILED"
    DECOUPLED_ATTRIBUTION_INITIALIZED = "DECOUPLED_ATTRIBUTION_INITIALIZED"
    DECOUPLED_ATTRIBUTION_STARTED = "DECOUPLED_ATTRIBUTION_STARTED"
    DECOUPLED_ATTRIBUTION_COMPLETED = "DECOUPLED_ATTRIBUTION_COMPLETED"
    DECOUPLED_ATTRIBUTION_FAILED = "DECOUPLED_ATTRIBUTION_FAILED"
    DECOUPLED_AGGREGATION_INITIALIZED = "DECOUPLED_AGGREGATION_INITIALIZED"
    DECOUPLED_AGGREGATION_STARTED = "DECOUPLED_AGGREGATION_STARTED"
    DECOUPLED_AGGREGATION_COMPLETED = "DECOUPLED_AGGREGATION_COMPLETED"
    DECOUPLED_AGGREGATION_FAILED = "DECOUPLED_AGGREGATION_FAILED"
    PCF2_LIFT_INITIALIZED = "PCF2_LIFT_INITIALIZED"
    PCF2_LIFT_STARTED = "PCF2_LIFT_STARTED"
    PCF2_LIFT_COMPLETED = "PCF2_LIFT_COMPLETED"
    PCF2_LIFT_FAILED = "PCF2_LIFT_FAILED"
    PCF2_LIFT_METADATA_COMPACTION_INITIALIZED = (
        "PCF2_LIFT_METADATA_COMPACTION_INITIALIZED"
    )
    PCF2_LIFT_METADATA_COMPACTION_STARTED = "PCF2_LIFT_METADATA_COMPACTION_STARTED"
    PCF2_LIFT_METADATA_COMPACTION_COMPLETED = "PCF2_LIFT_METADATA_COMPACTION_COMPLETED"
    PCF2_LIFT_METADATA_COMPACTION_FAILED = "PCF2_LIFT_METADATA_COMPACTION_FAILED"
    PCF2_ATTRIBUTION_INITIALIZED = "PCF2_ATTRIBUTION_INITIALIZED"
    PCF2_ATTRIBUTION_STARTED = "PCF2_ATTRIBUTION_STARTED"
    PCF2_ATTRIBUTION_COMPLETED = "PCF2_ATTRIBUTION_COMPLETED"
    PCF2_ATTRIBUTION_FAILED = "PCF2_ATTRIBUTION_FAILED"
    PCF2_AGGREGATION_INITIALIZED = "PCF2_AGGREGATION_INITIALIZED"
    PCF2_AGGREGATION_STARTED = "PCF2_AGGREGATION_STARTED"
    PCF2_AGGREGATION_COMPLETED = "PCF2_AGGREGATION_COMPLETED"
    PCF2_AGGREGATION_FAILED = "PCF2_AGGREGATION_FAILED"
    PCF2_SHARD_COMBINE_INITIALIZED = "PCF2_SHARD_COMBINE_INITIALIZED"
    PCF2_SHARD_COMBINE_STARTED = "PCF2_SHARD_COMBINE_STARTED"
    PCF2_SHARD_COMBINE_COMPLETED = "PCF2_SHARD_COMBINE_COMPLETED"
    PCF2_SHARD_COMBINE_FAILED = "PCF2_SHARD_COMBINE_FAILED"
    AGGREGATION_INITIALIZED = "AGGREGATION_INITIALIZED"
    AGGREGATION_STARTED = "AGGREGATION_STARTED"
    AGGREGATION_COMPLETED = "AGGREGATION_COMPLETED"
    AGGREGATION_FAILED = "AGGREGATION_FAILED"
    POST_PROCESSING_HANDLERS_INITIALIZED = "POST_PROCESSING_HANDLERS_INITIALIZED"
    POST_PROCESSING_HANDLERS_STARTED = "POST_PROCESSING_HANDLERS_STARTED"
    POST_PROCESSING_HANDLERS_COMPLETED = "POST_PROCESSING_HANDLERS_COMPLETED"
    POST_PROCESSING_HANDLERS_FAILED = "POST_PROCESSING_HANDLERS_FAILED"
    PROCESSING_REQUEST = "PROCESSING_REQUEST"
    TIMEOUT = "TIMEOUT"
