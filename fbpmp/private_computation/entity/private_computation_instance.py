#!/usr/bin/env python3
# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

import os
from dataclasses import dataclass
from enum import Enum
from typing import List, Union, Optional

from fbpcp.entity.mpc_instance import MPCInstance, MPCInstanceStatus
from fbpmp.common.entity.instance_base import InstanceBase
from fbpmp.pid.entity.pid_instance import PIDInstance, PIDInstanceStatus
from fbpmp.post_processing_handler.post_processing_instance import (
    PostProcessingInstance,
    PostProcessingInstanceStatus,
)
from fbpmp.private_lift.entity.breakdown_key import BreakdownKey
from fbpmp.private_lift.entity.pce_config import PCEConfig


class PrivateComputationRole(Enum):
    PUBLISHER = "PUBLISHER"
    PARTNER = "PARTNER"


class PrivateComputationInstanceStatus(Enum):
    UNKNOWN = "UNKNOWN"
    CREATED = "CREATED"
    ID_MATCHING_STARTED = "ID_MATCHING_STARTED"
    ID_MATCHING_COMPLETED = "ID_MATCHING_COMPLETED"
    ID_MATCHING_FAILED = "ID_MATCHING_FAILED"
    COMPUTATION_STARTED = "COMPUTATION_STARTED"
    COMPUTATION_COMPLETED = "COMPUTATION_COMPLETED"
    COMPUTATION_FAILED = "COMPUTATION_FAILED"
    AGGREGATION_STARTED = "AGGREGATION_STARTED"
    AGGREGATION_COMPLETED = "AGGREGATION_COMPLETED"
    AGGREGATION_FAILED = "AGGREGATION_FAILED"
    POST_PROCESSING_HANDLERS_STARTED = "POST_PROCESSING_HANDLERS_STARTED"
    POST_PROCESSING_HANDLERS_COMPLETED = "POST_PROCESSING_HANDLERS_COMPLETED"
    POST_PROCESSING_HANDLERS_FAILED = "POST_PROCESSING_HANDLERS_FAILED"
    PROCESSING_REQUEST = "PROCESSING_REQUEST"


UnionedPCInstance = Union[PIDInstance, MPCInstance, PostProcessingInstance]
UnionedPCInstanceStatus = Union[
    PIDInstanceStatus, MPCInstanceStatus, PostProcessingInstanceStatus
]


@dataclass
class PrivateComputationInstance(InstanceBase):
    instance_id: str
    role: PrivateComputationRole
    instances: List[UnionedPCInstance]
    status: PrivateComputationInstanceStatus
    status_update_ts: int
    retry_counter: int = 0
    # TODO: once the product is stabilized, we can enable this
    partial_container_retry_enabled: bool = False
    is_validating: Optional[bool] = False
    synthetic_shard_path: Optional[str] = None
    num_containers: Optional[
        int
    ] = None  # assign when create instance; reused by id match, compute and aggregate
    input_path: Optional[str] = None  # assign when create instance; reused by id match
    output_dir: Optional[
        str
    ] = None  # assign when create instance; reused by id match, compute and aggregate
    spine_path: Optional[str] = None  # assign when id match; reused by compute
    data_path: Optional[str] = None  # assign when id match; reused by compute
    compute_output_path: Optional[
        str
    ] = None  # assign when compute; reused by aggregate
    compute_num_shards: Optional[int] = None  # assign when compute; reused by aggregate
    aggregated_result_path: Optional[
        str
    ] = None  # assign when aggregate; reused by post processing handlers
    breakdown_key: Optional[BreakdownKey] = None
    pce_config: Optional[PCEConfig] = None
    is_test: Optional[bool] = False  # set to be true for testing account ID

    def get_instance_id(self) -> str:
        return self.instance_id

    @property
    def pid_stage_output_base_path(self) -> str:
        # pyre-fixme[7]: Expected `str` but got `Optional[str]`.
        return self._get_stage_output_path("pid_stage", "csv")

    @property
    def compute_stage_output_base_path(self) -> str:
        # pyre-fixme[7]: Expected `str` but got `Optional[str]`.
        return self._get_stage_output_path("compute_stage", "json")

    @property
    def shard_aggregate_stage_output_path(self) -> str:
        # pyre-fixme[7]: Expected `str` but got `Optional[str]`.
        return self._get_stage_output_path("shard_aggregation_stage", "json")

    def _get_stage_output_path(self, stage: str, extension_type: str) -> Optional[str]:
        if not self.output_dir:
            return None

        return os.path.join(
            self.output_dir,
            f"{self.instance_id}_out_dir",
            stage,
            f"out.{extension_type}",
        )
