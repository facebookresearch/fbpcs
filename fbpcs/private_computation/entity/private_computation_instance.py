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

from fbpcp.entity.mpc_instance import MPCInstanceStatus
from fbpcs.common.entity.instance_base import InstanceBase
from fbpcs.common.entity.pcs_mpc_instance import PCSMPCInstance
from fbpcs.pid.entity.pid_instance import PIDInstance, PIDInstanceStatus
from fbpcs.pid.entity.pid_stages import UnionPIDStage
from fbpcs.pid.service.pid_service.pid_stage_mapper import STAGE_TO_FILE_FORMAT_MAP
from fbpcs.post_processing_handler.post_processing_instance import (
    PostProcessingInstance,
    PostProcessingInstanceStatus,
)
from fbpcs.private_lift.entity.breakdown_key import BreakdownKey
from fbpcs.private_lift.entity.pce_config import PCEConfig


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


class PrivateComputationGameType(Enum):
    LIFT = "LIFT"
    ATTRIBUTION = "ATTRIBUTION"


UnionedPCInstance = Union[PIDInstance, PCSMPCInstance, PostProcessingInstance]
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
    num_files_per_mpc_container: int
    game_type: PrivateComputationGameType
    input_path: str
    output_dir: str
    num_pid_containers: int
    num_mpc_containers: int
    concurrency: int

    retry_counter: int = 0
    partial_container_retry_enabled: bool = (
        False  # TODO T98578624: once the product is stabilized, we can enable this
    )
    is_validating: Optional[bool] = False
    synthetic_shard_path: Optional[str] = None

    # TODO T98476320: make the following optional attributes non-optional. They are optional
    # because at the time the instance is created, pl might not provide any or all of them.
    hmac_key: Optional[str] = None
    padding_size: Optional[int] = None

    k_anonymity_threshold: int = 0
    retry_counter: int = 0

    breakdown_key: Optional[BreakdownKey] = None
    pce_config: Optional[PCEConfig] = None
    is_test: Optional[bool] = False  # set to be true for testing account ID

    def get_instance_id(self) -> str:
        return self.instance_id

    @property
    def pid_stage_output_base_path(self) -> str:
        return self._get_stage_output_path("pid_stage", "csv")

    @property
    def pid_stage_output_spine_path(self) -> str:
        spine_path_suffix = (
            STAGE_TO_FILE_FORMAT_MAP[UnionPIDStage.PUBLISHER_RUN_PID]
            if self.role is PrivateComputationRole.PUBLISHER
            else STAGE_TO_FILE_FORMAT_MAP[UnionPIDStage.ADV_RUN_PID]
        )

        return f"{self.pid_stage_output_base_path}{spine_path_suffix}"

    @property
    def pid_stage_output_data_path(self) -> str:
        data_path_suffix = (
            STAGE_TO_FILE_FORMAT_MAP[UnionPIDStage.PUBLISHER_SHARD]
            if self.role is PrivateComputationRole.PUBLISHER
            else STAGE_TO_FILE_FORMAT_MAP[UnionPIDStage.ADV_SHARD]
        )
        return f"{self.pid_stage_output_base_path}{data_path_suffix}"

    @property
    def data_processing_output_path(self) -> str:
        return self._get_stage_output_path("data_processing_stage", "csv")

    @property
    def compute_stage_output_base_path(self) -> str:
        return self._get_stage_output_path("compute_stage", "json")

    @property
    def shard_aggregate_stage_output_path(self) -> str:
        return self._get_stage_output_path("shard_aggregation_stage", "json")

    def _get_stage_output_path(self, stage: str, extension_type: str) -> str:
        return os.path.join(
            self.output_dir,
            f"{self.instance_id}_out_dir",
            stage,
            f"out.{extension_type}",
        )
