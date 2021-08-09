#!/usr/bin/env python3
# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

import os
from dataclasses import dataclass
from enum import Enum
from typing import List, Union

from fbpcs.entity.instance_base import InstanceBase
from fbpcs.entity.mpc_instance import MPCInstance, MPCInstanceStatus
from fbpmp.pid.entity.pid_instance import PIDInstance, PIDInstanceStatus
from fbpmp.pid.entity.pid_stages import UnionPIDStage
from fbpmp.pid.service.pid_service.pid_stage_mapper import STAGE_TO_FILE_FORMAT_MAP


class PrivateAttributionRole(Enum):
    PUBLISHER = "PUBLISHER"
    PARTNER = "PARTNER"


class PrivateAttributionInstanceStatus(Enum):
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


UnionedPAInstance = Union[PIDInstance, MPCInstance]
UnionedPAInstanceStatus = Union[PIDInstanceStatus, MPCInstanceStatus]


@dataclass
class PrivateAttributionInstance(InstanceBase):
    instance_id: str
    role: PrivateAttributionRole
    instances: List[UnionedPAInstance]
    input_path: str
    output_dir: str
    hmac_key: str
    num_pid_containers: int
    num_mpc_containers: int
    num_files_per_mpc_container: int
    padding_size: int
    concurrency: int = 1
    k_anonymity_threshold: int = 0
    retry_counter: int = 0
    status: PrivateAttributionInstanceStatus = PrivateAttributionInstanceStatus.UNKNOWN

    def get_instance_id(self) -> str:
        return self.instance_id

    @property
    def pid_stage_output_base_path(self) -> str:
        return self._get_stage_output_path("pid_stage", "csv")

    @property
    def spine_path(self) -> str:
        spine_path_suffix = (
            STAGE_TO_FILE_FORMAT_MAP[UnionPIDStage.PUBLISHER_RUN_PID]
            if self.role is PrivateAttributionRole.PUBLISHER
            else STAGE_TO_FILE_FORMAT_MAP[UnionPIDStage.ADV_RUN_PID]
        )

        return f"{self.pid_stage_output_base_path}{spine_path_suffix}"

    @property
    def pid_stage_out_data_path(self) -> str:
        data_path_suffix = (
            STAGE_TO_FILE_FORMAT_MAP[UnionPIDStage.PUBLISHER_SHARD]
            if self.role is PrivateAttributionRole.PUBLISHER
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
