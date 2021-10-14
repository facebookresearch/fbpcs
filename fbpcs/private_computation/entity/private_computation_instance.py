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
from fbpcs.private_computation.entity.breakdown_key import BreakdownKey
from fbpcs.private_computation.entity.pce_config import PCEConfig
from fbpcs.private_computation.entity.private_computation_base_stage_flow import (
    PrivateComputationBaseStageFlow,
)
from fbpcs.private_computation.entity.private_computation_status import (
    PrivateComputationInstanceStatus,
)


class PrivateComputationRole(Enum):
    PUBLISHER = "PUBLISHER"
    PARTNER = "PARTNER"


class PrivateComputationGameType(Enum):
    LIFT = "LIFT"
    ATTRIBUTION = "ATTRIBUTION"


class AttributionRule(Enum):
    LAST_CLICK_1D = "last_click_1d"
    LAST_CLICK_7D = "last_click_7d"
    LAST_CLICK_28D = "last_click_28d"
    LAST_TOUCH_1D = "last_touch_1d"
    LAST_TOUCH_7D = "last_touch_7d"
    LAST_TOUCH_28D = "last_touch_28d"


class AggregationType(Enum):
    MEASUREMENT = "measurement"


UnionedPCInstance = Union[PIDInstance, PCSMPCInstance, PostProcessingInstance]
UnionedPCInstanceStatus = Union[
    PIDInstanceStatus, MPCInstanceStatus, PostProcessingInstanceStatus
]


@dataclass
class PrivateComputationInstance(InstanceBase):
    """Stores metadata of a private computation instance

    Public attributes:
        attribution_rule: the rule that a conversion is attributed to an exposure (e.g., last_click_1d,
                            last_click_28d, last_touch_1d, last_touch_28d). Not currently used by Lift.
        aggregation_type: the level the statistics are aggregated at (e.g., ad-object, which includes ad,
                            campaign and campaign group). In the future, aggregation_type will also be
                            used to infer the metrics_format_type argument of the shard aggregator game.
                            Not currently used by Lift.
        concurrency: number of threads to run per container at the MPC compute metrics stage

    Private attributes:
        _stage_flow_cls_name: the name of a PrivateComputationBaseStageFlow subclass (cls.__name__)
    """

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

    attribution_rule: Optional[AttributionRule] = None
    aggregation_type: Optional[AggregationType] = None

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

    concurrency: int = 1  # used only by MPC compute metrics stage. TODO T102588568: rename to compute_metrics_concurrency
    k_anonymity_threshold: int = 0
    retry_counter: int = 0

    # This boolean is used to determine whether auto retry will be performed at any data processing step
    #   of the computation. For now, when fail_fast = False, the pid preparer step
    #   will retry up to MAX_RETRY times, which is set to be 0, because almost all problems
    #   we've seen so far won't get resolved by just retrying. In the future, when the product is more stable,
    #   we will increase MAX_RETRY and allow other steps to auto retry as well.
    fail_fast: bool = False

    breakdown_key: Optional[BreakdownKey] = None
    pce_config: Optional[PCEConfig] = None
    is_test: Optional[bool] = False  # set to be true for testing account ID
    # stored as a string because the enum was refusing to serialize to json, no matter what I tried.
    # TODO(T103299005): [BE] Figure out how to serialize StageFlow objects to json instead of using their class name
    _stage_flow_cls_name: str = "PrivateComputationStageFlow"

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

    @property
    def current_stage(self) -> PrivateComputationBaseStageFlow:
        return PrivateComputationBaseStageFlow.cls_name_to_cls(
            self._stage_flow_cls_name
        ).get_stage_from_status(self.status)
