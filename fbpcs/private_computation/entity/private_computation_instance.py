#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

import os
from dataclasses import dataclass
from enum import Enum, IntEnum
from typing import List, Union, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Type

    from fbpcs.private_computation.stage_flows.private_computation_base_stage_flow import (
        PrivateComputationBaseStageFlow,
    )

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
    LAST_CLICK_2_7D = "last_click_2_7d"
    LAST_TOUCH_2_7D = "last_touch_2_7d"


class AggregationType(Enum):
    MEASUREMENT = "measurement"


# This is the visibility defined in https://fburl.com/code/i1itu32l
class ResultVisibility(IntEnum):
    PUBLIC = 0
    PUBLISHER = 1
    PARTNER = 2


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
        padding_size: the id spine combiner would pad each partner row to have this number of conversions.
                        This is required by MPC compute metrics to support multiple conversions per id while
                        at the same time maintaining privacy. It is currently only used when game_type=attribution
                        because the lift id spine combiner uses a hard-coded value of 25.
                        TODO T104391012: pass padding size to lift id spine combiner.

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

    result_visibility: ResultVisibility = ResultVisibility.PUBLIC

    def __post_init__(self) -> None:
        if self.num_pid_containers > self.num_mpc_containers:
            raise ValueError(
                f"num_pid_containers must be less than or equal to num_mpc_containers. Received num_pid_containers = {self.num_pid_containers} and num_mpc_containers = {self.num_mpc_containers}"
            )

    def get_instance_id(self) -> str:
        return self.instance_id

    @property
    def get_flow_cls_name(self) -> str:
        return self._stage_flow_cls_name

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
    def decoupled_attribution_stage_output_base_path(self) -> str:
        return self._get_stage_output_path("decoupled_attribution_stage", "json")

    @property
    def decoupled_aggregation_stage_output_base_path(self) -> str:
        return self._get_stage_output_path("decoupled_aggregation_stage", "json")

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
    def stage_flow(self):
        # type: () -> Type[PrivateComputationBaseStageFlow]
        from fbpcs.private_computation.stage_flows.private_computation_base_stage_flow import (
            PrivateComputationBaseStageFlow,
        )

        return PrivateComputationBaseStageFlow.cls_name_to_cls(
            self._stage_flow_cls_name
        )

    @property
    def current_stage(self) -> "PrivateComputationBaseStageFlow":
        return self.stage_flow.get_stage_from_status(self.status)

    def get_next_runnable_stage(self) -> Optional["PrivateComputationBaseStageFlow"]:
        """Returns the next runnable stage in the instance's stage flow

        * If the instance has a start status, return None
        * If the instance has a failed status, return the current stage in the flow
        * If the instance has a completed status, return the next stage in the flow (which could be None)
        """
        return self.stage_flow.get_next_runnable_stage_from_status(self.status)
