#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

import os
import time
from dataclasses import dataclass
from enum import Enum, IntEnum
from typing import Any, Dict, List, Optional, TYPE_CHECKING, Union

if TYPE_CHECKING:
    from typing import Type

    from fbpcs.private_computation.stage_flows.private_computation_base_stage_flow import (
        PrivateComputationBaseStageFlow,
    )

from datetime import datetime, timezone
from logging import Logger

from fbpcp.entity.mpc_instance import MPCInstanceStatus
from fbpcs.common.entity.instance_base import InstanceBase
from fbpcs.common.entity.pcs_mpc_instance import PCSMPCInstance
from fbpcs.common.entity.stage_state_instance import (
    StageStateInstance,
    StageStateInstanceStatus,
)
from fbpcs.pid.entity.pid_instance import PIDInstance, PIDInstanceStatus
from fbpcs.pid.entity.pid_stages import UnionPIDStage
from fbpcs.pid.service.pid_service.pid_stage_mapper import STAGE_TO_FILE_FORMAT_MAP
from fbpcs.post_processing_handler.post_processing_instance import (
    PostProcessingInstance,
    PostProcessingInstanceStatus,
)
from fbpcs.private_computation.entity.breakdown_key import BreakdownKey
from fbpcs.private_computation.entity.infra_config import (
    InfraConfig,
    PrivateComputationRole,
)
from fbpcs.private_computation.entity.pce_config import PCEConfig
from fbpcs.private_computation.entity.post_processing_data import PostProcessingData
from fbpcs.private_computation.entity.private_computation_status import (
    PrivateComputationInstanceStatus,
)


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
    LAST_CLICK_1D_TARGETID = "last_click_1d_targetid"


class AggregationType(Enum):
    MEASUREMENT = "measurement"


# This is the visibility defined in https://fburl.com/code/i1itu32l
class ResultVisibility(IntEnum):
    PUBLIC = 0
    PUBLISHER = 1
    PARTNER = 2


UnionedPCInstance = Union[
    PIDInstance, PCSMPCInstance, PostProcessingInstance, StageStateInstance
]
UnionedPCInstanceStatus = Union[
    PIDInstanceStatus,
    MPCInstanceStatus,
    PostProcessingInstanceStatus,
    StageStateInstanceStatus,
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
        post_processing_data: fields to be sent to the post processing tier.

    Private attributes:
        _stage_flow_cls_name: the name of a PrivateComputationBaseStageFlow subclass (cls.__name__)
    """

    infra_config: InfraConfig

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
    tier: Optional[str] = None
    pid_configs: Optional[Dict[str, Any]] = None
    retry_counter: int = 0

    # TODO T98476320: make the following optional attributes non-optional. They are optional
    # because at the time the instance is created, pl might not provide any or all of them.
    hmac_key: Optional[str] = None
    padding_size: Optional[int] = None

    concurrency: int = 1  # used only by MPC compute metrics stage. TODO T102588568: rename to compute_metrics_concurrency
    k_anonymity_threshold: int = 0

    breakdown_key: Optional[BreakdownKey] = None
    pce_config: Optional[PCEConfig] = None

    # stored as a string because the enum was refusing to serialize to json, no matter what I tried.
    # TODO(T103299005): [BE] Figure out how to serialize StageFlow objects to json instead of using their class name
    _stage_flow_cls_name: str = "PrivateComputationStageFlow"

    result_visibility: ResultVisibility = ResultVisibility.PUBLIC

    # this is used by Private ID protocol to indicate whether we should
    # enable 'use-row-numbers' argument.
    pid_use_row_numbers: bool = True

    post_processing_data: Optional[PostProcessingData] = None

    creation_ts: int = 0
    end_ts: int = 0

    def __post_init__(self) -> None:
        if self.num_pid_containers > self.num_mpc_containers:
            raise ValueError(
                f"num_pid_containers must be less than or equal to num_mpc_containers. Received num_pid_containers = {self.num_pid_containers} and num_mpc_containers = {self.num_mpc_containers}"
            )
        if (
            self.game_type is PrivateComputationGameType.ATTRIBUTION
            and self.attribution_rule is None
        ):
            self.attribution_rule = AttributionRule.LAST_CLICK_1D

        if self.creation_ts == 0:
            self.creation_ts = int(time.time())

    def get_instance_id(self) -> str:
        return self.infra_config.instance_id

    @property
    def get_flow_cls_name(self) -> str:
        return self._stage_flow_cls_name

    @property
    def pid_stage_output_base_path(self) -> str:
        return self._get_stage_output_path("pid_stage", "csv")

    @property
    def pid_stage_output_prepare_path(self) -> str:
        suffix = (
            STAGE_TO_FILE_FORMAT_MAP[UnionPIDStage.PUBLISHER_PREPARE]
            if self.infra_config.role is PrivateComputationRole.PUBLISHER
            else STAGE_TO_FILE_FORMAT_MAP[UnionPIDStage.ADV_PREPARE]
        )

        return f"{self.pid_stage_output_base_path}{suffix}"

    @property
    def pid_stage_output_spine_path(self) -> str:
        spine_path_suffix = (
            STAGE_TO_FILE_FORMAT_MAP[UnionPIDStage.PUBLISHER_RUN_PID]
            if self.infra_config.role is PrivateComputationRole.PUBLISHER
            else STAGE_TO_FILE_FORMAT_MAP[UnionPIDStage.ADV_RUN_PID]
        )

        return f"{self.pid_stage_output_base_path}{spine_path_suffix}"

    @property
    def pid_stage_output_data_path(self) -> str:
        data_path_suffix = (
            STAGE_TO_FILE_FORMAT_MAP[UnionPIDStage.PUBLISHER_SHARD]
            if self.infra_config.role is PrivateComputationRole.PUBLISHER
            else STAGE_TO_FILE_FORMAT_MAP[UnionPIDStage.ADV_SHARD]
        )
        return f"{self.pid_stage_output_base_path}{data_path_suffix}"

    @property
    def pid_mr_stage_output_data_path(self) -> str:
        return os.path.join(
            self.output_dir,
            f"{self.infra_config.instance_id}_out_dir",
            "pid_mr",
        )

    @property
    def data_processing_output_path(self) -> str:
        return self._get_stage_output_path("data_processing_stage", "csv")

    @property
    def compute_stage_output_base_path(self) -> str:
        return self._get_stage_output_path("compute_stage", "json")

    @property
    def pcf2_lift_stage_output_base_path(self) -> str:
        return self._get_stage_output_path("pcf2_lift_stage", "json")

    @property
    def decoupled_attribution_stage_output_base_path(self) -> str:
        return self._get_stage_output_path("decoupled_attribution_stage", "json")

    @property
    def pcf2_attribution_stage_output_base_path(self) -> str:
        return self._get_stage_output_path("pcf2_attribution_stage", "json")

    @property
    def decoupled_aggregation_stage_output_base_path(self) -> str:
        return self._get_stage_output_path("decoupled_aggregation_stage", "json")

    @property
    def pcf2_aggregation_stage_output_base_path(self) -> str:
        return self._get_stage_output_path("pcf2_aggregation_stage", "json")

    @property
    def shard_aggregate_stage_output_path(self) -> str:
        return self._get_stage_output_path("shard_aggregation_stage", "json")

    def _get_stage_output_path(self, stage: str, extension_type: str) -> str:
        return os.path.join(
            self.output_dir,
            f"{self.infra_config.instance_id}_out_dir",
            stage,
            f"out.{extension_type}",
        )

    @property
    def stage_flow(self) -> "Type[PrivateComputationBaseStageFlow]":
        from fbpcs.private_computation.stage_flows.private_computation_base_stage_flow import (
            PrivateComputationBaseStageFlow,
        )

        return PrivateComputationBaseStageFlow.cls_name_to_cls(
            self._stage_flow_cls_name
        )

    @property
    def current_stage(self) -> "PrivateComputationBaseStageFlow":
        return self.stage_flow.get_stage_from_status(self.status)

    @property
    def elapsed_time(self) -> int:
        end_ts = self.end_ts or int(time.time())
        return end_ts - self.creation_ts

    def get_next_runnable_stage(self) -> Optional["PrivateComputationBaseStageFlow"]:
        """Returns the next runnable stage in the instance's stage flow

        * If the instance has a start status, return None
        * If the instance has a failed status, return the current stage in the flow
        * If the instance has a completed status, return the next stage in the flow (which could be None)
        """
        return self.stage_flow.get_next_runnable_stage_from_status(self.status)

    def is_stage_flow_completed(self) -> bool:
        return self.status is self.stage_flow.get_last_stage().completed_status

    def update_status(
        self, new_status: PrivateComputationInstanceStatus, logger: Logger
    ) -> None:
        old_status = self.status
        self.status = new_status
        if old_status is not new_status:
            self.status_update_ts = int(datetime.now(tz=timezone.utc).timestamp())
            logger.info(
                f"Updating status of {self.infra_config.instance_id} from {old_status} to {self.status} at time {self.status_update_ts}"
            )
        if self.is_stage_flow_completed():
            self.end_ts = int(time.time())

    @property
    def server_ips(self) -> List[str]:
        server_ips_list = []
        if not self.instances:
            return server_ips_list
        last_instance = self.instances[-1]
        if isinstance(last_instance, (PIDInstance, PCSMPCInstance, StageStateInstance)):
            server_ips_list = last_instance.server_ips or []
        return server_ips_list
