#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

import os
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, TYPE_CHECKING, Union

from dataclasses_json.mm import SchemaType

if TYPE_CHECKING:
    from typing import Type

    from fbpcs.private_computation.stage_flows.private_computation_base_stage_flow import (
        PrivateComputationBaseStageFlow,
    )

import json
import logging
from datetime import datetime, timezone

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
    PostProcessingInstanceStatus,
)
from fbpcs.private_computation.entity.infra_config import (
    InfraConfig,
    PrivateComputationRole,
)
from fbpcs.private_computation.entity.pcs_feature import PCSFeature
from fbpcs.private_computation.entity.private_computation_status import (
    PrivateComputationInstanceStatus,
)
from fbpcs.private_computation.entity.product_config import (
    AttributionConfig,
    LiftConfig,
    ProductConfig,
)

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

    """

    infra_config: InfraConfig
    product_config: ProductConfig

    def dumps_schema(self) -> str:
        json_object = json.loads(super().dumps_schema())

        # this is a helper field used in InstanceBase setter
        json_object.pop("initialized", None)

        json_object["product_config"] = json.loads(
            self.product_config.__class__.schema().dumps(self.product_config)
        )
        return json.dumps(json_object)

    @classmethod
    def loads_schema(cls, json_schema_str: str) -> "PrivateComputationInstance":
        json_object = json.loads(json_schema_str)

        # create infra config
        infra_config: InfraConfig = InfraConfig.schema().loads(
            json.dumps(json_object["infra_config"]), many=None
        )

        # create product config
        product_config: ProductConfig = cls._product_map(json_object).loads(
            json.dumps(json_object["product_config"]), many=None
        )

        return PrivateComputationInstance(
            infra_config=infra_config, product_config=product_config
        )

    @classmethod
    def _product_map(cls, json_object: Dict[str, Any]) -> SchemaType:
        """
        return the corresponding SchemaType object based on the product_config type
        """
        if json_object["infra_config"]["game_type"] == "ATTRIBUTION":
            return AttributionConfig.schema()
        elif json_object["infra_config"]["game_type"] == "LIFT":
            return LiftConfig.schema()
        raise RuntimeError(f"Invalid product config: {json_object}")

    def get_instance_id(self) -> str:
        return self.infra_config.instance_id

    @property
    def get_flow_cls_name(self) -> str:
        return self.infra_config._stage_flow_cls_name

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
            self.product_config.common.output_dir,
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
            self.product_config.common.output_dir,
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
            self.infra_config._stage_flow_cls_name
        )

    @property
    def current_stage(self) -> "PrivateComputationBaseStageFlow":
        return self.stage_flow.get_stage_from_status(self.infra_config.status)

    @property
    def elapsed_time(self) -> int:
        end_ts = self.infra_config.end_ts or int(time.time())
        return end_ts - self.infra_config.creation_ts

    def get_next_runnable_stage(self) -> Optional["PrivateComputationBaseStageFlow"]:
        """Returns the next runnable stage in the instance's stage flow

        * If the instance has a start status, return None
        * If the instance has a failed status, return the current stage in the flow
        * If the instance has a completed status, return the next stage in the flow (which could be None)
        """
        return self.stage_flow.get_next_runnable_stage_from_status(
            self.infra_config.status
        )

    def is_stage_flow_completed(self) -> bool:
        return (
            self.infra_config.status
            is self.stage_flow.get_last_stage().completed_status
        )

    def update_status(
        self, new_status: PrivateComputationInstanceStatus, logger: logging.Logger
    ) -> None:
        old_status = self.infra_config.status
        self.infra_config.status = new_status
        if old_status is not new_status:
            self.infra_config.status_update_ts = int(
                datetime.now(tz=timezone.utc).timestamp()
            )
            logger.info(
                f"Updating status of {self.infra_config.instance_id} from {old_status} to {self.infra_config.status} at time {self.infra_config.status_update_ts}"
            )
        if self.is_stage_flow_completed():
            self.infra_config.end_ts = int(time.time())

    @property
    def server_ips(self) -> List[str]:
        server_ips_list = []
        if not self.infra_config.instances:
            return server_ips_list
        last_instance = self.infra_config.instances[-1]
        if isinstance(last_instance, (PIDInstance, PCSMPCInstance, StageStateInstance)):
            server_ips_list = last_instance.server_ips or []
        return server_ips_list

    def has_feature(self, feature: PCSFeature) -> bool:
        if feature is PCSFeature.UNKNOWN:
            logging.warning(
                f"checking Unknown feature on instance {self.infra_config.instance_id}"
            )
            return False

        return feature in self.infra_config.pcs_features
