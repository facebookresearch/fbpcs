#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

import json
import logging
import os
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, TYPE_CHECKING, Union

import marshmallow
from dataclasses_json.mm import SchemaType

if TYPE_CHECKING:
    from typing import Type

    from fbpcs.private_computation.stage_flows.private_computation_base_stage_flow import (
        PrivateComputationBaseStageFlow,
    )

from pathlib import Path

from fbpcp.entity.container_instance import ContainerInstance
from fbpcs.common.entity.instance_base import InstanceBase
from fbpcs.common.entity.stage_state_instance import (
    StageStateInstance,
    StageStateInstanceStatus,
)
from fbpcs.pid.entity.pid_stages import STAGE_TO_FILE_FORMAT_MAP, UnionPIDStage
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
    PrivateIdDfcaConfig,
    ProductConfig,
)

UnionedPCInstanceStatus = Union[
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
            json.dumps(json_object["infra_config"]),
            unknown=marshmallow.utils.EXCLUDE,
            many=None,
        )

        # create product config
        product_config: ProductConfig
        product_cleaned_json = False
        product_json = json_object["product_config"]

        while not product_cleaned_json:
            # delete json contents which are not in config class
            # this makes old PCInstance still valid when deleting attributes
            try:
                product_config = cls._product_map(json_object).loads(
                    json.dumps(product_json),
                    unknown=marshmallow.utils.EXCLUDE,
                    many=None,
                )
                product_cleaned_json = True
            except marshmallow.exceptions.ValidationError as err:
                logging.warning(f"delete unknown contents: {err.args[0]}")
                cls._prune_json(product_json, err.args[0])

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
        elif json_object["infra_config"]["game_type"] == "PRIVATE_ID_DFCA":
            return PrivateIdDfcaConfig.schema()
        raise RuntimeError(f"Invalid product config: {json_object}")

    @classmethod
    def _prune_json(
        cls, product_json: Dict[str, Any], err_dict: Dict[str, Any]
    ) -> None:
        """
        This function will delete extra contents (which cannot be found in PCInstance) in product_json.
        We do this because extra fields can sometimes break the deserialization logic.
        """
        bottom: bool = False
        cannot_find_dict = err_dict

        while not bottom:
            for key, value in cannot_find_dict.items():
                if isinstance(value, dict):
                    product_json = product_json.get(key)
                    cannot_find_dict = value
                else:
                    product_json.pop(key)
                    bottom = True

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
    def pid_mr_stage_output_spine_path(self) -> str:
        # There are three kinds of paths:
        # *. unix path
        # *. windows path
        # *. cloud path
        # 1. create "pid_mr/matched_output" sub path
        sub_path = Path(
            "pid_mr",
            "matched_output",
        )
        # _get_stage_output_path() will output path by join path_prefix, sub_path and file_name
        # For example, “self.product_config.common.output_dir/f"{self.infra_config.instance_id}_out_dir”/pid_mr/matched_output/out.csv" is the base_path
        # “self.product_config.common.output_dir/f"{self.infra_config.instance_id}_out_dir” is prefix
        # sub_path is pid_mr/matched_output
        # file_name is out.csv
        base_path = self._get_stage_output_path(str(sub_path), "csv")
        # suffix is created according to the type of user
        # publisher suffix:  _publiser_mr_pid_matched_
        # partner suffix: _advertiser_mr_pid_matched_
        data_path_suffix = (
            STAGE_TO_FILE_FORMAT_MAP[UnionPIDStage.PUBLISHER_RUN_MR_PID]
            if self.infra_config.role is PrivateComputationRole.PUBLISHER
            else STAGE_TO_FILE_FORMAT_MAP[UnionPIDStage.ADV_RUN_MR_PID]
        )

        # concatenate base_path and suffix
        return f"{base_path}{data_path_suffix}"

    @property
    def data_processing_output_path(self) -> str:
        return self._get_stage_output_path("data_processing_stage", "csv")

    @property
    def compute_stage_output_base_path(self) -> str:
        return self._get_stage_output_path("compute_stage", "json")

    @property
    def pcf2_lift_metadata_compaction_output_base_path(self) -> str:
        return self._get_stage_output_path("metadata_compaction_stage", "csv")

    @property
    def secure_random_sharder_output_base_path(self) -> str:
        return self._get_stage_output_path("secure_random_sharder_stage", "csv")

    @property
    def pcf2_lift_stage_output_base_path(self) -> str:
        return self._get_stage_output_path("pcf2_lift_stage", "json")

    @property
    def pcf2_attribution_stage_output_base_path(self) -> str:
        return self._get_stage_output_path("pcf2_attribution_stage", "json")

    @property
    def pcf2_aggregation_stage_output_base_path(self) -> str:
        return self._get_stage_output_path("pcf2_aggregation_stage", "json")

    @property
    def shard_aggregate_stage_output_path(self) -> str:
        return self._get_stage_output_path("shard_aggregation_stage", "json")

    @property
    def private_id_dfca_aggregate_stage_output_path(self) -> str:
        return self._get_stage_output_path("private_id_dfca_aggregation_stage", "csv")

    def _get_stage_output_path(self, stage: str, extension_type: str) -> str:
        return os.path.join(
            self.product_config.common.output_dir,
            f"{self.infra_config.instance_id}_out_dir",
            stage,
            f"out.{extension_type}",
        )

    @property
    def stage_flow(self) -> "Type[PrivateComputationBaseStageFlow]":
        return self.infra_config.stage_flow

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
        return self.infra_config.is_stage_flow_completed()

    def update_status(
        self, new_status: PrivateComputationInstanceStatus, logger: logging.Logger
    ) -> None:
        old_status = self.infra_config.status
        if old_status is not new_status:
            self.infra_config.status = new_status
            logger.info(
                f"Updating status of {self.infra_config.instance_id} from {old_status} to {self.infra_config.status} at time {self.infra_config.status_update_ts}"
            )

    def get_status_elapsed_time(
        self,
        start_status: PrivateComputationInstanceStatus,
        end_status: PrivateComputationInstanceStatus,
    ) -> int:
        start_update_ts = end_update_ts = 0
        # reversed traverse from the last stage status
        for status_update in reversed(self.infra_config.status_updates):
            if start_status is status_update.status:
                start_update_ts = status_update.status_update_ts
            if end_status is status_update.status:
                end_update_ts = status_update.status_update_ts

        if start_update_ts and end_update_ts:
            return end_update_ts - start_update_ts

        return -1

    @property
    def server_ips(self) -> List[str]:
        server_ips_list = []
        if not self.infra_config.instances:
            return server_ips_list
        last_instance = self.infra_config.instances[-1]
        if isinstance(last_instance, StageStateInstance):
            server_ips_list = last_instance.server_ips or []
        return server_ips_list

    @property
    def server_uris(self) -> List[str]:
        server_uris_list = []
        if not self.infra_config.instances:
            return server_uris_list
        last_instance = self.infra_config.instances[-1]
        if isinstance(last_instance, StageStateInstance):
            server_uris_list = last_instance.server_uris or []
        return server_uris_list

    # TODO: T130501878 BE only support StageStateInstance for now, replace this to all self.infra_config.instances[-1] code
    def get_stage_instance(
        self, stage: Optional["PrivateComputationBaseStageFlow"] = None
    ) -> Optional[StageStateInstance]:
        if not self.infra_config.instances:
            return None

        stage = stage or self.current_stage
        # reversed traverse from the last stage instance
        for stage_instance in reversed(self.infra_config.instances):
            if isinstance(stage_instance, StageStateInstance):
                if stage.name == stage_instance.stage_name:
                    return stage_instance

        return None

    def get_existing_containers_for_retry(
        self,
    ) -> Optional[List[ContainerInstance]]:
        stage_instance = self.get_stage_instance()
        if self.infra_config.retry_counter == 0 or stage_instance is None:
            return None
        return self.containers

    @property
    def containers(self) -> Optional[List[ContainerInstance]]:
        instances = self.infra_config.instances
        if not instances:
            return None

        last_instance = instances[-1]
        if isinstance(last_instance, StageStateInstance):
            return last_instance.containers
        else:
            return None

    def has_feature(self, feature: PCSFeature) -> bool:
        if feature is PCSFeature.UNKNOWN:
            logging.warning(
                f"checking Unknown feature on instance {self.infra_config.instance_id}"
            )
            return False

        return feature in self.infra_config.pcs_features

    @property
    def feature_flags(self) -> Optional[str]:
        if self.infra_config.pcs_features:
            return ",".join(
                [feature.value for feature in self.infra_config.pcs_features]
            )

        return None
