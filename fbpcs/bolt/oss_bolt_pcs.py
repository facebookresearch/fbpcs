#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

import asyncio
import logging
from dataclasses import dataclass, field
from time import time
from typing import Any, Dict, List, Optional, Type

from dataclasses_json import config, DataClassJsonMixin

from fbpcs.bolt.bolt_client import BoltClient, BoltState
from fbpcs.bolt.bolt_job import BoltCreateInstanceArgs
from fbpcs.bolt.constants import DEFAULT_ATTRIBUTION_STAGE_FLOW, DEFAULT_LIFT_STAGE_FLOW
from fbpcs.private_computation.entity.breakdown_key import BreakdownKey
from fbpcs.private_computation.entity.infra_config import (
    PrivateComputationGameType,
    PrivateComputationRole,
)
from fbpcs.private_computation.entity.pce_config import PCEConfig
from fbpcs.private_computation.entity.post_processing_data import PostProcessingData

from fbpcs.private_computation.entity.product_config import (
    AggregationType,
    AttributionRule,
    ResultVisibility,
)
from fbpcs.private_computation.service.errors import (
    PrivateComputationServiceValidationError,
)

from fbpcs.private_computation.service.private_computation import (
    PrivateComputationService,
)
from fbpcs.private_computation.stage_flows.private_computation_base_stage_flow import (
    PrivateComputationBaseStageFlow,
)


@dataclass
class BoltPCSCreateInstanceArgs(BoltCreateInstanceArgs, DataClassJsonMixin):
    instance_id: str
    role: PrivateComputationRole
    game_type: PrivateComputationGameType
    input_path: str
    output_dir: str = ""
    num_pid_containers: int = 1
    num_mpc_containers: int = 1
    stage_flow_cls: Type[PrivateComputationBaseStageFlow] = field(
        default=PrivateComputationBaseStageFlow,
        metadata={
            **config(
                # the enum will be represented as a list of its members, so we can
                # use the first enum member to get the class name
                encoder=lambda x: x[0].get_cls_name(),
                # if no value is provided in the yaml file, then the dataclass json
                # library will return the default stage flow. Otherwise, if it was
                # provided in the yaml file, we should decode the string.
                decoder=lambda x: x
                if x is PrivateComputationBaseStageFlow
                else PrivateComputationBaseStageFlow.cls_name_to_cls(x),
            )
        },
    )
    concurrency: Optional[int] = None
    attribution_rule: Optional[AttributionRule] = None
    aggregation_type: Optional[AggregationType] = AggregationType.MEASUREMENT
    num_files_per_mpc_container: Optional[int] = None
    breakdown_key: Optional[BreakdownKey] = None
    pce_config: Optional[PCEConfig] = None
    hmac_key: Optional[str] = None
    padding_size: Optional[int] = None
    k_anonymity_threshold: Optional[int] = None
    result_visibility: ResultVisibility = ResultVisibility.PUBLIC
    tier: Optional[str] = None
    pid_use_row_numbers: bool = True
    post_processing_data_optional: Optional[PostProcessingData] = None
    pid_configs: Optional[Dict[str, Any]] = None
    pcs_features: Optional[List[str]] = None
    run_id: Optional[str] = None

    def __post_init__(self) -> None:
        if self.stage_flow_cls is PrivateComputationBaseStageFlow:
            self.stage_flow_cls = (
                DEFAULT_ATTRIBUTION_STAGE_FLOW
                if self.game_type is PrivateComputationGameType.ATTRIBUTION
                else DEFAULT_LIFT_STAGE_FLOW
            )
        if not self.output_dir:
            self.output_dir = self.input_path[: self.input_path.rfind("/")]

    @classmethod
    def from_yml_dict(cls, yml_dict: Dict[str, Any]) -> "BoltPCSCreateInstanceArgs":
        if "instance_id" not in yml_dict:
            role = yml_dict["role"]
            role_prefix = "Publisher_" if role.upper() == "PUBLISHER" else "Partner_"
            yml_dict["instance_id"] = (
                role_prefix + yml_dict["job_name"] + f"_{int(time())}"
            )
        yml_dict["game_type"] = yml_dict["game_type"].upper()
        return cls.from_dict(yml_dict)


class BoltPCSClient(BoltClient):
    def __init__(
        self, pcs: PrivateComputationService, logger: Optional[logging.Logger] = None
    ) -> None:
        self.pcs = pcs
        self.logger: logging.Logger = (
            logging.getLogger(__name__) if logger is None else logger
        )

    async def create_instance(self, instance_args: BoltCreateInstanceArgs) -> str:
        assert isinstance(
            instance_args, BoltPCSCreateInstanceArgs
        )  # We will add generics later so that we can move the check to the type checker
        instance = self.pcs.create_instance(
            instance_id=instance_args.instance_id,
            role=instance_args.role,
            game_type=instance_args.game_type,
            input_path=instance_args.input_path,
            output_dir=instance_args.output_dir,
            num_pid_containers=instance_args.num_pid_containers,
            num_mpc_containers=instance_args.num_mpc_containers,
            concurrency=instance_args.concurrency,
            attribution_rule=instance_args.attribution_rule,
            aggregation_type=instance_args.aggregation_type,
            num_files_per_mpc_container=instance_args.num_files_per_mpc_container,
            breakdown_key=instance_args.breakdown_key,
            pce_config=instance_args.pce_config,
            hmac_key=instance_args.hmac_key,
            padding_size=instance_args.padding_size,
            k_anonymity_threshold=instance_args.k_anonymity_threshold,
            stage_flow_cls=instance_args.stage_flow_cls,
            result_visibility=instance_args.result_visibility,
            tier=instance_args.tier,
            pid_use_row_numbers=instance_args.pid_use_row_numbers,
            post_processing_data_optional=instance_args.post_processing_data_optional,
            pid_configs=instance_args.pid_configs,
            pcs_features=instance_args.pcs_features,
            run_id=instance_args.run_id,
        )
        return instance.infra_config.instance_id

    async def run_stage(
        self,
        instance_id: str,
        stage: Optional[PrivateComputationBaseStageFlow] = None,
        server_ips: Optional[List[str]] = None,
    ) -> None:
        if stage:
            self.logger.info(f"Running stage {stage.name}")
            await self.pcs.run_stage_async(
                instance_id=instance_id, stage=stage, server_ips=server_ips
            )
        else:
            self.logger.info("Running next stage")
            await self.pcs.run_next_async(
                instance_id=instance_id, server_ips=server_ips
            )

    async def update_instance(self, instance_id: str) -> BoltState:
        loop = asyncio.get_running_loop()
        pc_instance = await loop.run_in_executor(
            None, self.pcs.update_instance, instance_id
        )
        state = BoltState(
            pc_instance_status=pc_instance.infra_config.status,
            server_ips=pc_instance.server_ips,
        )
        return state

    async def validate_results(
        self, instance_id: str, expected_result_path: Optional[str] = None
    ) -> bool:
        # No expected result path in production, so we just move on
        if not expected_result_path:
            self.logger.info(
                "No expected result path was given, so result validation was skipped."
            )
            return True
        else:
            try:
                self.pcs.validate_metrics(
                    instance_id=instance_id, expected_result_path=expected_result_path
                )
            except PrivateComputationServiceValidationError:
                self.logger.info(
                    f"Validate results for instance {instance_id} are not as expected."
                )
                return False
            else:
                self.logger.info(
                    f"Validate results for instance {instance_id} are as expected."
                )
                return True

    async def cancel_current_stage(self, instance_id: str) -> None:
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, self.pcs.cancel_current_stage, instance_id)
