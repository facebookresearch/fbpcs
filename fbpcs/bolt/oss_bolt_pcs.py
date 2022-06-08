#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

import logging
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Type

from fbpcs.bolt.bolt_job import BoltCreateInstanceArgs
from fbpcs.bolt.bolt_runner import BoltClient, BoltState
from fbpcs.private_computation.entity.breakdown_key import BreakdownKey
from fbpcs.private_computation.entity.pce_config import PCEConfig
from fbpcs.private_computation.entity.post_processing_data import PostProcessingData
from fbpcs.private_computation.entity.private_computation_instance import (
    AggregationType,
    AttributionRule,
    PrivateComputationGameType,
    PrivateComputationRole,
    ResultVisibility,
)

from fbpcs.private_computation.service.private_computation import (
    PrivateComputationService,
)
from fbpcs.private_computation.stage_flows.private_computation_base_stage_flow import (
    PrivateComputationBaseStageFlow,
)


@dataclass
class BoltPCSCreateInstanceArgs(BoltCreateInstanceArgs):
    instance_id: str
    role: PrivateComputationRole
    game_type: PrivateComputationGameType
    input_path: str
    output_dir: str
    num_pid_containers: int
    num_mpc_containers: int
    concurrency: Optional[int] = None
    attribution_rule: Optional[AttributionRule] = None
    aggregation_type: Optional[AggregationType] = None
    num_files_per_mpc_container: Optional[int] = None
    is_validating: Optional[bool] = False
    synthetic_shard_path: Optional[str] = None
    breakdown_key: Optional[BreakdownKey] = None
    pce_config: Optional[PCEConfig] = None
    is_test: Optional[bool] = False
    hmac_key: Optional[str] = None
    padding_size: Optional[int] = None
    k_anonymity_threshold: Optional[int] = None
    stage_flow_cls: Optional[Type[PrivateComputationBaseStageFlow]] = None
    result_visibility: ResultVisibility = ResultVisibility.PUBLIC
    tier: Optional[str] = None
    pid_use_row_numbers: bool = True
    post_processing_data_optional: Optional[PostProcessingData] = None
    pid_configs: Optional[Dict[str, Any]] = None


class BoltPCSClient(BoltClient):
    def __init__(self, pcs: PrivateComputationService) -> None:
        self.pcs = pcs
        self.logger: logging.Logger = logging.getLogger(__name__)

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
            is_validating=instance_args.is_validating,
            synthetic_shard_path=instance_args.synthetic_shard_path,
            breakdown_key=instance_args.breakdown_key,
            pce_config=instance_args.pce_config,
            is_test=instance_args.is_test,
            hmac_key=instance_args.hmac_key,
            padding_size=instance_args.padding_size,
            k_anonymity_threshold=instance_args.k_anonymity_threshold,
            stage_flow_cls=instance_args.stage_flow_cls,
            result_visibility=instance_args.result_visibility,
            tier=instance_args.tier,
            pid_use_row_numbers=instance_args.pid_use_row_numbers,
            post_processing_data_optional=instance_args.post_processing_data_optional,
            pid_configs=instance_args.pid_configs,
        )
        return instance.instance_id

    async def run_stage(
        self,
        instance_id: str,
        stage: PrivateComputationBaseStageFlow,
        server_ips: Optional[List[str]] = None,
    ) -> None:
        pass

    async def update_instance(self, instance_id: str) -> BoltState:
        raise NotImplementedError

    async def validate_results(
        self, instance_id: str, expected_result_path: Optional[str] = None
    ) -> bool:
        raise NotImplementedError
