# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

from typing import Optional, TYPE_CHECKING

from fbpcs.pid.entity.pid_stages import UnionPIDStage
from fbpcs.private_computation.service.aggregate_shards_stage_service import (
    AggregateShardsStageService,
)
from fbpcs.private_computation.service.dummy_stage_service import DummyStageService
from fbpcs.private_computation.service.id_spine_combiner_stage_service import (
    IdSpineCombinerStageService,
)
from fbpcs.private_computation.service.pc_pre_validation_stage_service import (
    PCPreValidationStageService,
)
from fbpcs.private_computation.service.pid_stage_service import PIDStageService
from fbpcs.private_computation.service.post_processing_stage_service import (
    PostProcessingStageService,
)

from fbpcs.private_computation.service.private_computation_stage_service import (
    PrivateComputationStageService,
    PrivateComputationStageServiceArgs,
)
from fbpcs.private_computation.service.shard_stage_service import ShardStageService


if TYPE_CHECKING:
    from fbpcs.private_computation.stage_flows.private_computation_base_stage_flow import (
        PrivateComputationBaseStageFlow,
    )


class StageSelector:
    @classmethod
    def get_stage_service(
        cls,
        stage_flow: "PrivateComputationBaseStageFlow",
        args: PrivateComputationStageServiceArgs,
    ) -> Optional[PrivateComputationStageService]:
        if stage_flow.name == "CREATED":
            return DummyStageService()
        elif stage_flow.name == "INPUT_DATA_VALIDATION":
            return PCPreValidationStageService(
                args.pc_validator_config,
                args.onedocker_svc,
                args.onedocker_binary_config_map,
            )
        elif stage_flow.name == "PID_SHARD":
            return PIDStageService(
                args.pid_svc,
                UnionPIDStage.PUBLISHER_SHARD,
                UnionPIDStage.ADV_SHARD,
            )
        elif stage_flow.name == "PID_PREPARE":
            return PIDStageService(
                args.pid_svc,
                UnionPIDStage.PUBLISHER_PREPARE,
                UnionPIDStage.ADV_PREPARE,
            )
        elif stage_flow.name == "ID_MATCH":
            return PIDStageService(
                args.pid_svc,
                UnionPIDStage.PUBLISHER_RUN_PID,
                UnionPIDStage.ADV_RUN_PID,
            )
        elif stage_flow.name == "ID_MATCH_POST_PROCESS":
            return PostProcessingStageService(
                args.storage_svc, args.pid_post_processing_handlers
            )
        elif stage_flow.name == "ID_SPINE_COMBINER":
            return IdSpineCombinerStageService(
                args.onedocker_svc,
                args.onedocker_binary_config_map,
                pid_svc=args.pid_svc,
            )
        elif stage_flow.name == "RESHARD":
            return ShardStageService(
                args.onedocker_svc,
                args.onedocker_binary_config_map,
            )
        elif stage_flow.name == "AGGREGATE":
            return AggregateShardsStageService(
                args.onedocker_binary_config_map,
                args.mpc_svc,
            )
        elif stage_flow.name == "POST_PROCESSING_HANDLERS":
            return PostProcessingStageService(
                args.storage_svc, args.post_processing_handlers
            )
        else:
            return None
