#!/usr/bin/env python3
# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

from typing import Any, Dict, List, Optional

from fbpcs.service.onedocker import OneDockerService
from fbpcs.service.storage import StorageService
from fbpmp.onedocker_binary_config import OneDockerBinaryConfig
from fbpmp.pid.entity.pid_stages import UnionPIDStage
from fbpmp.pid.repository.pid_instance import PIDInstanceRepository
from fbpmp.pid.service.pid_service.pid_prepare_stage import PIDPrepareStage
from fbpmp.pid.service.pid_service.pid_run_protocol_stage import PIDProtocolRunStage
from fbpmp.pid.service.pid_service.pid_shard_stage import PIDShardStage
from fbpmp.pid.service.pid_service.pid_stage import PIDStage
from fbpmp.pid.service.pid_service.pid_stage_input import PIDStageInput


STAGE_TO_FILE_FORMAT_MAP = {
    UnionPIDStage.PUBLISHER_SHARD: "_publisher_sharded",
    UnionPIDStage.PUBLISHER_PREPARE: "_publisher_prepared",
    UnionPIDStage.PUBLISHER_RUN_PID: "_publisher_pid_matched",
    UnionPIDStage.ADV_SHARD: "_advertiser_sharded",
    UnionPIDStage.ADV_PREPARE: "_advertiser_prepared",
    UnionPIDStage.ADV_RUN_PID: "_advertiser_pid_matched",
}

# Stage mapper
class PIDStageMapper:
    @staticmethod
    def get_stage(
        stage: UnionPIDStage,
        config: Dict[str, Any],
        instance_repository: PIDInstanceRepository,
        storage_svc: StorageService,
        onedocker_svc: OneDockerService,
        onedocker_binary_config: OneDockerBinaryConfig,
        server_ips: Optional[List[str]] = None,
    ) -> PIDStage:
        if stage is UnionPIDStage.PUBLISHER_SHARD:
            return PIDShardStage(
                stage,
                config,
                instance_repository,
                storage_svc,
                onedocker_svc,
                onedocker_binary_config,
            )
        elif stage is UnionPIDStage.PUBLISHER_PREPARE:
            return PIDPrepareStage(
                stage,
                config,
                instance_repository,
                storage_svc,
                onedocker_svc,
                onedocker_binary_config,
            )
        elif stage is UnionPIDStage.PUBLISHER_RUN_PID:
            return PIDProtocolRunStage(
                stage,
                config,
                instance_repository,
                storage_svc,
                onedocker_svc,
                onedocker_binary_config,
                server_ips,
            )
        elif stage is UnionPIDStage.ADV_SHARD:
            return PIDShardStage(
                stage,
                config,
                instance_repository,
                storage_svc,
                onedocker_svc,
                onedocker_binary_config,
            )
        elif stage is UnionPIDStage.ADV_PREPARE:
            return PIDPrepareStage(
                stage,
                config,
                instance_repository,
                storage_svc,
                onedocker_svc,
                onedocker_binary_config,
            )
        elif stage is UnionPIDStage.ADV_RUN_PID:
            return PIDProtocolRunStage(
                stage,
                config,
                instance_repository,
                storage_svc,
                onedocker_svc,
                onedocker_binary_config,
                server_ips,
            )
        else:
            raise ValueError("The stage you want is not supported")

    @staticmethod
    def get_input_for_stage(
        stage: UnionPIDStage,
        input_path: str,
        output_path: str,
        num_shards: int,
        run_id: str,
        fail_fast: bool = False,
        is_validating: Optional[bool] = False,
        synthetic_shard_path: Optional[str] = None,
        hmac_key: Optional[str] = None,
    ) -> PIDStageInput:
        try:
            return PIDStageInput(
                input_paths=[],
                output_paths=[f"{output_path}{STAGE_TO_FILE_FORMAT_MAP[stage]}"],
                num_shards=num_shards,
                instance_id=run_id,
                fail_fast=fail_fast,
                is_validating=is_validating,
                synthetic_shard_path=synthetic_shard_path,
                hmac_key=hmac_key,
            )
        except KeyError:
            raise ValueError(
                f"Stage {stage} doesn't have an output file format specified. Please double check your flow"
            )
