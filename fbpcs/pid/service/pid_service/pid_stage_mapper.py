#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

from typing import DefaultDict, Dict, List, Optional

from fbpcp.service.onedocker import OneDockerService
from fbpcp.service.storage import StorageService
from fbpcs.onedocker_binary_config import OneDockerBinaryConfig
from fbpcs.onedocker_binary_names import OneDockerBinaryNames
from fbpcs.pid.entity.pid_instance import PIDProtocol
from fbpcs.pid.entity.pid_stages import UnionPIDStage
from fbpcs.pid.repository.pid_instance import PIDInstanceRepository
from fbpcs.pid.service.pid_service.pid_prepare_stage import PIDPrepareStage
from fbpcs.pid.service.pid_service.pid_run_protocol_stage import PIDProtocolRunStage
from fbpcs.pid.service.pid_service.pid_shard_stage import PIDShardStage
from fbpcs.pid.service.pid_service.pid_stage import PIDStage
from fbpcs.pid.service.pid_service.pid_stage_input import PIDStageInput


STAGE_TO_FILE_FORMAT_MAP: Dict[UnionPIDStage, str] = {
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
        instance_repository: PIDInstanceRepository,
        storage_svc: StorageService,
        onedocker_svc: OneDockerService,
        protocol: PIDProtocol,
        onedocker_binary_config_map: DefaultDict[str, OneDockerBinaryConfig],
        server_ips: Optional[List[str]] = None,
        use_row_numbers: bool = False,
    ) -> PIDStage:
        if stage is UnionPIDStage.PUBLISHER_SHARD:
            return PIDShardStage(
                stage,
                instance_repository,
                storage_svc,
                onedocker_svc,
                onedocker_binary_config_map[
                    OneDockerBinaryNames.SHARDER_HASHED_FOR_PID.value
                ],
                protocol,
            )
        elif stage is UnionPIDStage.PUBLISHER_PREPARE:
            return PIDPrepareStage(
                stage,
                instance_repository,
                storage_svc,
                onedocker_svc,
                onedocker_binary_config_map[
                    OneDockerBinaryNames.UNION_PID_PREPARER.value
                ],
                protocol,
            )
        elif stage is UnionPIDStage.PUBLISHER_RUN_PID:
            binary_name = OneDockerBinaryNames.PID_SERVER.value
            if protocol == PIDProtocol.UNION_PID_MULTIKEY:
                binary_name = OneDockerBinaryNames.PID_MULTI_KEY_SERVER.value
            return PIDProtocolRunStage(
                stage,
                instance_repository,
                storage_svc,
                onedocker_svc,
                onedocker_binary_config_map[binary_name],
                protocol,
                server_ips,
            )
        elif stage is UnionPIDStage.ADV_SHARD:
            return PIDShardStage(
                stage,
                instance_repository,
                storage_svc,
                onedocker_svc,
                onedocker_binary_config_map[
                    OneDockerBinaryNames.SHARDER_HASHED_FOR_PID.value
                ],
                protocol,
            )
        elif stage is UnionPIDStage.ADV_PREPARE:
            return PIDPrepareStage(
                stage,
                instance_repository,
                storage_svc,
                onedocker_svc,
                onedocker_binary_config_map[
                    OneDockerBinaryNames.UNION_PID_PREPARER.value
                ],
                protocol,
            )
        elif stage is UnionPIDStage.ADV_RUN_PID:
            binary_name = OneDockerBinaryNames.PID_CLIENT.value
            if protocol == PIDProtocol.UNION_PID_MULTIKEY:
                binary_name = OneDockerBinaryNames.PID_MULTI_KEY_CLIENT.value
            return PIDProtocolRunStage(
                stage,
                instance_repository,
                storage_svc,
                onedocker_svc,
                onedocker_binary_config_map[binary_name],
                protocol,
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
        is_validating: Optional[bool] = False,
        synthetic_shard_path: Optional[str] = None,
        hmac_key: Optional[str] = None,
        pid_use_row_numbers: bool = False,
    ) -> PIDStageInput:
        try:
            return PIDStageInput(
                input_paths=[],
                output_paths=[f"{output_path}{STAGE_TO_FILE_FORMAT_MAP[stage]}"],
                num_shards=num_shards,
                instance_id=run_id,
                is_validating=is_validating,
                synthetic_shard_path=synthetic_shard_path,
                hmac_key=hmac_key,
                pid_use_row_numbers=pid_use_row_numbers,
            )
        except KeyError:
            raise ValueError(
                f"Stage {stage} doesn't have an output file format specified. Please double check your flow"
            )
