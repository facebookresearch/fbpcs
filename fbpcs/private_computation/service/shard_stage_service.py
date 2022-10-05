#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict


import logging
import math
from typing import DefaultDict, List, Optional

from fbpcp.entity.container_instance import ContainerInstance

from fbpcp.service.onedocker import OneDockerService
from fbpcs.common.entity.stage_state_instance import StageStateInstance
from fbpcs.data_processing.service.sharding_service import ShardingService, ShardType
from fbpcs.onedocker_binary_config import (
    ONEDOCKER_REPOSITORY_PATH,
    OneDockerBinaryConfig,
)
from fbpcs.onedocker_binary_names import OneDockerBinaryNames
from fbpcs.private_computation.entity.pcs_feature import PCSFeature
from fbpcs.private_computation.entity.private_computation_instance import (
    PrivateComputationInstance,
    PrivateComputationInstanceStatus,
    PrivateComputationRole,
)
from fbpcs.private_computation.service.pid_utils import get_sharded_filepath
from fbpcs.private_computation.service.private_computation_stage_service import (
    PrivateComputationStageService,
)
from fbpcs.private_computation.service.utils import get_pc_status_from_stage_state


class ShardStageService(PrivateComputationStageService):
    """Handles business logic for the private computation resharding stage

    Private attributes:
        _onedocker_svc: Spins up containers that run binaries in the cloud
        _onedocker_binary_config_map: Stores a mapping from mpc game to OneDockerBinaryConfig (binary version and tmp directory)
    """

    def __init__(
        self,
        onedocker_svc: OneDockerService,
        onedocker_binary_config_map: DefaultDict[str, OneDockerBinaryConfig],
    ) -> None:
        self._onedocker_svc = onedocker_svc
        self._onedocker_binary_config_map = onedocker_binary_config_map
        self._logger: logging.Logger = logging.getLogger(__name__)

    async def run_async(
        self,
        pc_instance: PrivateComputationInstance,
        server_ips: Optional[List[str]] = None,
    ) -> PrivateComputationInstance:
        """Runs the private computation prepare data stage - shard stage

        Args:
            pc_instance: the private computation instance to run prepare data with
            server_ips: ignored

        Returns:
            An updated version of pc_instance
        """

        if pc_instance.has_feature(PCSFeature.PRIVATE_LIFT_UNIFIED_DATA_PROCESS):
            output_path = pc_instance.pcf2_lift_metadata_compaction_output_base_path
            combine_output_path = output_path + "_secret_shares"
            self._logger.info("Resharding on Metadata Compaction Stage Output")
        else:
            output_path = pc_instance.data_processing_output_path
            combine_output_path = output_path + "_combine"
            self._logger.info("Resharding on ID Spine Combiner Stage Output")

        self._logger.info(f"[{self}] Starting reshard service")

        # reshard each file into x shards
        #     note we need each file to be sharded into the same # of files
        #     because we want to keep the data of each existing file to run
        #     on the same container
        should_wait_spin_up: bool = (
            pc_instance.infra_config.role is PrivateComputationRole.PARTNER
        )
        container_instances = await self._start_sharder_service(
            pc_instance,
            self._onedocker_svc,
            self._onedocker_binary_config_map,
            combine_output_path=combine_output_path,
            shard_output_base_path=output_path,
            wait_for_containers_to_start_up=should_wait_spin_up,
        )
        self._logger.info("All sharding coroutines finished")

        stage_state = StageStateInstance(
            pc_instance.infra_config.instance_id,
            pc_instance.current_stage.name,
            containers=container_instances,
        )

        pc_instance.infra_config.instances.append(stage_state)
        return pc_instance

    def get_status(
        self,
        pc_instance: PrivateComputationInstance,
    ) -> PrivateComputationInstanceStatus:
        """Gets the latest PrivateComputationInstance status.

        Arguments:
            private_computation_instance: The PC instance that is being updated

        Returns:
            The latest status for private_computation_instance
        """
        return get_pc_status_from_stage_state(pc_instance, self._onedocker_svc)

    async def _start_sharder_service(
        self,
        private_computation_instance: PrivateComputationInstance,
        onedocker_svc: OneDockerService,
        onedocker_binary_config_map: DefaultDict[str, OneDockerBinaryConfig],
        combine_output_path: str,
        shard_output_base_path: str,
        wait_for_containers: bool = False,
        wait_for_containers_to_start_up: bool = True,
    ) -> List[ContainerInstance]:
        """Run combiner service and return those container instances

        Args:
            private_computation_instance: The PC instance to run sharder service with
            onedocker_svc: Spins up containers that run binaries in the cloud
            onedocker_binary_config_map: Stores a mapping from mpc game to OneDockerBinaryConfig (binary version and tmp directory)
            combine_output_path: out put path for the combine result
            wait_for_containers: block until containers to finish running, default False

        Returns:
            return: list of container instances running combiner service
        """
        sharder = ShardingService()
        logging.info("Instantiated sharder")

        args_list = []
        for shard_index in range(
            private_computation_instance.infra_config.num_pid_containers
        ):
            path_to_input_shard = get_sharded_filepath(combine_output_path, shard_index)
            logging.info(f"Input path to sharder: {path_to_input_shard}")

            shards_per_file = math.ceil(
                (
                    private_computation_instance.infra_config.num_mpc_containers
                    / private_computation_instance.infra_config.num_pid_containers
                )
                * private_computation_instance.infra_config.num_files_per_mpc_container
            )
            shard_index_offset = shard_index * shards_per_file
            logging.info(
                f"Output base path to sharder: {shard_output_base_path}, {shard_index_offset=}"
            )

            binary_config = onedocker_binary_config_map[
                OneDockerBinaryNames.SHARDER.value
            ]
            args_per_shard = sharder.build_args(
                filepath=path_to_input_shard,
                output_base_path=shard_output_base_path,
                file_start_index=shard_index_offset,
                num_output_files=shards_per_file,
                tmp_directory=binary_config.tmp_directory,
            )
            args_list.append(args_per_shard)

        binary_name = sharder.get_binary_name(ShardType.ROUND_ROBIN)
        env_vars = {}
        if binary_config.repository_path:
            env_vars[ONEDOCKER_REPOSITORY_PATH] = binary_config.repository_path

        return await sharder.start_containers(
            cmd_args_list=args_list,
            onedocker_svc=onedocker_svc,
            binary_version=binary_config.binary_version,
            binary_name=binary_name,
            timeout=None,
            wait_for_containers_to_finish=wait_for_containers,
            env_vars=env_vars,
            wait_for_containers_to_start_up=wait_for_containers_to_start_up,
            existing_containers=private_computation_instance.get_existing_containers_for_retry(),
        )
