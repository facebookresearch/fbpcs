#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

import logging
from typing import DefaultDict, List, Optional

from fbpcp.entity.container_instance import ContainerInstance

from fbpcp.service.onedocker import OneDockerService
from fbpcp.service.storage import StorageService
from fbpcs.common.entity.stage_state_instance import StageStateInstance
from fbpcs.data_processing.service.sharding_service import ShardingService, ShardType
from fbpcs.onedocker_binary_config import (
    ONEDOCKER_REPOSITORY_PATH,
    OneDockerBinaryConfig,
)
from fbpcs.private_computation.entity.private_computation_instance import (
    PrivateComputationInstance,
    PrivateComputationInstanceStatus,
)
from fbpcs.private_computation.service.private_computation_stage_service import (
    PrivateComputationStageService,
)
from fbpcs.private_computation.service.utils import (
    DEFAULT_CONTAINER_TIMEOUT_IN_SEC,
    get_pc_status_from_stage_state,
)


class PIDShardStageService(PrivateComputationStageService):
    """Handles business logic for the PID_SHARD stage

    Private attributes:
        _storage_svc: used to read/write files during private computation runs
        _onedocker_svc: used to spin up containers that run binaries in the cloud
        _onedocker_binary_config: stores OneDocker information
        _containter_timeout: customed timeout for container
    """

    def __init__(
        self,
        storage_svc: StorageService,
        onedocker_svc: OneDockerService,
        onedocker_binary_config_map: DefaultDict[str, OneDockerBinaryConfig],
        container_timeout: Optional[int] = DEFAULT_CONTAINER_TIMEOUT_IN_SEC,
    ) -> None:
        self._storage_svc = storage_svc
        self._onedocker_svc = onedocker_svc
        self._onedocker_binary_config_map = onedocker_binary_config_map
        self._container_timeout = container_timeout
        self._logger: logging.Logger = logging.getLogger(__name__)

    async def run_async(
        self,
        pc_instance: PrivateComputationInstance,
        server_ips: Optional[List[str]] = None,
    ) -> PrivateComputationInstance:
        """Runs the PID shard stage service
        Args:
            pc_instance: the private computation instance to start pid shard stage service
            server_ips: No need in this stage.
        Returns:
            An updated version of pc_instance
        """
        self._logger.info(f"[{self}] Starting PIDShardStageService")
        container_instances = await self.start_pid_shard_service(pc_instance)

        self._logger.info("PIDShardStageService finished")
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
            pc_instance: The private computation instance that is being updated

        Returns:
            The latest status for private computation instance
        """
        return get_pc_status_from_stage_state(pc_instance, self._onedocker_svc)

    async def start_pid_shard_service(
        self,
        pc_instance: PrivateComputationInstance,
    ) -> List[ContainerInstance]:
        """start pid shard service and spine up the container instances"""
        logging.info("Instantiated PID shard stage")
        num_shards = pc_instance.infra_config.num_pid_containers
        input_path = pc_instance.product_config.common.input_path
        output_base_path = pc_instance.pid_stage_output_data_path
        pc_role = pc_instance.infra_config.role
        sharding_binary_service = ShardingService()
        # generate the list of command args for publisher or partner
        binary_name = ShardingService.get_binary_name(ShardType.HASHED_FOR_PID)
        onedocker_binary_config = self._onedocker_binary_config_map[binary_name]
        args = ShardingService.build_args(
            filepath=input_path,
            output_base_path=output_base_path,
            file_start_index=0,
            num_output_files=num_shards,
            tmp_directory=onedocker_binary_config.tmp_directory,
            hmac_key=pc_instance.product_config.common.hmac_key,
        )
        # start containers
        logging.info(f"{pc_role} spinning up containers")
        env_vars = {ONEDOCKER_REPOSITORY_PATH: onedocker_binary_config.repository_path}
        return await sharding_binary_service.start_containers(
            cmd_args_list=[args],
            onedocker_svc=self._onedocker_svc,
            binary_version=onedocker_binary_config.binary_version,
            binary_name=binary_name,
            timeout=self._container_timeout,
            env_vars=env_vars,
        )
