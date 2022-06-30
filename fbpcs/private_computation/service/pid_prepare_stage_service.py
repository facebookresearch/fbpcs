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
from fbpcs.data_processing.service.pid_prepare_binary_service import (
    PIDPrepareBinaryService,
)
from fbpcs.onedocker_binary_config import (
    ONEDOCKER_REPOSITORY_PATH,
    OneDockerBinaryConfig,
)
from fbpcs.pid.service.pid_service.pid_stage import PIDStage
from fbpcs.pid.service.pid_service.utils import (
    get_max_id_column_cnt,
    get_pid_protocol_from_num_shards,
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
    stop_stage_service,
)


class PIDPrepareStageService(PrivateComputationStageService):
    """Handles business logic for the PID prepare stage

    Private attributes:
        _storage_svc: used to read/write files during private computation runs
        _onedocker_svc: used to spin up containers that run binaries in the cloud
        _onedocker_binary_config: stores OneDocker information
        _multikey_enabled: use multiple columns for identifiers if true
        _containter_timeout: customed timeout for container
    """

    def __init__(
        self,
        storage_svc: StorageService,
        onedocker_svc: OneDockerService,
        onedocker_binary_config_map: DefaultDict[str, OneDockerBinaryConfig],
        multikey_enabled: bool = False,
        container_timeout: Optional[int] = DEFAULT_CONTAINER_TIMEOUT_IN_SEC,
    ) -> None:
        self._storage_svc = storage_svc
        self._onedocker_svc = onedocker_svc
        self._onedocker_binary_config_map = onedocker_binary_config_map
        self._multikey_enabled = multikey_enabled
        self._container_timeout = container_timeout
        self._logger: logging.Logger = logging.getLogger(__name__)

    async def run_async(
        self,
        pc_instance: PrivateComputationInstance,
        server_ips: Optional[List[str]] = None,
    ) -> PrivateComputationInstance:
        """Runs the PID prepare stage
        Args:
            pc_instance: the private computation instance to start pid prepare stage service
            server_ips: No need in this stage.
        Returns:
            An updated version of pc_instance
        """
        self._logger.info(f"[{self}] Starting PIDPrepareStageService")
        container_instances = await self.start_pid_prepare_service(
            pc_instance, server_ips
        )

        self._logger.info("PIDPrepareStageService finished")
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

    async def start_pid_prepare_service(
        self,
        pc_instance: PrivateComputationInstance,
        server_ips: Optional[List[str]],
    ) -> List[ContainerInstance]:
        """start pid prepare service and spine up the container instances"""
        logging.info("Instantiated PID prepare stage")
        num_shards = pc_instance.infra_config.num_pid_containers
        # input_path is the output_path from PID Shard Stage
        input_path = pc_instance.pid_stage_output_data_path
        output_path = pc_instance.pid_stage_output_prepare_path
        pc_role = pc_instance.infra_config.role
        # generate the list of command args for publisher or partner
        args_list = []
        # later mltikey_enabled, protocol, and max_col_cnt wil be centralized in PrivateComputationInstance.
        protocol = get_pid_protocol_from_num_shards(num_shards, self._multikey_enabled)
        binary_name = PIDPrepareBinaryService.get_binary_name()
        onedocker_binary_config = self._onedocker_binary_config_map[binary_name]
        for shard in range(num_shards):
            args_per_shard = PIDPrepareBinaryService.build_args(
                input_path=PIDStage.get_sharded_filepath(input_path, shard),
                output_path=PIDStage.get_sharded_filepath(output_path, shard),
                tmp_directory=onedocker_binary_config.tmp_directory,
                max_column_count=get_max_id_column_cnt(protocol),
            )
            args_list.append(args_per_shard)
        # start containers
        logging.info(f"{pc_role} spinning up containers")

        env_vars = {ONEDOCKER_REPOSITORY_PATH: onedocker_binary_config.repository_path}
        pid_prepare_binary_service = PIDPrepareBinaryService()
        return await pid_prepare_binary_service.start_containers(
            cmd_args_list=args_list,
            onedocker_svc=self._onedocker_svc,
            binary_version=onedocker_binary_config.binary_version,
            binary_name=binary_name,
            timeout=self._container_timeout,
            env_vars=env_vars,
        )

    def stop_service(
        self,
        pc_instance: PrivateComputationInstance,
    ) -> None:
        stop_stage_service(pc_instance, self._onedocker_svc)
