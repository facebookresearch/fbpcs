#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

import asyncio
import json
import logging
from typing import DefaultDict, List, Optional

from fbpcp.entity.container_instance import ContainerInstance
from fbpcp.service.onedocker import OneDockerService
from fbpcp.service.storage import StorageService
from fbpcs.common.entity.stage_state_instance import StageStateInstance
from fbpcs.common.service.trace_logging_service import (
    CheckpointStatus,
    TraceLoggingService,
)
from fbpcs.data_processing.service.sharding_service import ShardingService, ShardType
from fbpcs.infra.certificate.certificate_provider import CertificateProvider
from fbpcs.infra.certificate.private_key import PrivateKeyReferenceProvider
from fbpcs.onedocker_binary_config import OneDockerBinaryConfig
from fbpcs.private_computation.entity.private_computation_instance import (
    PrivateComputationInstance,
    PrivateComputationInstanceStatus,
    PrivateComputationRole,
)
from fbpcs.private_computation.service.constants import DEFAULT_CONTAINER_TIMEOUT_IN_SEC
from fbpcs.private_computation.service.pid_utils import get_sharded_filepath
from fbpcs.private_computation.service.private_computation_stage_service import (
    PrivateComputationStageService,
)
from fbpcs.private_computation.service.utils import (
    generate_env_vars_dict,
    get_pc_status_from_stage_state,
    stop_stage_service,
)

PID_LOG_SUFIX = "_shardDistribution"


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
        trace_logging_svc: TraceLoggingService,
        container_timeout: Optional[int] = DEFAULT_CONTAINER_TIMEOUT_IN_SEC,
    ) -> None:
        self._storage_svc = storage_svc
        self._onedocker_svc = onedocker_svc
        self._onedocker_binary_config_map = onedocker_binary_config_map
        self._container_timeout = container_timeout
        self._logger: logging.Logger = logging.getLogger(__name__)
        self._trace_logging_svc = trace_logging_svc

    async def run_async(
        self,
        pc_instance: PrivateComputationInstance,
        server_certificate_provider: CertificateProvider,
        ca_certificate_provider: CertificateProvider,
        server_certificate_path: str,
        ca_certificate_path: str,
        server_ips: Optional[List[str]] = None,
        server_hostnames: Optional[List[str]] = None,
        server_private_key_ref_provider: Optional[PrivateKeyReferenceProvider] = None,
    ) -> PrivateComputationInstance:
        """Runs the PID shard stage service
        Args:
            pc_instance: the private computation instance to start pid shard stage service
            server_certificate_providder: ignored
            ca_certificate_provider: ignored
            server_certificate_path: ignored
            ca_certificate_path: ignored
            server_ips: No need in this stage.
            server_hostnames: ignored
            server_private_key_ref_provider: ignored
        Returns:
            An updated version of pc_instance
        """
        self._logger.info(f"[{self}] Starting PIDShardStageService")
        self._add_trace_logging(pc_instance, CheckpointStatus.STARTED, None)

        container_instances = await self.start_pid_shard_service(pc_instance)

        self._logger.info("PIDShardStageService finished")
        stage_state = StageStateInstance(
            pc_instance.infra_config.instance_id,
            pc_instance.current_stage.name,
            containers=container_instances,
        )
        pc_instance.infra_config.instances.append(stage_state)

        pid_shard_checkpoint_data = await self._prepare_checkpoint_data(pc_instance)
        self._add_trace_logging(
            pc_instance, CheckpointStatus.COMPLETED, pid_shard_checkpoint_data
        )

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
        env_vars = generate_env_vars_dict(
            repository_path=onedocker_binary_config.repository_path
        )
        should_wait_spin_up: bool = (
            pc_instance.infra_config.role is PrivateComputationRole.PARTNER
        )
        return await sharding_binary_service.start_containers(
            cmd_args_list=[args],
            onedocker_svc=self._onedocker_svc,
            binary_version=onedocker_binary_config.binary_version,
            binary_name=binary_name,
            timeout=self._container_timeout,
            env_vars=env_vars,
            wait_for_containers_to_start_up=should_wait_spin_up,
            existing_containers=pc_instance.get_existing_containers_for_retry(),
        )

    def stop_service(
        self,
        pc_instance: PrivateComputationInstance,
    ) -> None:
        stop_stage_service(pc_instance, self._onedocker_svc)

    def _add_trace_logging(self, pc_instance, status, checkpoint_data):
        try:
            self._trace_logging_svc.write_checkpoint(
                run_id=pc_instance.infra_config.run_id,
                instance_id=pc_instance.infra_config.instance_id,
                checkpoint_name=pc_instance.current_stage.name,
                status=status,
                checkpoint_data=checkpoint_data,
            )
        except Exception:
            self._logger.info("Failed to trace logging in PID Shard stage service.")

    async def _prepare_checkpoint_data(self, pc_instance):
        pid_shard_checkpoint_data = {}
        data_path = pc_instance.pid_stage_output_data_path
        pid_shard_info_path = f"{get_sharded_filepath(data_path, 0)}" + PID_LOG_SUFIX
        try:
            if not self._storage_svc.file_exists(pid_shard_info_path):
                error_msg = f"PID shard metrics for {pc_instance.infra_config.instance_id=} is empty"
                self._add_trace_logging(
                    pc_instance, CheckpointStatus.FAILED, {"error": error_msg}
                )

            loop = asyncio.get_running_loop()
            pid_shard_info_json_str = await loop.run_in_executor(
                None, self._storage_svc.read, pid_shard_info_path
            )
            pid_shard_metrics = json.loads(pid_shard_info_json_str)

            num_id_cols, rows_sharded = 0, 0
            for k, v in pid_shard_metrics.items():
                if k == "num_ids":
                    num_id_cols = v
                else:
                    rows_sharded += v
            pid_shard_checkpoint_data["num_id_cols"] = str(num_id_cols)
            pid_shard_checkpoint_data["rows_sharded"] = str(rows_sharded)

        except Exception:
            self._logger.info(
                f"Failed to add trace logging from file {pid_shard_info_path}."
            )
        return pid_shard_checkpoint_data
