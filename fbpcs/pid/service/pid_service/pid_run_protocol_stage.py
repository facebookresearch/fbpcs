#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

import logging
from typing import Dict, List, Optional

from fbpcp.service.onedocker import OneDockerService
from fbpcp.service.storage import StorageService
from fbpcp.util.typing import checked_cast
from fbpcs.onedocker_binary_config import OneDockerBinaryConfig
from fbpcs.onedocker_binary_names import OneDockerBinaryNames
from fbpcs.pid.entity.pid_instance import PIDStageStatus
from fbpcs.pid.entity.pid_stages import UnionPIDStage
from fbpcs.pid.repository.pid_instance import PIDInstanceRepository
from fbpcs.pid.service.pid_service.pid_stage import PIDStage
from fbpcs.pid.service.pid_service.pid_stage_input import PIDStageInput
from fbpcs.private_computation.service.run_binary_base_service import (
    RunBinaryBaseService,
)


IP_ADDRS_COORD_OBJECT = "pid_ip_addrs"
SLEEP_UPDATE_SECONDS = 10
DEFAULT_CONTAINER_TIMEOUT_IN_SEC = 43200


class PIDProtocolRunStage(PIDStage):
    def __init__(
        self,
        stage: UnionPIDStage,
        instance_repository: PIDInstanceRepository,
        storage_svc: StorageService,
        onedocker_svc: OneDockerService,
        onedocker_binary_config: OneDockerBinaryConfig,
        server_ips: Optional[List[str]] = None,
    ) -> None:
        super().__init__(
            stage=stage,
            instance_repository=instance_repository,
            storage_svc=storage_svc,
            onedocker_svc=onedocker_svc,
            onedocker_binary_config=onedocker_binary_config,
            is_joint_stage=True,
        )

        self.server_ips = server_ips
        self.logger: logging.Logger = logging.getLogger(__name__)

    async def run(
        self,
        stage_input: PIDStageInput,
        container_timeout: Optional[int] = None,
        wait_for_containers: bool = True,
    ) -> PIDStageStatus:
        self.logger.info(f"[{self}] Called run")
        instance_id = stage_input.instance_id
        timeout = container_timeout or DEFAULT_CONTAINER_TIMEOUT_IN_SEC
        # Make sure status is READY before proceed
        status = await self._ready(stage_input)
        await self.update_instance_status(instance_id=instance_id, status=status)
        if status is not PIDStageStatus.READY:
            return status

        # Some invariant checking on the input and output paths
        input_paths = stage_input.input_paths
        output_paths = stage_input.output_paths
        num_shards = (
            stage_input.num_shards + 1
            if stage_input.is_validating
            else stage_input.num_shards
        )
        if len(input_paths) != 1:
            raise ValueError(f"Expected 1 input path, not {len(input_paths)}")
        if len(output_paths) != 1:
            raise ValueError(f"Expected 1 output path, not {len(output_paths)}")

        await self.update_instance_status(
            instance_id=instance_id, status=PIDStageStatus.STARTED
        )
        if stage_input.pid_use_row_numbers:
            self.logger.info("use-row-numbers is enabled for Private ID")
        if self.stage_type is UnionPIDStage.PUBLISHER_RUN_PID:
            # Run publisher commands in container
            self.logger.info("Publisher spinning up containers")
            try:
                pending_containers = self.onedocker_svc.start_containers(
                    package_name=OneDockerBinaryNames.PID_SERVER.value,
                    version=self.onedocker_binary_config.binary_version,
                    cmd_args_list=self._gen_command_args_list(
                        input_path=input_paths[0],
                        output_path=output_paths[0],
                        num_shards=num_shards,
                        use_row_numbers=stage_input.pid_use_row_numbers,
                    ),
                    env_vars=self._gen_env_vars(),
                    timeout=timeout,
                )

                containers = await self.onedocker_svc.wait_for_pending_containers(
                    [container.instance_id for container in pending_containers]
                )
            except Exception as e:
                status = PIDStageStatus.FAILED
                await self.update_instance_status(
                    instance_id=instance_id, status=status
                )
                self.logger.exception(f"Failed to spin up containers: {e}")
                return status

            # Write containers information to PID instance repository
            await self.update_instance_containers(
                instance_id=instance_id, containers=containers
            )

            # Get ips from containers and write them to pid instance repository
            self.logger.info("Storing servers' IPs")
            ip_addresses = [
                checked_cast(str, container.ip_address) for container in containers
            ]
            await self.put_server_ips(instance_id=instance_id, server_ips=ip_addresses)

            # Wait until the containers are finished
            if wait_for_containers:
                self.logger.info("Waiting for containers to finish")
                containers = await RunBinaryBaseService.wait_for_containers_async(
                    self.onedocker_svc, containers, SLEEP_UPDATE_SECONDS
                )
                await self.update_instance_containers(
                    instance_id=instance_id, containers=containers
                )
            status = self.get_stage_status_from_containers(containers)
        elif self.stage_type is UnionPIDStage.ADV_RUN_PID:
            server_ips = self.server_ips or []
            if not server_ips:
                self.logger.error("Missing server_ips")
                status = PIDStageStatus.FAILED
                await self.update_instance_status(
                    instance_id=instance_id, status=status
                )
                return status

            hostnames = [f"http://{ip}" for ip in server_ips]

            # Run partner commands in container
            self.logger.info("Partner spinning up containers")
            try:
                pending_containers = self.onedocker_svc.start_containers(
                    package_name=OneDockerBinaryNames.PID_CLIENT.value,
                    version=self.onedocker_binary_config.binary_version,
                    cmd_args_list=self._gen_command_args_list(
                        input_path=input_paths[0],
                        output_path=output_paths[0],
                        num_shards=num_shards,
                        server_hostnames=hostnames,
                        use_row_numbers=stage_input.pid_use_row_numbers,
                    ),
                    env_vars=self._gen_env_vars(),
                    timeout=timeout,
                )

                containers = await self.onedocker_svc.wait_for_pending_containers(
                    [container.instance_id for container in pending_containers]
                )
            except Exception as e:
                status = PIDStageStatus.FAILED
                await self.update_instance_status(
                    instance_id=instance_id, status=status
                )
                self.logger.exception(f"Failed to spin up containers: {e}")
                return status

            # Write containers information to PID instance repository
            await self.update_instance_containers(
                instance_id=instance_id, containers=containers
            )

            if wait_for_containers:
                # Wait until the containers are finished
                self.logger.info("Waiting for containers to finish")
                containers = await RunBinaryBaseService.wait_for_containers_async(
                    self.onedocker_svc, containers, SLEEP_UPDATE_SECONDS
                )
                await self.update_instance_containers(
                    instance_id=instance_id, containers=containers
                )
            status = self.get_stage_status_from_containers(containers)

        self.logger.info(f"PID Run protocol status: {status}")
        await self.update_instance_status(instance_id=instance_id, status=status)
        return status

    def _gen_command_args_list(
        self,
        input_path: str,
        output_path: str,
        num_shards: int,
        use_row_numbers: bool,
        server_hostnames: Optional[List[str]] = None,
        port: int = 15200,
    ) -> List[str]:
        # partner
        if server_hostnames:
            if len(server_hostnames) != num_shards:
                raise ValueError(
                    f"Supplied {len(server_hostnames)} server_hostnames, but num_shards == {num_shards} (these should agree)"
                )
            return [
                self._gen_command_args(
                    input_path=self.get_sharded_filepath(input_path, i),
                    output_path=self.get_sharded_filepath(output_path, i),
                    port=port,
                    server_hostname=server_hostnames[i],
                    use_row_numbers=use_row_numbers,
                )
                for i in range(num_shards)
            ]
        # publisher
        else:
            return [
                self._gen_command_args(
                    input_path=self.get_sharded_filepath(input_path, i),
                    output_path=self.get_sharded_filepath(output_path, i),
                    metric_path=self.get_metrics_filepath(output_path, i),
                    port=port,
                    server_hostname=None,
                    use_row_numbers=use_row_numbers,
                )
                for i in range(num_shards)
            ]

    def _gen_command_args(
        self,
        input_path: str,
        output_path: str,
        port: int,
        server_hostname: Optional[str] = None,
        use_row_numbers: bool = False,
        metric_path: Optional[str] = None,
    ) -> str:
        if server_hostname:
            return " ".join(
                [
                    f"--company {server_hostname}:{port}",
                    f"--input {input_path}",
                    f"--output {output_path}",
                    f"--metric-path {metric_path}" if metric_path is not None else "",
                    "--no-tls",
                    "--use-row-numbers" if use_row_numbers else "",
                ]
            )
        else:
            return " ".join(
                [
                    f"--host 0.0.0.0:{port}",
                    f"--input {input_path}",
                    f"--output {output_path}",
                    f"--metric-path {metric_path}" if metric_path is not None else "",
                    "--no-tls",
                    "--use-row-numbers" if use_row_numbers else "",
                ]
            )

    def _gen_env_vars(self) -> Dict[str, str]:
        env_vars = {
            "RUST_LOG": "info",
            "ONEDOCKER_REPOSITORY_PATH": self.onedocker_binary_config.repository_path,
        }
        return env_vars
