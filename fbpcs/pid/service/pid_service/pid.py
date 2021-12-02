#!/usr/bin/env python3
# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

import logging
from typing import Any, DefaultDict, Dict, List, Optional

from fbpcp.service.onedocker import OneDockerService
from fbpcp.service.storage import StorageService
from fbpcs.onedocker_binary_config import OneDockerBinaryConfig
from fbpcs.pid.entity.pid_instance import (
    PIDStageStatus,
    PIDInstance,
    PIDInstanceStatus,
    PIDProtocol,
    PIDRole,
)
from fbpcs.pid.entity.pid_stages import UnionPIDStage
from fbpcs.pid.repository.pid_instance import PIDInstanceRepository
from fbpcs.pid.service.pid_service.pid_dispatcher import PIDDispatcher
from fbpcs.pid.service.pid_service.pid_stage import PIDStage


class PIDService:
    """PID Service is responsible for creating, running and reading PID instance"""

    def __init__(
        self,
        onedocker_svc: OneDockerService,
        storage_svc: StorageService,
        instance_repository: PIDInstanceRepository,
        onedocker_binary_config_map: DefaultDict[str, OneDockerBinaryConfig],
    ) -> None:
        """Constructor of PIDService
        Keyword arguments:
        container_svc -- service to spawn container instances
        storage_svc -- service to read/write input/output files
        instance_repository -- repository to CRUD PIDInstance
        """
        self.onedocker_svc = onedocker_svc
        self.storage_svc = storage_svc
        self.instance_repository = instance_repository
        self.onedocker_binary_config_map = onedocker_binary_config_map
        self.logger: logging.Logger = logging.getLogger(__name__)

    def create_instance(
        self,
        instance_id: str,
        protocol: PIDProtocol,
        pid_role: PIDRole,
        num_shards: int,
        input_path: str,
        output_path: str,
        data_path: str = "",
        spine_path: str = "",
        is_validating: Optional[bool] = False,
        synthetic_shard_path: Optional[str] = None,
        hmac_key: Optional[str] = None,
    ) -> PIDInstance:
        self.logger.info(f"Creating PID instance: {instance_id}")
        instance = PIDInstance(
            instance_id=instance_id,
            protocol=protocol,
            pid_role=pid_role,
            num_shards=num_shards,
            input_path=input_path,
            output_path=output_path,
            is_validating=is_validating,
            synthetic_shard_path=synthetic_shard_path,
            status=PIDInstanceStatus.CREATED,
            data_path=data_path,
            spine_path=spine_path,
            hmac_key=hmac_key,
        )
        self.instance_repository.create(instance)
        return instance

    async def run_stage_or_next(
        self,
        instance_id: str,
        pid_config: Dict[str, Any],
        fail_fast: bool = False,
        server_ips: Optional[List[str]] = None,
        pid_union_stage: Optional[UnionPIDStage] = None,
        wait_for_containers: bool = True,
        container_timeout: Optional[int] = None,
    ) -> PIDInstance:
        """This function is similar to run_instance but instead of calling dispatcher.run_all,
        it will try to call dispatcher.run_next(), or if the pid_union_stage is given, it will
        try to call dispatcher.run_stage()
        """
        self.logger.info(
            f"Running PID instance: {instance_id} with PID stage: {pid_union_stage}"
        )

        # Get pid instance from repository
        instance = self.instance_repository.read(instance_id)

        # Create the dispatcher and build stages.
        dispatcher = self.__get_dispatcher_and_build_stages(
            instance, pid_config, fail_fast, server_ips
        )

        if pid_union_stage is None:
            # If pid_union_stage is not given, call run_next by default.
            await dispatcher.run_next()
        else:
            # Convert the UnionPIDStage into PIDStage
            pid_stage = dispatcher.get_pid_stage(pid_union_stage)

            # An error should be raised when pid_union_stage is given
            # but cannot find pid_stage
            if pid_stage is None:
                raise ValueError(
                    f"Can't find corresponding PID stage for {pid_union_stage}"
                )

            # if the stage is joint stage, partner must provide server_ips
            if (
                pid_stage.is_joint_stage
                and instance.pid_role is PIDRole.PARTNER
                and not server_ips
            ):
                raise ValueError("Missing server_ips")

            # Finally, call run_stage if the PID stage can be found
            # and all above checks are passed
            await dispatcher.run_stage(
                pid_stage, wait_for_containers, container_timeout
            )

        return self.update_instance(instance_id)

    async def run_instance(
        self,
        instance_id: str,
        pid_config: Dict[str, Any],
        fail_fast: bool = False,
        server_ips: Optional[List[str]] = None,
    ) -> PIDInstance:
        self.logger.info(f"Running PID instance: {instance_id}")
        # Get pid instance from repository
        instance = self.instance_repository.read(instance_id)

        # partner must provide server_ips
        if instance.pid_role is PIDRole.PARTNER and not server_ips:
            raise ValueError("Missing server_ips")

        # Call the dispatcher to run all stages
        dispatcher = self.__get_dispatcher_and_build_stages(
            instance, pid_config, fail_fast, server_ips
        )
        await dispatcher.run_all()

        # Return refreshed instance
        return self.update_instance(instance_id)

    def get_instance(self, instance_id: str) -> PIDInstance:
        self.logger.info(f"Getting PID instance: {instance_id}")
        return self.instance_repository.read(instance_id)

    def update_instance(self, instance_id: str) -> PIDInstance:
        self.logger.info(f"Updating PID Instance: {instance_id}")
        instance = self.instance_repository.read(instance_id)

        if instance.status in [PIDInstanceStatus.COMPLETED, PIDInstanceStatus.FAILED]:
            return instance

        for stage, status in instance.stages_status.copy().items():
            if status in [PIDStageStatus.COMPLETED, PIDStageStatus.FAILED]:
                continue
            containers = instance.stages_containers.get(stage, None)
            if containers:
                container_ids = [container.instance_id for container in containers]
                containers = list(
                    filter(None, self.onedocker_svc.get_containers(container_ids))
                )
                new_stage_status = PIDStage.get_stage_status_from_containers(containers)
                if new_stage_status is PIDStageStatus.FAILED:
                    instance.status = PIDInstanceStatus.FAILED
                instance.stages_status[stage] = new_stage_status
                instance.stages_containers[stage] = containers
                instance.current_stage = stage
        # if all of the stages are complete, then PID for instance is complete
        if all(
            status is PIDStageStatus.COMPLETED
            for status in instance.stages_status.values()
        ):
            instance.status = PIDInstanceStatus.COMPLETED
        self.instance_repository.update(instance)
        return instance

    def __get_dispatcher_and_build_stages(
        self,
        instance: PIDInstance,
        pid_config: Dict[str, Any],
        fail_fast: bool = False,
        server_ips: Optional[List[str]] = None,
    ) -> PIDDispatcher:
        dispatcher = PIDDispatcher(
            instance_id=instance.instance_id,
            instance_repository=self.instance_repository,
        )
        dispatcher.build_stages(
            input_path=instance.input_path,
            output_path=instance.output_path,
            num_shards=instance.num_shards,
            is_validating=instance.is_validating,
            synthetic_shard_path=instance.synthetic_shard_path,
            pid_config=pid_config,
            protocol=instance.protocol,
            role=instance.pid_role,
            onedocker_svc=self.onedocker_svc,
            storage_svc=self.storage_svc,
            onedocker_binary_config_map=self.onedocker_binary_config_map,
            fail_fast=fail_fast,
            server_ips=server_ips,
            data_path=instance.data_path,
            spine_path=instance.spine_path,
            hmac_key=instance.hmac_key,
        )
        return dispatcher
