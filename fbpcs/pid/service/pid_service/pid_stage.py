#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

import abc
import logging
import os
from typing import List
from typing import Optional

from fbpcp.entity.container_instance import ContainerInstanceStatus, ContainerInstance
from fbpcp.service.onedocker import OneDockerService
from fbpcp.service.storage import PathType, StorageService
from fbpcs.onedocker_binary_config import OneDockerBinaryConfig
from fbpcs.pid.entity.pid_instance import PIDProtocol, PIDInstanceStatus, PIDStageStatus
from fbpcs.pid.entity.pid_stages import UnionPIDStage
from fbpcs.pid.repository.pid_instance import PIDInstanceRepository
from fbpcs.pid.service.pid_service.pid_stage_input import PIDStageInput
from fbpcs.private_computation.service.constants import DEFAULT_PID_PROTOCOL


class PIDStage(abc.ABC):
    def __init__(
        self,
        stage: UnionPIDStage,
        instance_repository: PIDInstanceRepository,
        storage_svc: StorageService,
        onedocker_svc: OneDockerService,
        onedocker_binary_config: OneDockerBinaryConfig,
        protocol: PIDProtocol = DEFAULT_PID_PROTOCOL,
        is_joint_stage: bool = False,
    ) -> None:
        self.stage_type = stage
        self.storage_svc = storage_svc
        self.onedocker_svc = onedocker_svc
        self.onedocker_binary_config = onedocker_binary_config
        self.instance_repository = instance_repository
        self.logger: logging.Logger = logging.getLogger(__name__)
        self.is_joint_stage = is_joint_stage
        self.protocol = protocol

    @abc.abstractmethod
    async def run(
        self,
        stage_input: PIDStageInput,
        wait_for_containers: bool = True,
        container_timeout: Optional[int] = None,
    ) -> PIDStageStatus:
        """
        Invoke the stage to actually execute. Derived classes must implement
        this method to handle their specific execution.
        """
        pass

    async def _ready(
        self,
        stage_input: PIDStageInput,
    ) -> PIDStageStatus:
        """
        Check if this PIDStage is ready to run. Default behavior (which should
        work for most stages) is simply to check if all input files exist --
        including sub-files for each shard.
        """
        # The *default* behavior is to check for each output while appending
        # _shard to the file. This will not hold for the first (shard) stage,
        # but every other stage acts in that way, so we make it default.
        input_paths = [
            self.get_sharded_filepath(path, i)
            for i in range(stage_input.num_shards)
            for path in stage_input.input_paths
        ]
        num_paths = len(stage_input.input_paths)
        num_sharded_paths = len(input_paths)
        self.logger.info(
            f"Checking ready status of {num_paths} paths across {num_sharded_paths} total sharded files"
        )
        if not self.files_exist(input_paths):
            # If the files *don't* exist, something happened. _ready is only
            # supposed to be called when the previous stage(s) succeeded.
            self.logger.error(f"Missing a necessary input file from {input_paths}")
            return PIDStageStatus.FAILED
        self.logger.info("All files ready")
        return PIDStageStatus.READY

    @staticmethod
    def get_sharded_filepath(path: str, shard: int) -> str:
        """
        Although this function is incredibly simple, it's important that we
        centralize one definition for how sharded files should look. This will
        ensure that we remain consistent in how we "expect" sharded filepaths
        to be stored and will prevent any erroneous mistakes if one service
        gets changed in the future to change the filepath in the future. There
        are no software guarantees here, but it should hint to the developer
        that there's some special function to use to shard a filepath.
        """
        return f"{path}_{shard}"

    def files_exist(self, paths: List[str]) -> bool:
        """
        Check if a list of filepaths exist. These are checked from the given
        StorageService or on the local disk (which may be used for some stages).
        """
        for path in paths:
            # If the path isn't local, assume our storage_svc can handle it
            if StorageService.path_type(path) != PathType.Local:
                if not self.storage_svc.file_exists(path):
                    return False
            else:
                # Local path
                if not os.path.exists(path):
                    return False
        return True

    def copy_synthetic_shard(self, src: str, dest: str) -> None:
        self.storage_svc.copy(src, dest)

    async def update_instance_num_shards(
        self,
        instance_id: str,
        num_shards: int,
    ) -> None:
        with self.instance_repository.lock:
            # get the pid instance to be updated from repo
            instance = self.instance_repository.read(instance_id)

            # update instance.num_shards
            instance.num_shards = num_shards

            # write updated instance to repo
            self.instance_repository.update(instance)
            self.logger.info(
                f"Stage {self} wrote num_shards {num_shards} to instance {instance_id} in repository"
            )

    @staticmethod
    def get_stage_status_from_containers(
        containers: List[ContainerInstance],
    ) -> PIDStageStatus:
        statuses = [container.status for container in containers]
        if ContainerInstanceStatus.FAILED in statuses:
            return PIDStageStatus.FAILED
        elif all(status is ContainerInstanceStatus.COMPLETED for status in statuses):
            return PIDStageStatus.COMPLETED
        elif ContainerInstanceStatus.STARTED in statuses:
            return PIDStageStatus.STARTED
        else:
            return PIDStageStatus.UNKNOWN

    async def update_instance_containers(
        self, instance_id: str, containers: List[ContainerInstance]
    ) -> None:
        with self.instance_repository.lock:
            # get the pid instance to be updated from repo
            instance = self.instance_repository.read(instance_id)

            # update instance.stages_containers
            instance.stages_containers[self.stage_type] = containers

            # write updated instance to repo
            self.instance_repository.update(instance)
            container_ids = ",".join(container.instance_id for container in containers)
            self.logger.info(
                f"Stage {self} wrote containers {container_ids} to instance {instance_id} in repository"
            )

    async def update_instance_status(
        self,
        instance_id: str,
        status: PIDStageStatus,
    ) -> None:
        with self.instance_repository.lock:
            # get the pid instance to be updated from repo
            instance = self.instance_repository.read(instance_id)

            # add stage status to instance
            instance.stages_status[self.stage_type] = status

            # update the instance status to be FAILED if stage status is FAILED
            if status is PIDStageStatus.FAILED:
                instance.status = PIDInstanceStatus.FAILED

            # write updated instance to repo
            self.instance_repository.update(instance)
            self.logger.info(
                f"Stage {self} wrote status {status} to instance {instance_id} in repository"
            )

    async def put_server_ips(
        self,
        instance_id: str,
        server_ips: List[str],
    ) -> None:
        with self.instance_repository.lock:
            # get the pid instance to be updated from repo
            instance = self.instance_repository.read(instance_id)

            # put server_ips
            instance.server_ips = server_ips

            # write updated instance to repo
            self.instance_repository.update(instance)
            self.logger.info(
                f"Stage {self} wrote server_ips {server_ips} to instance {instance_id} in repository"
            )

    def __str__(self) -> str:
        return str(self.stage_type)

    def __repr__(self) -> str:
        return str(self)
