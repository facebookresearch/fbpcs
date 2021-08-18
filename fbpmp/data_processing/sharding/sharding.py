#!/usr/bin/env python3
# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

import abc
import enum
from typing import Optional

from fbpcp.entity.container_instance import ContainerInstance
from fbpcp.service.onedocker import OneDockerService
from fbpcp.service.storage import StorageService


class ShardType(enum.Enum):
    ROUND_ROBIN = 1
    HASHED_FOR_PID = 2


class ShardingService(abc.ABC):
    @abc.abstractmethod
    def shard(
        self,
        shard_type: ShardType,
        filepath: str,
        output_base_path: str,
        file_start_index: int,
        num_output_files: int,
        storage_svc: Optional[StorageService] = None,
        hmac_key: Optional[str] = None,
    ) -> None:
        pass

    @abc.abstractmethod
    def shard_on_container(
        self,
        shard_type: ShardType,
        filepath: str,
        output_base_path: str,
        file_start_index: int,
        num_output_files: int,
        onedocker_svc: OneDockerService,
        binary_version: str,
        tmp_directory: str = "/tmp/",
        hmac_key: Optional[str] = None,
        wait_for_containers: bool = True,
    ) -> ContainerInstance:
        pass

    @abc.abstractmethod
    async def shard_on_container_async(
        self,
        shard_type: ShardType,
        filepath: str,
        output_base_path: str,
        file_start_index: int,
        num_output_files: int,
        onedocker_svc: OneDockerService,
        binary_version: str,
        tmp_directory: str = "/tmp/",
        hmac_key: Optional[str] = None,
        wait_for_containers: bool = True,
    ) -> ContainerInstance:
        pass
