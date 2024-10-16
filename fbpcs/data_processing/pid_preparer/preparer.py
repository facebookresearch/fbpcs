#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe

import abc
import logging
import pathlib
from typing import Optional

# pyre-fixme[21]: Could not find module `fbpcp.entity.container_instance`.
from fbpcp.entity.container_instance import ContainerInstance

# pyre-fixme[21]: Could not find module `fbpcp.service.onedocker`.
from fbpcp.service.onedocker import OneDockerService

# pyre-fixme[21]: Could not find module `fbpcp.service.storage`.
from fbpcp.service.storage import StorageService


class UnionPIDDataPreparerService(abc.ABC):
    @abc.abstractmethod
    def prepare(
        self,
        input_path: str,
        output_path: str,
        log_path: Optional[pathlib.Path] = None,
        log_level: int = logging.INFO,
        # pyre-fixme[11]: Annotation `StorageService` is not defined as a type.
        storage_svc: Optional[StorageService] = None,
    ) -> None:
        pass

    @abc.abstractmethod
    def prepare_on_container(
        self,
        input_path: str,
        output_path: str,
        # TODO: Support custom log path
        # pyre-fixme[11]: Annotation `OneDockerService` is not defined as a type.
        onedocker_svc: OneDockerService,
        binary_version: str,
        tmp_directory: str = "/tmp/",
        wait_for_container: bool = True,
        # pyre-fixme[11]: Annotation `ContainerInstance` is not defined as a type.
    ) -> ContainerInstance:
        pass

    @abc.abstractmethod
    async def prepare_on_container_async(
        self,
        input_path: str,
        output_path: str,
        # TODO: Support custom log path
        onedocker_svc: OneDockerService,
        binary_version: str,
        tmp_directory: str = "/tmp/",
        wait_for_container: bool = True,
    ) -> ContainerInstance:
        pass
