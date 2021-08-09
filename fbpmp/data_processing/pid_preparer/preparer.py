#!/usr/bin/env python3
# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

import abc
import logging
import pathlib
from typing import Optional

from fbpcs.service.onedocker import OneDockerService
from fbpcs.service.storage import StorageService


class UnionPIDDataPreparerService(abc.ABC):
    @abc.abstractmethod
    def prepare(
        self,
        input_path: str,
        output_path: str,
        log_path: Optional[pathlib.Path] = None,
        log_level: int = logging.INFO,
        storage_svc: Optional[StorageService] = None,
    ) -> None:
        pass

    @abc.abstractmethod
    def prepare_on_container(
        self,
        input_path: str,
        output_path: str,
        # TODO: Support custom log path
        onedocker_svc: OneDockerService,
        binary_version: str,
        tmp_directory: str = "/tmp/",
    ) -> None:
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
    ) -> None:
        pass
