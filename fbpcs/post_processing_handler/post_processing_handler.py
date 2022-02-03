#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

import abc
from enum import Enum
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from fbpcs.private_computation.entity.private_computation_instance import (
        PrivateComputationInstance,
    )
from fbpcp.service.storage import StorageService


class PostProcessingHandlerStatus(Enum):
    UNKNOWN = "UNKNOWN"
    STARTED = "STARTED"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


class PostProcessingHandler(abc.ABC):
    @abc.abstractmethod
    async def run(
        self,
        storage_svc: StorageService,
        private_computation_instance: "PrivateComputationInstance",
    ) -> None:
        raise NotImplementedError
