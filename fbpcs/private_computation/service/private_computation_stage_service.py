#!/usr/bin/env python3
# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

import abc
from dataclasses import dataclass
from typing import Any, Dict, DefaultDict
from typing import List, Optional

from fbpcp.service.mpc import MPCService
from fbpcp.service.onedocker import OneDockerService
from fbpcp.service.storage import StorageService
from fbpcs.onedocker_binary_config import OneDockerBinaryConfig
from fbpcs.pid.service.pid_service.pid import PIDService
from fbpcs.post_processing_handler.post_processing_handler import PostProcessingHandler
from fbpcs.private_computation.entity.private_computation_instance import (
    PrivateComputationInstance,
    PrivateComputationInstanceStatus
)


@dataclass
class PrivateComputationStageServiceArgs:
    """
    These are all arguments that are guaranteed to exist in the PrivateComputationService at service
    creation time. A combination of these arguments is used to construct stage private computation stage services.
    """

    pid_svc: PIDService
    onedocker_binary_config_map: DefaultDict[str, OneDockerBinaryConfig]
    mpc_svc: MPCService
    storage_svc: StorageService
    post_processing_handlers: Dict[str, PostProcessingHandler]
    onedocker_svc: OneDockerService


class PrivateComputationStageService(abc.ABC):
    """
    Handles the business logic for each private computation stage. Each stage should subclass this service and implement the run_async method.
    Any parameters necessary to run the stage that aren't provided by run_async should be passed to the subclass' constructor.
    """

    @abc.abstractmethod
    async def run_async(
        self,
        pc_instance: PrivateComputationInstance,
        # TODO(T102471612): remove server_ips from run_async, move to subclass constructor instead
        server_ips: Optional[List[str]] = None,
    ) -> PrivateComputationInstance:
        ...

    @abc.abstractmethod
    def get_status(
        self,
        pc_instance: PrivateComputationInstance,
    ) -> PrivateComputationInstanceStatus:
        ...
