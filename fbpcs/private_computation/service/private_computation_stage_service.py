#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

import abc
from dataclasses import dataclass
from typing import DefaultDict, Dict, List, Optional

from fbpcp.service.mpc import MPCService
from fbpcp.service.onedocker import OneDockerService
from fbpcp.service.storage import StorageService

from fbpcs.common.service.metric_service import MetricService
from fbpcs.common.service.trace_logging_service import TraceLoggingService
from fbpcs.onedocker_binary_config import OneDockerBinaryConfig
from fbpcs.post_processing_handler.post_processing_handler import PostProcessingHandler
from fbpcs.private_computation.entity.pc_validator_config import PCValidatorConfig
from fbpcs.private_computation.entity.private_computation_instance import (
    PrivateComputationInstance,
    PrivateComputationInstanceStatus,
)
from fbpcs.service.workflow import WorkflowService


@dataclass
class PrivateComputationStageServiceArgs:
    """
    These are all arguments that are guaranteed to exist in the PrivateComputationService at service
    creation time. A combination of these arguments is used to construct stage private computation stage services.
    """

    onedocker_binary_config_map: DefaultDict[str, OneDockerBinaryConfig]
    mpc_svc: MPCService
    storage_svc: StorageService
    post_processing_handlers: Dict[str, PostProcessingHandler]
    pid_post_processing_handlers: Dict[str, PostProcessingHandler]
    onedocker_svc: OneDockerService
    pc_validator_config: PCValidatorConfig
    workflow_svc: Optional[WorkflowService]
    metric_svc: MetricService
    trace_logging_svc: TraceLoggingService


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

    def stop_service(
        self,
        pc_instance: PrivateComputationInstance,
    ) -> None:
        """after stop_service been called, you need to make sure get_status will return failed status for post-check"""
        # TODO: T124322832 make stop service as abstract method and enforce all stage service to implement
        raise NotImplementedError
