#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.


import logging

from fbpcp.service.onedocker import OneDockerService
from fbpcs.common.entity.stage_state_instance import StageStateInstance
from fbpcs.pid.service.pid_service.pid import PIDService
from fbpcs.private_computation.entity.private_computation_instance import (
    PrivateComputationInstance,
    PrivateComputationInstanceStatus,
)
from fbpcs.private_computation.service.private_computation_stage_service import (
    PrivateComputationStageService,
)

from fbpcs.private_computation.service.utils import get_pc_status_from_stage_state


class PIDRunProtocolStageService(PrivateComputationStageService):
    """Handles business logic for the PID run protocol stage

    Private attributes:
        _onedocker_svc: Spins up containers that run binaries in the cloud
        _pid_svc: Retains the parameters from PID config
    """

    def __init__(
        self,
        onedocker_svc: OneDockerService,
        pid_svc: PIDService,
    ) -> None:
        self._onedocker_svc = onedocker_svc
        self._pid_svc = pid_svc
        self._logger: logging.Logger = logging.getLogger(__name__)

    async def run_async(
        self,
        pc_instance: PrivateComputationInstance,
    ) -> PrivateComputationInstance:
        """Runs the PID run protocol stage

        Args:
            pc_instance: the private computation instance to run pid protocol stage

        Returns:
            An updated version of pc_instance
        """
        self._logger.info(f"[{self}] Starting PIDRunProtocolStageService")

        # TODO: implement run protocol stage

        container_instances = []
        self._logger.info("PIDRunProtocolStageService finished")

        stage_state = StageStateInstance(
            pc_instance.instance_id,
            pc_instance.current_stage.name,
            containers=container_instances,
        )

        pc_instance.instances.append(stage_state)
        return pc_instance

    def get_status(
        self,
        pc_instance: PrivateComputationInstance,
    ) -> PrivateComputationInstanceStatus:
        """Gets the latest PrivateComputationInstance status.

        Arguments:
            private_computation_instance: The PC instance that is being updated

        Returns:
            The latest status for private_computation_instance
        """
        return get_pc_status_from_stage_state(pc_instance, self._onedocker_svc)
