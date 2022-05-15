#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

from typing import List, Optional

from fbpcs.common.entity.stage_state_instance import (
    StageStateInstance,
    StageStateInstanceStatus,
)
from fbpcs.private_computation.entity.private_computation_instance import (
    PrivateComputationInstance,
    PrivateComputationInstanceStatus,
)
from fbpcs.private_computation.service.private_computation_stage_service import (
    PrivateComputationStageService,
)


class PIDMRStageService(PrivateComputationStageService):
    """Handles business logic for the PID Mapreduce match stage."""

    def __init__(
        self,
    ) -> None:
        pass

    async def run_async(
        self,
        pc_instance: PrivateComputationInstance,
        server_ips: Optional[List[str]] = None,
    ) -> PrivateComputationInstance:
        """This function run mr workflow service

        Args:
            pc_instance: the private computation instance to run mr match
            server_ips: only used by the partner role. These are the ip addresses of the publisher's containers.

        Returns:
            An updated version of pc_instance
        """
        stage_state = StageStateInstance(
            pc_instance.instance_id,
            pc_instance.current_stage.name,
        )

        pc_instance.instances.append(stage_state)
        return pc_instance

    def get_status(
        self,
        pc_instance: PrivateComputationInstance,
    ) -> PrivateComputationInstanceStatus:
        """Gets latest PrivateComputationInstance status

        Arguments:
            private_computation_instance: The PC instance that is being updated

        Returns:
            The latest status for private_computation_instance
        """
        status = pc_instance.status
        if pc_instance.instances:
            # TODO: we should have some identifier or stage_name
            # to pick up the right instance instead of the last one
            last_instance = pc_instance.instances[-1]
            if not isinstance(last_instance, StageStateInstance):
                raise ValueError(
                    f"The last instance type not StageStateInstance but {type(last_instance)}"
                )
            stage_name = last_instance.stage_name
            assert stage_name == pc_instance.current_stage.name

            stage_state_instance_status = StageStateInstanceStatus.COMPLETED
            current_stage = pc_instance.current_stage
            if stage_state_instance_status is StageStateInstanceStatus.STARTED:
                status = current_stage.started_status
            elif stage_state_instance_status is StageStateInstanceStatus.COMPLETED:
                status = current_stage.completed_status
            elif stage_state_instance_status is StageStateInstanceStatus.FAILED:
                status = current_stage.failed_status

        return status
