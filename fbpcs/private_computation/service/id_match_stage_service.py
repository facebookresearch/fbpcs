#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

from typing import List, Optional

from fbpcs.pid.entity.pid_instance import (
    PIDInstance,
    PIDInstanceStatus,
    PIDProtocol,
    PIDRole,
)
from fbpcs.pid.service.pid_service.pid import PIDService
from fbpcs.private_computation.entity.private_computation_instance import (
    PrivateComputationInstance,
    PrivateComputationInstanceStatus,
    PrivateComputationRole,
)
from fbpcs.private_computation.service.constants import DEFAULT_PID_PROTOCOL
from fbpcs.private_computation.service.private_computation_stage_service import (
    PrivateComputationStageService,
)


class IdMatchStageService(PrivateComputationStageService):
    """Handles business logic for the private computation id match stage.

    Private attributes:
        _pid_svc: Creates PID instances and runs PID SHARD, PID PREPARE, and PID RUN
        _is_validating: if a test shard is injected to do run time correctness validation
        _synthetic_shard_path: path to the test shard to be injected if _is_validating
    """

    def __init__(
        self,
        pid_svc: PIDService,
        is_validating: bool = False,
        synthetic_shard_path: Optional[str] = None,
    ) -> None:
        self._pid_svc = pid_svc
        self._is_validating = is_validating
        self._synthetic_shard_path = synthetic_shard_path

    # TODO T88759390: Make this function truly async. It is not because it calls blocking functions.
    # Make an async version of run_async() so that it can be called by Thrift
    async def run_async(
        self,
        pc_instance: PrivateComputationInstance,
        server_ips: Optional[List[str]] = None,
    ) -> PrivateComputationInstance:
        """Runs the private computation ID match stage

        This function creates a PIDInstance, stores it on the caller provided pc_instance, and
        runs PIDService.

        Args:
            pc_instance: the private computation instance to run ID match with
            server_ips: only used by the partner role. These are the ip addresses of the publisher's containers.

        Returns:
            An updated version of pc_instance that stores a PIDInstance
        """

        retry_counter_str = str(pc_instance.retry_counter)
        pid_instance_id = pc_instance.instance_id + "_id_match" + retry_counter_str
        pid_instance = self._pid_svc.create_instance(
            instance_id=pid_instance_id,
            pid_role=self._map_private_computation_role_to_pid_role(pc_instance.role),
            num_shards=pc_instance.num_pid_containers,
            input_path=pc_instance.input_path,
            output_path=pc_instance.pid_stage_output_base_path,
            is_validating=self._is_validating or pc_instance.is_validating,
            synthetic_shard_path=self._synthetic_shard_path
            or pc_instance.synthetic_shard_path,
            hmac_key=pc_instance.hmac_key,
            pid_use_row_numbers=pc_instance.pid_use_row_numbers,
        )

        # Run pid
        pid_instance = await self._pid_svc.run_instance(
            instance_id=pid_instance_id,
            server_ips=server_ips,
        )

        # Push PID instance to PrivateComputationInstance.instances
        pc_instance.instances.append(pid_instance)

        return pc_instance

    def get_status(
        self,
        pc_instance: PrivateComputationInstance,
    ) -> PrivateComputationInstanceStatus:
        """Updates the PIDInstances and gets latest PrivateComputationInstance status

        Arguments:
            private_computation_instance: The PC instance that is being updated

        Returns:
            The latest status for private_computation_instance
        """
        status = pc_instance.status
        if pc_instance.instances:
            # Only need to update the last stage/instance
            last_instance = pc_instance.instances[-1]
            if not isinstance(last_instance, PIDInstance):
                return status

            # PID service has to call update_instance to get the newest containers
            # information in case they are still running
            pc_instance.instances[-1] = self._pid_svc.update_instance(
                last_instance.instance_id
            )

            pid_instance_status = pc_instance.instances[-1].status

            stage = pc_instance.current_stage
            if pid_instance_status is PIDInstanceStatus.STARTED:
                status = stage.started_status
            elif pid_instance_status is PIDInstanceStatus.COMPLETED:
                status = stage.completed_status
            elif pid_instance_status is PIDInstanceStatus.FAILED:
                status = stage.failed_status

        return status

    @staticmethod
    def _map_private_computation_role_to_pid_role(
        pc_role: PrivateComputationRole,
    ) -> PIDRole:
        """Convert PrivateComputationRole to PIDRole

        Args:
            pc_role: The role played in the private computation game, e.g. publisher or partner

        Returns:
            The PIDRole that corresponds to the given PrivateComputationRole, e.g. publisher or partner

        Exceptions:
            ValueError: raised when there is no PIDRole associated with private_computation_role
        """
        if pc_role is PrivateComputationRole.PUBLISHER:
            return PIDRole.PUBLISHER
        elif pc_role is PrivateComputationRole.PARTNER:
            return PIDRole.PARTNER
        else:
            raise ValueError(f"{pc_role} has no associated PIDRole")
