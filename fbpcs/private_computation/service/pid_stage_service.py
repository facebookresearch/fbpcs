#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

from typing import List, Optional

from fbpcs.pid.entity.pid_instance import (
    PIDInstance,
    PIDRole,
    PIDStageStatus,
    UnionPIDStage,
)
from fbpcs.pid.service.pid_service.pid import PIDService
from fbpcs.private_computation.entity.private_computation_instance import (
    PrivateComputationInstance,
    PrivateComputationInstanceStatus,
    PrivateComputationRole,
)
from fbpcs.private_computation.service.private_computation_stage_service import (
    PrivateComputationStageService,
)


class PIDStageService(PrivateComputationStageService):
    """Handles business logic for the private computation id match stage.

    Private attributes:
        _pid_svc: Creates PID instances and runs PID SHARD, PID PREPARE, and PID RUN
        _publisher_stage: The pid stage that should be ran by the publisher
        _partner_stage: The pid stage that should be ran by the partner
        _container_timeout: optional duration in seconds before cloud containers timeout
    """

    def __init__(
        self,
        pid_svc: PIDService,
        publisher_stage: UnionPIDStage,
        partner_stage: UnionPIDStage,
        container_timeout: Optional[int] = None,
    ) -> None:
        self._pid_svc = pid_svc
        self._publisher_stage = publisher_stage
        self._partner_stage = partner_stage
        self._container_timeout = container_timeout

    # TODO T88759390: Make this function truly async. It is not because it calls blocking functions.
    # Make an async version of run_async() so that it can be called by Thrift
    async def run_async(
        self,
        pc_instance: PrivateComputationInstance,
        server_ips: Optional[List[str]] = None,
    ) -> PrivateComputationInstance:
        """Runs a pid service stage, e.g. pid shard, pid prepare, pid run

        This function creates a pid instance if necessary, stores it on the caller provided pc_instance, and
        runs PIDService for a given stage.

        Args:
            pc_instance: the private computation instance to run ID match with
            server_ips: only used by the partner role. These are the ip addresses of the publisher's containers.

        Returns:
            An updated version of pc_instance that stores a PIDInstance
        """

        # if this in the shard stage (first pid stage), then create the pid instance
        if (
            self._publisher_stage is UnionPIDStage.PUBLISHER_SHARD
            and self._partner_stage is UnionPIDStage.ADV_SHARD
        ):
            # increment the retry counter (starts at 0 for first attempt)
            pid_instance_id = f"{pc_instance.infra_config.instance_id}_id_match{pc_instance.infra_config.retry_counter}"
            pid_instance = self._pid_svc.create_instance(
                instance_id=pid_instance_id,
                pid_role=self._map_private_computation_role_to_pid_role(
                    pc_instance.infra_config.role
                ),
                num_shards=pc_instance.infra_config.num_pid_containers,
                input_path=pc_instance.input_path,
                output_path=pc_instance.pid_stage_output_base_path,
                hmac_key=pc_instance.hmac_key,
                pid_use_row_numbers=pc_instance.pid_use_row_numbers,
            )
        else:
            # If there no previous instance, then we should run shard first
            if not pc_instance.infra_config.instances:
                raise RuntimeError(
                    f"Cannot run PID stages {self._publisher_stage}, {self._partner_stage}. Run PID shard first."
                )
            pid_instance = pc_instance.infra_config.instances[-1]
            # if the last instance is not a pid instance, then we are out of order
            if not isinstance(pid_instance, PIDInstance):
                raise ValueError(
                    f"Cannot run PID stages {self._publisher_stage}, {self._partner_stage}. Last instance is not a PIDInstance."
                )

        # Run pid
        pid_instance = await self._pid_svc.run_stage_or_next(
            instance_id=pid_instance.instance_id,
            server_ips=server_ips,
            pid_union_stage=self._publisher_stage
            if pc_instance.infra_config.role is PrivateComputationRole.PUBLISHER
            else self._partner_stage,
            wait_for_containers=False,
            container_timeout=self._container_timeout,
        )

        if not pc_instance.infra_config.instances or not isinstance(
            pc_instance.infra_config.instances[-1], PIDInstance
        ):
            # Push PID instance to PrivateComputationInstance.instances
            pc_instance.infra_config.instances.append(pid_instance)
        else:
            # replace the outdated pid instance with the updated one
            pc_instance.infra_config.instances[-1] = pid_instance

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
        status = pc_instance.infra_config.status
        if pc_instance.infra_config.instances:
            # Only need to update the last stage/instance
            last_instance = pc_instance.infra_config.instances[-1]
            if not isinstance(last_instance, PIDInstance):
                raise ValueError(f"Expected {last_instance} to be a PIDInstance")

            # PID service has to call update_instance to get the newest containers
            # information in case they are still running
            pc_instance.infra_config.instances[-1] = self._pid_svc.update_instance(
                last_instance.instance_id
            )
            last_instance = pc_instance.infra_config.instances[-1]
            assert isinstance(last_instance, PIDInstance)  # appeasing pyre

            pid_current_stage = last_instance.current_stage
            if not pid_current_stage:
                return status
            pid_stage_status = last_instance.stages_status.get(pid_current_stage)

            stage = pc_instance.current_stage
            if pid_stage_status is PIDStageStatus.STARTED:
                status = stage.started_status
            elif pid_stage_status is PIDStageStatus.COMPLETED:
                status = stage.completed_status
            elif pid_stage_status is PIDStageStatus.FAILED:
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
