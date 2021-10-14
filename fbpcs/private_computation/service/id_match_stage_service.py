#!/usr/bin/env python3
# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

from typing import Any, Dict, List, Optional

from fbpcs.pid.entity.pid_instance import PIDInstanceStatus, PIDProtocol, PIDRole
from fbpcs.pid.service.pid_service.pid import PIDService
from fbpcs.private_computation.entity.private_computation_instance import (
    PrivateComputationInstance,
)
from fbpcs.private_computation.entity.private_computation_instance import (
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
        _pid_config: Consumed by PIDService to determine cloud credentials
        _protocol: An enum consumed by PIDService to determine which protocol to use, e.g. UNION_PID.
        _is_validating: if a test shard is injected to do run time correctness validation
        _synthetic_shard_path: path to the test shard to be injected if _is_validating
    """

    def __init__(
        self,
        pid_svc: PIDService,
        pid_config: Dict[str, Any],
        protocol: PIDProtocol = DEFAULT_PID_PROTOCOL,
        is_validating: bool = False,
        synthetic_shard_path: Optional[str] = None,
    ) -> None:
        self._pid_svc = pid_svc
        self._pid_config = pid_config
        self._protocol = protocol
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
            protocol=self._protocol,
            pid_role=self._map_private_computation_role_to_pid_role(pc_instance.role),
            num_shards=pc_instance.num_pid_containers,
            input_path=pc_instance.input_path,
            output_path=pc_instance.pid_stage_output_base_path,
            is_validating=self._is_validating or pc_instance.is_validating,
            synthetic_shard_path=self._synthetic_shard_path
            or pc_instance.synthetic_shard_path,
            hmac_key=pc_instance.hmac_key,
        )

        # Push PID instance to PrivateComputationInstance.instances and update PL Instance status
        pid_instance.status = PIDInstanceStatus.STARTED
        pc_instance.instances.append(pid_instance)

        # Run pid
        # With the current design, it won't return until everything is done
        await self._pid_svc.run_instance(
            instance_id=pid_instance_id,
            pid_config=self._pid_config,
            fail_fast=pc_instance.fail_fast,
            server_ips=server_ips,
        )

        return pc_instance

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
