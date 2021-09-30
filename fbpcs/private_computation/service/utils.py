#!/usr/bin/env python3
# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict


from typing import Any, Dict, List, Optional

from fbpcp.entity.container_instance import ContainerInstanceStatus
from fbpcp.entity.mpc_instance import MPCInstance, MPCParty
from fbpcp.service.mpc import MPCService
from fbpcs.private_computation.entity.private_computation_instance import (
    PrivateComputationInstance,
    PrivateComputationInstanceStatus,
)
from fbpcs.private_computation.entity.private_computation_instance import (
    PrivateComputationRole,
)

"""
43200 s = 12 hrs

We want to be conservative on this timeout just in case:
1) partner side is not able to connect in time. This is possible because it's a manual process
to run partner containers and humans can be slow;
2) during development, we add logic or complexity to the binaries running inside the containers
so that they take more than a few hours to run.
"""
DEFAULT_CONTAINER_TIMEOUT_IN_SEC = 43200

MAX_ROWS_PER_PID_CONTAINER = 10_000_000
TARGET_ROWS_PER_MPC_CONTAINER = 250_000
NUM_NEW_SHARDS_PER_FILE: int = round(
    MAX_ROWS_PER_PID_CONTAINER / TARGET_ROWS_PER_MPC_CONTAINER
)

# List of stages with 'STARTED' status.
STAGE_STARTED_STATUSES: List[PrivateComputationInstanceStatus] = [
    PrivateComputationInstanceStatus.ID_MATCHING_STARTED,
    PrivateComputationInstanceStatus.COMPUTATION_STARTED,
    PrivateComputationInstanceStatus.AGGREGATION_STARTED,
    PrivateComputationInstanceStatus.POST_PROCESSING_HANDLERS_STARTED,
]

# List of stages with 'FAILED' status.
STAGE_FAILED_STATUSES: List[PrivateComputationInstanceStatus] = [
    PrivateComputationInstanceStatus.ID_MATCHING_FAILED,
    PrivateComputationInstanceStatus.COMPUTATION_FAILED,
    PrivateComputationInstanceStatus.AGGREGATION_FAILED,
    PrivateComputationInstanceStatus.POST_PROCESSING_HANDLERS_FAILED,
]

DEFAULT_PADDING_SIZE = 4
DEFAULT_K_ANONYMITY_THRESHOLD = 100


async def create_and_start_mpc_instance(
    mpc_svc: MPCService,
    instance_id: str,
    game_name: str,
    mpc_party: MPCParty,
    num_containers: int,
    binary_version: str,
    server_ips: Optional[List[str]] = None,
    game_args: Optional[List[Dict[str, Any]]] = None,
    container_timeout: Optional[int] = None,
) -> MPCInstance:
    mpc_svc.create_instance(
        instance_id=instance_id,
        game_name=game_name,
        mpc_party=mpc_party,
        num_workers=num_containers,
        game_args=game_args,
    )

    return await mpc_svc.start_instance_async(
        instance_id=instance_id,
        server_ips=server_ips,
        timeout=container_timeout or DEFAULT_CONTAINER_TIMEOUT_IN_SEC,
        version=binary_version,
    )


def map_private_computation_role_to_mpc_party(
    private_computation_role: PrivateComputationRole,
) -> MPCParty:
    """
    TODO
    """
    if private_computation_role is PrivateComputationRole.PUBLISHER:
        return MPCParty.SERVER
    elif private_computation_role is PrivateComputationRole.PARTNER:
        return MPCParty.CLIENT
    else:
        raise ValueError(f"No mpc party defined for {private_computation_role}")


def ready_for_partial_container_retry(
    private_computation_instance: PrivateComputationInstance,
) -> bool:
    return (
        private_computation_instance.partial_container_retry_enabled
        and private_computation_instance.status
        is PrivateComputationInstanceStatus.COMPUTATION_FAILED
    )


def gen_mpc_game_args_to_retry(
    private_computation_instance: PrivateComputationInstance,
) -> Optional[List[Dict[str, Any]]]:
    # Get the last mpc instance
    last_mpc_instance = private_computation_instance.instances[-1]

    # Validate the last instance
    if not isinstance(last_mpc_instance, MPCInstance):
        raise ValueError(
            f"The last instance of PrivateComputationInstance {private_computation_instance.instance_id} is NOT an MPCInstance"
        )

    containers = last_mpc_instance.containers
    game_args = last_mpc_instance.game_args
    game_args_to_retry = game_args

    # We have to do the check here because occasionally when containers failed to spawn,
    #   len(containers) < len(game_args), in which case we should not get game args from
    #   failed containers; if we do, we will miss game args that belong to those containers
    #   failed to be spawned
    if containers and game_args and len(containers) == len(game_args):
        game_args_to_retry = [
            game_arg
            for game_arg, container_instance in zip(game_args, containers)
            if container_instance.status is not ContainerInstanceStatus.COMPLETED
        ]

    return game_args_to_retry
