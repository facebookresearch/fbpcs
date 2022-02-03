#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict


import functools
import logging
import warnings
from typing import Any, Dict, List, Optional

from fbpcp.entity.container_instance import ContainerInstanceStatus
from fbpcp.entity.mpc_instance import MPCInstance, MPCParty
from fbpcp.entity.mpc_instance import MPCInstanceStatus
from fbpcp.service.mpc import MPCService
from fbpcs.common.entity.pcs_mpc_instance import PCSMPCInstance
from fbpcs.experimental.cloud_logs.log_retriever import CloudProvider, LogRetriever
from fbpcs.pid.entity.pid_instance import PIDInstance
from fbpcs.private_computation.entity.private_computation_instance import (
    PrivateComputationInstance,
    PrivateComputationInstanceStatus,
)
from fbpcs.private_computation.entity.private_computation_instance import (
    PrivateComputationRole,
)
from fbpcs.private_computation.service.constants import DEFAULT_CONTAINER_TIMEOUT_IN_SEC


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
    """Creates an MPC instance and runs MPC service with it

    Args:
        mpc_svc: creates and runs MPC instances
        instance_id: unique id used to identify MPC instances
        game_name: the name of the MPC game to run, e.g. lift
        mpc_party: The role played by the MPC instance, e.g. SERVER or CLIENT
        num_containers: number of cloud containers to spawn and run mpc with
        binary_version: Onedocker version tag, e.g. latest
        server_ips: ip addresses of the publisher's containers.
        game_args: arguments that are passed to game binaries by onedocker
        container_timeout: optional duration in seconds before cloud containers timeout

    Returns:
        return: an mpc instance started by mpc service
    """

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
    """Convert PrivateComputationRole to MPCParty

    Args:
        pc_role: The role played in the private computation game, e.g. publisher or partner

    Returns:
        The MPCParty that corresponds to the given PrivateComputationRole, e.g. server or client

    Exceptions:
        ValueError: raised when there is no MPCParty associated with private_computation_role
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
    """Determines if private computation instance can attempt a partial container retry

    During the computation stage, if some containers fail, it is possible to only retry the
    containers that fail instead of starting from the beginning. This function determines if
    the proper conditions and settings are met.

    Args:
        pc_instance: the private computation instance to attempt partial container retry with

    Returns:
        True if the instance can perform a partial container retry, False otherwise.
    """
    return (
        private_computation_instance.partial_container_retry_enabled
        and private_computation_instance.status
        is PrivateComputationInstanceStatus.COMPUTATION_FAILED
    )


def gen_mpc_game_args_to_retry(
    private_computation_instance: PrivateComputationInstance,
) -> Optional[List[Dict[str, Any]]]:
    """Gets the game args associated with MPC containers that did not complete.

    During the computation stage, if some containers fail, it is possible to only retry the
    containers that fail instead of starting from the beginning. This function gets the game args
    for the containers that did not complete.

    Args:
        pc_instance: the private computation instance to attempt partial container retry with

    Returns:
        MPC game args for containers that did not complete

    Exceptions:
        ValueError: raised when the last instance stored by private_computation_instance is NOT an MPCInstance
    """
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


def get_updated_pc_status_mpc_game(
    private_computation_instance: PrivateComputationInstance,
    mpc_svc: MPCService,
) -> PrivateComputationInstanceStatus:
    """Updates the MPCInstances and gets latest PrivateComputationInstance status

    Arguments:
        private_computation_instance: The PC instance that is being updated
        mpc_svc: Used to update MPC instances stored on private_computation_instance

    Returns:
        The latest status for private_computation_instance
    """
    status = private_computation_instance.status
    if private_computation_instance.instances:
        # Only need to update the last stage/instance
        last_instance = private_computation_instance.instances[-1]
        if not isinstance(last_instance, MPCInstance):
            return status

        # MPC service has to call update_instance to get the newest containers
        # information in case they are still running
        private_computation_instance.instances[-1] = PCSMPCInstance.from_mpc_instance(
            mpc_svc.update_instance(last_instance.instance_id)
        )

        mpc_instance_status = private_computation_instance.instances[-1].status

        current_stage = private_computation_instance.current_stage
        if mpc_instance_status is MPCInstanceStatus.STARTED:
            status = current_stage.started_status
        elif mpc_instance_status is MPCInstanceStatus.COMPLETED:
            status = current_stage.completed_status
        elif mpc_instance_status in (
            MPCInstanceStatus.FAILED,
            MPCInstanceStatus.CANCELED,
        ):
            status = current_stage.failed_status

    return status


def get_log_urls(
    private_computation_instance: PrivateComputationInstance,
) -> Dict[str, str]:
    """Get log urls for most recently run containers

    Arguments:
        private_computation_instance: The PC instance that is being updated

    Returns:
        The latest status for private_computation_instance as an ordered dict
    """
    # Get the last pid or mpc instance
    last_instance = private_computation_instance.instances[-1]

    # TODO - hope we're using AWS!
    log_retriever = LogRetriever(CloudProvider.AWS)

    res = {}
    if isinstance(last_instance, PIDInstance):
        pid_current_stage = last_instance.current_stage
        if not pid_current_stage:
            logging.warning("Unreachable block: no stage has run yet")
            return res
        containers = last_instance.stages_containers[pid_current_stage]
        for i, container in enumerate(containers):
            res[f"{pid_current_stage}_{i}"] = log_retriever.get_log_url(
                container.instance_id
            )
    elif isinstance(last_instance, PCSMPCInstance):
        containers = last_instance.containers
        for i, container in enumerate(containers):
            res[str(i)] = log_retriever.get_log_url(container.instance_id)
    else:
        logging.warning(
            "The last instance of PrivateComputationInstance "
            f"{private_computation_instance.instance_id} has no supported way "
            "of retrieving log URLs"
        )
    return res


# decorators are a serious pain to add typing for, so I'm not going to bother...
# pyre-ignore return typing
def deprecated(reason: str):
    """
    Logs a warning that a function is deprecated
    """

    # pyre-ignore return typing
    def wrap(func):
        warning_color = "\033[93m"  # orange/yellow ascii escape sequence
        end = "\033[0m"  # end ascii escape sequence
        explanation: str = (
            f"{warning_color}{func.__name__} is deprecated! explanation: {reason}{end}"
        )

        @functools.wraps(func)
        # pyre-ignore typing on args, kwargs, and return
        def wrapped(*args, **kwargs):
            warnings.simplefilter("always", DeprecationWarning)
            warnings.warn(
                explanation,
                category=DeprecationWarning,
                stacklevel=2,
            )
            warnings.simplefilter("default", DeprecationWarning)
            return func(*args, **kwargs)

        return wrapped

    return wrap
