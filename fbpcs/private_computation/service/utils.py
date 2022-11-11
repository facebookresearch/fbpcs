#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict


import asyncio
import functools
import logging
import re
import warnings
from typing import Any, Dict, List, Optional

from fbpcp.entity.certificate_request import CertificateRequest
from fbpcp.service.onedocker import OneDockerService
from fbpcp.service.storage import StorageService
from fbpcs.common.entity.pcs_mpc_instance import PCSMPCInstance
from fbpcs.common.entity.stage_state_instance import StageStateInstanceStatus
from fbpcs.infra.certificate.certificate_provider import CertificateProvider
from fbpcs.onedocker_binary_config import ONEDOCKER_REPOSITORY_PATH
from fbpcs.private_computation.entity.private_computation_instance import (
    PrivateComputationInstance,
    PrivateComputationInstanceStatus,
    PrivateComputationRole,
)
from fbpcs.private_computation.service.constants import (
    CA_CERTIFICATE_ENV_VAR,
    CA_CERTIFICATE_PATH_ENV_VAR,
    DEFAULT_CONTAINER_TIMEOUT_IN_SEC,
    SERVER_CERTIFICATE_ENV_VAR,
    SERVER_CERTIFICATE_PATH_ENV_VAR,
)
from fbpcs.private_computation.service.mpc.entity.mpc_instance import (
    MPCInstance,
    MPCInstanceStatus,
    MPCParty,
)
from fbpcs.private_computation.service.mpc.mpc import MPCService
from fbpcs.private_computation.service.pid_utils import get_sharded_filepath


async def create_and_start_mpc_instance(
    mpc_svc: MPCService,
    instance_id: str,
    game_name: str,
    mpc_party: MPCParty,
    num_containers: int,
    binary_version: str,
    server_certificate_provider: CertificateProvider,
    ca_certificate_provider: CertificateProvider,
    server_certificate_path: str,
    ca_certificate_path: str,
    server_ips: Optional[List[str]] = None,
    game_args: Optional[List[Dict[str, Any]]] = None,
    container_timeout: Optional[int] = None,
    repository_path: Optional[str] = None,
    certificate_request: Optional[CertificateRequest] = None,
    wait_for_containers_to_start_up: bool = True,
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
        repository_path: Path from where we can download the required executable.
        certificate_request: Arguments to create a TLS certificate/key pair

    Returns:
        return: an mpc instance started by mpc service
    """
    try:
        mpc_svc.get_instance(instance_id)
    except Exception:
        logging.info(f"Failed to fetch MPC instance {instance_id} - trying to create")
        mpc_svc.create_instance(
            instance_id=instance_id,
            game_name=game_name,
            mpc_party=mpc_party,
            num_workers=num_containers,
            game_args=game_args,
        )

    env_vars = {}
    if repository_path:
        env_vars[ONEDOCKER_REPOSITORY_PATH] = repository_path
    server_cert = server_certificate_provider.get_certificate()
    ca_cert = ca_certificate_provider.get_certificate()
    if server_cert and server_certificate_path:
        env_vars[SERVER_CERTIFICATE_ENV_VAR] = server_cert
        env_vars[SERVER_CERTIFICATE_PATH_ENV_VAR] = server_certificate_path
    if ca_cert and ca_certificate_path:
        env_vars[CA_CERTIFICATE_ENV_VAR] = ca_cert
        env_vars[CA_CERTIFICATE_PATH_ENV_VAR] = ca_certificate_path

    return await mpc_svc.start_instance_async(
        instance_id=instance_id,
        server_ips=server_ips,
        timeout=container_timeout or DEFAULT_CONTAINER_TIMEOUT_IN_SEC,
        version=binary_version,
        env_vars=env_vars,
        certificate_request=certificate_request,
        wait_for_containers_to_start_up=wait_for_containers_to_start_up,
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
    status = private_computation_instance.infra_config.status
    if private_computation_instance.infra_config.instances:
        # Only need to update the last stage/instance
        last_instance = private_computation_instance.infra_config.instances[-1]
        if not isinstance(last_instance, MPCInstance):
            return status

        # MPC service has to call update_instance to get the newest containers
        # information in case they are still running
        private_computation_instance.infra_config.instances[
            -1
        ] = PCSMPCInstance.from_mpc_instance(
            mpc_svc.update_instance(last_instance.instance_id)
        )

        mpc_instance_status = private_computation_instance.infra_config.instances[
            -1
        ].status

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


def get_pc_status_from_stage_state(
    private_computation_instance: PrivateComputationInstance,
    onedocker_svc: OneDockerService,
    stage_name: Optional[str] = None,
) -> PrivateComputationInstanceStatus:
    """Updates the StageStateInstances and gets latest PrivateComputationInstance status

    Arguments:
        private_computation_instance: The PC instance that is being updated
        onedocker_svc: Used to get latest Containers to update StageState instance status
        stage_name: if passed stage_name, will check the stage name from StageState instance

    Returns:
        The latest status for private_computation_instance
    """
    status = private_computation_instance.infra_config.status
    stage_instance = private_computation_instance.get_stage_instance()
    if stage_instance is None:
        raise ValueError(
            f"The current instance type not StageStateInstance but {type(private_computation_instance.current_stage)}"
        )

    stage_name = stage_name or stage_instance.stage_name
    assert stage_name == private_computation_instance.current_stage.name
    # calling onedocker_svc to update newest containers in StageState
    stage_state_instance_status = stage_instance.update_status(onedocker_svc)
    current_stage = private_computation_instance.current_stage
    # only update when StageStateInstanceStatus transit to Started/Completed/Failed, or remain the current status
    if stage_state_instance_status is StageStateInstanceStatus.STARTED:
        status = current_stage.started_status
    elif stage_state_instance_status is StageStateInstanceStatus.COMPLETED:
        status = current_stage.completed_status
    elif stage_state_instance_status is StageStateInstanceStatus.FAILED:
        status = current_stage.failed_status

    return status


# pyre-ignore return typing
def deprecated_msg(msg: str):
    warning_color = "\033[93m"  # orange/yellow ascii escape sequence
    end = "\033[0m"  # end ascii escape sequence
    warnings.simplefilter("always", DeprecationWarning)
    warnings.warn(
        f"{warning_color}{msg}{end}",
        category=DeprecationWarning,
        stacklevel=2,
    )
    warnings.simplefilter("default", DeprecationWarning)


# decorators are a serious pain to add typing for, so I'm not going to bother...
# pyre-ignore return typing
def deprecated(reason: str):
    """
    Logs a warning that a function is deprecated
    """

    # pyre-ignore return typing
    def wrap(func):
        @functools.wraps(func)
        # pyre-ignore typing on args, kwargs, and return
        def wrapped(*args, **kwargs):
            deprecated_msg(msg=f"{func.__name__} is deprecated! explanation: {reason}")
            return func(*args, **kwargs)

        return wrapped

    return wrap


def transform_file_path(file_path: str, aws_region: Optional[str] = None) -> str:
    """Transforms URL paths passed through the CLI to preferred access formats

    Args:
        file_path: The path to be transformed

    Returns:
        A URL in our preffered format (virtual-hosted server or local)

    Exceptions:
        ValueError:
    """

    key_pattern = "."
    region_regex_pattern = "[a-zA-Z0-9.-]"
    bucket_name_regex_pattern = "[a-z0-9.-]"

    # Check if it matches the path style access format, https://s3.Region.amazonaws.com/bucket-name/key-name
    if re.search(
        rf"https://[sS]3\.{region_regex_pattern}+\.amazonaws\.com/{bucket_name_regex_pattern}+/{key_pattern}+",
        file_path,
    ):

        # Extract Bucket, Key, and Region
        key_name_search = re.search(
            rf"https://[sS]3\.{region_regex_pattern}+\.amazonaws\.com/{bucket_name_regex_pattern}+/",
            file_path,
        )
        bucket_name_search = re.search(
            rf"https://[sS]3\.{region_regex_pattern}+\.amazonaws\.com/", file_path
        )
        region_start_search = re.search(r"https://[sS]3\.", file_path)
        region_end_search = re.search(r".amazonaws\.com/", file_path)
        bucket = ""
        key = ""

        # Check for not None rather than extracting on search, to keep pyre happy
        if (
            key_name_search
            and bucket_name_search
            and region_start_search
            and region_end_search
        ):
            aws_region = file_path[
                region_start_search.span()[1] : region_end_search.span()[0]
            ]
            bucket = file_path[
                bucket_name_search.span()[1] : key_name_search.span()[1] - 1
            ]
            key = file_path[key_name_search.span()[1] :]

        file_path = f"https://{bucket}.s3.{aws_region}.amazonaws.com/{key}"

    # Check if it matches the s3 style access format, s3://bucket-name/key-name
    if re.search(rf"[sS]3://{bucket_name_regex_pattern}+/{key_pattern}+", file_path):

        if aws_region is not None:

            # Extract Bucket, Key
            bucket_name_search = re.search(r"[sS]3://", file_path)
            key_name_search = re.search(
                rf"[sS]3://{bucket_name_regex_pattern}+/", file_path
            )
            bucket = ""
            key = ""

            # Check for not None rather than extracting on search, to keep pyre happy
            if key_name_search and bucket_name_search:

                bucket = file_path[
                    bucket_name_search.span()[1] : key_name_search.span()[1] - 1
                ]
                key = file_path[key_name_search.span()[1] :]

            file_path = f"https://{bucket}.s3.{aws_region}.amazonaws.com/{key}"

        else:
            raise ValueError(
                "Cannot be parsed to expected virtual-hosted-file format "
                f"Please check your input path that aws_region need to be specified: [{file_path}]"
            )

    if re.search(
        rf"https://{bucket_name_regex_pattern}+\.s3\.{region_regex_pattern}+\.amazonaws.com/{key_pattern}+",
        file_path,
    ):
        return file_path
    else:
        raise ValueError(
            "Error transforming into expected virtual-hosted format. Bad input? "
            f"Please check your input path: [{file_path}]"
        )


async def file_exists_async(
    storage_svc: StorageService,
    file_path: str,
) -> bool:
    """
    Check if the file on the StorageService
    """
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, storage_svc.file_exists, file_path)


async def all_files_exist_on_cloud(
    input_path: str, num_shards: int, storage_svc: StorageService
) -> bool:
    input_paths = [
        get_sharded_filepath(input_path, shard) for shard in range(num_shards)
    ]
    # if all files exist on storage service, every element of file_exist_booleans should be True.
    tasks = await asyncio.gather(
        *[file_exists_async(storage_svc, path) for path in input_paths]
    )
    return sum(tasks) == num_shards


def stop_stage_service(
    pc_instance: PrivateComputationInstance, onedocker_svc: OneDockerService
) -> None:
    stage_instance = pc_instance.get_stage_instance()
    if stage_instance is not None:
        stage_instance.stop_containers(onedocker_svc)
    else:
        raise ValueError("Have no StageState for stop_service")
