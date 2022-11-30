#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

import asyncio
import logging
from typing import Any, Dict, List, Optional, Tuple

from fbpcp.entity.certificate_request import CertificateRequest

from fbpcp.entity.container_instance import ContainerInstance, ContainerInstanceStatus
from fbpcp.error.pcp import PcpError
from fbpcp.service.container import ContainerService
from fbpcp.service.onedocker import OneDockerService
from fbpcp.util.typing import checked_cast
from fbpcs.common.entity.pcs_mpc_instance import PCSMPCInstance

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
from fbpcs.private_computation.service.mpc.mpc_game import MPCGameService
from fbpcs.private_computation.service.mpc.repository.mpc_instance import (
    MPCInstanceRepository,
)
from fbpcs.private_computation.service.run_binary_base_service import (
    RunBinaryBaseService,
)

DEFAULT_BINARY_VERSION = "latest"


class MPCService(RunBinaryBaseService):
    """MPCService is responsible for distributing a larger MPC game to multiple
    MPC workers
    """

    def __init__(
        self,
        container_svc: ContainerService,
        instance_repository: MPCInstanceRepository,
        task_definition: str,
        mpc_game_svc: MPCGameService,
    ) -> None:
        """Constructor of MPCService
        Keyword arguments:
        container_svc -- service to spawn container instances
        instance_repository -- repository to CRUD MPCInstance
        task_definition -- containers task definition
        mpc_game_svc -- service to generate package name and game arguments.
        """
        if container_svc is None or instance_repository is None or mpc_game_svc is None:
            raise ValueError(
                f"Dependency is missing. container_svc={container_svc}, mpc_game_svc={mpc_game_svc}, "
                f"instance_repository={instance_repository}"
            )

        self.container_svc = container_svc
        self.instance_repository = instance_repository
        self.task_definition = task_definition
        self.mpc_game_svc: MPCGameService = mpc_game_svc
        self.logger: logging.Logger = logging.getLogger(__name__)

        self.onedocker_svc = OneDockerService(self.container_svc, self.task_definition)

    """
    The game_args should be consistent with the game_config, which should be
    defined in caller's game repository.

    For example,
    If the game config looks like this:

    game_config = {
    "game": {
        "onedocker_package_name": "package_name",
        "arguments": [
            {"name": "input_filenames", "required": True},
            {"name": "input_directory", "required": True},
            {"name": "output_filenames", "required": True},
            {"name": "output_directory", "required": True},
            {"name": "concurrency", "required": True},
        ],
    },

    The game args should look like this:
    [
        # 1st container
        {
            "input_filenames": input_path_1,
            "input_directory": input_directory,
            "output_filenames": output_path_1,
            "output_directory": output_directory,
            "concurrency": cocurrency,
        },
        # 2nd container
        {
            "input_filenames": input_path_2,
            "input_directory": input_directory,
            "output_filenames": output_path_2,
            "output_directory": output_directory,
            "concurrency": cocurrency,
        },
    ]
    """

    def create_instance(
        self,
        instance_id: str,
        game_name: str,
        mpc_party: MPCParty,
        num_workers: int,
        server_ips: Optional[List[str]] = None,
        game_args: Optional[List[Dict[str, Any]]] = None,
        server_uris: Optional[List[str]] = None,
    ) -> MPCInstance:
        self.logger.info(f"Creating MPC instance: {instance_id}")

        instance = MPCInstance(
            instance_id,
            game_name,
            mpc_party,
            num_workers,
            server_ips,
            [],
            MPCInstanceStatus.CREATED,
            game_args,
            server_uris,
        )

        self.instance_repository.create(instance)
        return instance

    def start_instance(
        self,
        instance_id: str,
        output_files: Optional[List[str]] = None,
        server_ips: Optional[List[str]] = None,
        timeout: Optional[int] = None,
        version: str = DEFAULT_BINARY_VERSION,
        env_vars: Optional[Dict[str, str]] = None,
        certificate_request: Optional[CertificateRequest] = None,
        wait_for_containers_to_start_up: bool = True,
    ) -> MPCInstance:
        return asyncio.run(
            self.start_instance_async(
                instance_id,
                output_files,
                server_ips,
                timeout,
                version,
                env_vars,
                certificate_request,
                wait_for_containers_to_start_up,
            )
        )

    def convert_cmd_args_list(
        self,
        game_name: str,
        game_args: List[Dict[str, Any]],
        mpc_party: MPCParty,
        server_ips: Optional[List[str]] = None,
    ) -> Tuple[str, List[str]]:
        """Convert Game args (MPC) to Cmd args be used by Onedocker service.

        Args:
            game_name: the name of the MPC game to run, e.g. lift
            game_args: arguments that are passed to game binaries by onedocker
            mpc_party: The role played by the MPC instance, e.g. SERVER or CLIENT
            server_ips: ip addresses of the publisher's containers.

        Returns:
            return: Tuple of (binary_name, cmd_args_list - compatible with oneDocker API)
        """
        if not game_args:
            raise ValueError("Missing game_args or it's empty")
        if mpc_party is MPCParty.CLIENT and not server_ips:
            raise ValueError("Missing server_ips")

        cmd_args_list = []
        binary_name = None
        for i in range(len(game_args)):
            game_arg = game_args[i] if game_args is not None else {}
            server_ip = server_ips[i] if server_ips is not None else None
            package_name, cmd_args = self.mpc_game_svc.build_onedocker_args(
                game_name=game_name,
                mpc_party=mpc_party,
                server_ip=server_ip,
                **game_arg,
            )
            if binary_name is None:
                binary_name = package_name

            cmd_args_list.append(cmd_args)

        if binary_name is None:
            raise ValueError("Can't get binary_name from game_args")

        return (binary_name, cmd_args_list)

    async def start_instance_async(
        self,
        instance_id: str,
        output_files: Optional[List[str]] = None,
        server_ips: Optional[List[str]] = None,
        timeout: Optional[int] = None,
        version: str = DEFAULT_BINARY_VERSION,
        env_vars: Optional[Dict[str, str]] = None,
        certificate_request: Optional[CertificateRequest] = None,
        wait_for_containers_to_start_up: bool = True,
    ) -> MPCInstance:
        """To run a distributed MPC game
        Keyword arguments:
        instance_id -- unique id to identify the MPC instance
        """
        instance = self.instance_repository.read(instance_id)
        self.logger.info(f"Starting MPC instance: {instance_id}")

        existing_containers = instance.containers
        game_args = instance.game_args
        if game_args is None or len(game_args) != instance.num_workers:
            raise ValueError(
                "The number of containers is not consistent with the number of game argument dictionary."
            )
        if server_ips is not None and len(server_ips) != instance.num_workers:
            raise ValueError(
                "The number of containers is not consistent with number of ip addresses."
            )
        binary_name, cmd_args_list = self.convert_cmd_args_list(
            game_name=instance.game_name,
            game_args=game_args,
            mpc_party=instance.mpc_party,
            server_ips=server_ips,
        )
        pending_containers = await self.start_containers(
            cmd_args_list=cmd_args_list,
            onedocker_svc=self.onedocker_svc,
            binary_version=version,
            binary_name=binary_name,
            timeout=timeout,
            env_vars=env_vars,
            wait_for_containers_to_start_up=wait_for_containers_to_start_up,
            existing_containers=existing_containers,
            certificate_request=certificate_request,
        )
        if pending_containers:
            instance.containers = pending_containers

        if len(instance.containers) != instance.num_workers:
            self.logger.warning(
                f"Instance {instance_id} has {len(instance.containers)} containers spun up, but expecting {instance.num_workers} containers!"
            )

        if wait_for_containers_to_start_up:
            if instance.mpc_party is MPCParty.SERVER:
                ip_addresses = [
                    checked_cast(str, instance.ip_address)
                    for instance in instance.containers
                ]
                instance.server_ips = ip_addresses

            instance.status = MPCInstanceStatus.STARTED
        else:
            instance.status = MPCInstanceStatus.CREATED

        self.instance_repository.update(instance)

        return instance

    def stop_instance(self, instance_id: str) -> MPCInstance:
        instance = self.instance_repository.read(instance_id)
        container_ids = [instance.instance_id for instance in instance.containers]
        if container_ids:
            errors = self.onedocker_svc.stop_containers(container_ids)
            error_msg = list(filter(lambda _: _[1], zip(container_ids, errors)))

            if error_msg:
                self.logger.error(
                    f"We encountered errors when stopping containers: {error_msg}"
                )

        instance.status = MPCInstanceStatus.CANCELED
        self.instance_repository.update(instance)
        self.logger.info(f"MPC instance {instance_id} has been successfully canceled.")

        return instance

    def get_instance(self, instance_id: str) -> MPCInstance:
        self.logger.info(f"Getting MPC instance: {instance_id}")
        return self.instance_repository.read(instance_id)

    def update_instance(self, instance_id: str) -> MPCInstance:
        instance = self.instance_repository.read(instance_id)

        self.logger.info(f"Updating MPC instance: {instance_id}")

        if instance.status in [
            MPCInstanceStatus.COMPLETED,
            MPCInstanceStatus.FAILED,
            MPCInstanceStatus.CANCELED,
        ]:
            return instance

        # skip if no containers registered under instance yet
        if instance.containers:
            instance.containers = self._update_container_instances(instance.containers)

            if len(instance.containers) != instance.num_workers:
                raise PcpError(
                    f"Instance {instance_id} has {len(instance.containers)} containers after update, but expecting {instance.num_workers} containers!"
                )

            instance.status = self._get_instance_status(instance)
            if (
                instance.status is MPCInstanceStatus.STARTED
                and instance.mpc_party is MPCParty.SERVER
            ):
                ip_addresses = [
                    checked_cast(str, c.ip_address) for c in instance.containers
                ]
                instance.server_ips = ip_addresses

            self.instance_repository.update(instance)

        return instance

    def _update_container_instances(
        self, containers: List[ContainerInstance]
    ) -> List[ContainerInstance]:
        ids = [container.instance_id for container in containers]
        return list(
            filter(
                None,
                map(
                    self._get_updated_container,
                    # TODO APIs of OneDocker service should be called here
                    self.container_svc.get_instances(ids),
                    containers,
                ),
            )
        )

    def _get_updated_container(
        self,
        queried_instance: Optional[ContainerInstance],
        existing_instance: ContainerInstance,
    ) -> Optional[ContainerInstance]:
        # 1. If ECS returns an instance, return the instance to callers
        # 2. If ECS could not find an instance and the status of existing instance is not stopped, return None to callers
        # 3. If ECS cound not find an instance and the status of existing instance is stopped, return the existing instance to callers.
        if queried_instance:
            return queried_instance
        elif existing_instance.status not in [
            ContainerInstanceStatus.COMPLETED,
            ContainerInstanceStatus.FAILED,
        ]:
            self.logger.error(
                f"The unstopped container instance {existing_instance.instance_id} is missing from Cloud. None is returned to callers."
            )
            return queried_instance
        else:
            self.logger.info(
                f"The stopped container instance {existing_instance.instance_id} is missing from Cloud. The existing container instance is returned."
            )
            return existing_instance

    def _get_instance_status(self, instance: MPCInstance) -> MPCInstanceStatus:
        if instance.status is MPCInstanceStatus.CANCELED:
            return instance.status
        status = MPCInstanceStatus.COMPLETED

        for container in instance.containers:
            if container.status == ContainerInstanceStatus.FAILED:
                return MPCInstanceStatus.FAILED
            if container.status == ContainerInstanceStatus.UNKNOWN:
                return MPCInstanceStatus.UNKNOWN
            if container.status == ContainerInstanceStatus.STARTED:
                status = MPCInstanceStatus.STARTED

        return status


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
    server_domain: Optional[str] = None,
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
        server_uris = _get_server_uris(server_domain, mpc_party, num_containers)
        mpc_svc.create_instance(
            instance_id=instance_id,
            game_name=game_name,
            mpc_party=mpc_party,
            num_workers=num_containers,
            game_args=game_args,
            server_uris=server_uris,
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


def _get_server_uris(
    server_domain: Optional[str], mpc_party: MPCParty, num_containers: int
) -> Optional[List[str]]:
    """For each container, create a unique server_uri based
    on the server_domain when the MPCParty is server.
    """
    if mpc_party is MPCParty.CLIENT or not server_domain:
        return None
    else:
        return [f"node{i}.{server_domain}" for i in range(num_containers)]


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
