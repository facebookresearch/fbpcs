#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict


import functools
import logging
import math
import re
import warnings
from typing import Any, DefaultDict, Dict, List, Optional

from fbpcp.entity.container_instance import ContainerInstance
from fbpcp.entity.mpc_instance import MPCInstance, MPCInstanceStatus, MPCParty
from fbpcp.service.mpc import MPCService
from fbpcp.service.onedocker import OneDockerService
from fbpcp.util.typing import checked_cast
from fbpcs.common.entity.pcs_mpc_instance import PCSMPCInstance
from fbpcs.common.entity.stage_state_instance import (
    StageStateInstance,
    StageStateInstanceStatus,
)
from fbpcs.data_processing.service.id_spine_combiner import IdSpineCombinerService
from fbpcs.data_processing.service.sharding_service import ShardingService, ShardType
from fbpcs.experimental.cloud_logs.log_retriever import CloudProvider, LogRetriever
from fbpcs.onedocker_binary_config import OneDockerBinaryConfig
from fbpcs.onedocker_binary_names import OneDockerBinaryNames
from fbpcs.pid.entity.pid_instance import PIDInstance
from fbpcs.pid.service.pid_service.pid_stage import PIDStage
from fbpcs.private_computation.entity.private_computation_instance import (
    PrivateComputationGameType,
    PrivateComputationInstance,
    PrivateComputationInstanceStatus,
    PrivateComputationRole,
)
from fbpcs.private_computation.service.constants import (
    DEFAULT_CONTAINER_TIMEOUT_IN_SEC,
    DEFAULT_LOG_COST_TO_S3,
)
from fbpcs.private_computation.service.private_computation_service_data import (
    PrivateComputationServiceData,
)


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
    repository_path: Optional[str] = None,
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

    env_vars = {}
    if repository_path:
        env_vars["ONEDOCKER_REPOSITORY_PATH"] = repository_path

    return await mpc_svc.start_instance_async(
        instance_id=instance_id,
        server_ips=server_ips,
        timeout=container_timeout or DEFAULT_CONTAINER_TIMEOUT_IN_SEC,
        version=binary_version,
        env_vars=env_vars,
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
    status = private_computation_instance.status
    if private_computation_instance.instances:
        # TODO: we should have some identifier or stage_name
        # to pick up the right instance instead of the last one
        last_instance = private_computation_instance.instances[-1]
        if not isinstance(last_instance, StageStateInstance):
            raise ValueError(
                f"The last instance type not StageStateInstance but {type(last_instance)}"
            )

        if stage_name is None:
            stage_name = last_instance.stage_name

        assert stage_name == private_computation_instance.current_stage.name
        # calling onedocker_svc to update newest containers in StageState
        stage_state_instance_status = last_instance.update_status(onedocker_svc)
        current_stage = private_computation_instance.current_stage
        if stage_state_instance_status is StageStateInstanceStatus.STARTED:
            status = current_stage.started_status
        elif stage_state_instance_status is StageStateInstanceStatus.COMPLETED:
            status = current_stage.completed_status
        elif stage_state_instance_status is StageStateInstanceStatus.FAILED:
            status = current_stage.failed_status

    return status


# TODO: If we're going to deprecate prepare_data_stage_service.py,
# we can just move this method to id_spine_combiner_stage_service.py as private method
async def start_combiner_service(
    private_computation_instance: PrivateComputationInstance,
    onedocker_svc: OneDockerService,
    onedocker_binary_config_map: DefaultDict[str, OneDockerBinaryConfig],
    combine_output_path: str,
    log_cost_to_s3: bool = DEFAULT_LOG_COST_TO_S3,
    wait_for_containers: bool = False,
    max_id_column_count: int = 1,
) -> List[ContainerInstance]:
    """Run combiner service and return those container instances

    Args:
        private_computation_instance: The PC instance to run combiner service with
        onedocker_svc: Spins up containers that run binaries in the cloud
        onedocker_binary_config_map: Stores a mapping from mpc game to OneDockerBinaryConfig (binary version and tmp directory)
        combine_output_path: out put path for the combine result
        log_cost_to_s3: if money cost of the computation will be logged to S3
        wait_for_containers: block until containers to finish running, default False

    Returns:
        return: list of container instances running combiner service
    """
    stage_data = PrivateComputationServiceData.get(
        private_computation_instance.game_type
    ).combiner_stage

    binary_name = stage_data.binary_name
    binary_config = onedocker_binary_config_map[binary_name]

    # TODO: T106159008 Add on attribution specific args
    if private_computation_instance.game_type is PrivateComputationGameType.ATTRIBUTION:
        run_name = private_computation_instance.instance_id if log_cost_to_s3 else ""
        padding_size = checked_cast(int, private_computation_instance.padding_size)
        log_cost = log_cost_to_s3
    else:
        run_name = None
        padding_size = None
        log_cost = None

    combiner_service = checked_cast(
        IdSpineCombinerService,
        stage_data.service,
    )

    args = combiner_service.build_args(
        spine_path=private_computation_instance.pid_stage_output_spine_path,
        data_path=private_computation_instance.pid_stage_output_data_path,
        output_path=combine_output_path,
        num_shards=private_computation_instance.num_pid_containers + 1
        if private_computation_instance.is_validating
        else private_computation_instance.num_pid_containers,
        tmp_directory=binary_config.tmp_directory,
        max_id_column_cnt=max_id_column_count,
        run_name=run_name,
        padding_size=padding_size,
        log_cost=log_cost,
    )
    env_vars = {"ONEDOCKER_REPOSITORY_PATH": binary_config.repository_path}
    return await combiner_service.start_containers(
        cmd_args_list=args,
        onedocker_svc=onedocker_svc,
        binary_version=binary_config.binary_version,
        binary_name=binary_name,
        timeout=None,
        wait_for_containers_to_finish=wait_for_containers,
        env_vars=env_vars,
    )


# TODO: If we're going to deprecate prepare_data_stage_service.py,
# we can just move this method to shard_stage_service.py as private method
async def start_sharder_service(
    private_computation_instance: PrivateComputationInstance,
    onedocker_svc: OneDockerService,
    onedocker_binary_config_map: DefaultDict[str, OneDockerBinaryConfig],
    combine_output_path: str,
    wait_for_containers: bool = False,
) -> List[ContainerInstance]:
    """Run combiner service and return those container instances

    Args:
        private_computation_instance: The PC instance to run sharder service with
        onedocker_svc: Spins up containers that run binaries in the cloud
        onedocker_binary_config_map: Stores a mapping from mpc game to OneDockerBinaryConfig (binary version and tmp directory)
        combine_output_path: out put path for the combine result
        wait_for_containers: block until containers to finish running, default False

    Returns:
        return: list of container instances running combiner service
    """
    sharder = ShardingService()
    logging.info("Instantiated sharder")

    args_list = []
    for shard_index in range(
        private_computation_instance.num_pid_containers + 1
        if private_computation_instance.is_validating
        else private_computation_instance.num_pid_containers
    ):
        path_to_shard = PIDStage.get_sharded_filepath(combine_output_path, shard_index)
        logging.info(f"Input path to sharder: {path_to_shard}")

        shards_per_file = math.ceil(
            (
                private_computation_instance.num_mpc_containers
                / private_computation_instance.num_pid_containers
            )
            * private_computation_instance.num_files_per_mpc_container
        )
        shard_index_offset = shard_index * shards_per_file
        logging.info(
            f"Output base path to sharder: {private_computation_instance.data_processing_output_path}, {shard_index_offset=}"
        )

        binary_config = onedocker_binary_config_map[OneDockerBinaryNames.SHARDER.value]
        args_per_shard = sharder.build_args(
            filepath=path_to_shard,
            output_base_path=private_computation_instance.data_processing_output_path,
            file_start_index=shard_index_offset,
            num_output_files=shards_per_file,
            tmp_directory=binary_config.tmp_directory,
        )
        args_list.append(args_per_shard)

    binary_name = sharder.get_binary_name(ShardType.ROUND_ROBIN)
    env_vars = {"ONEDOCKER_REPOSITORY_PATH": binary_config.repository_path}
    return await sharder.start_containers(
        cmd_args_list=args_list,
        onedocker_svc=onedocker_svc,
        binary_version=binary_config.binary_version,
        binary_name=binary_name,
        timeout=None,
        wait_for_containers_to_finish=wait_for_containers,
        env_vars=env_vars,
    )


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
