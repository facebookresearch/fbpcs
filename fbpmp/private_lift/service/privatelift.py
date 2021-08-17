#!/usr/bin/env python3
# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

import asyncio
import json
import logging
from datetime import datetime, timezone
from typing import DefaultDict, Dict, List, Optional, Any, TypeVar, Tuple, Iterator

from fbpcp.entity.container_instance import ContainerInstanceStatus
from fbpcp.entity.mpc_instance import MPCInstance, MPCInstanceStatus, MPCParty
from fbpcp.service.mpc import MPCService
from fbpcp.service.onedocker import OneDockerService
from fbpmp.data_processing.lift_id_combiner.lift_id_spine_combiner_cpp import (
    CppLiftIdSpineCombinerService,
)
from fbpmp.data_processing.sharding.sharding import ShardType
from fbpmp.data_processing.sharding.sharding_cpp import CppShardingService
from fbpmp.onedocker_binary_config import OneDockerBinaryConfig
from fbpmp.onedocker_binary_names import OneDockerBinaryNames
from fbpmp.pid.entity.pid_instance import PIDInstance, PIDInstanceStatus
from fbpmp.pid.entity.pid_instance import PIDProtocol, PIDRole
from fbpmp.pid.entity.pid_stages import UnionPIDStage
from fbpmp.pid.service.pid_service.pid import PIDService
from fbpmp.pid.service.pid_service.pid_stage import PIDStage
from fbpmp.pid.service.pid_service.pid_stage_mapper import STAGE_TO_FILE_FORMAT_MAP
from fbpmp.post_processing_handler.post_processing_handler import (
    PostProcessingHandler,
    PostProcessingHandlerStatus,
)
from fbpmp.post_processing_handler.post_processing_instance import (
    PostProcessingInstance,
    PostProcessingInstanceStatus,
)
from fbpmp.private_lift.entity.breakdown_key import BreakdownKey
from fbpmp.private_lift.entity.pce_config import PCEConfig
from fbpmp.private_computation.entity.private_computation_instance import (
    PrivateComputationInstance,
    PrivateComputationInstanceStatus,
    PrivateComputationRole,
    UnionedPCInstance,
    UniondePCInstanceStatus,
)
from fbpmp.private_lift.repository.privatelift_instance import (
    PrivateLiftInstanceRepository,
)
from fbpmp.private_lift.service.errors import PLServiceValidationError

T = TypeVar("T")

"""
43200 s = 12 hrs

We want to be conservative on this timeout just in case:
1) partner side is not able to connect in time. This is possible because it's a manual process
to run partner containers and humans can be slow;
2) during development, we add logic or complexity to the binaries running inside the containers
so that they take more than a few hours to run.
"""
DEFAULT_CONTAINER_TIMEOUT_IN_SEC = 43200


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


class PrivateLiftService:
    def __init__(
        self,
        instance_repository: PrivateLiftInstanceRepository,
        mpc_svc: MPCService,
        pid_svc: PIDService,
        onedocker_svc: OneDockerService,
        onedocker_binary_config_map: DefaultDict[str, OneDockerBinaryConfig],
    ) -> None:
        """Constructor of PrivateLiftService
        instance_repository -- repository to CRUD PrivateComputationInstance
        """
        self.instance_repository = instance_repository
        self.mpc_svc = mpc_svc
        self.pid_svc = pid_svc
        self.onedocker_svc = onedocker_svc
        self.onedocker_binary_config_map = onedocker_binary_config_map
        self.logger: logging.Logger = logging.getLogger(__name__)

    # TODO T88759390: make an async version of this function
    def create_instance(
        self,
        instance_id: str,
        role: PrivateComputationRole,
        num_containers: Optional[int] = None,
        input_path: Optional[str] = None,
        output_dir: Optional[str] = None,
        is_validating: Optional[bool] = False,
        synthetic_shard_path: Optional[str] = None,
        breakdown_key: Optional[BreakdownKey] = None,
        pce_config: Optional[PCEConfig] = None,
        is_test: Optional[bool] = False,
    ) -> PrivateComputationInstance:
        self.logger.info(f"Creating instance: {instance_id}")

        instance = PrivateComputationInstance(
            instance_id=instance_id,
            role=role,
            instances=[],
            status=PrivateComputationInstanceStatus.CREATED,
            status_update_ts=PrivateLiftService.get_ts_now(),
            is_validating=is_validating,
            synthetic_shard_path=synthetic_shard_path,
            num_containers=num_containers,
            input_path=input_path,
            output_dir=output_dir,
            breakdown_key=breakdown_key,
            pce_config=pce_config,
            is_test=is_test,
        )

        self.instance_repository.create(instance)
        return instance

    # TODO T88759390: make an async version of this function
    def get_instance(self, instance_id: str) -> PrivateComputationInstance:
        return self.instance_repository.read(instance_id=instance_id)

    # TODO T88759390: make an async version of this function
    def update_instance(self, instance_id: str) -> PrivateComputationInstance:
        pl_instance = self.instance_repository.read(instance_id)
        self.logger.info(f"Updating instance: {instance_id}")
        return self._update_instance(pl_instance=pl_instance)

    def _update_instance(self, pl_instance: PrivateComputationInstance) -> PrivateComputationInstance:
        if pl_instance.instances:
            # Only need to update the last stage/instance
            last_instance = pl_instance.instances[-1]

            if isinstance(last_instance, PIDInstance):
                # PID service simply reads instance information from repo
                pl_instance.instances[-1] = self.pid_svc.get_instance(
                    last_instance.instance_id
                )
            elif isinstance(last_instance, MPCInstance):
                # MPC service has to call update_instance to get the newest containers
                # information in case they are still running
                pl_instance.instances[-1] = self.mpc_svc.update_instance(
                    last_instance.instance_id
                )
            elif isinstance(last_instance, PostProcessingInstance):
                self.logger.info(
                    "PostProcessingInstance doesn't have its own instance repository and is already updated"
                )
            else:
                raise ValueError("Unknown type of instance")

            new_status = (
                self._get_status_from_stage(pl_instance.instances[-1])
                or pl_instance.status
            )
            pl_instance = self._update_status(
                pl_instance=pl_instance, new_status=new_status
            )
            self.instance_repository.update(pl_instance)

        return pl_instance

    # PID stage
    def id_match(
        self,
        instance_id: str,
        protocol: PIDProtocol,
        pid_config: Dict[str, Any],
        fail_fast: bool,
        num_containers: Optional[int] = None,
        input_path: Optional[str] = None,
        output_path: Optional[str] = None,
        is_validating: Optional[bool] = False,
        synthetic_shard_path: Optional[str] = None,
        server_ips: Optional[List[str]] = None,
        hmac_key: Optional[str] = None,
        dry_run: Optional[bool] = False,
    ) -> PrivateComputationInstance:
        return asyncio.run(
            self.id_match_async(
                instance_id,
                protocol,
                pid_config,
                fail_fast,
                num_containers,
                input_path,
                output_path,
                is_validating,
                synthetic_shard_path,
                server_ips,
                hmac_key,
                dry_run,
            )
        )

    # TODO T88759390: Make this function truly async. It is not because it calls blocking functions.
    # Make an async version of id_match() so that it can be called by Thrift
    async def id_match_async(
        self,
        instance_id: str,
        protocol: PIDProtocol,
        pid_config: Dict[str, Any],
        fail_fast: bool,
        num_containers: Optional[int] = None,
        input_path: Optional[str] = None,
        output_path: Optional[str] = None,
        is_validating: Optional[bool] = False,
        synthetic_shard_path: Optional[str] = None,
        server_ips: Optional[List[str]] = None,
        hmac_key: Optional[str] = None,
        dry_run: Optional[bool] = False,
    ) -> PrivateComputationInstance:
        # It's expected that the pl instance is in an updated status because:
        #   For publisher, a Chronos job is scheduled to update it every 60 seconds;
        #   for partner, PL-Coordinator should have updated it before calling this action.
        pl_instance = self.get_instance(instance_id)

        if pl_instance.role is PrivateComputationRole.PARTNER and not server_ips:
            raise ValueError("Missing server_ips")

        # default to be an empty string
        retry_counter_str = ""

        # Validate status of the instance
        if pl_instance.status is PrivateComputationInstanceStatus.CREATED:
            pl_instance.retry_counter = 0
        elif pl_instance.status is PrivateComputationInstanceStatus.ID_MATCHING_FAILED:
            pl_instance.retry_counter += 1
            retry_counter_str = str(pl_instance.retry_counter)
        elif pl_instance.status in STAGE_STARTED_STATUSES:
            # Whether this is a normal run or a test run with dry_run=True, we would like to make sure that
            # the instance is no longer in a running state before starting a new operation
            raise ValueError(
                f"Cannot start a new operation when instance {instance_id} has status {pl_instance.status}."
            )
        elif not dry_run:
            raise ValueError(
                f"Instance {instance_id} has status {pl_instance.status}. Not ready for id matching."
            )

        # If num_containers or input_path is not given as a parameter, get it from pl instance
        num_containers = self._get_param("num_containers", pl_instance.num_containers, num_containers)
        input_path = self._get_param("input_path", pl_instance.input_path, input_path)

        # If output_path is not given as a parameter, get it from pid_stage_output_base_path
        output_path = self._get_param("output_path", pl_instance.pid_stage_output_base_path, output_path)

        # Create a new pid instance
        pid_instance_id = instance_id + "_id_match" + retry_counter_str
        pid_instance = self.pid_svc.create_instance(
            instance_id=pid_instance_id,
            protocol=PIDProtocol.UNION_PID,
            pid_role=self._map_pl_role_to_pid_role(pl_instance.role),
            num_shards=num_containers,
            input_path=input_path,
            output_path=output_path,
            is_validating=is_validating,
            synthetic_shard_path=synthetic_shard_path,
            hmac_key=hmac_key,
        )

        # Push PID instance to PrivateComputationInstance.instances and update PL Instance status
        pid_instance.status = PIDInstanceStatus.STARTED
        pl_instance.instances.append(pid_instance)
        pl_instance.num_containers = num_containers

        # TODO T87544375: remove interdependency for PID internals
        spine_path_suffix = (
            STAGE_TO_FILE_FORMAT_MAP[UnionPIDStage.PUBLISHER_RUN_PID]
            if pl_instance.role is PrivateComputationRole.PUBLISHER
            else STAGE_TO_FILE_FORMAT_MAP[UnionPIDStage.ADV_RUN_PID]
        )
        data_path_suffix = (
            STAGE_TO_FILE_FORMAT_MAP[UnionPIDStage.PUBLISHER_SHARD]
            if pl_instance.role is PrivateComputationRole.PUBLISHER
            else STAGE_TO_FILE_FORMAT_MAP[UnionPIDStage.ADV_SHARD]
        )
        pl_instance.spine_path = f"{output_path}{spine_path_suffix}"
        pl_instance.data_path = f"{output_path}{data_path_suffix}"

        pl_instance = self._update_status(
            pl_instance=pl_instance,
            new_status=PrivateComputationInstanceStatus.ID_MATCHING_STARTED,
        )
        self.instance_repository.update(pl_instance)

        # Run pid
        # With the current design, it won't return until everything is done
        await self.pid_svc.run_instance(
            instance_id=pid_instance_id,
            pid_config=pid_config,
            fail_fast=fail_fast,
            server_ips=server_ips,
        )

        return self.get_instance(instance_id)

    def prepare_data(
        self,
        instance_id: str,
        output_path: str,
        num_containers: Optional[int] = None,
        is_validating: Optional[bool] = False,
        spine_path: Optional[str] = None,
        data_path: Optional[str] = None,
        dry_run: Optional[bool] = None,
    ) -> List[str]:
        return asyncio.run(
            self.prepare_data_async(
                instance_id,
                output_path,
                num_containers,
                is_validating,
                spine_path,
                data_path,
                dry_run,
            )
        )

    # TODO T88759390: Make this function truly async. It is not because it calls blocking functions.
    async def prepare_data_async(
        self,
        instance_id: str,
        output_path: str,
        num_containers: Optional[int] = None,
        is_validating: Optional[bool] = False,
        spine_path: Optional[str] = None,
        data_path: Optional[str] = None,
        dry_run: Optional[bool] = None,
    ) -> List[str]:
        # It's expected that the pl instance is in an updated status because:
        #   For publisher, a Chronos job is scheduled to update it every 60 seconds;
        #   for partner, PL-Coordinator should have updated it before calling this action.
        pl_instance = self.get_instance(instance_id)

        # Validate status of the instance
        if not dry_run and (
            pl_instance.status
            not in [
                PrivateComputationInstanceStatus.ID_MATCHING_COMPLETED,
                PrivateComputationInstanceStatus.COMPUTATION_FAILED,
            ]
        ):
            raise ValueError(
                f"Instance {instance_id} has status {pl_instance.status}. Not ready for data prep stage."
            )

        # If this request is made to recover from a previous mpc compute failure,
        #   then we skip the actual tasks running on containers. It's still necessary
        #   to run this function just because the caller needs the returned all_output_paths
        skip_tasks_on_container = (
            self._ready_for_partial_container_retry(pl_instance) and not dry_run
        )

        # If num_containers, spine_path or data_path is not given as a parameter, get it from pl instance.
        num_containers = self._get_param(
            "num_containers", pl_instance.num_containers, num_containers
        )
        spine_path = self._get_param("spine_path", pl_instance.spine_path, spine_path)
        data_path = self._get_param("data_path", pl_instance.data_path, data_path)

        combine_output_path = output_path + "_combine"
        # execute combiner step
        if skip_tasks_on_container:
            self.logger.info(f"[{self}] Skipping CppLiftIdSpineCombinerService")
        else:
            self.logger.info(f"[{self}] Starting CppLiftIdSpineCombinerService")
            combiner_service = CppLiftIdSpineCombinerService()
            binary_name = OneDockerBinaryNames.LIFT_ID_SPINE_COMBINER.value
            await combiner_service.combine_on_container_async(
                spine_path=spine_path,
                data_path=data_path,
                output_path=combine_output_path,
                num_shards=num_containers + 1
                if pl_instance.is_validating
                else num_containers,
                onedocker_svc=self.onedocker_svc,
                binary_version=self.onedocker_binary_config_map[binary_name].binary_version,
                tmp_directory=self.onedocker_binary_config_map[binary_name].tmp_directory,
            )

        logging.info("Finished running CombinerService, starting to reshard")

        # reshard each file into x shards
        #     note we need each file to be sharded into the same # of files
        #     because we want to keep the data of each existing file to run
        #     on the same container
        MAX_ROWS_PER_PID_CONTAINER = 10000000
        TARGET_ROWS_PER_MPC_CONTAINER = 250000
        num_new_shards_per_file = round(
            MAX_ROWS_PER_PID_CONTAINER / TARGET_ROWS_PER_MPC_CONTAINER
        )
        sharder = CppShardingService()

        logging.info("Instantiated sharder")

        all_output_paths = []

        coros = []
        for shard_index in range(
            num_containers + 1 if pl_instance.is_validating else num_containers
        ):
            path_to_shard = PIDStage.get_sharded_filepath(
                combine_output_path, shard_index
            )
            logging.info(f"Input path to sharder: {path_to_shard}")
            shard_index_offset = shard_index * num_new_shards_per_file
            output_paths = [
                PIDStage.get_sharded_filepath(output_path, shard + shard_index_offset)
                for shard in range(num_new_shards_per_file)
            ]
            all_output_paths += output_paths
            logging.info(
                f"Output base path to sharder: {output_path}, {shard_index_offset=}"
            )

            if skip_tasks_on_container:
                self.logger.info(f"[{self}] Skipping sharding on container")
            else:
                binary_name = OneDockerBinaryNames.SHARDER.value
                coro = sharder.shard_on_container_async(
                    shard_type=ShardType.ROUND_ROBIN,
                    filepath=path_to_shard,
                    output_base_path=output_path,
                    file_start_index=shard_index_offset,
                    num_output_files=num_new_shards_per_file,
                    onedocker_svc=self.onedocker_svc,
                    binary_version=self.onedocker_binary_config_map[binary_name].binary_version,
                    tmp_directory=self.onedocker_binary_config_map[binary_name].tmp_directory,
                )
                coros.append(coro)

        # Wait for all coroutines to finish
        await asyncio.gather(*coros)
        logging.info("All sharding coroutines finished")

        return all_output_paths

    # MPC step 1
    def compute_metrics(
        self,
        instance_id: str,
        game_name: str,
        input_files: List[str],
        output_files: List[str],
        concurrency: int,
        num_containers: Optional[int] = None,
        is_validating: Optional[bool] = False,
        server_ips: Optional[List[str]] = None,
        dry_run: Optional[bool] = None,
        container_timeout: Optional[int] = None,
    ) -> PrivateComputationInstance:
        return asyncio.run(
            self.compute_metrics_async(
                instance_id,
                game_name,
                input_files,
                output_files,
                concurrency,
                num_containers,
                is_validating,
                server_ips,
                dry_run,
                container_timeout,
            )
        )

    def calculate_file_start_index_and_num_shards(
        self,
        input_files: List[str],
        num_containers: int,
    ) -> Iterator[Tuple[int, int]]:
        """
        Calculate the file start index and number of shards to run per worker
        Examples:
        len(input_files) = 4, num_containers = 4 -> [(0, 1), (1, 1), (2, 1), (3, 1)]
        len(input_files) = 5, num_containers = 4 -> [(0, 2), (2, 1), (3, 1), (4, 1)]
        len(input_files) = 6, num_containers = 4 -> [(0, 2), (2, 2), (4, 1), (5, 1)]
        len(input_files) = 7, num_containers = 4 -> [(0, 2), (2, 2), (4, 2), (6, 1)]
        len(input_files) = 8, num_containers = 4 -> [(0, 2), (2, 2), (4, 2), (6, 2)]
        """
        file_start_index = 0
        for i in range(num_containers):
            num_files = len(input_files[i::num_containers])
            yield file_start_index, num_files
            file_start_index += num_files

    # TODO T88759390: Make this function truly async. It is not because it calls blocking functions.
    # Make an async version of compute_metrics() so that it can be called by Thrift
    async def compute_metrics_async(
        self,
        instance_id: str,
        game_name: str,
        input_files: List[str],
        output_files: List[str],
        concurrency: int,
        num_containers: Optional[int] = None,
        is_validating: Optional[bool] = False,
        server_ips: Optional[List[str]] = None,
        dry_run: Optional[bool] = None,
        container_timeout: Optional[int] = None,
    ) -> PrivateComputationInstance:
        # validate len(input_files)==len(output_files)
        if len(input_files) != len(output_files):
            raise ValueError(
                f"There're {len(input_files)} input file(s) but {len(output_files)} output file(s). # input files should equal to # output files"
            )

        # It's expected that the pl instance is in an updated status because:
        #   For publisher, a Chronos job is scheduled to update it every 60 seconds;
        #   for partner, PL-Coordinator should have updated it before calling this action.
        pl_instance = self.get_instance(instance_id)

        if pl_instance.role is PrivateComputationRole.PARTNER and not server_ips:
            raise ValueError("Missing server_ips")

        # default to be an empty string
        retry_counter_str = ""

        # Validate status of the instance
        if pl_instance.status is PrivateComputationInstanceStatus.ID_MATCHING_COMPLETED:
            pl_instance.retry_counter = 0
        elif pl_instance.status is PrivateComputationInstanceStatus.COMPUTATION_FAILED:
            pl_instance.retry_counter += 1
            retry_counter_str = str(pl_instance.retry_counter)
        elif pl_instance.status in STAGE_STARTED_STATUSES:
            # Whether this is a normal run or a test run with dry_run=True, we would like to make sure that
            # the instance is no longer in a running state before starting a new operation
            raise ValueError(
                f"Cannot start a new operation when instance {instance_id} has status {pl_instance.status}."
            )
        elif not dry_run:
            raise ValueError(
                f"Instance {instance_id} has status {pl_instance.status}. Not ready for computing metrics."
            )

        # Prepare arguments for lift game
        game_args = self._get_compute_metrics_game_args(
            pl_instance,
            input_files,
            output_files,
            concurrency,
            num_containers,
            is_validating,
            dry_run,
            container_timeout,
        )

        # We do this check here because depends on how game_args is generated, len(game_args) could be different,
        #   but we will always expect server_ips == len(game_args)
        if server_ips and len(server_ips) != len(game_args):
            raise ValueError(
                f"Unable to rerun MPC compute because there is a mismatch between the number of server ips given ({len(server_ips)}) and the number of containers ({len(game_args)}) to be spawned."
            )

        # Create and start MPC instance to run MPC compute
        logging.info("Starting to run MPC instance.")
        binary_name=OneDockerBinaryNames.LIFT_COMPUTE.value
        mpc_instance = await self._create_and_start_mpc_instance(
            instance_id=instance_id + "_compute_metrics" + retry_counter_str,
            game_name=game_name,
            mpc_party=self._map_pl_role_to_mpc_party(pl_instance.role),
            num_containers=len(game_args),
            binary_version=self.onedocker_binary_config_map[binary_name].binary_version,
            server_ips=server_ips,
            game_args=game_args,
            container_timeout=container_timeout,
        )

        logging.info("MPC instance started running.")

        # Push MPC instance to PrivateComputationInstance.instances and update PL Instance status
        pl_instance.instances.append(mpc_instance)
        pl_instance = self._update_status(
            pl_instance=pl_instance,
            new_status=PrivateComputationInstanceStatus.COMPUTATION_STARTED,
        )
        pl_instance.compute_output_path = output_files[0][
            :-2
        ]  # get the 1st output_files and remove suffix "_0"
        pl_instance.compute_num_shards = len(output_files)
        self.instance_repository.update(pl_instance)
        return pl_instance

    # MPC step 2
    def aggregate_metrics(
        self,
        instance_id: str,
        output_path: Optional[str] = None,
        input_path: Optional[str] = None,
        num_shards: Optional[int] = None,
        is_validating: Optional[bool] = False,
        server_ips: Optional[List[str]] = None,
        dry_run: Optional[bool] = False,
        container_timeout: Optional[int] = None,
    ) -> PrivateComputationInstance:
        return asyncio.run(
            self.aggregate_metrics_async(
                instance_id,
                output_path,
                input_path,
                num_shards,
                is_validating,
                server_ips,
                dry_run,
                container_timeout,
            )
        )

    # TODO T88759390: Make this function truly async. It is not because it calls blocking functions.
    # Make an async version of aggregate_metrics() so that it can be called by Thrift
    async def aggregate_metrics_async(
        self,
        instance_id: str,
        output_path: Optional[str] = None,
        input_path: Optional[str] = None,
        num_shards: Optional[int] = None,
        is_validating: Optional[bool] = False,
        server_ips: Optional[List[str]] = None,
        dry_run: Optional[bool] = False,
        container_timeout: Optional[int] = None,
    ) -> PrivateComputationInstance:
        # It's expected that the pl instance is in an updated status because:
        #   For publisher, a Chronos job is scheduled to update it every 60 seconds;
        #   for partner, PL-Coordinator should have updated it before calling this action.
        pl_instance = self.get_instance(instance_id)

        if pl_instance.role is PrivateComputationRole.PARTNER and not server_ips:
            raise ValueError("Missing server_ips")

        # default to be an empty string
        retry_counter_str = ""

        # Validate status of the instance
        if pl_instance.status is PrivateComputationInstanceStatus.COMPUTATION_COMPLETED:
            pl_instance.retry_counter = 0
        elif pl_instance.status is PrivateComputationInstanceStatus.AGGREGATION_FAILED:
            pl_instance.retry_counter += 1
            retry_counter_str = str(pl_instance.retry_counter)
        elif pl_instance.status in STAGE_STARTED_STATUSES:
            # Whether this is a normal run or a test run with dry_run=True, we would like to make sure that
            # the instance is no longer in a running state before starting a new operation
            raise ValueError(
                f"Cannot start a new operation when instance {instance_id} has status {pl_instance.status}."
            )
        elif not dry_run:
            raise ValueError(
                f"Instance {instance_id} has status {pl_instance.status}. Not ready for aggregating metrics."
            )

        # If input_path or num_shards is not given as a parameter, get it from pl instance.
        input_path = self._get_param(
            "input_path", pl_instance.compute_output_path, input_path
        )
        num_shards = self._get_param(
            "num_shards", pl_instance.compute_num_shards, num_shards
        )

        # If output_path is not given as a parameter, get it from shard_aggregate_stage_output_path
        output_path = self._get_param("output_path", pl_instance.shard_aggregate_stage_output_path, output_path)

        if is_validating:
            # num_containers_real_data is the number of containers processing real data
            # synthetic data is processed by a dedicated extra container, and this container is always the last container,
            # hence synthetic_data_shard_start_index = num_real_data_shards
            # each of the containers, processing real or synthetic data, processes the same number of shards due to our resharding mechanism
            # num_shards representing the total number of shards which is equal to num_real_data_shards + num_synthetic_data_shards
            # hence, when num_containers_real_data and num_shards are given, num_synthetic_data_shards = num_shards / (num_containers_real_data + 1)
            num_containers_real_data = pl_instance.num_containers
            if num_containers_real_data is None:
                raise ValueError("num_containers_real_data is None")
            num_synthetic_data_shards = num_shards // (num_containers_real_data + 1)
            num_real_data_shards = num_shards - num_synthetic_data_shards
            synthetic_data_shard_start_index = num_real_data_shards
            # Create and start MPC instance for real data shards and synthetic data shards
            game_args = [
                {
                    "input_base_path": input_path,
                    "num_shards": num_real_data_shards,
                    "metrics_format_type": "lift",
                    "output_path": output_path,
                    "first_shard_index": 0,
                },
                {
                    "input_base_path": input_path,
                    "num_shards": num_synthetic_data_shards,
                    "metrics_format_type": "lift",
                    "output_path": output_path + "_synthetic_data_shards",
                    "first_shard_index": synthetic_data_shard_start_index,
                },
            ]
            binary_name=OneDockerBinaryNames.SHARD_AGGREGATOR.value
            mpc_instance = await self._create_and_start_mpc_instance(
                instance_id=instance_id + "_aggregate_metrics" + retry_counter_str,
                game_name="shard_aggregator",
                mpc_party=self._map_pl_role_to_mpc_party(pl_instance.role),
                num_containers=2,
                binary_version=self.onedocker_binary_config_map[binary_name].binary_version,
                server_ips=server_ips,
                game_args=game_args,
                container_timeout=container_timeout,
            )
        else:
            # Create and start MPC instance
            game_args = [
                {
                    "input_base_path": input_path,
                    "metrics_format_type": "lift",
                    "num_shards": num_shards,
                    "output_path": output_path,
                },
            ]
            binary_name=OneDockerBinaryNames.SHARD_AGGREGATOR.value
            mpc_instance = await self._create_and_start_mpc_instance(
                instance_id=instance_id + "_aggregate_metrics" + retry_counter_str,
                game_name="shard_aggregator",
                mpc_party=self._map_pl_role_to_mpc_party(pl_instance.role),
                num_containers=1,
                binary_version=self.onedocker_binary_config_map[binary_name].binary_version,
                server_ips=server_ips,
                game_args=game_args,
                container_timeout=container_timeout,
            )
        # Push MPC instance to PrivateComputationInstance.instances and update PL Instance status
        pl_instance.instances.append(mpc_instance)
        self._update_status(
            pl_instance=pl_instance,
            new_status=PrivateComputationInstanceStatus.AGGREGATION_STARTED,
        )
        pl_instance.aggregated_result_path = output_path
        self.instance_repository.update(pl_instance)
        return pl_instance

    # TODO T88759390: make an async version of this function
    # Optioinal stage, validate the correctness of aggregated results for injected synthetic data
    def validate_metrics(
        self,
        instance_id: str,
        aggregated_result_path: str,
        expected_result_path: str,
    ) -> None:
        storage_service = self.mpc_svc.storage_svc
        expected_results_dict = json.loads(storage_service.read(expected_result_path))
        aggregated_results_dict = json.loads(
            storage_service.read(aggregated_result_path)
        )
        if expected_results_dict == aggregated_results_dict:
            self.logger.info(
                f"Aggregated results for instance {instance_id} on synthetic data is as expected."
            )
        else:
            raise PLServiceValidationError(
                f"Aggregated results for instance {instance_id} on synthetic data is NOT as expected."
            )

    def run_post_processing_handlers(
        self,
        instance_id: str,
        post_processing_handlers: Dict[str, PostProcessingHandler],
        aggregated_result_path: Optional[str] = None,
        dry_run: Optional[bool] = False,
    ) -> PrivateComputationInstance:
        return asyncio.run(
            self.run_post_processing_handlers_async(
                instance_id,
                post_processing_handlers,
                aggregated_result_path,
                dry_run,
            )
        )

    # Make an async version of run_post_processing_handlers so that
    # it can be called by Thrift
    async def run_post_processing_handlers_async(
        self,
        instance_id: str,
        post_processing_handlers: Dict[str, PostProcessingHandler],
        aggregated_result_path: Optional[str] = None,
        dry_run: Optional[bool] = False,
    ) -> PrivateComputationInstance:
        # It's expected that the pl instance is in an updated status because:
        #   For publisher, a Chronos job is scheduled to update it every 60 seconds;
        #   for partner, PL-Coordinator should have updated it before calling this action.
        pl_instance = self.get_instance(instance_id)
        post_processing_handlers_statuses = None

        # default to be an empty string
        retry_counter_str = ""

        # Validate status of the instance
        if pl_instance.status is PrivateComputationInstanceStatus.AGGREGATION_COMPLETED:
            pl_instance.retry_counter = 0
        elif (
            pl_instance.status
            is PrivateComputationInstanceStatus.POST_PROCESSING_HANDLERS_FAILED
        ):
            pl_instance.retry_counter += 1
            retry_counter_str = str(pl_instance.retry_counter)
            # copies the last instance's handler status so that we can
            # avoid reattempting already successfully completed handlers
            if pl_instance.instances:
                last_instance = pl_instance.instances[-1]
                if not isinstance(last_instance, PostProcessingInstance):
                    raise ValueError(
                        f"Expected PostProcessingInstance, found {type(last_instance)}"
                    )
                if (
                    last_instance.handler_statuses.keys()
                    == post_processing_handlers.keys()
                ):
                    self.logger.info("Copying statuses from last instance")
                    post_processing_handlers_statuses = (
                        last_instance.handler_statuses.copy()
                    )
        elif pl_instance.status in STAGE_STARTED_STATUSES:
            # Whether this is a normal run or a test run with dry_run=True, we would like to make sure that
            # the instance is no longer in a running state before starting a new operation
            raise ValueError(
                f"Cannot start a new operation when instance {instance_id} has status {pl_instance.status}."
            )
        elif not dry_run:
            raise ValueError(
                f"Instance {instance_id} has status {pl_instance.status}. Not ready for running post processing handlers."
            )

        pl_instance.aggregated_result_path = (
            pl_instance.aggregated_result_path or aggregated_result_path
        )

        post_processing_instance = PostProcessingInstance.create_instance(
            instance_id=instance_id + "_post_processing" + retry_counter_str,
            handlers=post_processing_handlers,
            handler_statuses=post_processing_handlers_statuses,
            status=PostProcessingInstanceStatus.STARTED,
        )

        pl_instance.instances.append(post_processing_instance)

        self._update_status(
            pl_instance=pl_instance,
            new_status=PrivateComputationInstanceStatus.POST_PROCESSING_HANDLERS_STARTED,
        )

        # if any handlers fail, then the post_processing_instance status will be
        # set to failed, as will the pl_instance status
        # self.instance_repository.update(pl_instance) is called each time within
        # the self._run_post_processing_handler method
        await asyncio.gather(
            *[
                self._run_post_processing_handler(
                    pl_instance, post_processing_instance, name, handler
                )
                for name, handler in post_processing_handlers.items()
                if post_processing_instance.handler_statuses[name]
                != PostProcessingHandlerStatus.COMPLETED
            ]
        )

        # if any of the handlers failed, then there is no need to update the status or the instance repository.
        # if they all suceeded, post_processing_instance status will be something other than FAILED and
        # post_processing_instance and pl_instance need status updates.
        if post_processing_instance.status != PostProcessingInstanceStatus.FAILED:
            post_processing_instance.status = PostProcessingInstanceStatus.COMPLETED
            self._update_status(
                pl_instance=pl_instance,
                new_status=PrivateComputationInstanceStatus.POST_PROCESSING_HANDLERS_COMPLETED,
            )
            await asyncio.get_running_loop().run_in_executor(
                None, self.instance_repository.update, pl_instance
            )

        return pl_instance

    def cancel_current_stage(
        self,
        instance_id: str,
    ) -> PrivateComputationInstance:
        pl_instance = self.get_instance(instance_id)

        # pre-checks to make sure it's in a cancel-able state
        if pl_instance.status not in STAGE_STARTED_STATUSES:
            raise ValueError(
                f"Instance {instance_id} has status {pl_instance.status}. Nothing to cancel."
            )

        if not pl_instance.instances:
            raise ValueError(
                f"Instance {instance_id} is in invalid state because no stages are registered under."
            )

        # cancel the running stage
        last_instance = pl_instance.instances[-1]
        if isinstance(last_instance, MPCInstance):
            self.mpc_svc.stop_instance(instance_id=last_instance.instance_id)
        else:
            self.logger.warning(
                f"Canceling the current stage of instance {instance_id} is not supported yet."
            )
            return pl_instance

        # post-checks to make sure the pl instance has the updated status
        pl_instance = self._update_instance(pl_instance=pl_instance)
        if pl_instance.status not in STAGE_FAILED_STATUSES:
            raise ValueError(
                f"Failed to cancel the current stage unexptectedly. Instance {instance_id} has status {pl_instance.status}"
            )

        self.logger.info(
            f"The current stage of instance {instance_id} has been canceled."
        )
        return pl_instance

    async def _run_post_processing_handler(
        self,
        pl_instance: PrivateComputationInstance,
        post_processing_instance: PostProcessingInstance,
        handler_name: str,
        handler: PostProcessingHandler,
    ) -> None:
        self.logger.info(f"Starting post processing handler: {handler_name=}")
        post_processing_instance.handler_statuses[
            handler_name
        ] = PostProcessingHandlerStatus.STARTED
        try:
            await handler.run(self, pl_instance)
            self.logger.info(f"Completed post processing handler: {handler_name=}")
            post_processing_instance.handler_statuses[
                handler_name
            ] = PostProcessingHandlerStatus.COMPLETED
        except Exception as e:
            self.logger.exception(e)
            self.logger.error(f"Failed post processing handler: {handler_name=}")
            post_processing_instance.handler_statuses[
                handler_name
            ] = PostProcessingHandlerStatus.FAILED
            post_processing_instance.status = PostProcessingInstanceStatus.FAILED
            self._update_status(
                pl_instance=pl_instance,
                new_status=PrivateComputationInstanceStatus.POST_PROCESSING_HANDLERS_FAILED,
            )
        finally:
            await asyncio.get_running_loop().run_in_executor(
                None, self.instance_repository.update, pl_instance
            )

    async def _create_and_start_mpc_instance(
        self,
        instance_id: str,
        game_name: str,
        mpc_party: MPCParty,
        num_containers: int,
        binary_version: str,
        server_ips: Optional[List[str]] = None,
        game_args: Optional[List[Dict[str, Any]]] = None,
        container_timeout: Optional[int] = None,
    ) -> MPCInstance:
        self.mpc_svc.create_instance(
            instance_id=instance_id,
            game_name=game_name,
            mpc_party=mpc_party,
            num_workers=num_containers,
            game_args=game_args,
        )
        return await self.mpc_svc.start_instance_async(
            instance_id=instance_id,
            server_ips=server_ips,
            timeout=container_timeout or DEFAULT_CONTAINER_TIMEOUT_IN_SEC,
            version=binary_version,
        )

    def _map_pl_role_to_mpc_party(self, pl_role: PrivateComputationRole) -> MPCParty:
        return {
            PrivateComputationRole.PUBLISHER: MPCParty.SERVER,
            PrivateComputationRole.PARTNER: MPCParty.CLIENT,
        }[pl_role]

    def _map_pl_role_to_pid_role(self, pl_role: PrivateComputationRole) -> PIDRole:
        return {
            PrivateComputationRole.PUBLISHER: PIDRole.PUBLISHER,
            PrivateComputationRole.PARTNER: PIDRole.PARTNER,
        }[pl_role]

    """
    Get Private Lift instance status from the given instance that represents a stage.
    Return None when no status returned from the mapper, indicating that we do not want
    the current status of the given stage to decide the status of Private Lift instance.
    """

    def _get_status_from_stage(
        self, instance: UnionedPCInstance
    ) -> Optional[PrivateComputationInstanceStatus]:
        computation_str = "computation"
        MPC_GAME_TO_STAGE_MAPPER: Dict[str, str] = {
            "conversion_lift": computation_str,
            "converter_lift": computation_str,
            "secret_share_lift": computation_str,
            "lift": computation_str,
            "secret_share_conversion_lift": computation_str,
            "secret_share_converter_lift": computation_str,
            "shard_aggregator": "aggregation",
        }

        STAGE_TO_STATUS_MAPPER: Dict[
            str,
            Dict[UniondePCInstanceStatus, PrivateComputationInstanceStatus],
        ] = {
            computation_str: {
                MPCInstanceStatus.STARTED: PrivateComputationInstanceStatus.COMPUTATION_STARTED,
                MPCInstanceStatus.COMPLETED: PrivateComputationInstanceStatus.COMPUTATION_COMPLETED,
                MPCInstanceStatus.FAILED: PrivateComputationInstanceStatus.COMPUTATION_FAILED,
                MPCInstanceStatus.CANCELED: PrivateComputationInstanceStatus.COMPUTATION_FAILED,
            },
            "aggregation": {
                MPCInstanceStatus.STARTED: PrivateComputationInstanceStatus.AGGREGATION_STARTED,
                MPCInstanceStatus.COMPLETED: PrivateComputationInstanceStatus.AGGREGATION_COMPLETED,
                MPCInstanceStatus.FAILED: PrivateComputationInstanceStatus.AGGREGATION_FAILED,
                MPCInstanceStatus.CANCELED: PrivateComputationInstanceStatus.AGGREGATION_FAILED,
            },
            "PID": {
                PIDInstanceStatus.STARTED: PrivateComputationInstanceStatus.ID_MATCHING_STARTED,
                PIDInstanceStatus.COMPLETED: PrivateComputationInstanceStatus.ID_MATCHING_COMPLETED,
                PIDInstanceStatus.FAILED: PrivateComputationInstanceStatus.ID_MATCHING_FAILED,
            },
            "post_processing": {
                PostProcessingInstanceStatus.STARTED: PrivateComputationInstanceStatus.POST_PROCESSING_HANDLERS_STARTED,
                PostProcessingInstanceStatus.COMPLETED: PrivateComputationInstanceStatus.POST_PROCESSING_HANDLERS_COMPLETED,
                PostProcessingInstanceStatus.FAILED: PrivateComputationInstanceStatus.POST_PROCESSING_HANDLERS_FAILED,
            },
        }

        stage: str
        if isinstance(instance, MPCInstance):
            stage = MPC_GAME_TO_STAGE_MAPPER[instance.game_name]
        elif isinstance(instance, PIDInstance):
            stage = "PID"
        elif isinstance(instance, PostProcessingInstance):
            stage = "post_processing"
        else:
            raise ValueError(f"Unknown stage in instance: {instance}")

        status = STAGE_TO_STATUS_MAPPER[stage].get(instance.status)

        return status

    @staticmethod
    def get_ts_now() -> int:
        return int(datetime.now(tz=timezone.utc).timestamp())

    def _update_status(
        self,
        pl_instance: PrivateComputationInstance,
        new_status: PrivateComputationInstanceStatus,
    ) -> PrivateComputationInstance:
        old_status = pl_instance.status
        pl_instance.status = new_status
        if old_status != new_status:
            pl_instance.status_update_ts = PrivateLiftService.get_ts_now()
            self.logger.info(
                f"Updating status of {pl_instance.instance_id} from {old_status} to {pl_instance.status} at time {pl_instance.status_update_ts}"
            )
        return pl_instance

    def _get_param(
        self, param_name: str, instance_param: Optional[T], override_param: Optional[T]
    ) -> T:
        res = override_param
        if override_param is not None:
            if instance_param is not None and instance_param != override_param:
                self.logger.warning(
                    f"{param_name}={override_param} is given and will be used, "
                    f"but it is inconsistent with {instance_param} recorded in the PrivateComputationInstance"
                )
        else:
            res = instance_param
        if res is None:
            raise ValueError(f"Missing value for parameter {param_name}")

        return res

    def _get_compute_metrics_game_args(
        self,
        pl_instance: PrivateComputationInstance,
        input_files: List[str],
        output_files: List[str],
        concurrency: int,
        num_containers: Optional[int] = None,
        is_validating: Optional[bool] = False,
        dry_run: Optional[bool] = None,
        container_timeout: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        game_args = []
        # If this is to recover from a previous MPC compute failure
        if self._ready_for_partial_container_retry(pl_instance) and not dry_run:
            game_args = self._gen_game_args_to_retry(pl_instance)

        # If this is a normal run, dry_run, or unable to get the game args to retry from mpc service
        if not game_args:
            # If num_containers is not given, get it from pl instance.
            num_containers = self._get_param(
                "num_containers", pl_instance.num_containers, num_containers
            )
            # update num_containers if is_vaildating = true
            if is_validating:
                num_containers += 1

            # Note: input_files/output_files are actually "input_directory/input_filename" and
            #   "output_directory/output_filename" respectively for each item in the list

            # Example:
            #   input_file: "my_directory/my_file_0"
            #   input_base_path: my_directory/my_file
            input_base_path = input_files[0].rsplit("_", 1)[0]
            output_base_path = output_files[0].rsplit("_", 1)[0]

            game_args = [
                {
                    "input_base_path": input_base_path,
                    "output_base_path": output_base_path,
                    "file_start_index": file_start_index,
                    "num_files": num_files,
                    "concurrency": concurrency,
                }
                for file_start_index, num_files in self.calculate_file_start_index_and_num_shards(
                    input_files, num_containers
                )
            ]

        return game_args

    def _gen_game_args_to_retry(
        self, pl_instance: PrivateComputationInstance
    ) -> Optional[List[Dict[str, Any]]]:
        # Get the last mpc instance
        last_mpc_instance = pl_instance.instances[-1]

        # Validate the last instance
        if not isinstance(last_mpc_instance, MPCInstance):
            raise ValueError(
                f"The last instance of PrivateComputationInstance {pl_instance.instance_id} is NOT an MPCInstance"
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

    def _ready_for_partial_container_retry(
        self, pl_instance: PrivateComputationInstance
    ) -> bool:
        return (
            pl_instance.partial_container_retry_enabled
            and pl_instance.status is PrivateComputationInstanceStatus.COMPUTATION_FAILED
        )
