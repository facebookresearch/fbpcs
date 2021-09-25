#!/usr/bin/env python3
# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

import asyncio
import json
import logging
import math
from datetime import datetime, timezone
from typing import DefaultDict, Dict, List, Optional, Any, TypeVar

from fbpcp.entity.container_instance import ContainerInstanceStatus
from fbpcp.entity.mpc_instance import MPCInstance, MPCInstanceStatus, MPCParty
from fbpcp.service.mpc import MPCService
from fbpcp.service.onedocker import OneDockerService
from fbpcp.util.typing import checked_cast
from fbpcs.common.entity.pcs_mpc_instance import PCSMPCInstance
from fbpcs.data_processing.attribution_id_combiner.attribution_id_spine_combiner_cpp import (
    CppAttributionIdSpineCombinerService,
)
from fbpcs.data_processing.lift_id_combiner.lift_id_spine_combiner_cpp import (
    CppLiftIdSpineCombinerService,
)
from fbpcs.data_processing.sharding.sharding import ShardType
from fbpcs.data_processing.sharding.sharding_cpp import CppShardingService
from fbpcs.onedocker_binary_config import OneDockerBinaryConfig
from fbpcs.onedocker_binary_names import OneDockerBinaryNames
from fbpcs.pid.entity.pid_instance import PIDInstance, PIDInstanceStatus
from fbpcs.pid.entity.pid_instance import PIDProtocol, PIDRole
from fbpcs.pid.service.pid_service.pid import PIDService
from fbpcs.pid.service.pid_service.pid_stage import PIDStage
from fbpcs.post_processing_handler.post_processing_handler import (
    PostProcessingHandler,
    PostProcessingHandlerStatus,
)
from fbpcs.post_processing_handler.post_processing_instance import (
    PostProcessingInstance,
    PostProcessingInstanceStatus,
)
from fbpcs.private_computation.entity.private_computation_instance import (
    PrivateComputationGameType,
    PrivateComputationInstance,
    PrivateComputationInstanceStatus,
    PrivateComputationRole,
    UnionedPCInstance,
    UnionedPCInstanceStatus,
)
from fbpcs.private_computation.repository.private_computation_game import GameNames
from fbpcs.private_computation.repository.private_computation_instance import (
    PrivateComputationInstanceRepository,
)
from fbpcs.private_computation.service.private_computation_service_data import (
    PrivateComputationServiceData,
)
from fbpcs.private_lift.entity.breakdown_key import BreakdownKey
from fbpcs.private_lift.entity.pce_config import PCEConfig
from fbpcs.private_lift.service.errors import PLServiceValidationError
from fbpcs.private_computation.service.private_computation_stage_service import (
    PrivateComputationStageService,
)

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


class PrivateLiftService:
    def __init__(
        self,
        instance_repository: PrivateComputationInstanceRepository,
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
        game_type: PrivateComputationGameType,
        input_path: str,
        output_dir: str,
        num_pid_containers: int,
        num_mpc_containers: int,
        concurrency: int,
        num_files_per_mpc_container: Optional[int] = None,
        is_validating: Optional[bool] = False,
        synthetic_shard_path: Optional[str] = None,
        breakdown_key: Optional[BreakdownKey] = None,
        pce_config: Optional[PCEConfig] = None,
        is_test: Optional[bool] = False,
        hmac_key: Optional[str] = None,
        padding_size: int = DEFAULT_PADDING_SIZE,
        k_anonymity_threshold: int = DEFAULT_K_ANONYMITY_THRESHOLD,
        fail_fast: bool = False,
    ) -> PrivateComputationInstance:
        self.logger.info(f"Creating instance: {instance_id}")

        instance = PrivateComputationInstance(
            instance_id=instance_id,
            role=role,
            instances=[],
            status=PrivateComputationInstanceStatus.CREATED,
            status_update_ts=PrivateLiftService.get_ts_now(),
            num_files_per_mpc_container=num_files_per_mpc_container
            or NUM_NEW_SHARDS_PER_FILE,
            game_type=game_type,
            is_validating=is_validating,
            synthetic_shard_path=synthetic_shard_path,
            num_pid_containers=num_pid_containers,
            num_mpc_containers=num_mpc_containers,
            input_path=input_path,
            output_dir=output_dir,
            breakdown_key=breakdown_key,
            pce_config=pce_config,
            is_test=is_test,
            hmac_key=hmac_key,
            padding_size=padding_size,
            concurrency=concurrency,
            k_anonymity_threshold=k_anonymity_threshold,
            fail_fast=fail_fast,
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

    def _update_instance(
        self, pl_instance: PrivateComputationInstance
    ) -> PrivateComputationInstance:
        if pl_instance.instances:
            # Only need to update the last stage/instance
            last_instance = pl_instance.instances[-1]

            if isinstance(last_instance, PIDInstance):
                # PID service has to call update_instance to get the newest containers
                # information in case they are still running
                pl_instance.instances[-1] = self.pid_svc.update_instance(
                    last_instance.instance_id
                )
            elif isinstance(last_instance, MPCInstance):
                # MPC service has to call update_instance to get the newest containers
                # information in case they are still running
                pl_instance.instances[-1] = PCSMPCInstance.from_mpc_instance(
                    self.mpc_svc.update_instance(last_instance.instance_id)
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
            self.logger.info(f"Finished updating instance: {pl_instance.instance_id}")

        return pl_instance

    def run_stage(
        self,
        instance_id: str,
        stage_svc: PrivateComputationStageService,
        server_ips: Optional[List[str]] = None,
        dry_run: bool = False,
    ) -> PrivateComputationInstance:
        return asyncio.run(
            self.run_stage_async(instance_id, stage_svc, server_ips, dry_run)
        )

    def _get_validated_instance(
        self,
        instance_id: str,
        stage_svc: PrivateComputationStageService,
        server_ips: Optional[List[str]] = None,
        dry_run: bool = False,
    ) -> PrivateComputationInstance:
        """
        Gets a private computation instance and checks that it's ready to run a given
        stage service
        """
        pc_instance = self.get_instance(instance_id)
        if pc_instance.role is PrivateComputationRole.PARTNER and not server_ips:
            raise ValueError("Missing server_ips")

        # if the instance status is the complete status of the previous stage, then we can run the target stage
        # e.g. if status == ID_MATCH_COMPLETE, then we can run COMPUTE_METRICS
        if pc_instance.status is stage_svc.stage_type.previous_stage.completed_status:
            pc_instance.retry_counter = 0
        # if the instance status is the fail status of the target stage, then we can retry the target stage
        # e.g. if status == COMPUTE_METRICS_FAILED, then we can run COMPUTE_METRICS
        elif pc_instance.status is stage_svc.stage_type.failed_status:
            pc_instance.retry_counter += 1
        # if the instance status is a start status, it's running something already. Don't run another stage, even if dry_run=True
        elif pc_instance.status in STAGE_STARTED_STATUSES:
            raise ValueError(
                f"Cannot start a new operation when instance {instance_id} has status {pc_instance.status}."
            )
        # if dry_run = True, then we can run the target stage. Otherwise, throw an error
        elif not dry_run:
            raise ValueError(
                f"Instance {instance_id} has status {pc_instance.status}. Not ready for {stage_svc.stage_type}."
            )

        return pc_instance

    async def run_stage_async(
        self,
        instance_id: str,
        stage_svc: PrivateComputationStageService,
        server_ips: Optional[List[str]] = None,
        dry_run: bool = False,
    ) -> PrivateComputationInstance:
        """
        Runs a stage for a given instance. If state of the instance is invalid (e.g. not ready to run a stage),
        an exception will be thrown.
        """

        pc_instance = self._get_validated_instance(
            instance_id, stage_svc, server_ips, dry_run
        )

        self._update_status(
            pl_instance=pc_instance,
            new_status=stage_svc.stage_type.start_status,
        )
        self.instance_repository.update(pc_instance)
        try:
            pc_instance = await stage_svc.run_async(pc_instance, server_ips)
        except Exception as e:
            self.logger.error(f"Caught exception when running {stage_svc.stage_type}")
            self._update_status(
                pl_instance=pc_instance, new_status=stage_svc.stage_type.failed_status
            )
            raise e
        finally:
            self.instance_repository.update(pc_instance)
        return pc_instance

    # PID stage
    def id_match(
        self,
        instance_id: str,
        protocol: PIDProtocol,
        pid_config: Dict[str, Any],
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
            raise ValueError("Missing server_ips for Partner")

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

        # Create a new pid instance
        pid_instance_id = instance_id + "_id_match" + retry_counter_str
        # TODO T101225909: remove the option here to pass in a hmac key at the id match stage
        #   instead, always pass in at create_instance
        pl_instance.hmac_key = hmac_key or pl_instance.hmac_key
        pid_instance = self.pid_svc.create_instance(
            instance_id=pid_instance_id,
            protocol=PIDProtocol.UNION_PID,
            pid_role=self._map_pl_role_to_pid_role(pl_instance.role),
            num_shards=pl_instance.num_pid_containers,
            input_path=pl_instance.input_path,
            output_path=pl_instance.pid_stage_output_base_path,
            is_validating=is_validating,
            synthetic_shard_path=synthetic_shard_path,
            hmac_key=pl_instance.hmac_key,
        )

        # Push PID instance to PrivateComputationInstance.instances and update PL Instance status
        pid_instance.status = PIDInstanceStatus.STARTED
        pl_instance.instances.append(pid_instance)

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
            fail_fast=pl_instance.fail_fast,
            server_ips=server_ips,
        )

        return self.get_instance(instance_id)

    def prepare_data(
        self,
        instance_id: str,
        is_validating: Optional[bool] = False,
        dry_run: Optional[bool] = None,
        log_cost_to_s3: bool = False,
    ) -> None:
        asyncio.run(
            self.prepare_data_async(
                instance_id=instance_id,
                is_validating=is_validating,
                dry_run=dry_run,
                log_cost_to_s3=log_cost_to_s3,
            )
        )

    # TODO T88759390: Make this function truly async. It is not because it calls blocking functions.
    async def prepare_data_async(
        self,
        instance_id: str,
        is_validating: Optional[bool] = False,
        dry_run: Optional[bool] = None,
        log_cost_to_s3: bool = False,
    ) -> None:
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

        output_path = pl_instance.data_processing_output_path
        combine_output_path = output_path + "_combine"

        # execute combiner step
        if skip_tasks_on_container:
            self.logger.info(f"[{self}] Skipping id spine combiner service")
        else:
            self.logger.info(f"[{self}] Starting id spine combiner service")

            # TODO: we will write log_cost_to_s3 to the instance, so this function interface
            #   will get simplified
            await self._run_combiner_service(
                pl_instance, combine_output_path, log_cost_to_s3
            )

        self.logger.info("Finished running CombinerService, starting to reshard")

        # reshard each file into x shards
        #     note we need each file to be sharded into the same # of files
        #     because we want to keep the data of each existing file to run
        #     on the same container
        if skip_tasks_on_container:
            self.logger.info(f"[{self}] Skipping sharding on container")
        else:
            await self._run_sharder_service(pl_instance, combine_output_path)

    # MPC step 1
    def compute_metrics(
        self,
        instance_id: str,
        concurrency: Optional[int] = None,
        attribution_rule: Optional[str] = None,
        aggregation_type: Optional[str] = None,
        is_validating: Optional[bool] = False,
        server_ips: Optional[List[str]] = None,
        dry_run: Optional[bool] = None,
        log_cost_to_s3: bool = False,
        container_timeout: Optional[int] = None,
    ) -> PrivateComputationInstance:
        return asyncio.run(
            self.compute_metrics_async(
                instance_id,
                concurrency,
                attribution_rule,
                aggregation_type,
                is_validating,
                server_ips,
                dry_run,
                log_cost_to_s3,
                container_timeout,
            )
        )

    # TODO T88759390: Make this function truly async. It is not because it calls blocking functions.
    # Make an async version of compute_metrics() so that it can be called by Thrift
    async def compute_metrics_async(
        self,
        instance_id: str,
        concurrency: Optional[int] = None,
        attribution_rule: Optional[str] = None,
        aggregation_type: Optional[str] = None,
        is_validating: Optional[bool] = False,
        server_ips: Optional[List[str]] = None,
        dry_run: Optional[bool] = None,
        log_cost_to_s3: bool = False,
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
        # TODO T101225909: remove the option to pass in concurrency at the compute stage
        #   instead, always pass in at create_instance
        pl_instance.concurrency = concurrency or pl_instance.concurrency
        game_args = self._get_compute_metrics_game_args(
            pl_instance,
            attribution_rule,
            aggregation_type,
            is_validating,
            dry_run,
            log_cost_to_s3,
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

        stage_data = PrivateComputationServiceData.get(
            pl_instance.game_type
        ).compute_stage
        binary_name = stage_data.binary_name
        game_name = checked_cast(str, stage_data.game_name)

        binary_config = self.onedocker_binary_config_map[binary_name]
        mpc_instance = await self._create_and_start_mpc_instance(
            instance_id=instance_id + "_compute_metrics" + retry_counter_str,
            game_name=game_name,
            mpc_party=self._map_pl_role_to_mpc_party(pl_instance.role),
            num_containers=len(game_args),
            binary_version=binary_config.binary_version,
            server_ips=server_ips,
            game_args=game_args,
            container_timeout=container_timeout,
        )

        logging.info("MPC instance started running.")

        # Push MPC instance to PrivateComputationInstance.instances and update PL Instance status
        pl_instance.instances.append(PCSMPCInstance.from_mpc_instance(mpc_instance))
        pl_instance = self._update_status(
            pl_instance=pl_instance,
            new_status=PrivateComputationInstanceStatus.COMPUTATION_STARTED,
        )

        self.instance_repository.update(pl_instance)
        return pl_instance

    # MPC step 2
    def aggregate_shards(
        self,
        instance_id: str,
        is_validating: Optional[bool] = False,
        server_ips: Optional[List[str]] = None,
        dry_run: Optional[bool] = False,
        log_cost_to_s3: bool = False,
        container_timeout: Optional[int] = None,
    ) -> PrivateComputationInstance:
        return asyncio.run(
            self.aggregate_shards_async(
                instance_id,
                is_validating,
                server_ips,
                dry_run,
                log_cost_to_s3,
                container_timeout,
            )
        )

    # TODO T88759390: Make this function truly async. It is not because it calls blocking functions.
    # Make an async version of aggregate_shards() so that it can be called by Thrift
    async def aggregate_shards_async(
        self,
        instance_id: str,
        is_validating: Optional[bool] = False,
        server_ips: Optional[List[str]] = None,
        dry_run: Optional[bool] = False,
        log_cost_to_s3: bool = False,
        container_timeout: Optional[int] = None,
    ) -> PrivateComputationInstance:
        # It's expected that the pl instance is in an updated status because:
        #   For publisher, a Chronos job is scheduled to update it every 60 seconds;
        #   for partner, PL-Coordinator should have updated it before calling this action.
        pl_instance = self.get_instance(instance_id)

        if pl_instance.role is PrivateComputationRole.PARTNER and not server_ips:
            raise ValueError("Missing server_ips for Partner")

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

        num_shards = (
            pl_instance.num_mpc_containers * pl_instance.num_files_per_mpc_container
        )

        # TODO T101225989: map aggregation_type from the compute stage to metrics_format_type
        metrics_format_type = (
            "lift"
            if pl_instance.game_type is PrivateComputationGameType.LIFT
            else "ad_object"
        )

        binary_name = OneDockerBinaryNames.SHARD_AGGREGATOR.value
        binary_config = self.onedocker_binary_config_map[binary_name]

        if is_validating:
            # num_containers_real_data is the number of containers processing real data
            # synthetic data is processed by a dedicated extra container, and this container is always the last container,
            # hence synthetic_data_shard_start_index = num_real_data_shards
            # each of the containers, processing real or synthetic data, processes the same number of shards due to our resharding mechanism
            # num_shards representing the total number of shards which is equal to num_real_data_shards + num_synthetic_data_shards
            # hence, when num_containers_real_data and num_shards are given, num_synthetic_data_shards = num_shards / (num_containers_real_data + 1)
            num_containers_real_data = pl_instance.num_pid_containers
            if num_containers_real_data is None:
                raise ValueError("num_containers_real_data is None")
            num_synthetic_data_shards = num_shards // (num_containers_real_data + 1)
            num_real_data_shards = num_shards - num_synthetic_data_shards
            synthetic_data_shard_start_index = num_real_data_shards

            # Create and start MPC instance for real data shards and synthetic data shards
            game_args = [
                {
                    "input_base_path": pl_instance.compute_stage_output_base_path,
                    "num_shards": num_real_data_shards,
                    "metrics_format_type": metrics_format_type,
                    "output_path": pl_instance.shard_aggregate_stage_output_path,
                    "first_shard_index": 0,
                    "threshold": pl_instance.k_anonymity_threshold,
                    "run_name": pl_instance.instance_id if log_cost_to_s3 else "",
                },
                {
                    "input_base_path": pl_instance.compute_stage_output_base_path,
                    "num_shards": num_synthetic_data_shards,
                    "metrics_format_type": metrics_format_type,
                    "output_path": pl_instance.shard_aggregate_stage_output_path
                    + "_synthetic_data_shards",
                    "first_shard_index": synthetic_data_shard_start_index,
                    "threshold": pl_instance.k_anonymity_threshold,
                    "run_name": pl_instance.instance_id if log_cost_to_s3 else "",
                },
            ]

            mpc_instance = await self._create_and_start_mpc_instance(
                instance_id=instance_id + "_aggregate_shards" + retry_counter_str,
                game_name=GameNames.SHARD_AGGREGATOR.value,
                mpc_party=self._map_pl_role_to_mpc_party(pl_instance.role),
                num_containers=2,
                binary_version=binary_config.binary_version,
                server_ips=server_ips,
                game_args=game_args,
                container_timeout=container_timeout,
            )
        else:
            # Create and start MPC instance
            game_args = [
                {
                    "input_base_path": pl_instance.compute_stage_output_base_path,
                    "metrics_format_type": metrics_format_type,
                    "num_shards": num_shards,
                    "output_path": pl_instance.shard_aggregate_stage_output_path,
                    "threshold": pl_instance.k_anonymity_threshold,
                    "run_name": pl_instance.instance_id if log_cost_to_s3 else "",
                },
            ]
            mpc_instance = await self._create_and_start_mpc_instance(
                instance_id=instance_id + "_aggregate_shards" + retry_counter_str,
                game_name=GameNames.SHARD_AGGREGATOR.value,
                mpc_party=self._map_pl_role_to_mpc_party(pl_instance.role),
                num_containers=1,
                binary_version=binary_config.binary_version,
                server_ips=server_ips,
                game_args=game_args,
                container_timeout=container_timeout,
            )
        # Push MPC instance to PrivateComputationInstance.instances and update PL Instance status
        pl_instance.instances.append(PCSMPCInstance.from_mpc_instance(mpc_instance))
        self._update_status(
            pl_instance=pl_instance,
            new_status=PrivateComputationInstanceStatus.AGGREGATION_STARTED,
        )
        self.instance_repository.update(pl_instance)
        return pl_instance

    # TODO T88759390: make an async version of this function
    # Optioinal stage, validate the correctness of aggregated results for injected synthetic data
    def validate_metrics(
        self,
        instance_id: str,
        expected_result_path: str,
        aggregated_result_path: Optional[str] = None,
    ) -> None:
        pl_instance = self.get_instance(instance_id)
        storage_service = self.mpc_svc.storage_svc
        expected_results_dict = json.loads(storage_service.read(expected_result_path))
        aggregated_results_dict = json.loads(
            storage_service.read(
                aggregated_result_path or pl_instance.shard_aggregate_stage_output_path
            )
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
        MPC_GAME_TO_STAGE_MAPPER: Dict[str, str] = {
            GameNames.LIFT.value: "computation",
            GameNames.ATTRIBUTION_COMPUTE.value: "computation",
            GameNames.SHARD_AGGREGATOR.value: "aggregation",
        }

        STAGE_TO_STATUS_MAPPER: Dict[
            str,
            Dict[UnionedPCInstanceStatus, PrivateComputationInstanceStatus],
        ] = {
            "computation": {
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
        attribution_rule: Optional[str] = None,
        aggregation_type: Optional[str] = None,
        is_validating: Optional[bool] = False,
        dry_run: Optional[bool] = None,
        log_cost_to_s3: bool = False,
        container_timeout: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        game_args = []

        # If this is to recover from a previous MPC compute failure
        if self._ready_for_partial_container_retry(pl_instance) and not dry_run:
            game_args_to_retry = self._gen_game_args_to_retry(pl_instance)
            if game_args_to_retry:
                game_args = game_args_to_retry

        # If this is a normal run, dry_run, or unable to get the game args to retry from mpc service
        if not game_args:
            num_containers = pl_instance.num_mpc_containers
            # update num_containers if is_vaildating = true
            if is_validating:
                num_containers += 1

            common_compute_game_args = {
                "input_base_path": pl_instance.data_processing_output_path,
                "output_base_path": pl_instance.compute_stage_output_base_path,
                "num_files": pl_instance.num_files_per_mpc_container,
                "concurrency": pl_instance.concurrency,
            }

            # TODO: we eventually will want to get rid of the if-else here, which will be
            #   easy to do once the Lift and Attribution MPC compute games are consolidated
            if pl_instance.game_type is PrivateComputationGameType.ATTRIBUTION:
                # TODO: we will write aggregation_type, attribution_rule and log_cost_to_s3
                #   to the instance, so later this function interface will get simplified
                game_args = self._get_attribution_game_args(
                    pl_instance,
                    common_compute_game_args,
                    checked_cast(str, aggregation_type),
                    checked_cast(str, attribution_rule),
                    log_cost_to_s3,
                )

            elif pl_instance.game_type is PrivateComputationGameType.LIFT:
                game_args = self._get_lift_game_args(
                    pl_instance, common_compute_game_args
                )

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
            and pl_instance.status
            is PrivateComputationInstanceStatus.COMPUTATION_FAILED
        )

    async def _run_combiner_service(
        self,
        pl_instance: PrivateComputationInstance,
        combine_output_path: str,
        log_cost_to_s3: bool,
    ) -> None:
        stage_data = PrivateComputationServiceData.get(
            pl_instance.game_type
        ).combiner_stage

        binary_name = stage_data.binary_name
        binary_config = self.onedocker_binary_config_map[binary_name]

        common_combiner_args = {
            "spine_path": pl_instance.pid_stage_output_spine_path,
            "data_path": pl_instance.pid_stage_output_data_path,
            "output_path": combine_output_path,
            "num_shards": pl_instance.num_pid_containers + 1
            if pl_instance.is_validating
            else pl_instance.num_pid_containers,
            "onedocker_svc": self.onedocker_svc,
            "binary_version": binary_config.binary_version,
            "tmp_directory": binary_config.tmp_directory,
        }

        # TODO T100977304: the if-else will be removed after the two combiners are consolidated
        if pl_instance.game_type is PrivateComputationGameType.LIFT:
            combiner_service = checked_cast(
                CppLiftIdSpineCombinerService,
                stage_data.service,
            )
            await combiner_service.combine_on_container_async(
                # pyre-ignore [6] Incompatible parameter type
                **common_combiner_args
            )
        elif pl_instance.game_type is PrivateComputationGameType.ATTRIBUTION:
            combiner_service = checked_cast(
                CppAttributionIdSpineCombinerService,
                stage_data.service,
            )
            common_combiner_args["run_name"] = (
                pl_instance.instance_id if log_cost_to_s3 else ""
            )
            common_combiner_args["padding_size"] = checked_cast(
                int, pl_instance.padding_size
            )
            await combiner_service.combine_on_container_async(
                # pyre-ignore [6] Incompatible parameter type
                **common_combiner_args
            )

    async def _run_sharder_service(
        self, pl_instance: PrivateComputationInstance, combine_output_path: str
    ) -> None:
        sharder = CppShardingService()
        self.logger.info("Instantiated sharder")

        coros = []
        for shard_index in range(
            pl_instance.num_pid_containers + 1
            if pl_instance.is_validating
            else pl_instance.num_pid_containers
        ):
            path_to_shard = PIDStage.get_sharded_filepath(
                combine_output_path, shard_index
            )
            self.logger.info(f"Input path to sharder: {path_to_shard}")

            shards_per_file = math.ceil(
                (pl_instance.num_mpc_containers / pl_instance.num_pid_containers)
                * pl_instance.num_files_per_mpc_container
            )
            shard_index_offset = shard_index * shards_per_file
            self.logger.info(
                f"Output base path to sharder: {pl_instance.data_processing_output_path}, {shard_index_offset=}"
            )

            binary_config = self.onedocker_binary_config_map[
                OneDockerBinaryNames.SHARDER.value
            ]
            coro = sharder.shard_on_container_async(
                shard_type=ShardType.ROUND_ROBIN,
                filepath=path_to_shard,
                output_base_path=pl_instance.data_processing_output_path,
                file_start_index=shard_index_offset,
                num_output_files=shards_per_file,
                onedocker_svc=self.onedocker_svc,
                binary_version=binary_config.binary_version,
                tmp_directory=binary_config.tmp_directory,
            )
            coros.append(coro)

        # Wait for all coroutines to finish
        await asyncio.gather(*coros)
        self.logger.info("All sharding coroutines finished")

    def _get_attribution_game_args(
        self,
        pl_instance: PrivateComputationInstance,
        common_compute_game_args: Dict[str, Any],
        aggregation_type: str,
        attribution_rule: str,
        log_cost_to_s3: bool,
    ) -> List[Dict[str, Any]]:
        game_args = []
        game_args = [
            {
                **common_compute_game_args,
                **{
                    "aggregators": aggregation_type,
                    "attribution_rules": attribution_rule,
                    "file_start_index": i * pl_instance.num_files_per_mpc_container,
                    "use_xor_encryption": True,
                    "run_name": pl_instance.instance_id if log_cost_to_s3 else "",
                    "max_num_touchpoints": pl_instance.padding_size,
                    "max_num_conversions": pl_instance.padding_size,
                },
            }
            for i in range(pl_instance.num_mpc_containers)
        ]
        return game_args

    def _get_lift_game_args(
        self,
        pl_instance: PrivateComputationInstance,
        common_compute_game_args: Dict[str, Any],
    ) -> List[Dict[str, Any]]:
        game_args = []
        game_args = [
            {
                **common_compute_game_args,
                **{"file_start_index": i * pl_instance.num_files_per_mpc_container},
            }
            for i in range(pl_instance.num_mpc_containers)
        ]
        return game_args
