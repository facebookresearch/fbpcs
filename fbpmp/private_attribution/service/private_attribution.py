#!/usr/bin/env python3
# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

import asyncio
import logging
import math
from typing import Any, DefaultDict, Dict, List, Optional

from fbpcp.entity.mpc_instance import MPCInstance, MPCInstanceStatus, MPCParty
from fbpcp.service.mpc import MPCService
from fbpcp.service.onedocker import OneDockerService
from fbpcp.service.storage import StorageService
from fbpcp.util.typing import checked_cast
from fbpmp.common.entity.pcs_mpc_instance import PCSMPCInstance
from fbpmp.data_processing.attribution_id_combiner.attribution_id_spine_combiner_cpp import (
    CppAttributionIdSpineCombinerService,
)
from fbpmp.data_processing.sharding.sharding import ShardType
from fbpmp.data_processing.sharding.sharding_cpp import CppShardingService
from fbpmp.onedocker_binary_config import OneDockerBinaryConfig
from fbpmp.onedocker_binary_names import OneDockerBinaryNames
from fbpmp.pid.entity.pid_instance import PIDInstance, PIDInstanceStatus
from fbpmp.pid.entity.pid_instance import PIDProtocol, PIDRole
from fbpmp.pid.service.pid_service.pid import PIDService
from fbpmp.pid.service.pid_service.pid_stage import PIDStage
from fbpmp.private_computation.entity.private_computation_instance import (
    PrivateComputationInstance,
    PrivateComputationInstanceStatus,
    PrivateComputationRole,
    UnionedPCInstance,
    UnionedPCInstanceStatus,
)
from fbpmp.private_computation.repository.private_computation_instance import (
    PrivateComputationInstanceRepository,
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


class PrivateAttributionService:
    MAX_ROWS_PER_PID_CONTAINER = 10000000
    TARGET_ROWS_PER_MPC_SHARD = 50000
    APPROX_BYTES_PER_PUBLISHER_ROW = 38

    def __init__(
        self,
        instance_repository: PrivateComputationInstanceRepository,
        mpc_svc: MPCService,
        pid_svc: PIDService,
        onedocker_svc: OneDockerService,
        onedocker_binary_config_map: DefaultDict[str, OneDockerBinaryConfig],
        storage_svc: StorageService,
    ) -> None:
        """Constructor of PrivateAttributionService
        instance_repository -- repository to CRUD PrivateAttributeInstance
        """
        self.instance_repository = instance_repository
        self.storage_svc = storage_svc
        self.mpc_svc = mpc_svc
        self.pid_svc = pid_svc
        self.onedocker_svc = onedocker_svc
        self.onedocker_binary_config_map = onedocker_binary_config_map
        self.logger: logging.Logger = logging.getLogger(__name__)

    def create_instance(
        self,
        instance_id: str,
        role: PrivateComputationRole,
        input_path: str,
        output_dir: str,
        hmac_key: str,
        num_pid_containers: int,
        num_mpc_containers: int,
        num_files_per_mpc_container: int,
        padding_size: int,
        logger: logging.Logger,
        concurrency: int = 1,
        k_anonymity_threshold: int = 0,
    ) -> PrivateComputationInstance:
        self.logger.info(f"Creating instance: {instance_id}")

        instance = PrivateComputationInstance(
            instance_id=instance_id,
            role=role,
            instances=[],
            status=PrivateComputationInstanceStatus.CREATED,
            status_update_ts=0,  # placeholder, not used by PA, will be used after PL+PA consolidation
            input_path=input_path,
            output_dir=output_dir,
            hmac_key=hmac_key,
            num_pid_containers=num_pid_containers,
            num_mpc_containers=num_mpc_containers,
            num_files_per_mpc_container=num_files_per_mpc_container,
            padding_size=padding_size,
            concurrency=concurrency,
            k_anonymity_threshold=k_anonymity_threshold,
        )

        self.instance_repository.create(instance)
        return instance

    def update_instance(self, instance_id: str) -> PrivateComputationInstance:
        pa_instance = self.instance_repository.read(instance_id)

        self.logger.info(f"Updating instance: {instance_id}")

        if pa_instance.instances:
            # Only need to update the last stage/instance
            last_instance = pa_instance.instances[-1]

            if isinstance(last_instance, PIDInstance):
                # PID service has to call update_instance to get the newest containers
                # information in case they are still running
                pa_instance.instances[-1] = self.pid_svc.update_instance(
                    last_instance.instance_id
                )
            elif isinstance(last_instance, MPCInstance):
                # MPC service has to call update_instance to get the newest containers
                # information in case they are still running
                pa_instance.instances[-1] = PCSMPCInstance.from_mpc_instance(
                    self.mpc_svc.update_instance(last_instance.instance_id)
                )
            else:
                raise ValueError("Unknow type of instance")

            pa_instance.status = (
                self._get_status_from_stage(pa_instance.instances[-1])
                or pa_instance.status
            )

        self.instance_repository.update(pa_instance)
        self.logger.info(f"Finished updating instance: {instance_id}")
        return pa_instance

    # PID stage
    def id_match(
        self,
        instance_id: str,
        protocol: PIDProtocol,
        pid_config: Dict[str, Any],
        server_ips: Optional[List[str]] = None,
        dry_run: Optional[bool] = False,
    ) -> PrivateComputationInstance:
        return asyncio.run(
            self.id_match_async(
                instance_id=instance_id,
                protocol=protocol,
                pid_config=pid_config,
                server_ips=server_ips,
                dry_run=dry_run,
            )
        )

    # Make an async version of id_match() so that it can be called by Thrift
    async def id_match_async(
        self,
        instance_id: str,
        protocol: PIDProtocol,
        pid_config: Dict[str, Any],
        server_ips: Optional[List[str]] = None,
        dry_run: Optional[bool] = False,
    ) -> PrivateComputationInstance:
        # Get the updated instance
        pa_instance = self.update_instance(instance_id)

        if pa_instance.role is PrivateComputationRole.PARTNER and not server_ips:
            raise ValueError("Missing server_ips for Partner")

        # default to be an empty string
        retry_counter_str = ""

        # Validate status of the instance
        if pa_instance.status is PrivateComputationInstanceStatus.CREATED:
            pa_instance.retry_counter = 0
        elif pa_instance.status is PrivateComputationInstanceStatus.ID_MATCHING_FAILED:
            pa_instance.retry_counter += 1
            retry_counter_str = str(pa_instance.retry_counter)
        elif pa_instance.status in [
            PrivateComputationInstanceStatus.ID_MATCHING_STARTED,
            PrivateComputationInstanceStatus.COMPUTATION_STARTED,
            PrivateComputationInstanceStatus.AGGREGATION_STARTED,
        ]:
            # Whether this is a normal run or a test run with dry_run=True, we would like to make sure that
            # the instance is no longer in a running state before starting a new operation
            raise ValueError(
                f"Cannot start a new operation when instance {instance_id} has status {pa_instance.status}."
            )
        elif not dry_run:
            raise ValueError(
                f"Instance {instance_id} has status {pa_instance.status}. Not ready for id matching."
            )

        # Create a new pid instance
        # TODO T98557692: remove all checked_casts in this class.
        # We will first have to make PL provide all attributes at PrivateComputationInstance
        # instance creation time, then mark the attributes required in PrivateComputationInstance,
        # then we can remove the checked_casts
        pid_instance_id = instance_id + "_id_match" + retry_counter_str
        pid_instance = self.pid_svc.create_instance(
            instance_id=pid_instance_id,
            protocol=PIDProtocol.UNION_PID,
            pid_role=self._map_pa_role_to_pid_role(pa_instance.role),
            num_shards=checked_cast(int, pa_instance.num_pid_containers),
            input_path=checked_cast(str, pa_instance.input_path),
            output_path=checked_cast(str, pa_instance.pid_stage_output_base_path),
            hmac_key=pa_instance.hmac_key,
        )

        # Push PID instance to PrivateComputationInstance.instances and update PA Instance status
        pid_instance.status = PIDInstanceStatus.STARTED
        pa_instance.instances.append(pid_instance)

        pid_instance.spine_path = checked_cast(
            str, pa_instance.pid_stage_output_spine_path
        )
        pid_instance.data_path = checked_cast(
            str, pa_instance.pid_stage_output_data_path
        )

        pa_instance.status = PrivateComputationInstanceStatus.ID_MATCHING_STARTED
        self.instance_repository.update(pa_instance)
        pa_instance = self.update_instance(instance_id)

        # Run pid
        # With the current design, it won't return until everything is done
        await self.pid_svc.run_instance(
            instance_id=pid_instance_id,
            pid_config=pid_config,
            server_ips=server_ips,
        )

        pa_instance = self.update_instance(instance_id)

        return pa_instance

    def prepare_data(
        self,
        instance_id: str,
        dry_run: Optional[bool] = None,
        log_cost_to_s3: bool = False,
    ) -> None:
        asyncio.run(
            self.prepare_data_async(
                instance_id=instance_id,
                dry_run=dry_run,
                log_cost_to_s3=log_cost_to_s3,
            )
        )

    async def prepare_data_async(
        self,
        instance_id: str,
        dry_run: Optional[bool] = None,
        log_cost_to_s3: bool = False,
    ) -> None:
        self.logger.info(f"[{self}] Starting CppAttributionIdSpineCombinerService")
        # Get the updated instance
        pa_instance = self.update_instance(instance_id)

        # Validate status of the instance
        if not dry_run and (
            pa_instance.status
            is not PrivateComputationInstanceStatus.ID_MATCHING_COMPLETED
        ):
            raise ValueError(
                f"Instance {instance_id} has status {pa_instance.status}. Not ready for data prep stage."
            )

        output_path = checked_cast(str, pa_instance.data_processing_output_path)
        combine_output_path = output_path + "_combine"
        # execute combiner step
        combiner_service = CppAttributionIdSpineCombinerService()
        binary_config = self.onedocker_binary_config_map[
            OneDockerBinaryNames.ATTRIBUTION_ID_SPINE_COMBINER.value
        ]
        await combiner_service.combine_on_container_async(
            spine_path=checked_cast(str, pa_instance.pid_stage_output_spine_path),
            data_path=checked_cast(str, pa_instance.pid_stage_output_data_path),
            output_path=combine_output_path,
            num_shards=checked_cast(int, pa_instance.num_pid_containers),
            run_name=pa_instance.instance_id if log_cost_to_s3 else "",
            onedocker_svc=self.onedocker_svc,
            tmp_directory=binary_config.tmp_directory,
            padding_size=checked_cast(int, pa_instance.padding_size),
            binary_version=binary_config.binary_version,
        )

        logging.info("Finished running CombinerService, starting to reshard")

        # reshard each file into x shards
        #     note we need each file to be sharded into the same # of files
        #     because we want to keep the data of each existing file to run
        #     on the same container
        sharder = CppShardingService()

        logging.info("Instantiated sharder")

        coros = []
        for shard_index in range(checked_cast(int, pa_instance.num_pid_containers)):
            path_to_shard = PIDStage.get_sharded_filepath(
                combine_output_path, shard_index
            )
            shards_per_file = math.ceil(
                (
                    checked_cast(int, pa_instance.num_mpc_containers)
                    / checked_cast(int, pa_instance.num_pid_containers)
                )
                * checked_cast(int, pa_instance.num_files_per_mpc_container)
            )
            logging.info(f"Input path to sharder: {path_to_shard}")
            shard_index_offset = shard_index * shards_per_file

            logging.info(
                f"Output base path to sharder: {output_path}, {shard_index_offset=}"
            )

            binary_config = self.onedocker_binary_config_map[
                OneDockerBinaryNames.SHARDER.value
            ]
            coro = sharder.shard_on_container_async(
                shard_type=ShardType.ROUND_ROBIN,
                filepath=path_to_shard,
                output_base_path=output_path,
                file_start_index=shard_index_offset,
                num_output_files=shards_per_file,
                onedocker_svc=self.onedocker_svc,
                binary_version=binary_config.binary_version,
                tmp_directory=binary_config.tmp_directory,
            )
            coros.append(coro)

        # Wait for all coroutines to finish
        await asyncio.gather(*coros)
        logging.info("All sharding coroutines finished")

    def _validate_compute_attribute_inputs(
        self,
        pa_instance: PrivateComputationInstance,
        server_ips: Optional[List[str]],
        dry_run: Optional[bool],
    ) -> str:
        if pa_instance.role is PrivateComputationRole.PARTNER and not server_ips:
            raise ValueError("Missing server_ips")

        # default to be an empty string
        retry_counter_str = ""

        # Validate status of the instance
        if pa_instance.status is PrivateComputationInstanceStatus.ID_MATCHING_COMPLETED:
            pa_instance.retry_counter = 0
        elif pa_instance.status in [
            PrivateComputationInstanceStatus.COMPUTATION_FAILED,
            PrivateComputationInstanceStatus.COMPUTATION_STARTED,
            PrivateComputationInstanceStatus.COMPUTATION_COMPLETED,
        ]:
            pa_instance.retry_counter += 1
            retry_counter_str = str(pa_instance.retry_counter)
        elif not dry_run:
            raise ValueError(
                f"Instance {pa_instance.instance_id} has status {pa_instance.status}. Not ready for computing metrics."
            )
        return retry_counter_str

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
        timeout = container_timeout or DEFAULT_CONTAINER_TIMEOUT_IN_SEC
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
            version=binary_version,
            timeout=timeout,
        )

    def compute_attribute(
        self,
        instance_id: str,
        game_name: str,
        attribution_rule: str,
        aggregation_type: str,
        server_ips: Optional[List[str]] = None,
        dry_run: Optional[bool] = None,
        log_cost_to_s3: bool = False,
        container_timeout: Optional[int] = None,
    ) -> PrivateComputationInstance:
        return asyncio.run(
            self.compute_attribute_async(
                instance_id=instance_id,
                game_name=game_name,
                attribution_rule=attribution_rule,
                aggregation_type=aggregation_type,
                server_ips=server_ips,
                dry_run=dry_run,
                log_cost_to_s3=log_cost_to_s3,
                container_timeout=container_timeout,
            )
        )

    async def compute_attribute_async(
        self,
        instance_id: str,
        game_name: str,
        attribution_rule: str,
        aggregation_type: str,
        server_ips: Optional[List[str]] = None,
        dry_run: Optional[bool] = None,
        log_cost_to_s3: bool = False,
        container_timeout: Optional[int] = None,
    ) -> PrivateComputationInstance:
        # Get the updated instance
        pa_instance = self.update_instance(instance_id)

        retry_counter_str = self._validate_compute_attribute_inputs(
            pa_instance, server_ips, dry_run
        )

        logging.info("Starting to run MPC instance.")
        # Create and start MPC instance
        game_args = [
            {
                "aggregators": aggregation_type,
                "input_base_path": pa_instance.data_processing_output_path,
                "output_base_path": pa_instance.compute_stage_output_base_path,
                "attribution_rules": attribution_rule,
                "concurrency": pa_instance.concurrency,
                "num_files": pa_instance.num_files_per_mpc_container,
                "file_start_index": i
                * checked_cast(int, pa_instance.num_files_per_mpc_container),
                "use_xor_encryption": True,
                "run_name": pa_instance.instance_id if log_cost_to_s3 else "",
                "max_num_touchpoints": pa_instance.padding_size,
                "max_num_conversions": pa_instance.padding_size,
            }
            for i in range(checked_cast(int, pa_instance.num_mpc_containers))
        ]
        binary_config = self.onedocker_binary_config_map[
            OneDockerBinaryNames.ATTRIBUTION_COMPUTE.value
        ]
        mpc_instance = await self._create_and_start_mpc_instance(
            instance_id=instance_id + "_compute_metrics" + retry_counter_str,
            game_name=game_name,
            mpc_party=self._map_pa_role_to_mpc_party(pa_instance.role),
            num_containers=checked_cast(int, pa_instance.num_mpc_containers),
            binary_version=binary_config.binary_version,
            server_ips=server_ips,
            game_args=game_args,
            container_timeout=container_timeout,
        )

        logging.info("Finished running MPC instance.")

        # Push MPC instance to PrivateComputationInstance.instances and update PL Instance status
        pa_instance.instances.append(PCSMPCInstance.from_mpc_instance(mpc_instance))
        pa_instance.status = PrivateComputationInstanceStatus.COMPUTATION_STARTED
        self.instance_repository.update(pa_instance)
        return pa_instance

    def _validate_aggregate_shards_inputs(
        self,
        pa_instance: PrivateComputationInstance,
        server_ips: Optional[List[str]],
        dry_run: Optional[bool],
    ) -> str:
        if pa_instance.role is PrivateComputationRole.PARTNER and not server_ips:
            raise ValueError("Missing server_ips")

        # default to be an empty string
        retry_counter_str = ""

        # Validate status of the instance
        if pa_instance.status is PrivateComputationInstanceStatus.COMPUTATION_COMPLETED:
            pa_instance.retry_counter = 0
        elif pa_instance.status is PrivateComputationInstanceStatus.AGGREGATION_FAILED:
            pa_instance.retry_counter += 1
            retry_counter_str = str(pa_instance.retry_counter)
        elif pa_instance.status in [
            PrivateComputationInstanceStatus.ID_MATCHING_STARTED,
            PrivateComputationInstanceStatus.COMPUTATION_STARTED,
            PrivateComputationInstanceStatus.AGGREGATION_STARTED,
        ]:
            # Whether this is a normal run or a test run with dry_run=True, we would like to make sure that
            # the instance is no longer in a running state before starting a new operation
            raise ValueError(
                f"Cannot start a new operation when instance {pa_instance.instance_id} has status {pa_instance.status}."
            )
        elif not dry_run:
            raise ValueError(
                f"Instance {pa_instance.instance_id} has status {pa_instance.status}. Not ready for aggregating metrics."
            )
        return retry_counter_str

    def aggregate_shards(
        self,
        instance_id: str,
        game: str,
        server_ips: Optional[List[str]],
        dry_run: Optional[bool],
        log_cost_to_s3: bool,
        container_timeout: Optional[int] = None,
    ) -> PrivateComputationInstance:
        return asyncio.run(
            self.aggregate_shards_async(
                instance_id=instance_id,
                game=game,
                server_ips=server_ips,
                dry_run=dry_run,
                log_cost_to_s3=log_cost_to_s3,
                container_timeout=container_timeout,
            )
        )

    async def aggregate_shards_async(
        self,
        instance_id: str,
        game: str,
        server_ips: Optional[List[str]],
        dry_run: Optional[bool],
        log_cost_to_s3: bool,
        container_timeout: Optional[int] = None,
    ) -> PrivateComputationInstance:
        pa_instance = pa_instance = self.update_instance(instance_id)

        retry_counter_str = self._validate_aggregate_shards_inputs(
            pa_instance, server_ips, dry_run
        )

        # Create and start MPC instance
        game_args = [
            {
                "input_base_path": pa_instance.compute_stage_output_base_path,
                "output_path": pa_instance.shard_aggregate_stage_output_path,
                "num_shards": pa_instance.num_mpc_containers,
                "first_shard_index": 0,
                "threshold": pa_instance.k_anonymity_threshold,
                "run_name": pa_instance.instance_id if log_cost_to_s3 else "",
            }
        ]
        binary_config = self.onedocker_binary_config_map[
            OneDockerBinaryNames.SHARD_AGGREGATOR.value
        ]
        mpc_instance = await self._create_and_start_mpc_instance(
            instance_id=instance_id + "_aggregate_shards" + retry_counter_str,
            game_name=game,
            mpc_party=self._map_pa_role_to_mpc_party(pa_instance.role),
            num_containers=1,
            binary_version=binary_config.binary_version,
            server_ips=server_ips,
            # Below are all kwargs
            game_args=game_args,
            container_timeout=container_timeout,
        )

        pa_instance.instances.append(PCSMPCInstance.from_mpc_instance(mpc_instance))
        pa_instance.status = PrivateComputationInstanceStatus.AGGREGATION_STARTED
        self.instance_repository.update(pa_instance)

        return pa_instance

    def _map_pa_role_to_mpc_party(self, pa_role: PrivateComputationRole) -> MPCParty:
        return {
            PrivateComputationRole.PUBLISHER: MPCParty.SERVER,
            PrivateComputationRole.PARTNER: MPCParty.CLIENT,
        }[pa_role]

    def _map_pa_role_to_pid_role(self, pa_role: PrivateComputationRole) -> PIDRole:
        return {
            PrivateComputationRole.PUBLISHER: PIDRole.PUBLISHER,
            PrivateComputationRole.PARTNER: PIDRole.PARTNER,
        }[pa_role]

    """
    Get Private Attribution Service instance status from the given instance that represents a stage.
    Return None when no status returned from the mapper, indicating that we do not want
    the current status of the given stage to decide the status of Private Attribution Service instance.
    """

    def _get_status_from_stage(
        self, instance: UnionedPCInstance
    ) -> Optional[PrivateComputationInstanceStatus]:
        STAGE_TO_STATUS_MAPPER: Dict[
            str,
            Dict[UnionedPCInstanceStatus, PrivateComputationInstanceStatus],
        ] = {
            "compute": {
                MPCInstanceStatus.STARTED: PrivateComputationInstanceStatus.COMPUTATION_STARTED,
                MPCInstanceStatus.COMPLETED: PrivateComputationInstanceStatus.COMPUTATION_COMPLETED,
                MPCInstanceStatus.FAILED: PrivateComputationInstanceStatus.COMPUTATION_FAILED,
            },
            "PID": {
                PIDInstanceStatus.STARTED: PrivateComputationInstanceStatus.ID_MATCHING_STARTED,
                PIDInstanceStatus.COMPLETED: PrivateComputationInstanceStatus.ID_MATCHING_COMPLETED,
                PIDInstanceStatus.FAILED: PrivateComputationInstanceStatus.ID_MATCHING_FAILED,
            },
        }

        stage: str
        if isinstance(instance, MPCInstance):
            stage = "compute"
        elif isinstance(instance, PIDInstance):
            stage = "PID"
        else:
            raise ValueError(f"Unknow stage in instance: {instance}")

        status = STAGE_TO_STATUS_MAPPER[stage].get(instance.status)

        return status
