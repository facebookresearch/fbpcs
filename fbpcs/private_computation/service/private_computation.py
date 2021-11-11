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
from typing import DefaultDict, Dict, List, Optional, Any, Type, TypeVar

from fbpcp.entity.mpc_instance import MPCInstance
from fbpcp.service.mpc import MPCService
from fbpcp.service.onedocker import OneDockerService
from fbpcp.service.storage import StorageService
from fbpcs.onedocker_binary_config import OneDockerBinaryConfig
from fbpcs.pid.service.pid_service.pid import PIDService
from fbpcs.post_processing_handler.post_processing_handler import PostProcessingHandler
from fbpcs.private_computation.entity.breakdown_key import BreakdownKey
from fbpcs.private_computation.entity.pce_config import PCEConfig
from fbpcs.private_computation.entity.private_computation_base_stage_flow import (
    PrivateComputationBaseStageFlow,
)
from fbpcs.private_computation.entity.private_computation_instance import (
    AggregationType,
    AttributionRule,
    PrivateComputationGameType,
    PrivateComputationInstance,
    PrivateComputationInstanceStatus,
    PrivateComputationRole,
)
from fbpcs.private_computation.entity.private_computation_legacy_stage_flow import (
    PrivateComputationLegacyStageFlow,
)
from fbpcs.private_computation.entity.private_computation_stage_flow import (
    PrivateComputationStageFlow,
)
from fbpcs.private_computation.repository.private_computation_instance import (
    PrivateComputationInstanceRepository,
)
from fbpcs.private_computation.service.aggregate_shards_stage_service import (
    AggregateShardsStageService,
)
from fbpcs.private_computation.service.compute_metrics_stage_service import (
    ComputeMetricsStageService,
)
from fbpcs.private_computation.service.constants import (
    NUM_NEW_SHARDS_PER_FILE,
    STAGE_STARTED_STATUSES,
    STAGE_FAILED_STATUSES,
    DEFAULT_CONCURRENCY,
    DEFAULT_HMAC_KEY,
    DEFAULT_K_ANONYMITY_THRESHOLD,
    DEFAULT_PID_PROTOCOL,
    LIFT_DEFAULT_PADDING_SIZE,
    ATTRIBUTION_DEFAULT_PADDING_SIZE,
)
from fbpcs.private_computation.service.errors import (
    PrivateComputationServiceValidationError,
)
from fbpcs.private_computation.service.id_match_stage_service import IdMatchStageService
from fbpcs.private_computation.service.post_processing_stage_service import (
    PostProcessingStageService,
)
from fbpcs.private_computation.service.prepare_data_stage_service import (
    PrepareDataStageService,
)
from fbpcs.private_computation.service.private_computation_stage_service import (
    PrivateComputationStageService,
    PrivateComputationStageServiceArgs,
)
from fbpcs.private_computation.service.utils import (
    ready_for_partial_container_retry,
    deprecated,
)
from fbpcs.utils.optional import unwrap_or_default

T = TypeVar("T")


class PrivateComputationService:
    # TODO(T103302669): [BE] Add documentation to PrivateComputationService class
    def __init__(
        self,
        instance_repository: PrivateComputationInstanceRepository,
        storage_svc: StorageService,
        mpc_svc: MPCService,
        pid_svc: PIDService,
        onedocker_svc: OneDockerService,
        onedocker_binary_config_map: DefaultDict[str, OneDockerBinaryConfig],
        pid_config: Dict[str, Any],
        post_processing_handlers: Optional[Dict[str, PostProcessingHandler]] = None,
    ) -> None:
        """Constructor of PrivateComputationService
        instance_repository -- repository to CRUD PrivateComputationInstance
        """
        self.instance_repository = instance_repository
        self.storage_svc = storage_svc
        self.mpc_svc = mpc_svc
        self.pid_svc = pid_svc
        self.onedocker_svc = onedocker_svc
        self.onedocker_binary_config_map = onedocker_binary_config_map
        self.pid_config = pid_config
        self.post_processing_handlers: Dict[str, PostProcessingHandler] = (
            post_processing_handlers or {}
        )
        self.stage_service_args = PrivateComputationStageServiceArgs(
            self.pid_svc,
            self.pid_config,
            self.onedocker_binary_config_map,
            self.mpc_svc,
            self.storage_svc,
            self.post_processing_handlers,
            self.onedocker_svc,
        )
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
        concurrency: Optional[int] = None,
        attribution_rule: Optional[AttributionRule] = None,
        aggregation_type: Optional[AggregationType] = None,
        num_files_per_mpc_container: Optional[int] = None,
        is_validating: Optional[bool] = False,
        synthetic_shard_path: Optional[str] = None,
        breakdown_key: Optional[BreakdownKey] = None,
        pce_config: Optional[PCEConfig] = None,
        is_test: Optional[bool] = False,
        hmac_key: Optional[str] = None,
        padding_size: Optional[int] = None,
        k_anonymity_threshold: Optional[int] = None,
        fail_fast: bool = False,
        stage_flow_cls: Type[
            PrivateComputationBaseStageFlow
        ] = PrivateComputationStageFlow,
    ) -> PrivateComputationInstance:
        self.logger.info(f"Creating instance: {instance_id}")
        instance = PrivateComputationInstance(
            instance_id=instance_id,
            role=role,
            instances=[],
            status=PrivateComputationInstanceStatus.CREATED,
            status_update_ts=PrivateComputationService.get_ts_now(),
            num_files_per_mpc_container=unwrap_or_default(
                optional=num_files_per_mpc_container, default=NUM_NEW_SHARDS_PER_FILE
            ),
            game_type=game_type,
            is_validating=is_validating,
            synthetic_shard_path=synthetic_shard_path,
            num_pid_containers=num_pid_containers,
            num_mpc_containers=num_mpc_containers,
            attribution_rule=attribution_rule,
            aggregation_type=aggregation_type,
            input_path=input_path,
            output_dir=output_dir,
            breakdown_key=breakdown_key,
            pce_config=pce_config,
            is_test=is_test,
            hmac_key=unwrap_or_default(optional=hmac_key, default=DEFAULT_HMAC_KEY),
            padding_size=unwrap_or_default(
                optional=padding_size,
                default=LIFT_DEFAULT_PADDING_SIZE
                if game_type is PrivateComputationGameType.LIFT
                else ATTRIBUTION_DEFAULT_PADDING_SIZE,
            ),
            concurrency=concurrency or DEFAULT_CONCURRENCY,
            k_anonymity_threshold=unwrap_or_default(
                optional=k_anonymity_threshold, default=DEFAULT_K_ANONYMITY_THRESHOLD
            ),
            fail_fast=fail_fast,
            _stage_flow_cls_name=stage_flow_cls.get_cls_name(),
        )

        self.instance_repository.create(instance)
        return instance

    # TODO T88759390: make an async version of this function
    def get_instance(self, instance_id: str) -> PrivateComputationInstance:
        return self.instance_repository.read(instance_id=instance_id)

    # TODO T88759390: make an async version of this function
    def update_instance(self, instance_id: str) -> PrivateComputationInstance:
        private_computation_instance = self.instance_repository.read(instance_id)
        self.logger.info(f"Updating instance: {instance_id}")
        return self._update_instance(
            private_computation_instance=private_computation_instance
        )

    def _update_instance(
        self, private_computation_instance: PrivateComputationInstance
    ) -> PrivateComputationInstance:
        stage = private_computation_instance.current_stage
        stage_svc = stage.get_stage_service(self.stage_service_args)
        self.logger.info(f"Updating instance | {stage}={stage!r}")
        new_status = stage_svc.get_status(private_computation_instance)
        private_computation_instance = self._update_status(
            private_computation_instance, new_status
        )
        self.instance_repository.update(private_computation_instance)
        self.logger.info(
            f"Finished updating instance: {private_computation_instance.instance_id}"
        )

        return private_computation_instance

    def run_next(
        self, instance_id: str, server_ips: Optional[List[str]] = None
    ) -> PrivateComputationInstance:
        return asyncio.run(self.run_next_async(instance_id, server_ips))

    async def run_next_async(
        self, instance_id: str, server_ips: Optional[List[str]] = None
    ) -> PrivateComputationInstance:
        """Fetches the next eligible stage in the instance's stage flow and runs it"""
        pc_instance = self.get_instance(instance_id)
        next_stage = pc_instance.get_next_runnable_stage()
        if not next_stage:
            self.logger.warning("There are no eligble stages to be ran at this time.")
            return pc_instance
        return await self.run_stage_async(
            instance_id, next_stage, server_ips=server_ips
        )

    def run_stage(
        self,
        instance_id: str,
        stage: PrivateComputationBaseStageFlow,
        stage_svc: Optional[PrivateComputationStageService] = None,
        server_ips: Optional[List[str]] = None,
        dry_run: bool = False,
    ) -> PrivateComputationInstance:
        return asyncio.run(
            self.run_stage_async(instance_id, stage, stage_svc, server_ips, dry_run)
        )

    def _get_validated_instance(
        self,
        instance_id: str,
        stage: PrivateComputationBaseStageFlow,
        server_ips: Optional[List[str]] = None,
        dry_run: bool = False,
    ) -> PrivateComputationInstance:
        """
        Gets a private computation instance and checks that it's ready to run a given
        stage service
        """
        pc_instance = self.get_instance(instance_id)
        if (
            stage.is_joint_stage
            and pc_instance.role is PrivateComputationRole.PARTNER
            and not server_ips
        ):
            raise ValueError("Missing server_ips")

        # if the instance status is the complete status of the previous stage, then we can run the target stage
        # e.g. if status == ID_MATCH_COMPLETE, then we can run COMPUTE_METRICS
        if pc_instance.status is stage.previous_stage.completed_status:
            pc_instance.retry_counter = 0
        # if the instance status is the fail status of the target stage, then we can retry the target stage
        # e.g. if status == COMPUTE_METRICS_FAILED, then we can run COMPUTE_METRICS
        elif pc_instance.status is stage.failed_status:
            pc_instance.retry_counter += 1
        # if the instance status is a start status, it's running something already. Don't run another stage, even if dry_run=True
        elif stage.is_started_status(pc_instance.status):
            raise ValueError(
                f"Cannot start a new operation when instance {instance_id} has status {pc_instance.status}."
            )
        # if dry_run = True, then we can run the target stage. Otherwise, throw an error
        elif not dry_run:
            raise ValueError(
                f"Instance {instance_id} has status {pc_instance.status}. Not ready for {stage}."
            )

        return pc_instance

    # TODO T88759390: Make this function truly async. It is not because it calls blocking functions.
    # Make an async version of run_stage_async() so that it can be called by Thrift
    async def run_stage_async(
        self,
        instance_id: str,
        stage: PrivateComputationBaseStageFlow,
        stage_svc: Optional[PrivateComputationStageService] = None,
        server_ips: Optional[List[str]] = None,
        dry_run: bool = False,
    ) -> PrivateComputationInstance:
        """
        Runs a stage for a given instance. If state of the instance is invalid (e.g. not ready to run a stage),
        an exception will be thrown.
        """

        pc_instance = self._get_validated_instance(
            instance_id, stage, server_ips, dry_run
        )

        self._update_status(
            private_computation_instance=pc_instance,
            new_status=stage.started_status,
        )
        self.logger.info(repr(stage))
        try:
            stage_svc = stage_svc or stage.get_stage_service(self.stage_service_args)
            pc_instance = await stage_svc.run_async(pc_instance, server_ips)
        except Exception as e:
            self.logger.error(f"Caught exception when running {stage}\n{e}")
            self._update_status(
                private_computation_instance=pc_instance,
                new_status=stage.failed_status,
            )
            raise e
        finally:
            self.instance_repository.update(pc_instance)
        return pc_instance

    # PID stage
    @deprecated("DO NOT USE! This is replaced by the generic run_next + run_stage functions and will soon be deleted.")
    def id_match(
        self,
        instance_id: str,
        pid_config: Dict[str, Any],
        is_validating: Optional[bool] = False,
        synthetic_shard_path: Optional[str] = None,
        server_ips: Optional[List[str]] = None,
        dry_run: Optional[bool] = False,
    ) -> PrivateComputationInstance:
        return asyncio.run(
            self.id_match_async(
                instance_id,
                pid_config,
                is_validating,
                synthetic_shard_path,
                server_ips,
                dry_run,
            )
        )

    def _override_stage_flow(
        self, instance_id: str, flow: Type[PrivateComputationBaseStageFlow]
    ) -> None:
        """Replace the stage flow stored in an instance with a new flow"""
        instance = self.get_instance(instance_id)
        new_class_name = flow.get_cls_name()
        if instance._stage_flow_cls_name != new_class_name:
            self.logger.info(
                f"Changing stage flow of instance {instance_id} from {instance._stage_flow_cls_name} to {new_class_name}"
            )
            instance._stage_flow_cls_name = new_class_name
            self.instance_repository.update(instance)

    # TODD T101783992: delete this function and call run_stage directly
    @deprecated("DO NOT USE! This is replaced by the generic run_next + run_stage functions and will soon be deleted.")
    async def id_match_async(
        self,
        instance_id: str,
        pid_config: Dict[str, Any],
        is_validating: Optional[bool] = False,
        synthetic_shard_path: Optional[str] = None,
        server_ips: Optional[List[str]] = None,
        dry_run: Optional[bool] = False,
    ) -> PrivateComputationInstance:
        # if calling PC the legacy way, make sure that the legacy stage
        # flow is being used
        self._override_stage_flow(instance_id, PrivateComputationLegacyStageFlow)
        return await self.run_stage_async(
            instance_id,
            PrivateComputationLegacyStageFlow.ID_MATCH,
            IdMatchStageService(
                self.pid_svc,
                pid_config,
                DEFAULT_PID_PROTOCOL,
                is_validating or False,
                synthetic_shard_path,
            ),
            server_ips,
            dry_run or False,
        )

    @deprecated("DO NOT USE! This is replaced by the generic run_next + run_stage functions and will soon be deleted.")
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
    @deprecated("DO NOT USE! This is replaced by the generic run_next + run_stage functions and will soon be deleted.")
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
        private_computation_instance = self.get_instance(instance_id)

        # Validate status of the instance
        if not dry_run and (
            private_computation_instance.status
            not in [
                PrivateComputationInstanceStatus.ID_MATCHING_COMPLETED,
                PrivateComputationInstanceStatus.COMPUTATION_FAILED,
            ]
        ):
            raise ValueError(
                f"Instance {instance_id} has status {private_computation_instance.status}. Not ready for data prep stage."
            )

        # If this request is made to recover from a previous mpc compute failure,
        #   then we skip the actual tasks running on containers. It's still necessary
        #   to run this function just because the caller needs the returned all_output_paths
        skip_tasks_on_container = (
            ready_for_partial_container_retry(private_computation_instance)
            and not dry_run
        )

        # execute combiner step
        if skip_tasks_on_container:
            self.logger.info(f"[{self}] Skipping id spine combiner service")
            self.logger.info(f"[{self}] Skipping sharding on container")
        else:
            stage_svc = PrepareDataStageService(
                self.onedocker_svc,
                self.onedocker_binary_config_map,
                is_validating or False,
                log_cost_to_s3,
            )
            await stage_svc.run_async(private_computation_instance)

    # MPC step 1
    @deprecated("DO NOT USE! This is replaced by the generic run_next + run_stage functions and will soon be deleted.")
    def compute_metrics(
        self,
        instance_id: str,
        is_validating: Optional[bool] = False,
        server_ips: Optional[List[str]] = None,
        dry_run: Optional[bool] = None,
        log_cost_to_s3: bool = False,
        container_timeout: Optional[int] = None,
    ) -> PrivateComputationInstance:
        return asyncio.run(
            self.compute_metrics_async(
                instance_id,
                is_validating,
                server_ips,
                dry_run,
                log_cost_to_s3,
                container_timeout,
            )
        )

    # TODO T88759390: Make this function truly async. It is not because it calls blocking functions.
    # Make an async version of compute_metrics() so that it can be called by Thrift
    @deprecated("DO NOT USE! This is replaced by the generic run_next + run_stage functions and will soon be deleted.")
    async def compute_metrics_async(
        self,
        instance_id: str,
        is_validating: Optional[bool] = False,
        server_ips: Optional[List[str]] = None,
        dry_run: Optional[bool] = None,
        log_cost_to_s3: bool = False,
        container_timeout: Optional[int] = None,
    ) -> PrivateComputationInstance:
        # if calling PC the legacy way, make sure that the legacy stage
        # flow is being used
        self._override_stage_flow(instance_id, PrivateComputationLegacyStageFlow)
        return await self.run_stage_async(
            instance_id,
            PrivateComputationLegacyStageFlow.COMPUTE,
            ComputeMetricsStageService(
                self.onedocker_binary_config_map,
                self.mpc_svc,
                is_validating or False,
                log_cost_to_s3,
                container_timeout,
                dry_run or False,
            ),
            server_ips,
            dry_run or False,
        )

    # MPC step 2
    @deprecated("DO NOT USE! This is replaced by the generic run_next + run_stage functions and will soon be deleted.")
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
    @deprecated("DO NOT USE! This is replaced by the generic run_next + run_stage functions and will soon be deleted.")
    async def aggregate_shards_async(
        self,
        instance_id: str,
        is_validating: Optional[bool] = False,
        server_ips: Optional[List[str]] = None,
        dry_run: Optional[bool] = False,
        log_cost_to_s3: bool = False,
        container_timeout: Optional[int] = None,
    ) -> PrivateComputationInstance:
        # if calling PC the legacy way, make sure that the legacy stage
        # flow is being used
        self._override_stage_flow(instance_id, PrivateComputationLegacyStageFlow)
        return await self.run_stage_async(
            instance_id,
            PrivateComputationLegacyStageFlow.AGGREGATE,
            AggregateShardsStageService(
                self.onedocker_binary_config_map,
                self.mpc_svc,
                is_validating or False,
                log_cost_to_s3,
                container_timeout,
            ),
            server_ips,
            dry_run or False,
        )

    # TODO T88759390: make an async version of this function
    # Optioinal stage, validate the correctness of aggregated results for injected synthetic data
    def validate_metrics(
        self,
        instance_id: str,
        expected_result_path: str,
        aggregated_result_path: Optional[str] = None,
    ) -> None:
        private_computation_instance = self.get_instance(instance_id)
        expected_results_dict = json.loads(self.storage_svc.read(expected_result_path))
        aggregated_results_dict = json.loads(
            self.storage_svc.read(
                aggregated_result_path
                or private_computation_instance.shard_aggregate_stage_output_path
            )
        )
        if expected_results_dict == aggregated_results_dict:
            self.logger.info(
                f"Aggregated results for instance {instance_id} on synthetic data is as expected."
            )
        else:
            raise PrivateComputationServiceValidationError(
                f"Aggregated results for instance {instance_id} on synthetic data is NOT as expected."
            )

    def run_post_processing_handlers(
        self,
        instance_id: str,
        aggregated_result_path: Optional[str] = None,
        dry_run: Optional[bool] = False,
    ) -> PrivateComputationInstance:
        return asyncio.run(
            self.run_post_processing_handlers_async(
                instance_id,
                aggregated_result_path,
                dry_run,
            )
        )

    # Make an async version of run_post_processing_handlers so that
    # it can be called by Thrift
    async def run_post_processing_handlers_async(
        self,
        instance_id: str,
        aggregated_result_path: Optional[str] = None,
        dry_run: Optional[bool] = False,
    ) -> PrivateComputationInstance:
        instance = self.get_instance(instance_id)
        # this gets the stage object associated with the post processing handler stage.
        # It will be validated later in the run to make sure that the stage is actually ready
        # to be run.
        pph_stage = instance.stage_flow.get_stage_from_status(
            PrivateComputationInstanceStatus.POST_PROCESSING_HANDLERS_STARTED
        )
        return await self.run_stage_async(
            instance_id,
            pph_stage,
            PostProcessingStageService(
                self.storage_svc, self.post_processing_handlers, aggregated_result_path
            ),
            dry_run=dry_run or False,
        )

    def cancel_current_stage(
        self,
        instance_id: str,
    ) -> PrivateComputationInstance:
        private_computation_instance = self.get_instance(instance_id)

        # pre-checks to make sure it's in a cancel-able state
        if private_computation_instance.status not in STAGE_STARTED_STATUSES:
            raise ValueError(
                f"Instance {instance_id} has status {private_computation_instance.status}. Nothing to cancel."
            )

        if not private_computation_instance.instances:
            raise ValueError(
                f"Instance {instance_id} is in invalid state because no stages are registered under."
            )

        # cancel the running stage
        last_instance = private_computation_instance.instances[-1]
        if isinstance(last_instance, MPCInstance):
            self.mpc_svc.stop_instance(instance_id=last_instance.instance_id)
        else:
            self.logger.warning(
                f"Canceling the current stage of instance {instance_id} is not supported yet."
            )
            return private_computation_instance

        # post-checks to make sure the pl instance has the updated status
        private_computation_instance = self._update_instance(
            private_computation_instance=private_computation_instance
        )
        if private_computation_instance.status not in STAGE_FAILED_STATUSES:
            raise ValueError(
                f"Failed to cancel the current stage unexptectedly. Instance {instance_id} has status {private_computation_instance.status}"
            )

        self.logger.info(
            f"The current stage of instance {instance_id} has been canceled."
        )
        return private_computation_instance

    @staticmethod
    def get_ts_now() -> int:
        return int(datetime.now(tz=timezone.utc).timestamp())

    def _update_status(
        self,
        private_computation_instance: PrivateComputationInstance,
        new_status: PrivateComputationInstanceStatus,
    ) -> PrivateComputationInstance:
        old_status = private_computation_instance.status
        private_computation_instance.status = new_status
        if old_status != new_status:
            private_computation_instance.status_update_ts = (
                PrivateComputationService.get_ts_now()
            )
            self.logger.info(
                f"Updating status of {private_computation_instance.instance_id} from {old_status} to {private_computation_instance.status} at time {private_computation_instance.status_update_ts}"
            )
        return private_computation_instance

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
