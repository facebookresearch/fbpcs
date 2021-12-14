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
from typing import DefaultDict, Dict, List, Optional, Type, TypeVar

from fbpcp.entity.mpc_instance import MPCInstance
from fbpcp.service.mpc import MPCService
from fbpcp.service.onedocker import OneDockerService
from fbpcp.service.storage import StorageService
from fbpcs.onedocker_binary_config import OneDockerBinaryConfig
from fbpcs.pid.service.pid_service.pid import PIDService
from fbpcs.post_processing_handler.post_processing_handler import PostProcessingHandler
from fbpcs.private_computation.entity.breakdown_key import BreakdownKey
from fbpcs.private_computation.entity.pce_config import PCEConfig
from fbpcs.private_computation.entity.private_computation_instance import (
    AggregationType,
    AttributionRule,
    PrivateComputationGameType,
    PrivateComputationInstance,
    PrivateComputationInstanceStatus,
    PrivateComputationRole,
)
from fbpcs.private_computation.repository.private_computation_instance import (
    PrivateComputationInstanceRepository,
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
from fbpcs.private_computation.service.private_computation_stage_service import (
    PrivateComputationStageService,
    PrivateComputationStageServiceArgs,
)
from fbpcs.private_computation.service.utils import get_log_urls
from fbpcs.private_computation.stage_flows.private_computation_base_stage_flow import (
    PrivateComputationBaseStageFlow,
)
from fbpcs.private_computation.stage_flows.private_computation_decoupled_stage_flow import (
    PrivateComputationDecoupledStageFlow,
)
from fbpcs.private_computation.stage_flows.private_computation_stage_flow import (
    PrivateComputationStageFlow,
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
        self.post_processing_handlers: Dict[str, PostProcessingHandler] = (
            post_processing_handlers or {}
        )
        self.stage_service_args = PrivateComputationStageServiceArgs(
            self.pid_svc,
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
        stage_flow_cls: Optional[Type[PrivateComputationBaseStageFlow]] = None,
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
            _stage_flow_cls_name=unwrap_or_default(
                optional=stage_flow_cls,
                default=PrivateComputationDecoupledStageFlow
                if game_type is PrivateComputationGameType.ATTRIBUTION
                else PrivateComputationStageFlow,
            ).get_cls_name(),
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
            # TODO(T106517341): Raise a custom exception instead of something generic
            raise RuntimeError(
                f"Instance {instance_id} has no eligible stages to run at this time (status: {pc_instance.status})"
            )
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

        try:
            log_urls = get_log_urls(pc_instance)
            for key, url in log_urls.items():
                self.logger.info(f"Log for {key} at {url}")
        except Exception:
            self.logger.warning("Failed to retrieve log URLs for instance")

        return pc_instance

    # TODO T88759390: make an async version of this function
    # Optional stage, validate the correctness of aggregated results for injected synthetic data
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
                f"Failed to cancel the current stage unexpectedly. Instance {instance_id} has status {private_computation_instance.status}"
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
