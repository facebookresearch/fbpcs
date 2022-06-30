#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

import asyncio
import json
import logging
from datetime import datetime, timedelta, timezone
from typing import Any, DefaultDict, Dict, List, Optional, Type, TypeVar

from fbpcp.entity.mpc_instance import MPCInstance
from fbpcp.error.pcp import ThrottlingError
from fbpcp.service.mpc import MPCService
from fbpcp.service.onedocker import OneDockerService
from fbpcp.service.storage import StorageService
from fbpcs.onedocker_binary_config import OneDockerBinaryConfig
from fbpcs.pid.entity.pid_instance import PIDInstance
from fbpcs.pid.service.pid_service.pid import PIDService
from fbpcs.post_processing_handler.post_processing_handler import PostProcessingHandler
from fbpcs.private_computation.entity.breakdown_key import BreakdownKey
from fbpcs.private_computation.entity.infra_config import InfraConfig
from fbpcs.private_computation.entity.pc_validator_config import PCValidatorConfig
from fbpcs.private_computation.entity.pce_config import PCEConfig
from fbpcs.private_computation.entity.post_processing_data import PostProcessingData
from fbpcs.private_computation.entity.private_computation_instance import (
    AggregationType,
    AttributionRule,
    PrivateComputationGameType,
    PrivateComputationInstance,
    PrivateComputationInstanceStatus,
    PrivateComputationRole,
    ResultVisibility,
)
from fbpcs.private_computation.repository.private_computation_instance import (
    PrivateComputationInstanceRepository,
)
from fbpcs.private_computation.service.constants import (
    ATTRIBUTION_DEFAULT_PADDING_SIZE,
    DEFAULT_CONCURRENCY,
    DEFAULT_HMAC_KEY,
    DEFAULT_K_ANONYMITY_THRESHOLD_PA,
    DEFAULT_K_ANONYMITY_THRESHOLD_PL,
    LIFT_DEFAULT_PADDING_SIZE,
    NUM_NEW_SHARDS_PER_FILE,
)
from fbpcs.private_computation.service.errors import (
    PrivateComputationServiceInvalidStageError,
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
from fbpcs.private_computation.stage_flows.private_computation_pcf2_stage_flow import (
    PrivateComputationPCF2StageFlow,
)
from fbpcs.private_computation.stage_flows.private_computation_stage_flow import (
    PrivateComputationStageFlow,
)
from fbpcs.service.workflow import WorkflowService
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
        pc_validator_config: PCValidatorConfig,
        post_processing_handlers: Optional[Dict[str, PostProcessingHandler]] = None,
        pid_post_processing_handlers: Optional[Dict[str, PostProcessingHandler]] = None,
        workflow_svc: Optional[WorkflowService] = None,
    ) -> None:
        """Constructor of PrivateComputationService
        instance_repository -- repository to CRUD PrivateComputationInstance
        """
        self.instance_repository = instance_repository
        self.storage_svc = storage_svc
        self.mpc_svc = mpc_svc
        self.pid_svc = pid_svc
        self.onedocker_svc = onedocker_svc
        self.workflow_svc = workflow_svc
        self.onedocker_binary_config_map = onedocker_binary_config_map
        self.post_processing_handlers: Dict[str, PostProcessingHandler] = (
            post_processing_handlers or {}
        )
        self.pid_post_processing_handlers: Dict[str, PostProcessingHandler] = (
            pid_post_processing_handlers or {}
        )
        self.pc_validator_config = pc_validator_config
        self.stage_service_args = PrivateComputationStageServiceArgs(
            self.pid_svc,
            self.onedocker_binary_config_map,
            self.mpc_svc,
            self.storage_svc,
            self.post_processing_handlers,
            self.pid_post_processing_handlers,
            self.onedocker_svc,
            self.pc_validator_config,
            self.workflow_svc,
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
        breakdown_key: Optional[BreakdownKey] = None,
        pce_config: Optional[PCEConfig] = None,
        hmac_key: Optional[str] = None,
        padding_size: Optional[int] = None,
        k_anonymity_threshold: Optional[int] = None,
        stage_flow_cls: Optional[Type[PrivateComputationBaseStageFlow]] = None,
        result_visibility: Optional[ResultVisibility] = None,
        tier: Optional[str] = None,
        pid_use_row_numbers: bool = True,
        post_processing_data_optional: Optional[PostProcessingData] = None,
        pid_configs: Optional[Dict[str, Any]] = None,
    ) -> PrivateComputationInstance:
        self.logger.info(f"Creating instance: {instance_id}")

        # For Private Attribution daily recurrent runs, we would need dataset_timestamp of data used for computation.
        # Assigning a default value of day before the computation for dataset_timestamp.
        yesterday_date = datetime.now(tz=timezone.utc) - timedelta(days=1)
        yesterday_timestamp = datetime.timestamp(yesterday_date)

        post_processing_data = post_processing_data_optional or PostProcessingData(
            dataset_timestamp=int(yesterday_timestamp)
        )
        infra_config: InfraConfig = InfraConfig(
            instance_id=instance_id,
            role=role,
            status=PrivateComputationInstanceStatus.CREATED,
            status_update_ts=PrivateComputationService.get_ts_now(),
            instances=[],
            game_type=game_type,
            tier=tier,
            pce_config=pce_config,
            _stage_flow_cls_name=unwrap_or_default(
                optional=stage_flow_cls,
                default=PrivateComputationPCF2StageFlow
                if game_type is PrivateComputationGameType.ATTRIBUTION
                else PrivateComputationStageFlow,
            ).get_cls_name(),
            num_pid_containers=num_pid_containers,
            num_mpc_containers=self._get_number_of_mpc_containers(
                game_type, num_pid_containers, num_mpc_containers
            ),
            num_files_per_mpc_container=unwrap_or_default(
                optional=num_files_per_mpc_container, default=NUM_NEW_SHARDS_PER_FILE
            ),
        )
        instance = PrivateComputationInstance(
            infra_config,
            attribution_rule=attribution_rule,
            aggregation_type=aggregation_type,
            input_path=input_path,
            output_dir=output_dir,
            breakdown_key=breakdown_key,
            hmac_key=unwrap_or_default(optional=hmac_key, default=DEFAULT_HMAC_KEY),
            padding_size=unwrap_or_default(
                optional=padding_size,
                default=LIFT_DEFAULT_PADDING_SIZE
                if game_type is PrivateComputationGameType.LIFT
                else ATTRIBUTION_DEFAULT_PADDING_SIZE,
            ),
            concurrency=concurrency or DEFAULT_CONCURRENCY,
            k_anonymity_threshold=unwrap_or_default(
                optional=k_anonymity_threshold,
                default=DEFAULT_K_ANONYMITY_THRESHOLD_PA
                if game_type is PrivateComputationGameType.ATTRIBUTION
                else DEFAULT_K_ANONYMITY_THRESHOLD_PL,
            ),
            result_visibility=result_visibility or ResultVisibility.PUBLIC,
            pid_use_row_numbers=pid_use_row_numbers,
            post_processing_data=post_processing_data,
            pid_configs=pid_configs,
        )

        self.instance_repository.create(instance)
        return instance

    def _get_number_of_mpc_containers(
        self,
        game_type: PrivateComputationGameType,
        num_pid_containers: int,
        num_mpc_containers: int,
    ) -> int:
        # short-term plan of T117906435
        # tl;dr for PL, nums of pid/mpc containers is coupled and decided by SVs
        # https://www.internalfb.com/intern/sv/PRIVATE_LIFT_MAX_ROWS_PER_SHARD/
        # we need to revisit it to decouple mpc/pid containers for PL
        # by returning both values separately through graph API
        return (
            5 * num_pid_containers
            if game_type is PrivateComputationGameType.LIFT
            else num_mpc_containers
        )

    # TODO T88759390: make an async version of this function
    def get_instance(self, instance_id: str) -> PrivateComputationInstance:
        return self.instance_repository.read(instance_id=instance_id)

    def update_input_path(
        self, instance_id: str, input_path: str
    ) -> PrivateComputationInstance:
        """
        override input path only allow partner side
        """
        pc_instance = self.get_instance(instance_id)
        if pc_instance.infra_config.role is PrivateComputationRole.PARTNER:
            pc_instance.input_path = input_path
            self.instance_repository.update(pc_instance)

        return pc_instance

    # TODO T88759390: make an async version of this function
    def update_instance(self, instance_id: str) -> PrivateComputationInstance:
        private_computation_instance = self.instance_repository.read(instance_id)
        # if the status is started, then we need to update the instance
        # to either failed, started, or completed
        if private_computation_instance.stage_flow.is_started_status(
            private_computation_instance.infra_config.status
        ):
            self.logger.info(f"Updating instance: {instance_id}")
            return self._update_instance(
                private_computation_instance=private_computation_instance
            )
        else:
            # if the status is not started, then nothing should have changed and we
            # don't need to update the status
            # trying to prevent issues like this: https://fburl.com/yrrozywg
            self.logger.info(
                f"Not updating {instance_id}: status is {private_computation_instance.infra_config.status}"
            )
            return private_computation_instance

    def _update_instance(
        self, private_computation_instance: PrivateComputationInstance
    ) -> PrivateComputationInstance:
        stage = private_computation_instance.current_stage
        stage_svc = stage.get_stage_service(self.stage_service_args)
        self.logger.info(f"Updating instance | {stage}={stage!r}")
        try:
            new_status = stage_svc.get_status(private_computation_instance)
            private_computation_instance.update_status(new_status, self.logger)
            self.instance_repository.update(private_computation_instance)
        except ThrottlingError as e:
            self.logger.warning(
                f"Got ThrottlingError when updating instance. Skipping update! Error: {e}"
            )
        self.logger.info(
            f"Finished updating instance: {private_computation_instance.infra_config.instance_id}"
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
        if pc_instance.is_stage_flow_completed():
            raise PrivateComputationServiceInvalidStageError(
                f"Instance {instance_id} stage flow completed. (status: {pc_instance.infra_config.status}). Ignored"
            )

        next_stage = pc_instance.get_next_runnable_stage()
        if not next_stage:
            raise PrivateComputationServiceInvalidStageError(
                f"Instance {instance_id} has no eligible stages to run at this time (status: {pc_instance.infra_config.status})"
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
            and pc_instance.infra_config.role is PrivateComputationRole.PARTNER
            and not server_ips
        ):
            raise ValueError("Missing server_ips")

        # if the instance status is the complete status of the previous stage, then we can run the target stage
        # e.g. if status == ID_MATCH_COMPLETE, then we can run COMPUTE_METRICS
        # pyre-fixme[16]: `Optional` has no attribute `completed_status`.
        if pc_instance.infra_config.status is stage.previous_stage.completed_status:
            pc_instance.infra_config.retry_counter = 0
        # if the instance status is the fail status of the target stage, then we can retry the target stage
        # e.g. if status == COMPUTE_METRICS_FAILED, then we can run COMPUTE_METRICS
        elif pc_instance.infra_config.status is stage.failed_status:
            pc_instance.infra_config.retry_counter += 1
        # if the instance status is a start status, it's running something already. Don't run another stage, even if dry_run=True
        elif stage.is_started_status(pc_instance.infra_config.status):
            raise ValueError(
                f"Cannot start a new operation when instance {instance_id} has status {pc_instance.infra_config.status}."
            )
        # if dry_run = True, then we can run the target stage. Otherwise, throw an error
        elif not dry_run:
            raise ValueError(
                f"Instance {instance_id} has status {pc_instance.infra_config.status}. Not ready for {stage}."
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

        pc_instance.update_status(new_status=stage.started_status, logger=self.logger)
        self.logger.info(repr(stage))
        try:
            stage_svc = stage_svc or stage.get_stage_service(self.stage_service_args)
            pc_instance = await stage_svc.run_async(pc_instance, server_ips)
        except Exception as e:
            self.logger.error(f"Caught exception when running {stage}\n{e}")
            pc_instance.update_status(
                new_status=stage.failed_status, logger=self.logger
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
        if private_computation_instance.stage_flow.is_completed_status(
            private_computation_instance.infra_config.status
        ):
            raise ValueError(
                f"Instance {instance_id} has status {private_computation_instance.infra_config.status}. Nothing to cancel."
            )
            return private_computation_instance

        if not private_computation_instance.infra_config.instances:
            raise ValueError(
                f"Instance {instance_id} is in invalid state because no stages are registered under."
            )

        # cancel the running stage
        last_instance = private_computation_instance.infra_config.instances[-1]
        stage = private_computation_instance.current_stage
        self.logger.info(
            f"Canceling the current stage {stage} of instance {instance_id}"
        )
        # TODO: T124324848 move MPCInstance/PIDInstance stop instance to StageService.stop_service()
        if isinstance(last_instance, MPCInstance):
            self.mpc_svc.stop_instance(instance_id=last_instance.instance_id)
        elif isinstance(last_instance, PIDInstance):
            self.pid_svc.stop_instance(instance_id=last_instance.instance_id)
        else:
            stage_svc = stage.get_stage_service(self.stage_service_args)
            # TODO: T124322832 make stop service as abstract method and enforce all stage service to implement
            try:
                stage_svc.stop_service(private_computation_instance)
            except NotImplementedError:
                self.logger.warning(
                    f"Canceling the current stage {stage} of instance {instance_id} is not supported yet."
                )
                return private_computation_instance

        # post-checks to make sure the pl instance has the updated status
        private_computation_instance = self._update_instance(
            private_computation_instance=private_computation_instance
        )

        if not private_computation_instance.stage_flow.is_failed_status(
            private_computation_instance.infra_config.status
        ):
            raise ValueError(
                f"Failed to cancel the current stage unexpectedly. Instance {instance_id} has status {private_computation_instance.infra_config.status}"
            )

        self.logger.info(
            f"The current stage {stage} of instance {instance_id} has been canceled."
        )
        return private_computation_instance

    @staticmethod
    def get_ts_now() -> int:
        return int(datetime.now(tz=timezone.utc).timestamp())

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
