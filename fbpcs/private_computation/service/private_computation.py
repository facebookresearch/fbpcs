#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

import asyncio
import json
import logging
import time
from datetime import datetime, timedelta, timezone
from typing import Any, DefaultDict, Dict, List, Optional, Type, TypeVar, Union

from fbpcp.entity.container_instance import ContainerInstanceStatus
from fbpcp.error.pcp import ThrottlingError
from fbpcp.service.onedocker import OneDockerService
from fbpcp.service.storage import StorageService
from fbpcs.common.entity.stage_state_instance import StageStateInstance
from fbpcs.common.feature.pcs_feature_gate_utils import get_stage_flow
from fbpcs.common.service.metric_service import MetricService
from fbpcs.common.service.simple_metric_service import SimpleMetricService
from fbpcs.common.service.simple_trace_logging_service import SimpleTraceLoggingService
from fbpcs.common.service.trace_logging_service import (
    CheckpointStatus,
    TraceLoggingService,
)
from fbpcs.experimental.cloud_logs.dummy_log_retriever import DummyLogRetriever
from fbpcs.experimental.cloud_logs.log_retriever import LogRetriever
from fbpcs.infra.certificate.basic_ca_certificate_provider import (
    BasicCaCertificateProvider,
)
from fbpcs.infra.certificate.certificate_provider import CertificateProvider
from fbpcs.infra.certificate.null_certificate_provider import NullCertificateProvider
from fbpcs.infra.certificate.pc_instance_ca_certificate_provider import (
    PCInstanceCaCertificateProvider,
)
from fbpcs.infra.certificate.pc_instance_server_certificate import (
    PCInstanceServerCertificateProvider,
)
from fbpcs.infra.certificate.private_key import (
    NullPrivateKeyReferenceProvider,
    PrivateKeyReferenceProvider,
    StaticPrivateKeyReferenceProvider,
)
from fbpcs.onedocker_binary_config import OneDockerBinaryConfig
from fbpcs.post_processing_handler.post_processing_handler import PostProcessingHandler
from fbpcs.private_computation.entity.breakdown_key import BreakdownKey
from fbpcs.private_computation.entity.infra_config import (
    InfraConfig,
    PrivateComputationGameType,
)
from fbpcs.private_computation.entity.pc_validator_config import PCValidatorConfig
from fbpcs.private_computation.entity.pce_config import PCEConfig
from fbpcs.private_computation.entity.pcs_feature import PCSFeature
from fbpcs.private_computation.entity.post_processing_data import PostProcessingData
from fbpcs.private_computation.entity.private_computation_instance import (
    PrivateComputationInstance,
    PrivateComputationInstanceStatus,
    PrivateComputationRole,
)
from fbpcs.private_computation.entity.product_config import (
    AggregationType,
    AnonymizerConfig,
    AttributionConfig,
    AttributionRule,
    CommonProductConfig,
    LiftConfig,
    PrivateIdDfcaConfig,
    ProductConfig,
    ResultVisibility,
)
from fbpcs.private_computation.repository.private_computation_instance import (
    PrivateComputationInstanceRepository,
)
from fbpcs.private_computation.service.constants import (
    CA_CERT_PATH,
    DEFAULT_CONCURRENCY,
    DEFAULT_HMAC_KEY,
    DEFAULT_K_ANONYMITY_THRESHOLD_PA,
    DEFAULT_K_ANONYMITY_THRESHOLD_PL,
    DEFAULT_PADDING_SIZE,
    NUM_NEW_SHARDS_PER_FILE,
    PRIVATE_KEY_PATH,
    SERVER_CERT_PATH,
)
from fbpcs.private_computation.service.errors import (
    PrivateComputationServiceInvalidStageError,
    PrivateComputationServiceValidationError,
)
from fbpcs.private_computation.service.mpc.mpc import MPCService
from fbpcs.private_computation.service.pid_utils import (
    get_max_id_column_cnt,
    get_pid_protocol_from_num_shards,
    pid_should_use_row_numbers,
)
from fbpcs.private_computation.service.private_computation_stage_service import (
    PrivateComputationStageService,
    PrivateComputationStageServiceArgs,
)
from fbpcs.private_computation.stage_flows.private_computation_base_stage_flow import (
    PrivateComputationBaseStageFlow,
)
from fbpcs.service.workflow import WorkflowService
from fbpcs.utils.color import colored
from fbpcs.utils.optional import unwrap_or_default


T = TypeVar("T")

PCSERVICE_ENTITY_NAME = "pcservice"


class PrivateComputationService:
    # TODO(T103302669): [BE] Add documentation to PrivateComputationService class
    def __init__(
        self,
        instance_repository: PrivateComputationInstanceRepository,
        storage_svc: StorageService,
        mpc_svc: MPCService,
        onedocker_svc: OneDockerService,
        onedocker_binary_config_map: DefaultDict[str, OneDockerBinaryConfig],
        pc_validator_config: PCValidatorConfig,
        post_processing_handlers: Optional[Dict[str, PostProcessingHandler]] = None,
        pid_post_processing_handlers: Optional[Dict[str, PostProcessingHandler]] = None,
        workflow_svc: Optional[WorkflowService] = None,
        metric_svc: Optional[MetricService] = None,
        trace_logging_svc: Optional[TraceLoggingService] = None,
        log_retriever: Optional[LogRetriever] = None,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        """Constructor of PrivateComputationService
        instance_repository -- repository to CRUD PrivateComputationInstance
        """
        self.instance_repository = instance_repository
        self.storage_svc = storage_svc
        self.mpc_svc = mpc_svc
        self.onedocker_svc = onedocker_svc
        self.workflow_svc = workflow_svc
        self.onedocker_binary_config_map = onedocker_binary_config_map
        # If a metric service isn't provided, just use a SimpleMetricService
        # so a caller will never have to worry about this being None
        self.metric_svc: MetricService = metric_svc or SimpleMetricService()
        # Same deal with trace_logging_svc
        self.trace_logging_svc: TraceLoggingService = (
            trace_logging_svc or SimpleTraceLoggingService()
        )
        self.log_retriever: LogRetriever = log_retriever or DummyLogRetriever()
        self.post_processing_handlers: Dict[str, PostProcessingHandler] = (
            post_processing_handlers or {}
        )
        self.pid_post_processing_handlers: Dict[str, PostProcessingHandler] = (
            pid_post_processing_handlers or {}
        )
        self.pc_validator_config = pc_validator_config
        self.stage_service_args = PrivateComputationStageServiceArgs(
            self.onedocker_binary_config_map,
            self.mpc_svc,
            self.storage_svc,
            self.post_processing_handlers,
            self.pid_post_processing_handlers,
            self.onedocker_svc,
            self.pc_validator_config,
            self.workflow_svc,
            self.metric_svc,
            self.trace_logging_svc,
        )
        self.logger: logging.Logger = (
            logging.getLogger(__name__) if logger is None else logger
        )

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
        pcs_features: Optional[List[str]] = None,
        run_id: Optional[str] = None,
        log_cost_bucket: Optional[str] = None,
        input_path_start_ts: Optional[str] = None,
        input_path_end_ts: Optional[str] = None,
        server_certificate: Optional[str] = None,
        ca_certificate: Optional[str] = None,
        server_domain: Optional[str] = None,
        server_key_secret_ref: Optional[str] = None,
    ) -> PrivateComputationInstance:
        self.logger.info(f"Creating instance: {instance_id}")
        self.metric_svc.bump_entity_key(PCSERVICE_ENTITY_NAME, "create_instance")

        checkpoint_name = f"{role.value}_CREATE"
        self.trace_logging_svc.write_checkpoint(
            run_id=run_id,
            instance_id=instance_id,
            checkpoint_name=checkpoint_name,
            status=CheckpointStatus.STARTED,
        )

        # For Private Attribution daily recurrent runs, we would need dataset_timestamp of data used for computation.
        # Assigning a default value of day before the computation for dataset_timestamp.
        yesterday_date = datetime.now(tz=timezone.utc) - timedelta(days=1)
        yesterday_timestamp = datetime.timestamp(yesterday_date)

        post_processing_data = post_processing_data_optional or PostProcessingData(
            dataset_timestamp=int(yesterday_timestamp)
        )
        pcs_feature_enums = set()
        for feature in unwrap_or_default(optional=pcs_features, default=[]):
            pcs_feature_enums.add(PCSFeature.from_str(feature))
            self.metric_svc.bump_entity_key(
                PCSERVICE_ENTITY_NAME, f"pcs_feature_{feature.lower()}_enabled"
            )

        infra_config: InfraConfig = InfraConfig(
            instance_id=instance_id,
            role=role,
            status=PrivateComputationInstanceStatus.CREATED,
            status_update_ts=PrivateComputationService.get_ts_now(),
            instances=[],
            game_type=game_type,
            tier=tier,
            pcs_features=pcs_feature_enums,
            pce_config=pce_config,
            _stage_flow_cls_name=get_stage_flow(
                game_type=game_type,
                pcs_feature_enums=pcs_feature_enums,
                stage_flow_cls=stage_flow_cls,
            ).get_cls_name(),
            num_pid_containers=num_pid_containers,
            num_mpc_containers=self._get_number_of_mpc_containers(
                game_type, num_pid_containers, num_mpc_containers
            ),
            num_files_per_mpc_container=unwrap_or_default(
                optional=num_files_per_mpc_container, default=NUM_NEW_SHARDS_PER_FILE
            ),
            mpc_compute_concurrency=concurrency or DEFAULT_CONCURRENCY,
            status_updates=[],
            run_id=run_id,
            log_cost_bucket=log_cost_bucket,
            server_certificate=server_certificate,
            ca_certificate=ca_certificate,
            server_key_ref=server_key_secret_ref,
            server_domain=server_domain,
        )
        multikey_enabled = True
        if pid_configs and "multikey_enabled" in pid_configs.keys():
            multikey_enabled = pid_configs["multikey_enabled"]
        pid_protocol = get_pid_protocol_from_num_shards(
            num_pid_containers, multikey_enabled
        )
        pid_max_column_count = get_max_id_column_cnt(pid_protocol)
        common: CommonProductConfig = CommonProductConfig(
            input_path=input_path,
            output_dir=output_dir,
            hmac_key=unwrap_or_default(optional=hmac_key, default=DEFAULT_HMAC_KEY),
            padding_size=unwrap_or_default(
                optional=padding_size,
                default=DEFAULT_PADDING_SIZE[game_type],
            ),
            result_visibility=result_visibility or ResultVisibility.PUBLIC,
            pid_use_row_numbers=pid_should_use_row_numbers(
                pid_use_row_numbers, pid_protocol
            ),
            post_processing_data=post_processing_data,
            pid_configs=pid_configs,
            multikey_enabled=multikey_enabled,
            pid_protocol=pid_protocol,
            pid_max_column_count=pid_max_column_count,
            input_path_start_ts=input_path_start_ts,
            input_path_end_ts=input_path_end_ts,
        )
        product_config: ProductConfig
        if game_type is PrivateComputationGameType.ATTRIBUTION:
            if aggregation_type is None:
                raise RuntimeError("Missing attribution input: aggregation_type.")
            if attribution_rule is None:
                raise RuntimeError("Missing attribution input: attribution_rule.")
            product_config = AttributionConfig(
                common=common,
                attribution_rule=attribution_rule,
                aggregation_type=aggregation_type,
            )
        elif game_type is PrivateComputationGameType.LIFT:
            product_config = LiftConfig(
                common=common,
                k_anonymity_threshold=unwrap_or_default(
                    optional=k_anonymity_threshold,
                    default=DEFAULT_K_ANONYMITY_THRESHOLD_PA
                    if game_type is PrivateComputationGameType.ATTRIBUTION
                    else DEFAULT_K_ANONYMITY_THRESHOLD_PL,
                ),
                breakdown_key=breakdown_key,
            )
        elif game_type is PrivateComputationGameType.PRIVATE_ID_DFCA:
            product_config = PrivateIdDfcaConfig(
                common=common,
            )
        elif game_type is PrivateComputationGameType.ANONYMIZER:
            product_config = AnonymizerConfig(
                common=common,
            )
        instance = PrivateComputationInstance(
            infra_config=infra_config,
            product_config=product_config,
        )

        self._instance_repo_create(instance)

        self.trace_logging_svc.write_checkpoint(
            run_id=instance.infra_config.run_id,
            instance_id=instance_id,
            checkpoint_name=checkpoint_name,
            status=CheckpointStatus.COMPLETED,
        )

        return instance

    def _instance_repo_create(self, instance: PrivateComputationInstance) -> None:
        with self.metric_svc.bump_num_times_called_and_error_count(
            entity=PCSERVICE_ENTITY_NAME, prefix="instance_repo_create"
        ), self.metric_svc.timer(
            entity=PCSERVICE_ENTITY_NAME, prefix="instance_repo_create"
        ):
            self.instance_repository.create(instance=instance)

    def _instance_repo_read(self, instance_id: str) -> PrivateComputationInstance:
        with self.metric_svc.bump_num_times_called_and_error_count(
            entity=PCSERVICE_ENTITY_NAME, prefix="instance_repo_read"
        ), self.metric_svc.timer(
            entity=PCSERVICE_ENTITY_NAME, prefix="instance_repo_read"
        ):
            return self.instance_repository.read(instance_id=instance_id)

    def _instance_repo_update(self, instance: PrivateComputationInstance) -> None:
        with self.metric_svc.bump_num_times_called_and_error_count(
            entity=PCSERVICE_ENTITY_NAME, prefix="instance_repo_update"
        ), self.metric_svc.timer(
            entity=PCSERVICE_ENTITY_NAME, prefix="instance_repo_update"
        ):
            self.instance_repository.update(instance=instance)

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
        self.metric_svc.bump_entity_key(PCSERVICE_ENTITY_NAME, "get_instance")
        return self._instance_repo_read(instance_id=instance_id)

    def update_input_path(
        self, instance_id: str, input_path: str
    ) -> PrivateComputationInstance:
        """
        override input path only allow partner side
        """
        self.metric_svc.bump_entity_key(PCSERVICE_ENTITY_NAME, "update_input_path")
        pc_instance = self._instance_repo_read(instance_id)
        if pc_instance.infra_config.role is PrivateComputationRole.PARTNER:
            pc_instance.product_config.common.input_path = input_path
            self._instance_repo_update(pc_instance)

        return pc_instance

    # TODO T88759390: make an async version of this function
    def update_instance(self, instance_id: str) -> PrivateComputationInstance:
        self.metric_svc.bump_entity_key(PCSERVICE_ENTITY_NAME, "update_instance")
        private_computation_instance = self._instance_repo_read(instance_id)
        # if the status is initialized or started, then we need to update the instance
        # to either failed, started, or completed
        if private_computation_instance.stage_flow.is_initialized_status(
            private_computation_instance.infra_config.status
        ) or private_computation_instance.stage_flow.is_started_status(
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
            self._instance_repo_update(private_computation_instance)
            if private_computation_instance.stage_flow.is_completed_status(new_status):
                stage_elapsed_time = (
                    private_computation_instance.get_status_elapsed_time(
                        start_status=stage.initialized_status, end_status=new_status
                    )
                )
                self.metric_svc.bump_entity_key_avg(
                    PCSERVICE_ENTITY_NAME,
                    f"{stage.name}.time_ms",
                    stage_elapsed_time * 1000,
                )

        except ThrottlingError as e:
            self.logger.warning(
                f"Got ThrottlingError when updating instance. Skipping update! Error: {e}"
            )
        self.logger.info(
            f"Finished updating instance: {private_computation_instance.infra_config.instance_id}"
        )

        return private_computation_instance

    def log_failed_containers(
        self,
        pc_instance_id: str,
        log_only_first_failure: bool = True,
        num_lines: int = 30,
    ) -> None:
        checkpoint_name = "log_failed_containers"
        checkpoint_data = {}
        try:
            private_computation_instance = self._instance_repo_read(pc_instance_id)
            self.trace_logging_svc.write_checkpoint(
                run_id=private_computation_instance.infra_config.run_id,
                instance_id=pc_instance_id,
                checkpoint_name=checkpoint_name,
                status=CheckpointStatus.STARTED,
            )
            if private_computation_instance.infra_config.stage_flow.is_failed_status(
                private_computation_instance.infra_config.status
            ):
                containers = private_computation_instance.containers or []
                for i, container in enumerate(containers):
                    if container.status is ContainerInstanceStatus.FAILED:
                        self.logger.info(
                            f"Logging failed container {container.instance_id}"
                        )
                        # get log url
                        log_url = self.log_retriever.get_log_url(container.instance_id)
                        # get last num_lines events. If num_lines > len(events),
                        # then len(events) events would be logged.
                        log_events = self.log_retriever.fetch(container.instance_id)[
                            -num_lines:
                        ]
                        log_events_str = self.log_retriever.log_events_to_str(
                            log_events
                        )
                        self.logger.info(f"Failed container URL: {log_url}")
                        self.logger.info(log_events_str)

                        checkpoint_data[f"container_{i}"] = log_events_str.replace(
                            "\n", "\\n"
                        )

                        if log_only_first_failure:
                            break
        except Exception as e:
            self.logger.warning("error when trying to log cloudwatch logs")
            self.logger.debug(e)
            self.trace_logging_svc.write_checkpoint(
                run_id=private_computation_instance.infra_config.run_id,
                instance_id=pc_instance_id,
                checkpoint_name=checkpoint_name,
                status=CheckpointStatus.FAILED,
                checkpoint_data=checkpoint_data,
            )
        else:
            self.trace_logging_svc.write_checkpoint(
                run_id=private_computation_instance.infra_config.run_id,
                instance_id=pc_instance_id,
                checkpoint_name=checkpoint_name,
                status=CheckpointStatus.COMPLETED,
                checkpoint_data=checkpoint_data,
            )

    def run_next(
        self,
        instance_id: str,
        server_ips: Optional[List[str]] = None,
        ca_certificate: Optional[str] = None,
        server_hostnames: Optional[List[str]] = None,
    ) -> PrivateComputationInstance:
        self.metric_svc.bump_entity_key(PCSERVICE_ENTITY_NAME, "run_next")
        return asyncio.run(
            self.run_next_async(
                instance_id,
                server_ips,
                ca_certificate,
                server_hostnames,
            )
        )

    async def run_next_async(
        self,
        instance_id: str,
        server_ips: Optional[List[str]] = None,
        ca_certificate: Optional[str] = None,
        server_hostnames: Optional[List[str]] = None,
    ) -> PrivateComputationInstance:
        """Fetches the next eligible stage in the instance's stage flow and runs it"""
        self.metric_svc.bump_entity_key(PCSERVICE_ENTITY_NAME, "run_next_async")
        pc_instance = self._instance_repo_read(instance_id)
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
            instance_id,
            next_stage,
            server_ips=server_ips,
            ca_certificate=ca_certificate,
            server_hostnames=server_hostnames,
        )

    def run_stage(
        self,
        instance_id: str,
        stage: PrivateComputationBaseStageFlow,
        stage_svc: Optional[PrivateComputationStageService] = None,
        server_ips: Optional[List[str]] = None,
        dry_run: bool = False,
        ca_certificate: Optional[str] = None,
        server_hostnames: Optional[List[str]] = None,
    ) -> PrivateComputationInstance:
        self.metric_svc.bump_entity_key(PCSERVICE_ENTITY_NAME, "run_stage")
        return asyncio.run(
            self.run_stage_async(
                instance_id,
                stage,
                stage_svc,
                server_ips,
                dry_run,
                ca_certificate,
                server_hostnames,
            )
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
        pc_instance = self._instance_repo_read(instance_id)
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
        # if the instance status is a initialize/start status, it's running something already. Don't run another stage, even if dry_run=True
        elif stage.is_initialized_status(
            pc_instance.infra_config.status
        ) or stage.is_started_status(pc_instance.infra_config.status):
            raise ValueError(
                f"Cannot start a new operation when instance {instance_id} has status {pc_instance.infra_config.status}."
            )
        # if dry_run = True, then we can run the target stage. Otherwise, throw an error
        elif not dry_run:
            raise ValueError(
                f"Instance {instance_id} has status {pc_instance.infra_config.status}. Not ready for {stage}."
            )

        return pc_instance

    def _validate_tls_data(
        self,
        pc_instance: PrivateComputationInstance,
        stage: PrivateComputationBaseStageFlow,
        server_hostnames: Optional[List[str]],
        ca_certificate: Optional[str],
    ) -> None:
        """
        Validates data required to enable TLS
        """
        if (
            stage.is_joint_stage
            and pc_instance.infra_config.role is PrivateComputationRole.PARTNER
        ):
            if not server_hostnames:
                raise ValueError("Missing server_hostnames, required for TLS")

            if not ca_certificate:
                raise ValueError("Missing ca_certificate, required for TLS")

    def _get_server_certificate_provider(
        self, pc_instance: PrivateComputationInstance
    ) -> CertificateProvider:
        if (
            pc_instance.infra_config.role == PrivateComputationRole.PUBLISHER
            and pc_instance.has_feature(PCSFeature.PCF_TLS)
        ):
            return PCInstanceServerCertificateProvider(pc_instance)
        else:
            return NullCertificateProvider()

    def _get_server_private_key_ref_provider(
        self, pc_instance: PrivateComputationInstance
    ) -> PrivateKeyReferenceProvider:
        if (
            pc_instance.infra_config.role == PrivateComputationRole.PUBLISHER
            and pc_instance.has_feature(PCSFeature.PCF_TLS)
            and pc_instance.infra_config.server_key_ref
            and pc_instance.infra_config.pce_config
        ):
            return StaticPrivateKeyReferenceProvider(
                resource_id=pc_instance.infra_config.server_key_ref,
                region=pc_instance.infra_config.pce_config.region,
                install_path=PRIVATE_KEY_PATH,
            )
        else:
            return NullPrivateKeyReferenceProvider()

    def _get_ca_certificate_provider(
        self, pc_instance: PrivateComputationInstance, ca_certificate: Optional[str]
    ) -> CertificateProvider:
        if (
            pc_instance.infra_config.role == PrivateComputationRole.PUBLISHER
            and pc_instance.has_feature(PCSFeature.PCF_TLS)
        ):
            return PCInstanceCaCertificateProvider(pc_instance)
        if (
            pc_instance.infra_config.role == PrivateComputationRole.PARTNER
            and ca_certificate
            and pc_instance.has_feature(PCSFeature.PCF_TLS)
        ):
            return BasicCaCertificateProvider(ca_certificate)
        return NullCertificateProvider()

    # TODO T88759390: Make this function truly async. It is not because it calls blocking functions.
    # Make an async version of run_stage_async() so that it can be called by Thrift
    async def run_stage_async(
        self,
        instance_id: str,
        stage: PrivateComputationBaseStageFlow,
        stage_svc: Optional[PrivateComputationStageService] = None,
        server_ips: Optional[List[str]] = None,
        dry_run: bool = False,
        ca_certificate: Optional[str] = None,
        server_hostnames: Optional[List[str]] = None,
    ) -> PrivateComputationInstance:
        """
        Runs a stage for a given instance. If state of the instance is invalid (e.g. not ready to run a stage),
        an exception will be thrown.
        """

        self.metric_svc.bump_entity_key(PCSERVICE_ENTITY_NAME, "run_stage_async")
        pc_instance = self._get_validated_instance(
            instance_id, stage, server_ips, dry_run
        )

        # TODO: T136265785 refactor the tls input validation logic into a TLS config class
        enable_tls = pc_instance.has_feature(PCSFeature.PCF_TLS)
        if enable_tls:
            self._validate_tls_data(
                pc_instance, stage, server_hostnames, ca_certificate
            )

        server_certificate_provider = self._get_server_certificate_provider(pc_instance)
        server_private_key_ref_provider = self._get_server_private_key_ref_provider(
            pc_instance
        )
        ca_certificate_provider = self._get_ca_certificate_provider(
            pc_instance, ca_certificate
        )

        # update initial status
        pc_instance.update_status(
            new_status=stage.initialized_status, logger=self.logger
        )
        self.logger.info(repr(stage))

        checkpoint_name = (
            f"{pc_instance.infra_config.role.value}_{stage.name}_run_async"
        )
        self.trace_logging_svc.write_checkpoint(
            run_id=pc_instance.infra_config.run_id,
            instance_id=instance_id,
            checkpoint_name=checkpoint_name,
            status=CheckpointStatus.STARTED,
        )
        try:
            stage_svc = stage_svc or stage.get_stage_service(self.stage_service_args)
            pc_instance = await stage_svc.run_async(
                pc_instance,
                server_certificate_provider,
                ca_certificate_provider,
                SERVER_CERT_PATH if enable_tls else "",
                CA_CERT_PATH if enable_tls else "",
                server_ips,
                server_hostnames if enable_tls else None,
                server_private_key_ref_provider,
            )
        except Exception as e:
            self.logger.error(f"Caught exception when running {stage}\n{e}")
            self.trace_logging_svc.write_checkpoint(
                run_id=pc_instance.infra_config.run_id,
                instance_id=instance_id,
                checkpoint_name=checkpoint_name,
                status=CheckpointStatus.FAILED,
            )
            pc_instance.update_status(
                new_status=stage.failed_status, logger=self.logger
            )
            raise e
        finally:
            self._instance_repo_update(pc_instance)

        try:
            log_urls = self.get_log_urls(pc_instance)
            for key, url in log_urls.items():
                self.logger.info(f"Log for {key} at {url}")
        except Exception:
            self.logger.warning("Failed to retrieve log URLs for instance")

        self.trace_logging_svc.write_checkpoint(
            run_id=pc_instance.infra_config.run_id,
            instance_id=instance_id,
            checkpoint_name=checkpoint_name,
            status=CheckpointStatus.COMPLETED,
        )
        return pc_instance

    def get_log_urls(
        self,
        instance_or_id: Union[str, PrivateComputationInstance],
        all_stages: bool = False,
        failed_only: bool = False,
    ) -> Dict[str, str]:
        """Get log urls for [most recently] run containers

        Arguments:
            instance_or_id: The PC instance to get logs from or its instance id
            all_stages: Default False only get log urls for most recently run containers. True to retriave all_stages logs
            failed_only: only get log urls for failed status. Default False


        Returns:
            A mapping of log index to log url
        """

        if isinstance(instance_or_id, str):
            private_computation_instance = self._instance_repo_read(instance_or_id)
        elif isinstance(instance_or_id, PrivateComputationInstance):
            private_computation_instance = instance_or_id
        else:
            raise ValueError(
                "instance_or_id must be either a str or PrivateComputationInstance"
            )

        if not private_computation_instance.infra_config.instances:
            return {}

        # Get the stage state instance
        stage_instances = private_computation_instance.infra_config.instances
        if not all_stages:
            stage_instances = [private_computation_instance.infra_config.instances[-1]]

        res = {}
        for s, stage_instance in enumerate(stage_instances):
            if isinstance(stage_instance, StageStateInstance):
                stage_name = stage_instance.stage_name
                containers = stage_instance.containers
                for i, container in enumerate(containers):
                    if (
                        failed_only
                        and container.status is not ContainerInstanceStatus.FAILED
                    ):
                        continue

                    res[
                        f"{s}_{stage_name}|container_{i}|{container.status.value}"
                    ] = self.log_retriever.get_log_url(container.instance_id)

        return res

    # TODO T88759390: make an async version of this function
    # Optional stage, validate the correctness of aggregated results for injected synthetic data
    def validate_metrics(
        self,
        instance_id: str,
        expected_result_path: str,
        aggregated_result_path: Optional[str] = None,
    ) -> None:
        self.metric_svc.bump_entity_key(PCSERVICE_ENTITY_NAME, "validate_metrics")
        private_computation_instance = self._instance_repo_read(instance_id)
        if (
            private_computation_instance.infra_config.game_type
            is PrivateComputationGameType.PRIVATE_ID_DFCA
        ):
            if (
                private_computation_instance.infra_config.role
                is PrivateComputationRole.PARTNER
            ):
                return

            expected_results = [
                sorted(
                    self.storage_svc.read(f"{expected_result_path}_{shard_n}").split(
                        "\n"
                    )
                )
                for shard_n in range(
                    private_computation_instance.infra_config.num_pid_containers
                )
            ]
            aggregated_results = [
                sorted(
                    self.storage_svc.read(
                        f"{aggregated_result_path or private_computation_instance.private_id_dfca_aggregate_stage_output_path}_{shard_n}"
                    ).split("\n")
                )
                for shard_n in range(
                    private_computation_instance.infra_config.num_pid_containers
                )
            ]

        else:
            expected_results = json.loads(self.storage_svc.read(expected_result_path))
            aggregated_results = json.loads(
                self.storage_svc.read(
                    aggregated_result_path
                    or private_computation_instance.shard_aggregate_stage_output_path
                )
            )

        if expected_results == aggregated_results:
            self.logger.info(
                colored(
                    f"Aggregated results for instance {instance_id} on synthetic data is as expected.",
                    "green",
                )
            )
        else:
            raise PrivateComputationServiceValidationError(
                f"Aggregated results for instance {instance_id} on synthetic data is NOT as expected."
            )

    def cancel_current_stage(
        self,
        instance_id: str,
    ) -> PrivateComputationInstance:
        self.metric_svc.bump_entity_key(PCSERVICE_ENTITY_NAME, "cancel_current_stage")
        private_computation_instance = self._instance_repo_read(instance_id)

        # pre-checks to make sure it's in a cancel-able state
        if private_computation_instance.stage_flow.is_completed_status(
            private_computation_instance.infra_config.status
        ):
            raise PrivateComputationServiceInvalidStageError(
                f"Instance {instance_id} stage flow completed. (status: {private_computation_instance.infra_config.status}). Nothing to cancel."
            )

        if not private_computation_instance.infra_config.instances:
            raise ValueError(
                f"Instance {instance_id} is in invalid state because no stages are registered under."
            )

        # cancel the running stage
        stage = private_computation_instance.current_stage
        self.logger.info(
            f"Canceling the current stage {stage} of instance {instance_id}"
        )
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
        polling_frequency_seconds = 5
        timeout_seconds = 120
        for _ in range(timeout_seconds // polling_frequency_seconds):
            time.sleep(polling_frequency_seconds)
            private_computation_instance = self._update_instance(
                private_computation_instance=private_computation_instance
            )
            stage_instance = private_computation_instance.get_stage_instance()
            if stage_instance is None:
                return private_computation_instance
            running_containers = stage_instance.get_running_containers(
                stage_instance.containers
            )

            if (
                private_computation_instance.stage_flow.is_failed_status(
                    private_computation_instance.infra_config.status
                )
                and len(running_containers) == 0
            ):
                self.logger.info(
                    f"The current stage {stage} of instance {instance_id} has been canceled. All containers are stopped."
                )
                return private_computation_instance

        raise TimeoutError(
            f"Timed out after {timeout_seconds} seconds while waiting for {stage} of instance {instance_id} to be cancelled."
        )

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
