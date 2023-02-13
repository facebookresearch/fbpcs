#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

import logging
from typing import DefaultDict, List, Optional

from fbpcp.entity.container_instance import ContainerInstance
from fbpcp.entity.container_type import ContainerType

from fbpcp.service.onedocker import OneDockerService
from fbpcp.service.storage import StorageService

from fbpcs.common.entity.stage_state_instance import StageStateInstance
from fbpcs.data_processing.service.pid_prepare_binary_service import (
    PIDPrepareBinaryService,
)
from fbpcs.infra.certificate.certificate_provider import CertificateProvider
from fbpcs.infra.certificate.private_key import PrivateKeyReferenceProvider
from fbpcs.onedocker_binary_config import OneDockerBinaryConfig
from fbpcs.pid.entity.pid_instance import PIDProtocol
from fbpcs.private_computation.entity.pcs_feature import PCSFeature

from fbpcs.private_computation.entity.private_computation_instance import (
    PrivateComputationInstance,
    PrivateComputationInstanceStatus,
    PrivateComputationRole,
)
from fbpcs.private_computation.service.constants import (
    DEFAULT_CONTAINER_TIMEOUT_IN_SEC,
    DEFAULT_IDENTIFIER_FILTER_THRESH,
)
from fbpcs.private_computation.service.pid_utils import get_sharded_filepath
from fbpcs.private_computation.service.private_computation_stage_service import (
    PrivateComputationStageService,
)
from fbpcs.private_computation.service.utils import (
    generate_env_vars_dict,
    get_pc_status_from_stage_state,
    stop_stage_service,
)


class PIDPrepareStageService(PrivateComputationStageService):
    """Handles business logic for the PID prepare stage

    Private attributes:
        _storage_svc: used to read/write files during private computation runs
        _onedocker_svc: used to spin up containers that run binaries in the cloud
        _onedocker_binary_config: stores OneDocker information
        _containter_timeout: customed timeout for container
    """

    def __init__(
        self,
        storage_svc: StorageService,
        onedocker_svc: OneDockerService,
        onedocker_binary_config_map: DefaultDict[str, OneDockerBinaryConfig],
        container_timeout: Optional[int] = DEFAULT_CONTAINER_TIMEOUT_IN_SEC,
    ) -> None:
        self._storage_svc = storage_svc
        self._onedocker_svc = onedocker_svc
        self._onedocker_binary_config_map = onedocker_binary_config_map
        self._container_timeout = container_timeout
        self._logger: logging.Logger = logging.getLogger(__name__)

    async def run_async(
        self,
        pc_instance: PrivateComputationInstance,
        server_certificate_provider: CertificateProvider,
        ca_certificate_provider: CertificateProvider,
        server_certificate_path: str,
        ca_certificate_path: str,
        server_ips: Optional[List[str]] = None,
        server_hostnames: Optional[List[str]] = None,
        server_private_key_ref_provider: Optional[PrivateKeyReferenceProvider] = None,
    ) -> PrivateComputationInstance:
        """Runs the PID prepare stage
        Args:
            pc_instance: the private computation instance to start pid prepare stage service
            server_certificate_providder: ignored
            ca_certificate_provider: ignored
            server_certificate_path: ignored
            ca_certificate_path: ignored
            server_ips: No need in this stage.
            server_hostnames: ignored
            server_private_key_ref_provider: ignored
        Returns:
            An updated version of pc_instance
        """
        self._logger.info(f"[{self}] Starting PIDPrepareStageService")
        container_instances = await self.start_pid_prepare_service(
            pc_instance, server_ips
        )

        self._logger.info("PIDPrepareStageService finished")
        stage_state = StageStateInstance(
            pc_instance.infra_config.instance_id,
            pc_instance.current_stage.name,
            containers=container_instances,
        )
        pc_instance.infra_config.instances.append(stage_state)
        return pc_instance

    def get_status(
        self,
        pc_instance: PrivateComputationInstance,
    ) -> PrivateComputationInstanceStatus:
        """Gets the latest PrivateComputationInstance status.

        Arguments:
            pc_instance: The private computation instance that is being updated

        Returns:
            The latest status for private computation instance
        """
        return get_pc_status_from_stage_state(pc_instance, self._onedocker_svc)

    async def start_pid_prepare_service(
        self,
        pc_instance: PrivateComputationInstance,
        server_ips: Optional[List[str]],
    ) -> List[ContainerInstance]:
        """start pid prepare service and spine up the container instances"""
        logging.info("Instantiated PID prepare stage")
        num_shards = pc_instance.infra_config.num_pid_containers
        # input_path is the output_path from PID Shard Stage
        input_path = pc_instance.pid_stage_output_data_path
        output_path = pc_instance.pid_stage_output_prepare_path
        pc_role = pc_instance.infra_config.role
        # generate the list of command args for publisher or partner
        args_list = []
        binary_name = PIDPrepareBinaryService.get_binary_name()
        onedocker_binary_config = self._onedocker_binary_config_map[binary_name]
        id_filter_thresh = -1

        if pc_instance.has_feature(
            PCSFeature.PID_FILTER_LOW_QUALITY_IDENTIFIER_THRESH166
        ) and (
            pc_instance.product_config.common.pid_protocol
            == PIDProtocol.UNION_PID_MULTIKEY
        ):
            # if it is multi-key and if feature is enabled,
            # we will be filtering identifiers with its appearance above threshold.
            id_filter_thresh = DEFAULT_IDENTIFIER_FILTER_THRESH

        for shard in range(num_shards):
            args_per_shard = PIDPrepareBinaryService.build_args(
                input_path=get_sharded_filepath(input_path, shard),
                output_path=get_sharded_filepath(output_path, shard),
                tmp_directory=onedocker_binary_config.tmp_directory,
                max_column_count=pc_instance.product_config.common.pid_max_column_count,
                id_filter_thresh=id_filter_thresh,
                run_id=pc_instance.infra_config.run_id,
            )
            args_list.append(args_per_shard)
        # start containers
        logging.info(f"{pc_role} spinning up containers")

        pid_prepare_binary_service = PIDPrepareBinaryService()
        env_vars = generate_env_vars_dict(
            repository_path=onedocker_binary_config.repository_path
        )
        should_wait_spin_up: bool = (
            pc_instance.infra_config.role is PrivateComputationRole.PARTNER
        )

        container_type = None
        if num_shards == 1 and pc_instance.has_feature(
            PCSFeature.PID_SNMK_LARGER_CONTAINER_TYPE
        ):
            # Use large FARGATE container for SNMK
            logging.info("Setting pid prepare stage container to LARGE")
            container_type = ContainerType.LARGE

        return await pid_prepare_binary_service.start_containers(
            cmd_args_list=args_list,
            onedocker_svc=self._onedocker_svc,
            binary_version=onedocker_binary_config.binary_version,
            binary_name=binary_name,
            timeout=self._container_timeout,
            env_vars=env_vars,
            wait_for_containers_to_start_up=should_wait_spin_up,
            existing_containers=pc_instance.get_existing_containers_for_retry(),
            container_type=container_type,
        )

    def stop_service(
        self,
        pc_instance: PrivateComputationInstance,
    ) -> None:
        stop_stage_service(pc_instance, self._onedocker_svc)
