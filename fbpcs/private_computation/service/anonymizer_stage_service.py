#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

import logging
from typing import DefaultDict, List, Optional

from fbpcp.entity.container_instance import ContainerInstance, ContainerInstanceStatus

from fbpcp.service.onedocker import OneDockerService
from fbpcs.common.entity.stage_state_instance import (
    StageStateInstance,
    StageStateInstanceStatus,
)

from fbpcs.infra.certificate.certificate_provider import CertificateProvider
from fbpcs.infra.certificate.private_key import PrivateKeyReferenceProvider
from fbpcs.onedocker_binary_config import OneDockerBinaryConfig

from fbpcs.private_computation.entity.private_computation_instance import (
    PrivateComputationInstance,
    PrivateComputationInstanceStatus,
)
from fbpcs.private_computation.service.private_computation_stage_service import (
    PrivateComputationStageService,
)
from fbpcs.private_computation.service.utils import stop_stage_service


class AnonymizerStageService(PrivateComputationStageService):
    """
    StageService for running the Anonymizer stage for PD.


    Private attributes:
        _onedocker_svc: Spins up containers that run binaries in the cloud
        _onedocker_binary_config_map: Stores a mapping from mpc game to OneDockerBinaryConfig (binary version and tmp directory)
    """

    def __init__(
        self,
        onedocker_svc: OneDockerService,
        onedocker_binary_config_map: DefaultDict[str, OneDockerBinaryConfig],
    ) -> None:
        self._onedocker_svc = onedocker_svc
        self._onedocker_binary_config_map = onedocker_binary_config_map
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
        self._logger.info("Running anonymizer")
        container_instances = await self._start_containers(
            pc_instance, server_ips=server_ips
        )

        stage_state = StageStateInstance(
            pc_instance.infra_config.instance_id,
            pc_instance.current_stage.name,
            containers=container_instances,
            status=StageStateInstanceStatus.STARTED,
        )

        pc_instance.infra_config.instances.append(stage_state)
        return pc_instance

    async def _start_containers(
        self,
        pc_instance: PrivateComputationInstance,
        server_ips: Optional[List[str]] = None,
    ) -> List[ContainerInstance]:
        """
        UNIMPLEMENTED

        After implementing this method, implement self.get_status as well.

        Tips for implementing:

        - Store any required arguments, such as input_path, subshare_path, etc on
            PrivateComputationInstance. Likely on the Anonymizer product config.
        - If you add new arguments to PC instance, also add them to...
            - BoltPCSCreateInstanceArgs
            - PCS thrift server create instance method
        - If you want to derive an output path for the stage based on instance id,
            you can add a property on the PrivateComputationInstance, as is done for
            other stages.
        - Refer to other StageServices for examples of how to find and call binary
        """

        self._logger.info(
            "Starting anonymizer containers (well, not actually - I'm a skeleton)"
        )
        return [
            ContainerInstance(
                "dummy_container_dne",
                ip_address="127.0.0.1",
                status=ContainerInstanceStatus.STARTED,
            )
        ]

    def _dummy_get_status(
        self, pc_instance: PrivateComputationInstance
    ) -> PrivateComputationInstanceStatus:
        """Transition to "next" status. Delete after implementing _start_containers"""

        self._logger.info(
            "Fetching anonymizer status (well, not actually - I'm a skeleton)"
        )

        stage_flow = pc_instance.infra_config.stage_flow
        status = pc_instance.infra_config.status

        if stage_flow.is_initialized_status(status):
            return pc_instance.current_stage.started_status
        elif stage_flow.is_started_status(status):
            return pc_instance.current_stage.completed_status
        else:
            # if the status is failed or completed, don't update status
            return status

    def get_status(
        self,
        pc_instance: PrivateComputationInstance,
    ) -> PrivateComputationInstanceStatus:
        # delete this after implementing _start_containers
        return self._dummy_get_status(pc_instance)
        # Uncomment this after implementing _start_containers
        # return get_pc_status_from_stage_state(pc_instance, self._onedocker_svc)

    def stop_service(
        self,
        pc_instance: PrivateComputationInstance,
    ) -> None:
        stop_stage_service(pc_instance, self._onedocker_svc)
