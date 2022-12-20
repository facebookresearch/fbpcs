#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

from collections import defaultdict
from unittest import IsolatedAsyncioTestCase
from unittest.mock import MagicMock

from fbpcp.entity.container_instance import ContainerInstance, ContainerInstanceStatus
from fbpcs.infra.certificate.null_certificate_provider import NullCertificateProvider
from fbpcs.onedocker_binary_config import OneDockerBinaryConfig
from fbpcs.private_computation.entity.infra_config import (
    InfraConfig,
    PrivateComputationGameType,
)
from fbpcs.private_computation.entity.pcs_feature import PCSFeature
from fbpcs.private_computation.entity.private_computation_instance import (
    PrivateComputationInstance,
    PrivateComputationInstanceStatus,
    PrivateComputationRole,
)
from fbpcs.private_computation.entity.product_config import (
    CommonProductConfig,
    PrivateIdDfcaConfig,
    ProductConfig,
)
from fbpcs.private_computation.service.constants import NUM_NEW_SHARDS_PER_FILE

from fbpcs.private_computation.service.mpc.mpc import MPCService
from fbpcs.private_computation.service.private_id_dfca_aggregate_stage_service import (
    PrivateIdDfcaAggregateStageService,
)


class TestPrivateIdDfcaAggregateStageService(IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.mock_mpc_svc = MagicMock(spec=MPCService)
        self.mock_mpc_svc.onedocker_svc = MagicMock()
        self.run_id = "681ba82c-16d9-11ed-861d-0242ac120002"

        onedocker_binary_config_map = defaultdict(
            lambda: OneDockerBinaryConfig(
                tmp_directory="/test_tmp_directory/",
                binary_version="latest",
                repository_path="test_path/",
            )
        )
        self.stage_svc = PrivateIdDfcaAggregateStageService(
            onedocker_binary_config_map, self.mock_mpc_svc
        )

    async def test_private_id_dfca_aggregate(self) -> None:
        containers = [
            ContainerInstance(
                instance_id="test_container_id", status=ContainerInstanceStatus.STARTED
            )
        ]
        self.mock_mpc_svc.start_containers.return_value = containers
        private_computation_instance = self._create_pc_instance()
        binary_name = "private_id_dfca/private_id_dfca_aggregator"
        test_server_ips = [
            f"192.0.2.{i}"
            for i in range(private_computation_instance.infra_config.num_mpc_containers)
        ]
        self.mock_mpc_svc.convert_cmd_args_list.return_value = (
            binary_name,
            ["cmd_1", "cmd_2"],
        )

        # act
        await self.stage_svc.run_async(
            private_computation_instance,
            NullCertificateProvider(),
            NullCertificateProvider(),
            "",
            "",
            test_server_ips,
        )

        # asserts
        self.mock_mpc_svc.start_containers.assert_called_once_with(
            cmd_args_list=["cmd_1", "cmd_2"],
            onedocker_svc=self.mock_mpc_svc.onedocker_svc,
            binary_version="latest",
            binary_name=binary_name,
            timeout=None,
            env_vars={"ONEDOCKER_REPOSITORY_PATH": "test_path/"},
            wait_for_containers_to_start_up=True,
            existing_containers=None,
        )
        self.assertEqual(
            containers,
            # pyre-ignore
            private_computation_instance.infra_config.instances[-1].containers,
        )
        self.assertEqual(
            "PRIVATE_ID_DFCA_AGGREGATE",
            # pyre-ignore
            private_computation_instance.infra_config.instances[-1].stage_name,
        )

    def _create_pc_instance(self) -> PrivateComputationInstance:
        infra_config: InfraConfig = InfraConfig(
            instance_id="test_instance_123",
            role=PrivateComputationRole.PARTNER,
            _stage_flow_cls_name="PrivateComputationPrivateIdDfcaStageFlow",
            status=PrivateComputationInstanceStatus.PRIVATE_ID_DFCA_AGGREGATION_STARTED,
            status_update_ts=1600000000,
            instances=[],
            game_type=PrivateComputationGameType.PRIVATE_ID_DFCA,
            num_pid_containers=2,
            num_mpc_containers=2,
            num_files_per_mpc_container=NUM_NEW_SHARDS_PER_FILE,
            status_updates=[],
            run_id=self.run_id,
            pcs_features={PCSFeature.PCS_DUMMY},
        )
        common: CommonProductConfig = CommonProductConfig(
            input_path="456",
            output_dir="789",
        )
        product_config: ProductConfig = PrivateIdDfcaConfig(
            common=common,
        )
        return PrivateComputationInstance(
            infra_config=infra_config,
            product_config=product_config,
        )
