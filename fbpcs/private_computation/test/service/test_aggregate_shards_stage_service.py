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
from fbpcs.infra.certificate.private_key import StaticPrivateKeyReferenceProvider
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
    LiftConfig,
    ProductConfig,
)
from fbpcs.private_computation.service.aggregate_shards_stage_service import (
    AggregateShardsStageService,
)
from fbpcs.private_computation.service.constants import (
    CA_CERTIFICATE_ENV_VAR,
    CA_CERTIFICATE_PATH_ENV_VAR,
    NUM_NEW_SHARDS_PER_FILE,
    SERVER_CERTIFICATE_ENV_VAR,
    SERVER_CERTIFICATE_PATH_ENV_VAR,
    SERVER_HOSTNAME_ENV_VAR,
    SERVER_IP_ADDRESS_ENV_VAR,
    SERVER_PRIVATE_KEY_PATH_ENV_VAR,
    SERVER_PRIVATE_KEY_REF_ENV_VAR,
    SERVER_PRIVATE_KEY_REGION_ENV_VAR,
)
from fbpcs.private_computation.service.mpc.mpc import MPCService


class TestAggregateShardsStageService(IsolatedAsyncioTestCase):
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
        self.stage_svc = AggregateShardsStageService(
            onedocker_binary_config_map, self.mock_mpc_svc
        )

    async def test_aggregate_shards(self) -> None:
        containers = [
            ContainerInstance(
                instance_id="test_container_id", status=ContainerInstanceStatus.STARTED
            )
        ]
        self.mock_mpc_svc.start_containers.return_value = containers
        private_computation_instance = self._create_pc_instance()
        binary_name = "private_attribution/shard-aggregator"
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
            "AGGREGATE",
            # pyre-ignore
            private_computation_instance.infra_config.instances[-1].stage_name,
        )

    async def test_tls_env_vars(self) -> None:
        self.mock_mpc_svc.start_containers.return_value = [
            ContainerInstance(
                instance_id="test_container_id", status=ContainerInstanceStatus.STARTED
            )
        ]
        private_computation_instance = self._create_pc_instance()
        test_server_ips = [
            f"192.0.2.{i}"
            for i in range(private_computation_instance.infra_config.num_mpc_containers)
        ]
        test_server_hostnames = [
            f"node{i}.test.com"
            for i in range(private_computation_instance.infra_config.num_mpc_containers)
        ]
        self.mock_mpc_svc.convert_cmd_args_list.return_value = (
            "private_attribution/shard-aggregator",
            ["cmd_1", "cmd_2"],
        )

        expected_server_certificate = "test_server_cert"
        expected_ca_certificate = "test_ca_cert"
        expected_server_key_resource_id = "test_key"
        expected_server_key_region = "test_region"
        expected_server_key_install_path = "test/path"
        expected_server_certificate_path = "/test/server_certificate_path"
        expected_ca_certificate_path = "/test/server_certificate_path"

        # act
        await self.stage_svc.run_async(
            private_computation_instance,
            self._get_mock_certificate_provider(expected_server_certificate),
            self._get_mock_certificate_provider(expected_ca_certificate),
            expected_server_certificate_path,
            expected_ca_certificate_path,
            test_server_ips,
            test_server_hostnames,
            StaticPrivateKeyReferenceProvider(
                expected_server_key_resource_id,
                expected_server_key_region,
                expected_server_key_install_path,
            ),
        )

        # asserts
        self.mock_mpc_svc.start_containers.assert_called_once()
        call_kwargs = self.mock_mpc_svc.start_containers.call_args[1]
        call_env_args = call_kwargs["env_vars"]

        self.assertTrue(call_env_args)

        self.assertTrue("ONEDOCKER_REPOSITORY_PATH" in call_env_args)
        self.assertEqual("test_path/", call_env_args["ONEDOCKER_REPOSITORY_PATH"])

        self.assertTrue(SERVER_CERTIFICATE_ENV_VAR in call_env_args)
        self.assertEqual(
            expected_server_certificate, call_env_args[SERVER_CERTIFICATE_ENV_VAR]
        )

        self.assertTrue(SERVER_PRIVATE_KEY_REF_ENV_VAR in call_env_args)
        self.assertEqual(
            expected_server_key_resource_id,
            call_env_args[SERVER_PRIVATE_KEY_REF_ENV_VAR],
        )

        self.assertTrue(SERVER_PRIVATE_KEY_REGION_ENV_VAR in call_env_args)
        self.assertEqual(
            expected_server_key_region, call_env_args[SERVER_PRIVATE_KEY_REGION_ENV_VAR]
        )

        self.assertTrue(SERVER_PRIVATE_KEY_PATH_ENV_VAR in call_env_args)
        self.assertEqual(
            expected_server_key_install_path,
            call_env_args[SERVER_PRIVATE_KEY_PATH_ENV_VAR],
        )

        self.assertTrue(CA_CERTIFICATE_ENV_VAR in call_env_args)
        self.assertEqual(expected_ca_certificate, call_env_args[CA_CERTIFICATE_ENV_VAR])

        self.assertTrue(SERVER_CERTIFICATE_PATH_ENV_VAR in call_env_args)
        self.assertEqual(
            expected_server_certificate_path,
            call_env_args[SERVER_CERTIFICATE_PATH_ENV_VAR],
        )

        self.assertTrue(CA_CERTIFICATE_PATH_ENV_VAR in call_env_args)
        self.assertEqual(
            expected_ca_certificate_path, call_env_args[CA_CERTIFICATE_PATH_ENV_VAR]
        )

        self.assertTrue(SERVER_IP_ADDRESS_ENV_VAR in call_env_args)
        self.assertEqual(test_server_ips[0], call_env_args[SERVER_IP_ADDRESS_ENV_VAR])

        self.assertTrue(SERVER_HOSTNAME_ENV_VAR in call_env_args)
        self.assertEqual(
            test_server_hostnames[0], call_env_args[SERVER_HOSTNAME_ENV_VAR]
        )

    def _create_pc_instance(self) -> PrivateComputationInstance:
        infra_config: InfraConfig = InfraConfig(
            instance_id="test_instance_123",
            role=PrivateComputationRole.PARTNER,
            status=PrivateComputationInstanceStatus.AGGREGATION_STARTED,
            status_update_ts=1600000000,
            instances=[],
            game_type=PrivateComputationGameType.LIFT,
            num_pid_containers=2,
            num_mpc_containers=2,
            num_files_per_mpc_container=NUM_NEW_SHARDS_PER_FILE,
            status_updates=[],
            run_id=self.run_id,
            pcs_features={PCSFeature.PCS_DUMMY},
            log_cost_bucket="test_log_cost_bucket",
        )
        common: CommonProductConfig = CommonProductConfig(
            input_path="456",
            output_dir="789",
        )
        product_config: ProductConfig = LiftConfig(
            common=common,
        )
        return PrivateComputationInstance(
            infra_config=infra_config,
            product_config=product_config,
        )

    def _get_mock_certificate_provider(self, certificate):
        certificate_provider = MagicMock()
        certificate_provider.get_certificate.return_value = certificate

        return certificate_provider
