#!/usr/bin/env fbpython
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.


from unittest import IsolatedAsyncioTestCase
from unittest.mock import patch

from fbpcs.infra.certificate.basic_ca_certificate_provider import (
    BasicCaCertificateProvider,
)
from fbpcs.infra.certificate.pc_instance_server_certificate import (
    PCInstanceServerCertificateProvider,
)

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
from fbpcs.private_computation.service.argument_helper import (
    TLS_ARG_KEY_CA_CERT_PATH,
    TLS_ARG_KEY_SERVER_CERT_PATH,
)

from fbpcs.private_computation.service.constants import (
    CA_CERT_PATH,
    CA_CERTIFICATE_ENV_VAR,
    CA_CERTIFICATE_PATH_ENV_VAR,
    SERVER_CERT_PATH,
    SERVER_CERTIFICATE_ENV_VAR,
    SERVER_CERTIFICATE_PATH_ENV_VAR,
)

from fbpcs.private_computation.service.mpc.entity.mpc_instance import MPCParty

from fbpcs.private_computation.service.mpc.mpc import (
    create_and_start_mpc_instance,
    MPCService,
)

ca_cert_content = "ca certificate"
server_cert_content = "server certificate"


class TestUtils(IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.instance_id = "test_instance_123"
        self.pc_instance = self._create_pc_instance()
        # TODO: replace this with checked in certificates

        self.ca_certificate_provider = BasicCaCertificateProvider(ca_cert_content)
        self.server_certificate_provider = PCInstanceServerCertificateProvider(
            self.pc_instance
        )
        self.test_binary_version = "latest"
        self.mpc_svc = self._create_mpc_svc()
        self.server_domain = "study123.pci.facebook.com"
        self.game_name = "private_lift"

    @patch.object(MPCService, "start_instance_async")
    @patch.object(
        BasicCaCertificateProvider, "get_certificate", return_value=ca_cert_content
    )
    @patch.object(
        PCInstanceServerCertificateProvider,
        "get_certificate",
        return_value=server_cert_content,
    )
    @patch.object(MPCService, "get_instance")
    async def test_create_and_start_mpc_instance_env_vars_setup(
        self,
        getInstanceMock,
        serverCertProviderMock,
        caCertProviderMock,
        startInstanceAsyncMock,
    ) -> None:
        # Arrange
        expected_env_vars = {
            SERVER_CERTIFICATE_ENV_VAR: server_cert_content,
            SERVER_CERTIFICATE_PATH_ENV_VAR: SERVER_CERT_PATH,
            CA_CERTIFICATE_ENV_VAR: ca_cert_content,
            CA_CERTIFICATE_PATH_ENV_VAR: CA_CERT_PATH,
        }
        game_args = [
            {
                TLS_ARG_KEY_CA_CERT_PATH: CA_CERT_PATH,
                TLS_ARG_KEY_SERVER_CERT_PATH: SERVER_CERT_PATH,
            }
        ]
        # Act
        await create_and_start_mpc_instance(
            self.mpc_svc,
            self.instance_id,
            "private_lift",
            MPCParty.SERVER,
            num_containers=1,
            binary_version=self.test_binary_version,
            server_certificate_path=SERVER_CERT_PATH,
            ca_certificate_path=CA_CERT_PATH,
            game_args=game_args,
            server_certificate_provider=self.server_certificate_provider,
            ca_certificate_provider=self.ca_certificate_provider,
            server_domain=self.server_domain,
        )

        # Assert
        getInstanceMock.assert_called_once_with(self.instance_id)
        startInstanceAsyncMock.assert_awaited_once_with(
            instance_id=self.instance_id,
            server_ips=None,
            timeout=43200,
            version=self.test_binary_version,
            env_vars=expected_env_vars,
            certificate_request=None,
            wait_for_containers_to_start_up=True,
        )

    @patch.object(MPCService, "start_instance_async")
    @patch.object(
        BasicCaCertificateProvider, "get_certificate", return_value=ca_cert_content
    )
    @patch.object(
        PCInstanceServerCertificateProvider,
        "get_certificate",
        return_value=server_cert_content,
    )
    @patch.object(MPCService, "create_instance")
    @patch.object(
        MPCService, "get_instance", side_effect=Exception("Instance Not Found")
    )
    async def test_create_and_start_mpc_instance_server_uris(
        self,
        getInstanceMock,
        createInstanceMock,
        serverCertProviderMock,
        caCertProviderMock,
        startInstanceAsyncMock,
    ) -> None:
        # Arrange
        game_args = [
            {
                TLS_ARG_KEY_CA_CERT_PATH: CA_CERT_PATH,
                TLS_ARG_KEY_SERVER_CERT_PATH: SERVER_CERT_PATH,
            }
        ]
        expected_server_uris = [
            f"node0.{self.server_domain}",
            f"node1.{self.server_domain}",
        ]
        # Act
        await create_and_start_mpc_instance(
            self.mpc_svc,
            self.instance_id,
            self.game_name,
            MPCParty.SERVER,
            num_containers=2,
            binary_version=self.test_binary_version,
            server_certificate_path=SERVER_CERT_PATH,
            ca_certificate_path=CA_CERT_PATH,
            game_args=game_args,
            server_certificate_provider=self.server_certificate_provider,
            ca_certificate_provider=self.ca_certificate_provider,
            server_domain=self.server_domain,
        )

        # Assert
        getInstanceMock.assert_called_once_with(self.instance_id)
        createInstanceMock.assert_called_once_with(
            instance_id=self.instance_id,
            game_name=self.game_name,
            mpc_party=MPCParty.SERVER,
            num_workers=2,
            game_args=game_args,
            server_uris=expected_server_uris,
        )

    def _create_pc_instance(self) -> PrivateComputationInstance:
        infra_config: InfraConfig = InfraConfig(
            instance_id=self.instance_id,
            role=PrivateComputationRole.PARTNER,
            status=PrivateComputationInstanceStatus.PID_PREPARE_COMPLETED,
            status_update_ts=1600000000,
            instances=[],
            game_type=PrivateComputationGameType.LIFT,
            num_pid_containers=2,
            num_mpc_containers=2,
            num_files_per_mpc_container=4,
            status_updates=[],
            run_id="681ba82c-16d9-11ed-861d-0242ac120002",
            pcs_features={PCSFeature.PCF_TLS},
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

    def _create_mpc_svc(self) -> MPCService:
        cspatcher = patch("fbpcp.service.container.ContainerService")
        irpatcher = patch(
            "fbpcs.private_computation.service.mpc.repository.mpc_instance.MPCInstanceRepository"
        )
        gspatcher = patch(
            "fbpcs.private_computation.service.mpc.mpc_game.MPCGameService"
        )
        container_svc = cspatcher.start()
        instance_repository = irpatcher.start()
        mpc_game_svc = gspatcher.start()
        for patcher in (cspatcher, irpatcher, gspatcher):
            self.addCleanup(patcher.stop)
        return MPCService(
            container_svc,
            instance_repository,
            "test_task_definition",
            mpc_game_svc,
        )
