#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

import unittest

from fbpcs.infra.certificate.pc_instance_ca_certificate_provider import (
    PCInstanceCaCertificateProvider,
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


class TestCertificateProviders(unittest.TestCase):
    def setUp(self) -> None:
        self.instance_id = "test_instance_123"
        self.test_server_cert_content = "test_server_certificate"
        self.test_ca_cert_content = "test_ca_certificate"
        self.pc_instance = self._create_pc_instance()

    def test_pc_instance_server_certificate_provider(self) -> None:
        # Arrange
        cert_provider = PCInstanceServerCertificateProvider(self.pc_instance)
        # Act
        actual_cert_content = cert_provider.get_certificate()
        # Assert
        self.assertEqual(self.test_server_cert_content, actual_cert_content)

    def test_pc_instance_ca_certificate_provider(self) -> None:
        # Arrange
        cert_provider = PCInstanceCaCertificateProvider(self.pc_instance)
        # Act
        actual_cert_content = cert_provider.get_certificate()
        # Assert
        self.assertEqual(self.test_ca_cert_content, actual_cert_content)

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
            server_certificate=self.test_server_cert_content,
            ca_certificate=self.test_ca_cert_content,
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
