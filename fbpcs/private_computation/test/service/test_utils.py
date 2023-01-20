#!/usr/bin/env fbpython
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.


import random
from unittest import IsolatedAsyncioTestCase
from unittest.mock import MagicMock

from fbpcs.infra.certificate.private_key import (
    NullPrivateKeyReferenceProvider,
    StaticPrivateKeyReferenceProvider,
)

from fbpcs.onedocker_binary_config import ONEDOCKER_REPOSITORY_PATH

from fbpcs.private_computation.entity.private_computation_instance import (
    PrivateComputationRole,
)
from fbpcs.private_computation.service.constants import (
    CA_CERTIFICATE_ENV_VAR,
    CA_CERTIFICATE_PATH_ENV_VAR,
    SERVER_CERTIFICATE_ENV_VAR,
    SERVER_CERTIFICATE_PATH_ENV_VAR,
    SERVER_HOSTNAME_ENV_VAR,
    SERVER_IP_ADDRESS_ENV_VAR,
    SERVER_PRIVATE_KEY_PATH_ENV_VAR,
    SERVER_PRIVATE_KEY_REF_ENV_VAR,
    SERVER_PRIVATE_KEY_REGION_ENV_VAR,
)

from fbpcs.private_computation.service.utils import (
    distribute_files_among_containers,
    gen_tls_server_hostnames_for_publisher,
    generate_env_vars_dict,
    generate_env_vars_dicts_list,
)


class TestUtils(IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.instance_id = "test_instance_123"
        self.server_domain = "study123.pci.facebook.com"
        self.server_key_ref_env_var_name = SERVER_PRIVATE_KEY_REF_ENV_VAR
        self.server_key_region_env_var_name = SERVER_PRIVATE_KEY_REGION_ENV_VAR
        self.server_key_install_path_env_var_name = SERVER_PRIVATE_KEY_PATH_ENV_VAR

    def test_distribute_files_among_containers(self) -> None:
        test_size = random.randint(10, 20)
        test_number_files = []
        test_number_conatiners = []
        for _ in range(test_size):
            test_number_files.append(random.randint(1, 100))
            test_number_conatiners.append(random.randint(1, 50))
        test_files_per_conatiners = [
            distribute_files_among_containers(
                test_number_files[i], test_number_conatiners[i]
            )
            for i in range(test_size)
        ]
        for i in range(test_size):
            self.assertEqual(test_number_files[i], sum(test_files_per_conatiners[i]))
            self.assertLessEqual(
                max(test_files_per_conatiners[i]) - min(test_files_per_conatiners[i]), 1
            )

    def test_get_server_uris(self) -> None:
        # Arrange
        expected_result_1 = [
            "node0.study123.pci.facebook.com",
            "node1.study123.pci.facebook.com",
        ]
        expected_result_2 = None

        # Act
        actual_result_1 = gen_tls_server_hostnames_for_publisher(
            self.server_domain, PrivateComputationRole.PUBLISHER, 2
        )
        actual_result_2 = gen_tls_server_hostnames_for_publisher(
            self.server_domain, PrivateComputationRole.PARTNER, 2
        )
        actual_result_3 = gen_tls_server_hostnames_for_publisher(
            None, PrivateComputationRole.PUBLISHER, 2
        )

        # Assert
        self.assertEqual(expected_result_1, actual_result_1)
        self.assertEqual(expected_result_2, actual_result_2)
        self.assertEqual(expected_result_2, actual_result_3)

    def test_generate_env_vars_missing_parameters(self) -> None:
        # Act
        result = generate_env_vars_dict()

        # Assert
        self.assertFalse("SERVER_HOSTNAME" in result)
        self.assertFalse("IP_ADDRESS" in result)
        self.assertFalse(self.server_key_ref_env_var_name in result)
        self.assertFalse(self.server_key_region_env_var_name in result)
        self.assertFalse(self.server_key_install_path_env_var_name in result)

    def test_generate_env_vars_server_hostname_no_ip(self) -> None:
        # Arrange
        key_name = "SERVER_HOSTNAME"

        # Act
        result = generate_env_vars_dict(
            server_hostname="test_hostname",
        )

        # Assert
        self.assertFalse(key_name in result)

    def test_generate_env_vars_server_ip_no_hostname(self) -> None:
        # Arrange
        key_name = "IP_ADDRESS"

        # Act
        result = generate_env_vars_dict(
            server_ip_address="127.0.0.1",
        )

        # Assert
        self.assertFalse(key_name in result)

    def test_generate_env_vars_both_server_addresses(self) -> None:
        # Arrange
        expected_ip = "127.0.0.1"
        expected_ip_key_name = "IP_ADDRESS"
        expected_hostname = "test_hostname"
        expected_hostname_key_name = "SERVER_HOSTNAME"

        # Act
        result = generate_env_vars_dict(
            server_ip_address=expected_ip,
            server_hostname=expected_hostname,
        )

        # Assert
        self.assertTrue(expected_ip_key_name in result)
        self.assertEqual(expected_ip, result[expected_ip_key_name])

        self.assertTrue(expected_hostname_key_name in result)
        self.assertEqual(expected_hostname, result[expected_hostname_key_name])

    def test_generate_env_vars_dicts_list(self) -> None:
        # Arrange
        num_containers = 2
        server_ip_addresses = ["test_ip_1", "test_ip_2"]
        server_ip_addresses_invalid = ["test_ip_1"]
        server_hostnames = ["test_hostname_1", "test_hostname_2"]
        repository_path = "test_path"
        server_cert = "test_server_cert"
        ca_cert = "test_ca_certificate"
        cert_path = "test_path"
        server_private_key_resource_id = "test_key1"
        server_private_key_region = "test-region"
        server_private_key_install_path = "test/path"

        expected_result = [
            {
                ONEDOCKER_REPOSITORY_PATH: repository_path,
                SERVER_CERTIFICATE_ENV_VAR: server_cert,
                SERVER_CERTIFICATE_PATH_ENV_VAR: cert_path,
                CA_CERTIFICATE_ENV_VAR: ca_cert,
                CA_CERTIFICATE_PATH_ENV_VAR: cert_path,
                SERVER_IP_ADDRESS_ENV_VAR: "test_ip_1",
                SERVER_HOSTNAME_ENV_VAR: "test_hostname_1",
                SERVER_PRIVATE_KEY_REF_ENV_VAR: server_private_key_resource_id,
                SERVER_PRIVATE_KEY_REGION_ENV_VAR: server_private_key_region,
                SERVER_PRIVATE_KEY_PATH_ENV_VAR: server_private_key_install_path,
            },
            {
                ONEDOCKER_REPOSITORY_PATH: repository_path,
                SERVER_CERTIFICATE_ENV_VAR: server_cert,
                SERVER_CERTIFICATE_PATH_ENV_VAR: cert_path,
                CA_CERTIFICATE_ENV_VAR: ca_cert,
                CA_CERTIFICATE_PATH_ENV_VAR: cert_path,
                SERVER_IP_ADDRESS_ENV_VAR: "test_ip_2",
                SERVER_HOSTNAME_ENV_VAR: "test_hostname_2",
                SERVER_PRIVATE_KEY_REF_ENV_VAR: server_private_key_resource_id,
                SERVER_PRIVATE_KEY_REGION_ENV_VAR: server_private_key_region,
                SERVER_PRIVATE_KEY_PATH_ENV_VAR: server_private_key_install_path,
            },
        ]
        server_certificate_provider = MagicMock()
        server_certificate_provider.get_certificate.return_value = server_cert
        ca_certificate_provider = MagicMock()
        ca_certificate_provider.get_certificate.return_value = ca_cert
        server_key_ref_provider = StaticPrivateKeyReferenceProvider(
            server_private_key_resource_id,
            server_private_key_region,
            server_private_key_install_path,
        )

        # Act
        result = generate_env_vars_dicts_list(
            num_containers=num_containers,
            repository_path=repository_path,
            server_certificate_provider=server_certificate_provider,
            server_certificate_path="test_path",
            ca_certificate_provider=ca_certificate_provider,
            ca_certificate_path="test_path",
            server_ip_addresses=server_ip_addresses,
            server_hostnames=server_hostnames,
            server_private_key_ref_provider=server_key_ref_provider,
        )

        # Assert
        self.assertEqual(result, expected_result)
        with self.assertRaises(ValueError) as e:
            generate_env_vars_dicts_list(
                num_containers=num_containers,
                repository_path=repository_path,
                server_certificate_provider=server_certificate_provider,
                server_certificate_path="test_path",
                ca_certificate_provider=ca_certificate_provider,
                ca_certificate_path="test_path",
                server_ip_addresses=server_ip_addresses_invalid,
                server_hostnames=server_hostnames,
                server_private_key_ref_provider=server_key_ref_provider,
            )
            self.assertIn(
                "num_contaienrs 2; {SERVER_IP_ADDRESS_ENV_VAR} 1",
                str(e.exception),
            )

    def test_generate_env_vars_null_server_key_ref(self) -> None:
        # Arrange

        # Act
        result = generate_env_vars_dict(
            server_private_key_ref_provider=NullPrivateKeyReferenceProvider()
        )

        # Assert
        self.assertFalse(self.server_key_ref_env_var_name in result)
        self.assertFalse(self.server_key_region_env_var_name in result)
        self.assertFalse(self.server_key_install_path_env_var_name in result)

    def test_generate_env_vars_server_key_ref(self) -> None:
        # Arrange
        expected_resource_id = "12345"
        expected_region = "test-region"
        expected_install_path = "test/path"
        key_ref_provider = StaticPrivateKeyReferenceProvider(
            resource_id=expected_resource_id,
            region=expected_region,
            install_path=expected_install_path,
        )

        # Act
        result = generate_env_vars_dict(
            server_private_key_ref_provider=key_ref_provider
        )

        # Assert
        self.assertEqual(expected_resource_id, result[self.server_key_ref_env_var_name])
        self.assertEqual(expected_region, result[self.server_key_region_env_var_name])
        self.assertEqual(
            expected_install_path, result[self.server_key_install_path_env_var_name]
        )
