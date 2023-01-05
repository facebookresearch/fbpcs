#!/usr/bin/env fbpython
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.


import random
from unittest import IsolatedAsyncioTestCase

from fbpcs.private_computation.entity.private_computation_instance import (
    PrivateComputationRole,
)

from fbpcs.private_computation.service.utils import (
    distribute_files_among_containers,
    generate_env_vars_dict,
    get_server_uris,
)


class TestUtils(IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.instance_id = "test_instance_123"
        self.server_domain = "study123.pci.facebook.com"

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
        actual_result_1 = get_server_uris(
            self.server_domain, PrivateComputationRole.PUBLISHER, 2
        )
        actual_result_2 = get_server_uris(
            self.server_domain, PrivateComputationRole.PARTNER, 2
        )
        actual_result_3 = get_server_uris(None, PrivateComputationRole.PUBLISHER, 2)

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
