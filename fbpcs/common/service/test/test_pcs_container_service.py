#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

import unittest
from typing import List
from unittest.mock import call, patch

from fbpcp.entity.cloud_provider import CloudProvider
from fbpcp.entity.container_instance import ContainerInstance, ContainerInstanceStatus
from fbpcp.entity.container_type import ContainerType, ContainerTypeConfig
from fbpcp.service.container_aws import AWSContainerService
from fbpcs.common.service.pcs_container_service import PCSContainerService


TEST_INSTANCE_ID_1 = "test-instance-id-1"
TEST_INSTANCE_ID_2 = "test-instance-id-2"
TEST_INSTANCE_ID_DNE = "test-instance-id-dne"
TEST_REGION = "us-west-2"
TEST_KEY_ID = "test-key-id"
TEST_KEY_DATA = "test-key-data"
TEST_SESSION_TOKEN = "test-session-token"
TEST_CLUSTER = "test-cluster"
TEST_SUBNETS = ["test-subnet0", "test-subnet1"]
TEST_IP_ADDRESS = "127.0.0.1"
TEST_TASK_DEFNITION = "test-task-definition:1"
TEST_CONTAINER_DEFNITION = "test-container-definition"

TEST_ENV_VARS = {"k1": "v1", "k2": "v2"}
TEST_ENV_VARS_2 = {"k3": "v3", "k4": "v4"}
TEST_CMD_1 = "test_1"
TEST_CMD_2 = "test_2"
TEST_CLOUD_PROVIDER = CloudProvider.AWS
TEST_CONTAINER_TYPE = ContainerType.MEDIUM


class TestPcsContainerService(unittest.TestCase):
    @patch("fbpcp.gateway.ecs.ECSGateway")
    def setUp(self, MockECSGateway):
        inner_container_svc = AWSContainerService(
            TEST_REGION, TEST_CLUSTER, TEST_SUBNETS, TEST_KEY_ID, TEST_KEY_DATA
        )
        inner_container_svc.ecs_gateway = MockECSGateway()
        self.container_svc = PCSContainerService(
            inner_container_service=inner_container_svc
        )
        self.test_container_config: ContainerTypeConfig = (
            ContainerTypeConfig.get_config(TEST_CLOUD_PROVIDER, TEST_CONTAINER_TYPE)
        )

    @patch.object(PCSContainerService, "create_instance")
    def test_create_instances_with_list_of_env_vars(self, mock_create_instance):
        # Arrange
        created_instances: List[ContainerInstance] = [
            ContainerInstance(
                TEST_INSTANCE_ID_1,
                TEST_IP_ADDRESS,
                ContainerInstanceStatus.STARTED,
                cpu=self.test_container_config.cpu,
                memory=self.test_container_config.memory,
            ),
            ContainerInstance(
                TEST_INSTANCE_ID_2,
                TEST_IP_ADDRESS,
                ContainerInstanceStatus.STARTED,
                cpu=self.test_container_config.cpu,
                memory=self.test_container_config.memory,
            ),
        ]

        mock_create_instance.side_effect = created_instances

        cmd_list = [TEST_CMD_1, TEST_CMD_2]

        create_instance_calls = [
            call(
                container_definition=f"{TEST_TASK_DEFNITION}#{TEST_CONTAINER_DEFNITION}",
                cmd=TEST_CMD_1,
                env_vars=TEST_ENV_VARS,
                container_type=TEST_CONTAINER_TYPE,
            ),
            call(
                container_definition=f"{TEST_TASK_DEFNITION}#{TEST_CONTAINER_DEFNITION}",
                cmd=TEST_CMD_2,
                env_vars=TEST_ENV_VARS_2,
                container_type=TEST_CONTAINER_TYPE,
            ),
        ]

        # Act
        container_instances: List[
            ContainerInstance
        ] = self.container_svc.create_instances(
            container_definition=f"{TEST_TASK_DEFNITION}#{TEST_CONTAINER_DEFNITION}",
            cmds=cmd_list,
            env_vars=[TEST_ENV_VARS, TEST_ENV_VARS_2],
            container_type=TEST_CONTAINER_TYPE,
        )

        # Assert
        self.assertEqual(container_instances, created_instances)
        mock_create_instance.assert_has_calls(create_instance_calls)

    def test_create_instances_throw_with_invalid_list_of_env_vars(self):
        # Arrange
        cmd_list = [TEST_CMD_1, TEST_CMD_2, TEST_CMD_2]

        # Act & Assert
        with self.assertRaises(ValueError):
            self.container_svc.create_instances(
                container_definition=f"{TEST_TASK_DEFNITION}#{TEST_CONTAINER_DEFNITION}",
                cmds=cmd_list,
                env_vars=[TEST_ENV_VARS],
                container_type=TEST_CONTAINER_TYPE,
            )

        with self.assertRaises(ValueError):
            self.container_svc.create_instances(
                container_definition=f"{TEST_TASK_DEFNITION}#{TEST_CONTAINER_DEFNITION}",
                cmds=cmd_list,
                env_vars=[],
                container_type=TEST_CONTAINER_TYPE,
            )
