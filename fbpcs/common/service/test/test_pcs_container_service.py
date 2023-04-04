#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

import unittest
from typing import List, Optional
from unittest.mock import call, MagicMock, patch

from fbpcp.entity.cloud_provider import CloudProvider
from fbpcp.entity.container_instance import ContainerInstance, ContainerInstanceStatus
from fbpcp.entity.container_permission import ContainerPermissionConfig
from fbpcp.entity.container_type import ContainerType, ContainerTypeConfig
from fbpcp.gateway.ecs import ECSGateway
from fbpcp.service.container_aws import AWSContainerService
from fbpcs.common.entity.pcs_container_instance import PCSContainerInstance
from fbpcs.common.service.pcs_container_service import PCSContainerService


TEST_INSTANCE_ID_1 = "test-instance-id-1"
TEST_INSTANCE_ID_2 = "test-instance-id-2"
TEST_INSTANCE_ID_DNE = "test-instance-id-dne"
TEST_ACCOUNT_ID = "123456789"
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
TEST_CONTAINER_PERMISSION = ContainerPermissionConfig("test-role-id")


class TestPcsContainerService(unittest.TestCase):
    CONTAINER_CPU = 4
    CONTAINER_MEMORY = 32

    def setUp(self):
        self.mock_ecs_boto_client = MagicMock()
        example_tasks_responses = [
            {
                "tasks": [
                    {
                        "taskArn": f"arn:aws:ecs:{TEST_REGION}:{TEST_ACCOUNT_ID}:task/{TEST_CLUSTER}/{x}",
                        "containers": [
                            {
                                "lastStatus": "RUNNING",
                                "networkInterfaces": [
                                    {"privateIpv4Address": f"10.0.0.{x}"}
                                ],
                            }
                        ],
                    }
                    for x in range(y, z)
                ],
                "failures": [],
            }
            for y, z in [(0, 100), (100, 150)]
        ]
        self.mock_ecs_boto_client.describe_tasks.side_effect = example_tasks_responses
        self.ecs_gateway = ECSGateway(TEST_REGION)
        self.ecs_gateway.client = self.mock_ecs_boto_client
        inner_container_svc = AWSContainerService(
            TEST_REGION, TEST_CLUSTER, TEST_SUBNETS, TEST_KEY_ID, TEST_KEY_DATA
        )
        inner_container_svc.ecs_gateway = self.ecs_gateway
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
                permission=TEST_CONTAINER_PERMISSION,
            ),
            call(
                container_definition=f"{TEST_TASK_DEFNITION}#{TEST_CONTAINER_DEFNITION}",
                cmd=TEST_CMD_2,
                env_vars=TEST_ENV_VARS_2,
                container_type=TEST_CONTAINER_TYPE,
                permission=TEST_CONTAINER_PERMISSION,
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
            permission=TEST_CONTAINER_PERMISSION,
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

    def test_get_instances(self) -> None:
        instance_ids = [
            f"arn:aws:ecs:{TEST_REGION}:{TEST_ACCOUNT_ID}:task/{TEST_CLUSTER}/{x}"
            for x in range(150)
        ]
        expected_log_urls = []
        expected_container_name = TEST_CLUSTER.replace("-cluster", "-container")
        expected_log_group = f"/ecs/{expected_container_name}".replace("/", "$252F")
        for x in range(150):
            expected_log_stream = f"ecs/{expected_container_name}/{x}".replace(
                "/", "$252F"
            )
            expected_log_url = (
                f"https://{TEST_REGION}.console.aws.amazon.com/cloudwatch/home?"
                f"region={TEST_REGION}#logsV2:log-groups/"
                f"log-group/{expected_log_group}/"
                f"log-events/{expected_log_stream}"
            )
            expected_log_urls.append(expected_log_url)
        expected_instances_by_id = {}
        for x in range(150):
            instance_id = (
                f"arn:aws:ecs:{TEST_REGION}:{TEST_ACCOUNT_ID}:task/{TEST_CLUSTER}/{x}"
            )
            expected_instances_by_id[instance_id] = PCSContainerInstance(
                instance_id=instance_id,
                ip_address=f"10.0.0.{x}",
                status=ContainerInstanceStatus.STARTED,
                log_url=expected_log_urls[x],
                cpu=None,
                memory=None,
            )

        instances: list[Optional[ContainerInstance]] = self.container_svc.get_instances(
            instance_ids
        )

        for instance in instances:
            self.assertIsNotNone(instance)
            self.assertEqual(
                instance, expected_instances_by_id.get(instance.instance_id)
            )
        self.assertEqual(self.mock_ecs_boto_client.describe_tasks.call_count, 2)
        self.mock_ecs_boto_client.describe_tasks.assert_has_calls(
            [
                call(cluster="test-cluster", tasks=instance_ids[:100]),
                call(cluster="test-cluster", tasks=instance_ids[100:]),
            ]
        )
