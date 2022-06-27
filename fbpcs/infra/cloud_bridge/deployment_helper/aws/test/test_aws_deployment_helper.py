#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

import pathlib
import unittest
from unittest.mock import MagicMock, patch

from botocore.exceptions import ClientError

from fbpcs.infra.cloud_bridge.deployment_helper.aws.aws_deployment_helper import (
    AwsDeploymentHelper,
)


class TestAwsDeploymentHelper(unittest.TestCase):
    def setUp(self) -> None:
        with patch(
            "fbpcs.infra.cloud_bridge.deployment_helper.aws.aws_deployment_helper.boto3"
        ):
            self.aws_deployment_helper = AwsDeploymentHelper()
            self.aws_deployment_helper.log = MagicMock()

    def test_create_user(self) -> None:
        self.aws_deployment_helper.iam.create_user.return_value = True

        with self.subTest("basic"):
            self.assertIsNone(self.aws_deployment_helper.create_user("abc"))
            self.aws_deployment_helper.iam.create_user.was_called_once()

        # Check client error
        with self.subTest("create_user.ClientError"):
            self.aws_deployment_helper.iam.create_user.reset_mock()
            self.aws_deployment_helper.iam.create_user.side_effect = ClientError(
                error_response={"Error": {}},
                operation_name="create_user",
            )
            self.assertIsNone(self.aws_deployment_helper.create_user(""))
            self.aws_deployment_helper.iam.create_user.was_called_once()

    def test_delete_user(self) -> None:
        self.aws_deployment_helper.iam.delete_user.return_value = True

        # Basic test case.
        with self.subTest("basic"):
            self.assertEqual(None, self.aws_deployment_helper.delete_user("user"))

        # Check client error
        with self.subTest("delete_user.ClientError"):
            self.aws_deployment_helper.iam.delete_user.reset_mock()
            self.aws_deployment_helper.iam.delete_user.side_effect = ClientError(
                error_response={"Error": {}},
                operation_name="delete_user",
            )
            self.assertEqual(None, self.aws_deployment_helper.delete_user(""))
        pass

    def test_create_policy(self) -> None:
        test_policy_name = "TestIamPolicyName"
        test_policy_params = {"test-key-1": "test-val-1"}
        test_user_name = "test-user-name"
        test_policy_json_data = {"test-key-2": "test-val-2"}
        test_policy_json_data_string = '{"test-key-2": "test-val-2"}'
        self.aws_deployment_helper.read_json_file = MagicMock()
        self.aws_deployment_helper.read_json_file.return_value = test_policy_json_data
        self.aws_deployment_helper.iam.create_policy.return_value = {}

        # Test creation successful
        with self.subTest("basic"):
            # Test
            self.aws_deployment_helper.create_policy(
                test_policy_name, test_policy_params
            )
            # Assert
            self.aws_deployment_helper.read_json_file.assert_called_once_with(
                file_name="iam_policies/fb_pc_iam_policy.json",
                policy_params=test_policy_params,
            )
            self.aws_deployment_helper.iam.create_policy.assert_called_once_with(
                PolicyName=test_policy_name, PolicyDocument=test_policy_json_data_string
            )

        # Test creation failed with client errors
        with self.subTest("client_error_EntityAlreadyExists"):
            self.aws_deployment_helper.iam.create_policy.reset_mock()
            self.aws_deployment_helper.iam.create_policy.side_effect = ClientError(
                error_response={"Error": {"Code": "EntityAlreadyExists"}},
                operation_name="create_policy",
            )
            # Test
            self.aws_deployment_helper.create_policy(
                test_policy_name, test_policy_params
            )
            # Assert
            self.aws_deployment_helper.iam.create_policy.assert_called_once_with(
                PolicyName=test_policy_name,
                PolicyDocument=test_policy_json_data_string,
            )
            self.aws_deployment_helper.log.error.assert_called_once()

        with self.subTest("client_error_without_username"):
            self.aws_deployment_helper.log.error.reset_mock()
            self.aws_deployment_helper.iam.create_policy.reset_mock()
            self.aws_deployment_helper.iam.create_policy.side_effect = ClientError(
                error_response={"Error": {"Code": "InvalidInput"}},
                operation_name="create_policy",
            )
            # Test
            self.aws_deployment_helper.create_policy(
                test_policy_name, test_policy_params
            )
            # Assert
            self.aws_deployment_helper.log.error.assert_called_once()
        with self.subTest("client_error_with_username"):
            self.aws_deployment_helper.log.error.reset_mock()
            self.aws_deployment_helper.iam.create_policy.reset_mock()
            self.aws_deployment_helper.iam.create_policy.side_effect = ClientError(
                error_response={"Error": {"Code": "InvalidInput"}},
                operation_name="create_policy",
            )
            # Test
            self.aws_deployment_helper.create_policy(
                test_policy_name, test_policy_params, test_user_name
            )
            # Assert
            self.aws_deployment_helper.log.error.assert_called_once()

    def test_delete_policy(self) -> None:
        self.aws_deployment_helper.iam.delete_policy.return_value = True

        # Basic test case.
        with self.subTest("basic"):
            self.assertEqual(None, self.aws_deployment_helper.delete_policy("abc"))

        # Check client error
        with self.subTest("delete_policy.ClientError"):
            self.aws_deployment_helper.iam.delete_policy.reset_mock()
            self.aws_deployment_helper.iam.delete_policy.side_effect = ClientError(
                error_response={"Error": {}},
                operation_name="delete_policy",
            )
            self.assertEqual(None, self.aws_deployment_helper.delete_policy(""))

    def test_attach_user_policy(self) -> None:
        self.aws_deployment_helper.iam.list_policies.return_value = {
            "Policies": [{"PolicyName": "A"}]
        }
        self.aws_deployment_helper.iam.list_users.return_value = {
            "Users": [{"UserName": "Z"}]
        }

        # Basic green path test
        with self.subTest("basic"):
            self.assertEqual(
                None, self.aws_deployment_helper.attach_user_policy("A", "Z")
            )

        # Username not in current users
        with self.subTest("user_name not in current_users"):
            self.assertRaises(
                Exception, self.aws_deployment_helper.attach_user_policy, "A", "Y"
            )

        # Policy not in current policies
        with self.subTest("policy_name not in current_policies"):
            self.assertRaises(
                Exception, self.aws_deployment_helper.attach_user_policy, "B", "Z"
            )

        # Client error
        with self.subTest("attach_user_policy.ClientError"):
            self.aws_deployment_helper.iam.attach_user_policy.reset_mock()
            self.aws_deployment_helper.iam.attach_user_policy.side_effect = ClientError(
                error_response={"Error": {}},
                operation_name="attach_user_policy",
            )
            self.assertEqual(
                None, self.aws_deployment_helper.attach_user_policy("A", "Z")
            )
            self.aws_deployment_helper.iam.attach_user_policy.assert_called_once()

    def test_detach_user_policy(self) -> None:
        # T122887211
        pass

    def test_list_policies(self) -> None:
        self.aws_deployment_helper.iam.list_policies.return_value = {
            "Policies": [{"PolicyName": "A"}, {"PolicyName": "B"}, {"PolicyName": "C"}]
        }

        with self.subTest("basic"):
            expected = ["A", "B", "C"]
            self.assertEqual(expected, self.aws_deployment_helper.list_policies())
            self.aws_deployment_helper.iam.list_policies.assert_called_once()

        # Check client error
        with self.subTest("list_policies.ClientError"):
            self.aws_deployment_helper.iam.list_policies.reset_mock()
            self.aws_deployment_helper.iam.list_policies.side_effect = ClientError(
                error_response={"Error": {}},
                operation_name="list_policies",
            )
            self.assertEqual([], self.aws_deployment_helper.list_policies())
            self.aws_deployment_helper.iam.list_policies.assert_called_once()

    def test_list_users(self) -> None:
        self.aws_deployment_helper.iam.list_users.return_value = {
            "Users": [{"UserName": "A"}, {"UserName": "B"}, {"UserName": "C"}]
        }

        with self.subTest("basic"):
            expected = ["A", "B", "C"]
            self.assertEqual(expected, self.aws_deployment_helper.list_users())
            self.aws_deployment_helper.iam.list_users.assert_called_once()

        # Check client error
        with self.subTest("list_users.ClientError"):
            self.aws_deployment_helper.iam.list_users.reset_mock()
            self.aws_deployment_helper.iam.list_users.side_effect = ClientError(
                error_response={"Error": {}},
                operation_name="list_users",
            )
            self.assertEqual([], self.aws_deployment_helper.list_users())
            self.aws_deployment_helper.iam.list_users.assert_called_once()

    def test_create_access_key(self) -> None:

        self.aws_deployment_helper.iam.create_access_key.return_value = {
            "AccessKey": {"AccessKeyId": 1, "SecretAccessKey": 2}
        }

        # Basic test case
        with self.subTest("basic"):
            self.assertIsNone(self.aws_deployment_helper.create_access_key("user1"))

        self.aws_deployment_helper.iam.create_access_key.assert_called_once_with(
            UserName="user1"
        )

        # Check client error
        with self.subTest("create_access_key.ClientError"):
            self.aws_deployment_helper.iam.create_access_key.reset_mock()
            self.aws_deployment_helper.iam.create_access_key.side_effect = ClientError(
                error_response={"Error": {}},
                operation_name="create_access_key",
            )
            self.assertIsNone(self.aws_deployment_helper.create_access_key("user1"))
            self.aws_deployment_helper.iam.create_access_key.assert_called_once()

    def test_delete_access_key(self) -> None:
        # T122887297
        with self.subTest("basic"):
            self.assertIsNone(
                self.aws_deployment_helper.delete_access_key("user", "key")
            )
            self.aws_deployment_helper.iam.delete_access_key.assert_called_once()

        with self.subTest("delete_access_key.ClientError"):
            self.aws_deployment_helper.iam.delete_access_key.reset_mock()
            self.aws_deployment_helper.iam.delete_access_key.side_effect = ClientError(
                error_response={"Error": {}},
                operation_name="delete_access_key",
            )
            self.assertIsNone(
                self.aws_deployment_helper.delete_access_key(
                    "another_user", "another_key"
                )
            )
            self.aws_deployment_helper.iam.delete_access_key.assert_called_once()

    def test_list_access_keys(self) -> None:
        self.aws_deployment_helper.iam.list_access_keys.return_value = {
            "AccessKeyMetadata": [
                {"AccessKeyId": "A"},
                {"AccessKeyId": "B"},
                {"AccessKeyId": "C"},
            ]
        }

        with self.subTest("basic"):
            expected = ["A", "B", "C"]
            self.assertEqual(expected, self.aws_deployment_helper.list_access_keys(""))
            self.aws_deployment_helper.iam.list_access_keys.assert_called_once()

        with self.subTest("list_access_keys.ClientError"):
            self.aws_deployment_helper.iam.list_access_keys.reset_mock()
            self.aws_deployment_helper.iam.list_access_keys.side_effect = ClientError(
                error_response={"Error": {}},
                operation_name="list_access_keys",
            )
            self.assertEqual([], self.aws_deployment_helper.list_access_keys(""))
            self.aws_deployment_helper.iam.list_access_keys.assert_called_once()

    def test_read_json_file(self) -> None:
        self.aws_deployment_helper.region = "test_region"
        test_policy = MagicMock()
        test_policy.cluster_name = "test_cluster_name"

        test_file = (
            pathlib.Path(__file__).parent
            / "test_resources"
            / "test_aws_deployment_helper_config.json"
        )
        test_data = self.aws_deployment_helper.read_json_file(test_file, test_policy)
        self.assertEqual(test_data["REGION"], self.aws_deployment_helper.region)
        self.assertEqual(test_data["CLUSTER_NAME"], test_policy.cluster_name)

    def test_create_user_workflow(self) -> None:
        self.aws_deployment_helper.iam.create_user_workflow.return_value = True
        self.assertEqual(None, self.aws_deployment_helper.create_user_workflow("user1"))
        self.aws_deployment_helper.iam.create_user.assert_called_once_with(
            UserName="user1"
        )
        self.aws_deployment_helper.iam.create_access_key.assert_called_once_with(
            UserName="user1"
        )

    def test_delete_user_workflow(self) -> None:
        self.aws_deployment_helper.iam.delete_user_workflow.return_value = True
        self.assertIsNone(self.aws_deployment_helper.delete_user_workflow("user1"))
        self.aws_deployment_helper.iam.list_access_keys.assert_called_once_with(
            UserName="user1"
        )
        self.aws_deployment_helper.iam.delete_user.assert_called_once_with(
            UserName="user1"
        )

        with self.subTest("list_access_keys.ClientError"):
            self.aws_deployment_helper.iam.list_access_keys.reset_mock()
            self.aws_deployment_helper.iam.list_access_keys.side_effect = ClientError(
                error_response={"Error": {}},
                operation_name="list_access_keys",
            )
            self.assertEqual([], self.aws_deployment_helper.list_access_keys(""))
            self.aws_deployment_helper.iam.list_access_keys.assert_called_once()

        with self.subTest("delete_access_key.ClientError"):
            self.aws_deployment_helper.iam.delete_access_key.reset_mock()
            self.aws_deployment_helper.iam.delete_access_key.side_effect = ClientError(
                error_response={"Error": {}},
                operation_name="delete_access_key",
            )
            self.assertIsNone(
                self.aws_deployment_helper.delete_access_key(
                    "another_user", "another_key"
                )
            )
            self.aws_deployment_helper.iam.delete_access_key.assert_called_once()

        with self.subTest("delete_user.ClientError"):
            self.aws_deployment_helper.iam.delete_user.reset_mock()
            self.aws_deployment_helper.iam.delete_user.side_effect = ClientError(
                error_response={"Error": {}},
                operation_name="delete_user",
            )
            self.assertIsNone(self.aws_deployment_helper.delete_user(""))
