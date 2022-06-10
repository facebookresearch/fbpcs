#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

import unittest
from unittest.mock import patch

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

    def test_create_user(self) -> None:
        # T122887119
        pass

    def test_delete_user(self) -> None:
        # T122887147
        pass

    def test_create_policy(self) -> None:
        # T122887174
        pass

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
        # T122887247
        pass

    def test_create_access_key(self) -> None:
        # T122887269
        pass

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
        # T122887335
        pass

    def test_read_json_file(self) -> None:
        # T122887357
        pass

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
        # T122887387
        pass
