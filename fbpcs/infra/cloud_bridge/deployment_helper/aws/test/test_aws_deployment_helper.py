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
        # T122887191
        pass

    def test_attach_user_policy(self) -> None:
        # T122887198
        pass

    def test_detach_user_policy(self) -> None:
        # T122887211
        pass

    def test_list_policies(self) -> None:
        # T122887235
        self.aws_deployment_helper.iam.list_policies.return_value = {
            "Policies": [{"PolicyName": "A"}, {"PolicyName": "B"}, {"PolicyName": "C"}]
        }

        with self.subTest("basic"):
            expected = ["A", "B", "C"]
            self.assertEqual(expected, self.aws_deployment_helper.list_policies())

        # Check client error
        with self.subTest("list_policies.ClientError"):
            self.aws_deployment_helper.iam.list_policies.reset_mock()
            self.aws_deployment_helper.iam.list_policies.return_value = {
                "Policies": [
                    {"PolicyName": "A"},
                    {"PolicyName": "B"},
                    {"PolicyName": "C"},
                ]
            }
            self.aws_deployment_helper.iam.list_policies.side_effect = ClientError(
                error_response={"Error": {}},
                operation_name="list_policies",
            )
            self.assertEqual([], self.aws_deployment_helper.list_policies())

    def test_list_users(self) -> None:
        # T122887247
        pass

    def test_create_access_key(self) -> None:
        # T122887269
        pass

    def test_delete_access_key(self) -> None:
        # T122887297
        pass

    def test_list_access_keys(self) -> None:
        # T122887335
        pass

    def test_read_json_file(self) -> None:
        # T122887357
        pass

    def test_create_user_workflow(self) -> None:
        # T122887368
        pass

    def test_delete_user_workflow(self) -> None:
        # T122887387
        pass
