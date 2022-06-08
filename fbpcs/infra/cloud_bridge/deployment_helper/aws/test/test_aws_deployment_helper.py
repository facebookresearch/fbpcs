#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

import unittest
from unittest.mock import patch
import boto3

from fbpcs.infra.cloud_bridge.deployment_helper.aws.aws_deployment_helper import (
    AwsDeploymentHelper,
)
class TestAwsDeploymentHelper(unittest.TestCase):
    def setUp(self) -> None:
        with patch("fbpcs.infra.cloud_bridge.deployment_helper.aws.aws_deployment_helper.boto3"):
            self.aws_deployment_helper = AwsDeploymentHelper()

    def test_create_user(self) -> None:
        pass

    def test_delete_user(self) -> None:
        pass

    def test_create_policy(self) -> None:
        pass

    def test_delete_policy(self) -> None:
        pass

    def test_attach_user_policy(self) -> None:
        pass

    def test_detach_user_policy(self) -> None:
        pass

    def test_list_policies(self) -> None:
        pass

    def test_list_users(self) -> None:
        pass

    def test_create_access_key(self) -> None:
        pass

    def test_delete_access_key(self) -> None:

        # Basic test
        with self.subTest("basic"):
            self.aws_deployment_helper = AwsDeploymentHelper()
            self.aws_deployment_helper.create_user('userA')
            self.aws_deployment_helper.create_user_workflow('userA')
            access_key_list = set(self.list_access_keys(user_name='userA'))
            access_key_set = set(access_key_list)

            for access_key in access_key_list:
                self.delete_access_key(user_name='userA', access_key=access_key)
                access_key_set.remove(access_key)
                self.assertEqual(
                    access_key_set,
                    self.aws_deployment_helper.list_access_keys('userA')
                )
            self.delete_user(user_name='userA')

    def test_list_access_keys(self) -> None:
        pass

    def test_read_json_file(self) -> None:
        pass

    def test_create_user_workflow(self) -> None:
        pass

    def test_delete_user_workflow(self) -> None:
        pass
