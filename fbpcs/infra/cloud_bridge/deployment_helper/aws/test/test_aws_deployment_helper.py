#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

import unittest
from unittest.mock import patch

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
        pass

    def test_list_access_keys(self) -> None:
        pass

    def test_read_json_file(self) -> None:
        pass

    def test_create_user_workflow(self) -> None:
        pass

    def test_delete_user_workflow(self) -> None:
        pass
