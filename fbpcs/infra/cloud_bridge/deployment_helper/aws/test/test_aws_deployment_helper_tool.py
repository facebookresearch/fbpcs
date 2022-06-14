#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

import unittest
from unittest.mock import MagicMock, patch

from fbpcs.infra.cloud_bridge.deployment_helper.aws.aws_deployment_helper_tool import (
    AwsDeploymentHelperTool,
)


class TestAwsDeploymentHelperTool(unittest.TestCase):
    def setUp(self) -> None:
        with patch(
            "fbpcs.infra.cloud_bridge.deployment_helper.aws.aws_deployment_helper_tool.AwsDeploymentHelper"
        ):
            cli_args = MagicMock()
            self.aws_deployment_helper_tool = AwsDeploymentHelperTool(cli_args)

    def test_create(self) -> None:
        # T123419617
        pass

    def test_destroy(self) -> None:
        # T123420230
        pass
