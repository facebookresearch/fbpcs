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

    @patch(
        "fbpcs.infra.cloud_bridge.deployment_helper.aws.aws_deployment_helper_tool.AwsDeploymentHelper"
    )
    @patch.object(AwsDeploymentHelperTool, "aws_deployment_helper_obj", create=True)
    def test_destroy(
        self,
        mock_aws_deployment_helper: MagicMock,
        mock_aws_deployment_helper_obj: MagicMock,
    ) -> None:
        with self.subTest("should delete resources correctly"):
            mock_cli_args = MagicMock()
            mock_cli_args.user_name = "test_user"
            mock_cli_args.policy_name = "test_policy"
            mock_cli_args.iam_policy_name = "test_iam_policy"
            mock_cli_args.iam_user_name = "test_iam_user"
            aws_deployment_helper_tool = AwsDeploymentHelperTool(mock_cli_args)

            aws_deployment_helper_tool.destroy()
            aws_deployment_helper_tool.aws_deployment_helper_obj.delete_user_workflow.assert_called_once_with(
                user_name=mock_cli_args.user_name
            )
            aws_deployment_helper_tool.aws_deployment_helper_obj.delete_policy.assert_called_once_with(
                policy_name=mock_cli_args.policy_name
            )
            aws_deployment_helper_tool.aws_deployment_helper_obj.detach_user_policy.assert_called_once_with(
                user_name=mock_cli_args.iam_user_name,
                policy_name=mock_cli_args.iam_policy_name,
            )

        with self.subTest("should throw when iam_policy_name is None"):
            mock_cli_args = MagicMock()
            mock_cli_args.iam_policy_name = None
            mock_cli_args.iam_user_name = "test_iam_user"
            aws_deployment_helper_tool = AwsDeploymentHelperTool(mock_cli_args)
            with self.assertRaisesRegex(
                Exception,
                "Need username and policy_name to detach policy to user. Please use"
                " --user_name and --policy_name arguments in cli.py",
            ):
                aws_deployment_helper_tool.destroy()

        with self.subTest("should throw when iam_user_name is None"):
            mock_cli_args = MagicMock()
            mock_cli_args.iam_policy_name = "test_iam_policy"
            mock_cli_args.iam_user_name = None
            aws_deployment_helper_tool = AwsDeploymentHelperTool(mock_cli_args)
            with self.assertRaisesRegex(
                Exception,
                "Need username and policy_name to detach policy to user. Please use"
                " --user_name and --policy_name arguments in cli.py",
            ):
                aws_deployment_helper_tool.destroy()

        pass
