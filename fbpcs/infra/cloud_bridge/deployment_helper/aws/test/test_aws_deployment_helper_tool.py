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
from fbpcs.infra.cloud_bridge.deployment_helper.aws.policy_params import PolicyParams


class TestAwsDeploymentHelperTool(unittest.TestCase):
    @patch(
        "fbpcs.infra.cloud_bridge.deployment_helper.aws.aws_deployment_helper_tool.AwsDeploymentHelper"
    )
    def test_create(self, mock_aws_deployment_helper: MagicMock) -> None:
        test_user_name = "test-user"
        test_policy_name = "test-policy"
        test_region = "us-east-1"
        test_firehose_stream_name = "test-firehose"
        test_data_bucket_name = "test-data-bucket"
        test_config_bucket_name = "test-config-bucket"
        test_database_name = "test-database"
        test_table_name = "test-table"
        test_cluster_name = "test-cluster"
        test_ecs_task_execution_role_name = "test-ecs-execution-role"

        with self.subTest("add_iam_user_basic"):
            cli_args = self.setup_cli_args_mock()
            aws_deployment_helper_tool = AwsDeploymentHelperTool(cli_args)
            cli_args.add_iam_user = True
            cli_args.user_name = test_user_name

            aws_deployment_helper_tool.create()

            aws_deployment_helper_tool.aws_deployment_helper_obj.create_user_workflow.assert_called_once_with(
                user_name=test_user_name
            )

        with self.subTest("add_iam_user_exception_user_name_not_defined"):
            cli_args = self.setup_cli_args_mock()
            cli_args.add_iam_user = True
            cli_args.user_name = None
            aws_deployment_helper_tool = AwsDeploymentHelperTool(cli_args)

            with self.assertRaisesRegex(Exception, "Need username to add user*"):
                aws_deployment_helper_tool.create()

        with self.subTest("add_iam_policy_basic"):
            cli_args = self.setup_cli_args_mock()
            cli_args.add_iam_policy = True
            cli_args.policy_name = test_policy_name
            cli_args.region = test_region
            cli_args.firehose_stream_name = test_firehose_stream_name
            cli_args.data_bucket_name = test_data_bucket_name
            cli_args.config_bucket_name = test_config_bucket_name
            cli_args.database_name = test_database_name
            cli_args.table_name = test_table_name
            cli_args.cluster_name = test_cluster_name
            cli_args.ecs_task_execution_role_name = test_ecs_task_execution_role_name
            aws_deployment_helper_tool = AwsDeploymentHelperTool(cli_args)

            aws_deployment_helper_tool.create()

            aws_deployment_helper_tool.aws_deployment_helper_obj.create_policy.assert_called_once_with(
                policy_name=test_policy_name,
                policy_params=PolicyParams(
                    firehose_stream_name=test_firehose_stream_name,
                    data_bucket_name=test_data_bucket_name,
                    config_bucket_name=test_config_bucket_name,
                    database_name=test_database_name,
                    table_name=test_table_name,
                    cluster_name=test_cluster_name,
                    ecs_task_execution_role_name=test_ecs_task_execution_role_name,
                ),
            )

        with self.subTest("add_iam_policy_exception_region_not_defined"):
            cli_args = self.setup_cli_args_mock()
            cli_args.add_iam_policy = True
            cli_args.policy_name = test_policy_name
            cli_args.region = None
            aws_deployment_helper_tool = AwsDeploymentHelperTool(cli_args)

            with self.assertRaisesRegex(Exception, "Need policy name and region*"):
                aws_deployment_helper_tool.create()

        with self.subTest("add_iam_policy_exception_policy_name_not_defined"):
            cli_args = self.setup_cli_args_mock()
            cli_args.add_iam_policy = True
            cli_args.region = test_region
            cli_args.policy_name = None
            aws_deployment_helper_tool = AwsDeploymentHelperTool(cli_args)

            with self.assertRaisesRegex(Exception, "Need policy name and region*"):
                aws_deployment_helper_tool.create()

        with self.subTest("attach_iam_policy_basic"):
            cli_args = self.setup_cli_args_mock()
            cli_args.attach_iam_policy = True
            cli_args.iam_policy_name = test_policy_name
            cli_args.iam_user_name = test_user_name
            aws_deployment_helper_tool = AwsDeploymentHelperTool(cli_args)

            aws_deployment_helper_tool.create()

            aws_deployment_helper_tool.aws_deployment_helper_obj.attach_user_policy.assert_called_once_with(
                policy_name=test_policy_name, user_name=test_user_name
            )

        with self.subTest("attach_iam_policy_exception_user_name_not_defined"):
            cli_args = self.setup_cli_args_mock()
            cli_args.attach_iam_policy = True
            cli_args.region = test_policy_name
            cli_args.iam_user_name = None
            cli_args.iam_policy_name = test_policy_name
            aws_deployment_helper_tool = AwsDeploymentHelperTool(cli_args)

            with self.assertRaisesRegex(Exception, "Need username and policy_name*"):
                aws_deployment_helper_tool.create()

        with self.subTest("attach_iam_policy_exception_policy_name_not_defined"):
            cli_args = self.setup_cli_args_mock()
            cli_args.attach_iam_policy = True
            cli_args.user_name = test_user_name
            cli_args.iam_policy_name = None
            aws_deployment_helper_tool = AwsDeploymentHelperTool(cli_args)

            with self.assertRaisesRegex(Exception, "Need username and policy_name*"):
                aws_deployment_helper_tool.create()

    def setup_cli_args_mock(self) -> MagicMock:
        cli_args = MagicMock()
        cli_args.add_iam_user = False
        cli_args.add_iam_policy = False
        cli_args.attach_iam_policy = False

        return cli_args

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
