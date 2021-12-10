# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

import argparse

from .aws_deployment_helper import AwsDeploymentHelper
from .policy_params import PolicyParams


class AwsDeploymentHelperTool:
    def __init__(self, cli_args: argparse):
        self.aws_deployment_helper_obj = AwsDeploymentHelper(
            cli_args.access_key,
            cli_args.secret_key,
            cli_args.account_id,
            cli_args.region,
        )
        self.cli_args = cli_args

    def create(self):
        if self.cli_args.add_iam_user:
            if self.cli_args.user_name is None:
                raise Exception(
                    "Need username to add user. Please add username using --user_name argument in cli.py"
                )
            self.aws_deployment_helper_obj.create_user_workflow(
                user_name=self.cli_args.user_name
            )

        if self.cli_args.add_iam_policy:
            if self.cli_args.policy_name is None:
                raise Exception(
                    "Need policy name to add IAM policy. Please add username using --policy_name argument in cli.py"
                )
            policy_params = PolicyParams(
                firehose_stream_name=self.cli_args.firehose_stream_name,
                data_bucket_name=self.cli_args.data_bucket_name,
                config_bucket_name=self.cli_args.config_bucket_name,
                database_name=self.cli_args.database_name,
                data_ingestion_kms_key=self.cli_args.data_ingestion_kms_key,
                cluster_name=self.cli_args.cluster_name,
                ecs_task_execution_role_name=self.cli_args.ecs_task_execution_role_name,
            )
            self.aws_deployment_helper_obj.create_policy(
                policy_name=self.cli_args.policy_name, policy_params=policy_params
            )

    def destroy(self):
        if self.cli_args.delete_iam_user:
            self.aws_deployment_helper_obj.delete_user_workflow(
                user_name=self.cli_args.user_name
            )

        if self.cli_args.delete_iam_policy:
            self.aws_deployment_helper_obj.delete_policy(
                policy_name=self.cli_args.policy_name
            )
