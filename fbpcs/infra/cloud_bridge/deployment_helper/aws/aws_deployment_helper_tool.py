# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

import argparse

from .aws_deployment_helper import AwsDeploymentHelper


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
            self.aws_deployment_helper_obj.add_iam_user(
                user_name=self.cli_args.user_name
            )

    def destroy(self):
        if self.cli_args.delete_iam_user:
            self.aws_deployment_helper_obj.delete_iam_user(
                user_name=self.cli_args.user_name
            )
