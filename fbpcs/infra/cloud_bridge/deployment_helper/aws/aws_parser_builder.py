#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict


import argparse
from typing import TypeVar


ParserBuilder = TypeVar("T", bound="AwsParserBuilder")


class AwsParserBuilder:
    def __init__(self, from_parser: argparse.ArgumentParser) -> None:
        self.parser = from_parser
        self.with_default_parser_arguments()

    def get(self) -> argparse.ArgumentParser:
        return self.parser

    def with_default_parser_arguments(self: ParserBuilder) -> ParserBuilder:
        self.parser.add_argument(
            "--access_key",
            type=str,
            required=False,
            help="Access key of the AWS account",
        )
        self.parser.add_argument(
            "--secret_key",
            type=str,
            required=False,
            help="Secret key of the AWS account",
        )
        self.parser.add_argument(
            "--account_id",
            type=str,
            required=False,
            help="Account ID of the AWS account",
        )
        self.parser.add_argument(
            "--region", type=str, required=False, help="Region of the AWS account"
        )
        return self

    def with_create_iam_user_parser_arguments(
        self: ParserBuilder,
    ) -> ParserBuilder:
        iam_user_command_group = self.parser.add_argument_group(
            "iam_user", "Arguments to add IAM user"
        )

        iam_user_command_group.add_argument(
            "--add_iam_user", action="store_true", help="Add IAM user to AWS account"
        )
        iam_user_command_group.add_argument(
            "--user_name", type=str, required=False, help="User name of the IAM user"
        )
        return self

    def with_create_iam_policy_parser_arguments(
        self: ParserBuilder,
    ) -> ParserBuilder:
        iam_policy_command_group = self.parser.add_argument_group(
            "iam_policy", "Arguments to add IAM policy"
        )

        iam_policy_command_group.add_argument(
            "--add_iam_policy",
            action="store_true",
            help="Add IAM policy to AWS account",
        )
        iam_policy_command_group.add_argument(
            "--policy_name", type=str, required=False, help="Policy name to be created"
        )

        iam_policy_command_group.add_argument(
            "--firehose_stream_name",
            type=str,
            required=False,
            help="Firehose stream name",
        )

        iam_policy_command_group.add_argument(
            "--data_bucket_name", type=str, required=False, help="Data bucket name"
        )

        iam_policy_command_group.add_argument(
            "--config_bucket_name", type=str, required=False, help="Config bucket name"
        )

        iam_policy_command_group.add_argument(
            "--database_name", type=str, required=False, help="Database name"
        )

        iam_policy_command_group.add_argument(
            "--table_name", type=str, required=False, help="Table name"
        )

        iam_policy_command_group.add_argument(
            "--cluster_name", type=str, required=False, help="ECS cluster name"
        )

        iam_policy_command_group.add_argument(
            "--ecs_task_execution_role_name",
            type=str,
            required=False,
            help="ECS task execution role name",
        )
        return self

    def with_attach_iam_policy_parser_arguments(
        self: ParserBuilder,
    ) -> ParserBuilder:
        iam_policy_command_group = self.parser.add_argument_group(
            "iam_policy_attach", "Arguments to attach IAM policy to the user"
        )

        iam_policy_command_group.add_argument(
            "--attach_iam_policy",
            action="store_true",
            help="Attaches IAM policy to a user",
        )
        iam_policy_command_group.add_argument(
            "--iam_policy_name", type=str, required=False, help="Policy to be attached"
        )
        iam_policy_command_group.add_argument(
            "--iam_user_name", type=str, required=False, help="Attach policy to user"
        )
        return self

    def with_destroy_iam_user_parser_arguments(
        self: ParserBuilder,
    ) -> ParserBuilder:
        iam_user_command_group = self.parser.add_argument_group(
            "iam_user", "Arguments to delete iam user"
        )

        iam_user_command_group.add_argument(
            "--delete_iam_user",
            action="store_true",
            help="Delete IAM user to AWS account",
        )
        iam_user_command_group.add_argument(
            "--user_name", type=str, required=False, help="User name of the IAM user"
        )
        return self

    def with_destroy_iam_policy_parser_arguments(
        self: ParserBuilder,
    ) -> ParserBuilder:
        iam_policy_command_group = self.parser.add_argument_group(
            "iam_policy", "Arguments to add IAM policy"
        )

        iam_policy_command_group.add_argument(
            "--delete_iam_policy",
            action="store_true",
            help="Delete IAM policy from AWS account",
        )
        iam_policy_command_group.add_argument(
            "--policy_name",
            type=str,
            required=False,
            help="Policy name to be destroyed",
        )
        return self

    def with_detach_iam_policy_parser_arguments(
        self: ParserBuilder,
    ) -> ParserBuilder:
        iam_policy_command_group = self.parser.add_argument_group(
            "iam_policy_detach", "Arguments to detach IAM policy to the user"
        )

        iam_policy_command_group.add_argument(
            "--detach_iam_policy",
            action="store_true",
            help="Detaches IAM policy to a user",
        )
        iam_policy_command_group.add_argument(
            "--iam_policy_name",
            type=str,
            required=False,
            help="Policy that is to be detached",
        )
        iam_policy_command_group.add_argument(
            "--iam_user_name", type=str, required=False, help="Detach policy from user"
        )
        return self
