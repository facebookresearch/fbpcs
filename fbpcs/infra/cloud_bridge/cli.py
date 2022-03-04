# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.


import argparse

from deployment_helper.aws.aws_deployment_helper_tool import AwsDeploymentHelperTool


def main():
    cli_parser = argparse.ArgumentParser()

    subparsers = cli_parser.add_subparsers(dest="action")

    # Create subparsers are listed here

    create_parser = subparsers.add_parser(
        "create", help="creates public cloud resources"
    )
    destroy_parser = subparsers.add_parser(
        "destroy", help="destroys public cloud resources"
    )

    create_subparser = create_parser.add_subparsers(dest="platform")
    destroy_subparser = destroy_parser.add_subparsers(dest="platform")

    """
    Add parser for each cloud platform in the section below.

    TODO: add GCP to this cli when it's ready
    """
    aws_create_parser = create_subparser.add_parser("aws", help="creates aws resources")
    aws_destroy_parser = destroy_subparser.add_parser(
        "aws", help="destroys aws resources"
    )

    """
    AWS Section
    """
    aws_create_parser = aws_parser_arguments(aws_create_parser)
    aws_destroy_parser = aws_parser_arguments(aws_destroy_parser)

    """
    Each functionality is added a group command
    """
    aws_create_iam_user_parser_arguments(aws_create_parser)
    aws_destroy_iam_user_parser_arguments(aws_destroy_parser)

    aws_create_iam_policy_parser_arguments(aws_create_parser)
    aws_destroy_iam_policy_parser_arguments(aws_destroy_parser)

    aws_attach_iam_policy_parser_arguments(aws_create_parser)
    aws_detach_iam_policy_parser_arguments(aws_destroy_parser)

    cli_args = cli_parser.parse_args()

    if cli_args.platform == "aws":
        aws_obj = AwsDeploymentHelperTool(cli_args)
        action_to_perform = getattr(aws_obj, cli_args.action)
        action_to_perform()


def aws_parser_arguments(aws_parser: argparse) -> argparse:
    aws_parser.add_argument(
        "--access_key", type=str, required=False, help="Access key of the AWS account"
    )
    aws_parser.add_argument(
        "--secret_key", type=str, required=False, help="Secret key of the AWS account"
    )
    aws_parser.add_argument(
        "--account_id", type=str, required=False, help="Account ID of the AWS account"
    )
    aws_parser.add_argument(
        "--region", type=str, required=False, help="Region of the AWS account"
    )
    return aws_parser


def aws_create_iam_user_parser_arguments(aws_parser: argparse):
    iam_user_command_group = aws_parser.add_argument_group(
        "iam_user", "Arguments to add IAM user"
    )

    iam_user_command_group.add_argument(
        "--add_iam_user", action="store_true", help="Add IAM user to AWS account"
    )
    iam_user_command_group.add_argument(
        "--user_name", type=str, required=False, help="User name of the IAM user"
    )


def aws_create_iam_policy_parser_arguments(aws_parser: argparse):
    iam_policy_command_group = aws_parser.add_argument_group(
        "iam_policy", "Arguments to add IAM policy"
    )

    iam_policy_command_group.add_argument(
        "--add_iam_policy", action="store_true", help="Add IAM policy to AWS account"
    )
    iam_policy_command_group.add_argument(
        "--policy_name", type=str, required=False, help="Policy name to be created"
    )

    iam_policy_command_group.add_argument(
        "--firehose_stream_name", type=str, required=False, help="Firehose stream name"
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
        "--cluster_name", type=str, required=False, help="ECS cluster name"
    )

    iam_policy_command_group.add_argument(
        "--ecs_task_execution_role_name",
        type=str,
        required=False,
        help="ECS task execution role name",
    )


def aws_attach_iam_policy_parser_arguments(aws_parser: argparse):
    iam_policy_command_group = aws_parser.add_argument_group(
        "iam_policy_attach", "Arguments to attach IAM policy to the user"
    )

    iam_policy_command_group.add_argument(
        "--attach_iam_policy", action="store_true", help="Attaches IAM policy to a user"
    )
    iam_policy_command_group.add_argument(
        "--iam_policy_name", type=str, required=False, help="Policy to be attached"
    )
    iam_policy_command_group.add_argument(
        "--iam_user_name", type=str, required=False, help="Attach policy to user"
    )


def aws_destroy_iam_user_parser_arguments(aws_parser: argparse):
    iam_user_command_group = aws_parser.add_argument_group(
        "iam_user", "Arguments to delete iam user"
    )

    iam_user_command_group.add_argument(
        "--delete_iam_user", action="store_true", help="Delete IAM user to AWS account"
    )
    iam_user_command_group.add_argument(
        "--user_name", type=str, required=False, help="User name of the IAM user"
    )


def aws_destroy_iam_policy_parser_arguments(aws_parser: argparse):
    iam_policy_command_group = aws_parser.add_argument_group(
        "iam_policy", "Arguments to add IAM policy"
    )

    iam_policy_command_group.add_argument(
        "--delete_iam_policy",
        action="store_true",
        help="Delete IAM policy from AWS account",
    )
    iam_policy_command_group.add_argument(
        "--policy_name", type=str, required=False, help="Policy name to be destroyed"
    )


def aws_detach_iam_policy_parser_arguments(aws_parser: argparse):
    iam_policy_command_group = aws_parser.add_argument_group(
        "iam_policy_detach", "Arguments to detach IAM policy to the user"
    )

    iam_policy_command_group.add_argument(
        "--detach_iam_policy", action="store_true", help="Detaches IAM policy to a user"
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


if __name__ == "__main__":
    main()
