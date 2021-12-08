# Copyright (c) Facebook, Inc. and its affiliates.
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
        "iam_user", "Arguments to add iam user"
    )

    iam_user_command_group.add_argument(
        "--add_iam_user", action="store_true", help="Add IAM user to AWS account"
    )
    iam_user_command_group.add_argument(
        "--user_name", type=str, required=True, help="User name of the IAM user"
    )


def aws_destroy_iam_user_parser_arguments(aws_parser: argparse):
    iam_user_command_group = aws_parser.add_argument_group(
        "iam_user", "Arguments to delete iam user"
    )

    iam_user_command_group.add_argument(
        "--delete_iam_user", action="store_true", help="Delete IAM user to AWS account"
    )
    iam_user_command_group.add_argument(
        "--user_name", type=str, required=True, help="User name of the IAM user"
    )


if __name__ == "__main__":
    main()
