#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict


import argparse
import sys
from typing import List, Optional

from fbpcs.infra.cloud_bridge.deployment_helper.aws.aws_deployment_helper_tool import (
    AwsDeploymentHelperTool,
)

from fbpcs.infra.cloud_bridge.deployment_helper.aws.aws_parser_builder import (
    AwsParserBuilder,
)


def get_parser() -> argparse.ArgumentParser:
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
    aws_create_subparser = create_subparser.add_parser(
        "aws", help="creates aws resources"
    )
    aws_destroy_subparser = destroy_subparser.add_parser(
        "aws", help="destroys aws resources"
    )

    # The formatter gets pretty wild without the surrounding `()`
    # and puts everything on one giant line for some reason
    (
        AwsParserBuilder(aws_create_subparser)
        .with_create_iam_user_parser_arguments()
        .with_create_iam_policy_parser_arguments()
        .with_attach_iam_policy_parser_arguments()
        .get()
    )

    (
        AwsParserBuilder(aws_destroy_subparser)
        .with_destroy_iam_user_parser_arguments()
        .with_destroy_iam_policy_parser_arguments()
        .with_detach_iam_policy_parser_arguments()
        .get()
    )
    return cli_parser


def main(args: Optional[List[str]] = None) -> None:
    cli_parser = get_parser()
    cli_args = cli_parser.parse_args(args)

    if cli_args.platform == "aws":
        aws_obj = AwsDeploymentHelperTool(cli_args)
        action_to_perform = getattr(aws_obj, cli_args.action)
        action_to_perform()
    else:
        sys.exit("Unsupported platform: {cli_args.platform}")


if __name__ == "__main__":
    main()
