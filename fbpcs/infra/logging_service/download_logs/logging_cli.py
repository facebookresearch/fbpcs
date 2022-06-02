# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

import argparse

from fbpcs.infra.logging_service.download_logs.download_logs import AwsContainerLogs


def main():
    cli_parser = argparse.ArgumentParser()

    subparsers = cli_parser.add_subparsers(dest="platform")

    cloud_parser = subparsers.add_parser(
        "aws",
        help="Download logs for aws containers. ACCESS and SECRET keys should be env variables.",
    )

    cloud_parser = aws_parser_arguments(cloud_parser)

    download_logs_parser_arguments(cloud_parser)

    cli_args = cli_parser.parse_args()

    aws_download_logs_obj = AwsContainerLogs(
        tag_name=cli_args.tag_name,
        aws_region=cli_args.aws_region,
    )

    if cli_args.download_logs:
        aws_download_logs_obj.download_logs(
            s3_bucket_name=cli_args.s3_bucket_name,
            tag_name=cli_args.tag_name,
            local_download_dir=cli_args.download_log_dir,
        )


def aws_parser_arguments(
    aws_parser: argparse.ArgumentParser,
) -> argparse.ArgumentParser:
    """
    Common arguments needed for AWS verification
    """
    aws_parser.add_argument(
        "--aws_region", type=str, required=False, help="Region of the AWS account"
    )
    aws_parser.add_argument(
        "--tag_name",
        type=str,
        required=True,
        help="Tag name to uniquely identify downloaded logs",
    )
    return aws_parser


def download_logs_parser_arguments(aws_parser: argparse.ArgumentParser) -> None:
    """
    Arguments for downloading logs
    """
    download_logs_command_group = aws_parser.add_argument_group(
        "download_logs", "Arguments to download logs"
    )

    download_logs_command_group.add_argument(
        "--download_logs",
        action="store_true",
        help="Download container logs from cloudwatch to AWS S3",
    )
    download_logs_command_group.add_argument(
        "--s3_bucket_name",
        type=str,
        required=True,
        help="S3 bucket name to store the logs intermediately",
    )
    download_logs_command_group.add_argument(
        "--download_log_dir",
        type=str,
        required=False,
        help="Local folder location where logs should be downloaded",
    )


if __name__ == "__main__":
    main()
