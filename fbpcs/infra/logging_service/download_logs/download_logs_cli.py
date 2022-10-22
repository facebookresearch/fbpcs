# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

"""
CLI for parsing the log file to extract cloud container ID's, downloading the container logs from
AWS CloudWatch and uploading the archived logs to S3.
The input log file should by default come from execution of private computation cli script.
S3 bucket can be the bucket name, or the URL of the bucket (e.g. "https://bucketname.s3.us...").

Usage:
    download_logs_cli <cli_log_file> <s3_bucket> <archive_tag> <deployment_tag> [options]


Options:
    -h --help                       Show this help
    --input_ids                     The input log file has container ID's, with one ID per line
    --log_path=<path>               Override the default path where logs are saved
    --get_deployment_logs           Fetche and add deployment logs in the log zip folder
    --get_data_pipeline_logs        Fetche and add data infra pipeline logs in the log zip folder
    --verbose                       Set logging level to DEBUG
"""

import logging
import re
import sys
import tempfile
import time
from os.path import abspath
from pathlib import Path
from typing import List, Optional, Tuple

import schema
from docopt import docopt
from fbpcs.infra.logging_service.download_logs.download_logs import AwsContainerLogs
from fbpcs.infra.logging_service.download_logs.utils.utils import Utils
from fbpcs.infra.logging_service.log_analyzer.log_analyzer import LogDigest


class DownloadLogsCli:
    # When a file in the logs archive has name beginning with '.', the file will be handled specially during logs upload.
    # E.g. container info file contains mapping of container ID (i.e. ARN) to run flow instance ID.
    CONTAINER_INFO_FILENAME = ".container_info.csv"
    CONTAINER_INFO_CSV_HEADER = "#container_id,instance_id"
    TEMP_DIR_PREFIX = "download_"

    def __init__(
        self,
    ) -> None:
        self.logger: logging.Logger = logging.getLogger(__name__)
        self.s3_bucket = ""
        self.container_ids: List[str] = []
        self.aws_region = ""
        self.aws_container_logs: Optional[AwsContainerLogs] = None
        self.utils = Utils()

    def run(self, argv: Optional[List[str]] = None) -> None:
        s = schema.Schema(
            {
                "<cli_log_file>": schema.Use(Path),
                "<s3_bucket>": str,
                "<archive_tag>": str,
                "<deployment_tag>": str,
                "--input_ids": bool,
                "--log_path": schema.Or(None, schema.Use(Path)),
                "--get_deployment_logs": bool,
                "--get_data_pipeline_logs": bool,
                "--verbose": bool,
                "--help": bool,
            }
        )

        arguments = s.validate(docopt(__doc__, argv))

        cli_log_file = arguments["<cli_log_file>"]
        archive_tag = arguments["<archive_tag>"]
        log_path = arguments["--log_path"]
        deployment_tag = arguments["<deployment_tag>"]
        get_deployment_logs = arguments["--get_deployment_logs"]
        get_data_pipeline_logs = arguments["--get_data_pipeline_logs"]

        # if log_path specified, logging using FileHandler, or console StreamHandler
        log_handler = (
            logging.FileHandler(log_path) if log_path else logging.StreamHandler()
        )
        logging.Formatter.converter = time.gmtime
        logging.basicConfig(
            level=logging.INFO,
            handlers=[log_handler],
            format="%(asctime)sZ %(levelname)s t:%(threadName)s n:%(name)s ! %(message)s",
        )
        self.logger = logging.getLogger(__name__)
        log_level = logging.DEBUG if arguments["--verbose"] else logging.INFO
        self.logger.setLevel(log_level)
        # Concatenate all arguments to a string, with every argument wrapped by quotes.
        all_options = f"{sys.argv[1:]}"[1:-1].replace("', '", "' '")
        # E.g. Command line: log_analyzer 'sample_log/intern-output.txt' '--log_path=a.intern.log' ...
        logging.info(f"Command line: {Path(__file__).stem} {all_options}")

        self.s3_bucket = self._get_s3_bucket_name(arguments["<s3_bucket>"])
        self.logger.info(f"Will upload log archive to S3 bucket: {self.s3_bucket}")

        # Each tuple is (container ID, run flow instance ID)
        container_infos = self._extract_container_infos(
            cli_log_file, arguments["--input_ids"] or False
        )
        self.logger.info(f"Found container count: {len(container_infos)}")
        info_csv_lines = [self.CONTAINER_INFO_CSV_HEADER]
        info_csv_lines.extend(
            [
                f"{container_id},{instance_id}"
                for container_id, instance_id in container_infos
            ]
        )

        if log_level == logging.DEBUG:
            combined_info_lines = "\n".join(info_csv_lines)
            self.logger.debug(f"Found container info:\n{combined_info_lines}")

        self.container_ids = [
            container_id for container_id, instance_id in container_infos
        ]
        self.aws_region = self._get_aws_region(self.container_ids)
        self.logger.info(f"Found aws_region: {self.aws_region}")

        self.aws_container_logs = AwsContainerLogs(
            tag_name=archive_tag,
            aws_region=self.aws_region,
            deployment_tag=deployment_tag,
        )

        with tempfile.TemporaryDirectory(prefix=self.TEMP_DIR_PREFIX) as tempdir:
            info_csv_file_path = self._export_container_info(
                str(tempdir), info_csv_lines
            )
            include_local_files = [abspath(cli_log_file), info_csv_file_path]
            # pyre-ignore[16]: `Optional` has no attribute `upload_logs_to_s3_from_cloudwatch`.
            self.aws_container_logs.upload_logs_to_s3_from_cloudwatch(
                self.s3_bucket,
                self.container_ids,
                include_local_files=include_local_files,
                enable_data_pipeline_logs=get_data_pipeline_logs,
                enable_deployment_logs=get_deployment_logs,
            )
        self.logger.info("After uploading log archive")

    def _get_s3_bucket_name(
        self,
        s3_bucket: str,
    ) -> str:
        """
        Input string can be any of the following:
        https://bucket-name.s3.us-west-2.amazonaws.com/photos/puppy.jpg
        bucket-name
        """
        if match := re.compile(r"^https://([^\.]+)\.s3\.").match(s3_bucket):
            return match.group(1)
        return s3_bucket

    def _get_aws_region(
        self,
        container_ids: List[str],
    ) -> str:
        # container_id is like "arn:aws:ecs:us-west-2:5592513842793:task/onedocker-cluster-pc-e2e-test/70b0b78386774e14afccd92762f4b10d"
        pattern = re.compile("^arn:aws:ecs:([^:]+):")
        regions = set()
        for container_id in container_ids:
            if match := pattern.match(container_id):
                regions.add(match.group(1))
        if not regions:
            return ""
        if len(regions) > 1:
            error_message = f"Failed to handle two or more regions: {regions}."
            self.logger.error(error_message)
            raise ValueError(error_message)
        return list(regions)[0]

    def _extract_container_infos(
        self,
        cli_log_file: Path,
        is_input_ids: bool,
    ) -> List[Tuple[str, str]]:
        """
        Extract the mapping container_id:instance_id for all worker containers.
        Output:
        Each tuple is (container ID (i.e. ARN), run flow instance ID).
        """

        container_ids = []
        if is_input_ids:
            # Input file has container ID's, with one ID per line.
            with open(cli_log_file) as infile:
                container_ids = [line.rstrip() for line in infile if len(line) > 0]
            return [(id, "") for id in container_ids]

        # Input file has the logs from execution of private computation cli script.
        digest = LogDigest(cli_log_file, self.logger)
        run_study = digest.analyze_logs()
        self.logger.info(
            f"Parsed log line count: {run_study.total_line_num}, instance count: {len(run_study.instances)}"
        )
        for instance_flow in run_study.instances.values():
            for flow_stage in instance_flow.stages:
                container_ids.extend(
                    [
                        (c.container_id, instance_flow.instance_id)
                        for c in flow_stage.containers
                    ]
                )
        return container_ids

    def _export_container_info(self, tempdir: str, info_csv_lines: List[str]) -> str:
        tempfile_path = f"{tempdir}/{self.CONTAINER_INFO_FILENAME}"
        self.logger.info(
            f"Exporting container infos, line count={len(info_csv_lines)}, to temp file '{tempfile_path}'"
        )
        self.utils.create_file(
            file_location=tempfile_path,
            content=info_csv_lines,
        )
        self.logger.info("Exported container infos to file.")
        return tempfile_path


if __name__ == "__main__":
    cli = DownloadLogsCli()
    cli.run()
