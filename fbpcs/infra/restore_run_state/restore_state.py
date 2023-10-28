# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

"""
Usage:
    restore_state --run_data_path=<str> --dest_folder=<path> --region_name=<str> [options]

Options:
    --run_data_path=<str>               S3 path to run data
    --dest_folder=<path>                The local path to copy the data to
    --region_name=<str>                 Cloud region
    --log_path=<path>                   Override the default path where logs are saved
"""

import logging
import os
import re
import sys
import time
from pathlib import Path
from typing import List, Optional, Tuple

import boto3

import schema
from docopt import docopt


class RestoreRunState:
    def __init__(
        self,
    ) -> None:
        self.logger: logging.Logger = logging.getLogger(__name__)
        self.s3_bucket = ""
        self.container_ids: List[str] = []
        self.deployment_tag = ""
        # pyre-ignore
        self.s3 = None

    def run(self, argv: Optional[List[str]] = None) -> None:
        s = schema.Schema(
            {
                "--run_data_path": str,
                "--dest_folder": schema.Use(Path),
                "--region_name": str,
                "--log_path": schema.Or(None, schema.Use(Path)),
            }
        )

        arguments = s.validate(docopt(__doc__, argv))
        run_data_path = arguments["--run_data_path"]
        dest_folder = arguments["--dest_folder"]
        region_name = arguments["--region_name"]
        log_path = arguments["--log_path"]

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
        self.logger.setLevel(logging.INFO)

        args = " ".join(sys.argv)
        # E.g. Command line: log_analyzer 'sample_log/intern-output.txt' '--log_path=a.intern.log' ...
        logging.info(f"Command line: {Path(__file__).stem} {args}")

        self._init_s3(region_name)

        self._copy_files(run_data_path, dest_folder)
        self.logger.info(f"Downloaded run state to {dest_folder}")

    def _copy_files(self, run_data_path: str, dest_folder: str) -> None:

        # DataPath is like s3://fb-pc-data-nov07test1-vwxz/query-results/fbpcs_instances_638479584559395_1/
        splits = self._split_path(run_data_path)
        if splits is None:
            raise RuntimeError(f"Invalid path passed in {run_data_path}")
        (bucket_name, key) = splits

        bucket = self.s3.Bucket(bucket_name)
        # List files under run_data_path
        for obj in bucket.objects.filter(Prefix=key):
            target = os.path.join(dest_folder, self._get_name(obj.key))
            if obj.key[-1] == "/":
                continue
            bucket.download_file(obj.key, target)

    def _get_name(self, path: str) -> str:
        parts = path.split("/")
        return parts[-1]

    def _split_path(self, s3_path: str) -> Optional[Tuple[str, str]]:
        p = re.compile("s3://(.+?)/(.+)")
        m = p.match(s3_path)

        if m is not None:
            return m.group(1, 2)
        return None

    def _init_s3(self, region_name: str) -> None:
        aws_access_key_id = os.environ.get("AWS_ACCESS_KEY_ID")
        aws_secret_access_key = os.environ.get("AWS_SECRET_ACCESS_KEY")
        aws_session_token = os.environ.get("AWS_SESSION_TOKEN")
        self.s3 = boto3.resource(
            "s3",
            region_name=region_name,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            aws_session_token=aws_session_token,
        )


def main() -> None:
    cli = RestoreRunState()
    cli.run()


if __name__ == "__main__":
    main()
