# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

import logging
import os
import sys
import time


class Utils:
    @staticmethod
    def get_server_port() -> int:
        return 9090

    @staticmethod
    def configure_logger(
        log_file: str,
    ) -> None:
        console_handler = logging.StreamHandler(sys.stdout)

        # Create the directory path if necessary
        dir_path = os.path.dirname(log_file)
        if dir_path:
            os.makedirs(dir_path, exist_ok=True)

        file_handler = logging.FileHandler(log_file)

        logging.Formatter.converter = time.gmtime
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)sZ %(levelname)s t:%(threadName)s n:%(name)s ! %(message)s",
            handlers=[file_handler, console_handler],
        )
