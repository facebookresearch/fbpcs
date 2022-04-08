# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

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
        logger,
        log_file: str,
    ) -> None:
        logger.setLevel(logging.INFO)
        logging.Formatter.converter = time.gmtime
        formatter = logging.Formatter(
            "%(asctime)sZ %(levelname)s p:%(processName)s t:%(threadName)s s:%(filename)s:%(lineno)s ~%(message)s"
        )

        log_handler = logging.StreamHandler(sys.stdout)
        log_handler.setFormatter(formatter)
        logger.addHandler(log_handler)

        # Create the directory path if necessary
        dir_path = os.path.dirname(log_file)
        if dir_path:
            os.makedirs(dir_path, exist_ok=True)

        log_handler = logging.FileHandler(log_file)
        log_handler.setFormatter(formatter)
        logger.addHandler(log_handler)
