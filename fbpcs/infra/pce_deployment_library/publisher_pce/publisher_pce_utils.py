# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

import logging
import re

from fbpcs.infra.pce_deployment_library.errors_library.terraform_errors import (
    TerraformCommandExectionError,
)


class PublisherPceUtils:
    def __init__(self):
        self.log: logging.Logger = logging.getLogger(__name__)
        # set logger
        self.log.setLevel(logging.DEBUG)
        # create file handler which logs even debug messages
        fh = logging.FileHandler("test.log")
        fh.setLevel(logging.DEBUG)
        self.log.addHandler(fh)

    def parse_command_output(self, command, command_result) -> str:
        ret_code = command_result.return_code
        if ret_code != 0:
            self.log.error(f"Failed to run terraform {command}")
            error = f"Command terraform {command} execution failed with error, {command_result.error}"
            self.log.error(f"{error}")
            raise TerraformCommandExectionError(f"{error}")

        return self.sanitize_command_output_logs(command_result.output)

    def sanitize_command_output_logs(self, command_output_log: str) -> str:
        if not command_output_log:
            return ""

        ansi_escape = re.compile(
            r"""
            \x1B  # ESC
            (?:   # 7-bit C1 Fe (except CSI)
                [@-Z\\-_]
            |     # or [ for CSI, followed by a control sequence
                \[
                [0-?]*  # Parameter bytes
                [ -/]*  # Intermediate bytes
                [@-~]   # Final byte
            )
        """,
            re.VERBOSE,
        )
        return ansi_escape.sub("", command_output_log)
