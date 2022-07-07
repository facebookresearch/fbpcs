# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

import logging
import sys
from subprocess import PIPE, Popen
from typing import Any, List

from fbpcs.infra.pce_deployment_library.deploy_library.deploy_base.deploy_base import (
    DeployBase,
)

from fbpcs.infra.pce_deployment_library.deploy_library.models import RunCommandReturn


class Terraform(DeployBase):
    def __init__(self) -> None:
        self.log: logging.Logger = logging.getLogger(__name__)

    def apply(self) -> None:
        pass

    def destroy(self) -> None:
        pass

    def init(self) -> None:
        pass

    def plan(self) -> None:
        pass

    def get_command_list(self, command: str, *args: Any, **kwargs: Any) -> List[str]:
        """
        Converts string to list, which will be consumed by subprocess
        """
        # TODO: Add option to pass more arguments through args and kwargs
        return command.split()

    def run_command(
        self,
        command: str,
        capture_output: bool = True,
    ) -> RunCommandReturn:
        """
        Executes Terraform CLIs apply/destroy/init/plan
        """

        if capture_output:
            stderr = PIPE
            stdout = PIPE
        else:
            stderr = sys.stderr
            stdout = sys.stdout

        command_list = self.get_command_list(command)
        command_str = " ".join(command_list)
        self.log.info(f"Command: {command_str}")
        out, err = None, None

        with Popen(command_list, stdout=stdout, stderr=stderr) as p:
            out, err = p.communicate()
            ret_code = p.returncode
            self.log.info(f"output: {out}")

            if capture_output:
                out = out.decode()
                err = err.decode()
            else:
                out = None
                err = None

        return RunCommandReturn(return_code=ret_code, output=out, error=err)
