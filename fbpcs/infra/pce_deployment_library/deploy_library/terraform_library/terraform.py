# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

import logging
import sys
from subprocess import PIPE, Popen
from typing import Dict, List, Optional

from fbpcs.infra.pce_deployment_library.deploy_library.deploy_base.deploy_base import (
    DeployBase,
)

from fbpcs.infra.pce_deployment_library.deploy_library.models import RunCommandReturn
from fbpcs.infra.pce_deployment_library.deploy_library.terraform_library.terraform_utils import (
    TerraformUtils,
)


class Terraform(DeployBase):
    def __init__(
        self,
        state_file_path: Optional[str] = None,
        terraform_variables: Optional[Dict[str, str]] = None,
        parallelism: Optional[str] = None,
        resource_targets: Optional[List[str]] = None,
        var_definition_file: Optional[str] = None,
    ) -> None:
        """
        Accepts options to create Terraform CLIs apply/destroy/plan/init
        Args:
            state_file_path:    Path to store terraform state files
                                More information about terraform state: https://www.terraform.io/language/state
            variables:          -var option in terraform CLI. This arguments provides default variables.
                                These variables can be overwritten by commands also.
                                More information on terraform vairables: https://www.terraform.io/language/values/variables
            parallelism:        -parallelism=n option in Terraform CLI.
                                Limits the number  of concurrent operation as Terraform walks the graph
                                More information on terraform parallelism: https://www.terraform.io/cli/commands/apply#parallelism-n
            resource_targets:   -target option in Terraform CLI. Used to target specific resource in terraform apply/destroy
                                More information on terraform targets: https://learn.hashicorp.com/tutorials/terraform/resource-targeting
            var_definition_file: -var-file option in Terraform CLI. Used to define terraform variables in bulk though .tfvars file
                                 More information on var_definition_file :https://www.terraform.io/language/values/variables#variable-definitions-tfvars-files
        """
        self.log: logging.Logger = logging.getLogger(__name__)
        self.utils = TerraformUtils(
            state_file_path=state_file_path,
            resource_targets=resource_targets,
            terraform_variables=terraform_variables,
            parallelism=parallelism,
            var_definition_file=var_definition_file,
        )

    def apply(self) -> None:
        pass

    def destroy(self) -> None:
        pass

    def init(self) -> None:
        pass

    def plan(self) -> None:
        pass

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

        command_list = self.utils.get_command_list(command)
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
