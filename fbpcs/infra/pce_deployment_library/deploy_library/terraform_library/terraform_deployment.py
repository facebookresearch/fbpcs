# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

import logging
import sys
from subprocess import PIPE, Popen
from typing import Any, Dict, List, Optional, Type

from fbpcs.infra.pce_deployment_library.deploy_library.deploy_base.deploy_base import (
    DeployBase,
)

from fbpcs.infra.pce_deployment_library.deploy_library.models import (
    FlaggedOption,
    RunCommandResult,
    TerraformCliOptions,
    TerraformCommand,
)
from fbpcs.infra.pce_deployment_library.deploy_library.terraform_library.terraform_deployment_utils import (
    TerraformDeploymentUtils,
)


class TerraformDeployment(DeployBase):

    TERRAFORM_DEFAULT_PARALLELISM = 10

    def __init__(
        self,
        state_file_path: Optional[str] = None,
        terraform_variables: Optional[Dict[str, str]] = None,
        parallelism: int = TERRAFORM_DEFAULT_PARALLELISM,
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
        self.utils = TerraformDeploymentUtils(
            state_file_path=state_file_path,
            resource_targets=resource_targets,
            terraform_variables=terraform_variables,
            parallelism=parallelism,
            var_definition_file=var_definition_file,
        )

    def create(
        self,
        terraform_input: bool = False,
        auto_approve: bool = True,
        **kwargs: Dict[str, Any],
    ) -> RunCommandResult:
        """
        Implements `terraform apply` of terraform CLI.
        `terraform apply` command executes the actions proposed in a `terraform plan`.

        More information: https://www.terraform.io/cli/commands/apply

        terraform_input:
            Provides `-input=false`. It disables all of Terraform's interactive prompts.
            More information: https://www.terraform.io/cli/commands/apply#apply-options

        auto_approve:
            Skips interactive approval of plan before applying
            More information: https://www.terraform.io/cli/commands/apply#apply-options
        """
        options: Dict[str, Any] = kwargs.copy()
        options["input"] = terraform_input
        options["auto-approve"] = auto_approve  # a False value will require an input
        options = self.utils.get_default_options(TerraformCommand.APPLY, options)
        return self.run_command("terraform apply", **options)

    def destroy(
        self, auto_approve: bool = True, **kwargs: Dict[str, Any]
    ) -> RunCommandResult:
        """
        Implements `terraform destroy` of terraform CLI.
        `terraform destroy` destroys all remote objects managed by a particular Terraform configuration.

        More information: https://www.terraform.io/docs/commands/destroy.html

        auto_approve:
            Skips interactive approval of plan before applying
            More information: https://www.terraform.io/cli/commands/apply#apply-options

        """
        options: Dict[str, Any] = kwargs.copy()
        options["auto-approve"] = auto_approve

        options = self.utils.get_default_options(TerraformCommand.DESTROY, options)
        return self.run_command("terraform destroy", **options)

    def terraform_init(
        self,
        backend_config: Optional[Dict[str, str]] = None,
        reconfigure: Type[FlaggedOption] = FlaggedOption,
        **kwargs: Dict[str, Any],
    ) -> RunCommandResult:
        """
        Implements `terraform init` of terraform CLI.
        `terraform init` command is used to initialize a working directory containing Terraform configuration files.

        More information: https://www.terraform.io/cli/commands/init

        backend_config:
            Provides backend config information using key-value pairs.
            Usage:
                terraform = Terraform()
                terraform.terraform_init(backend_config = {"bucket": s3_bucket,"region": region})
            More info: https://www.terraform.io/language/settings/backends/configuration#command-line-key-value-pairs

        reconfigure:
            in `terraform init` reconfigure disregards any existing configuration, preventing migration of any existing state.
            More information: https://www.terraform.io/cli/commands/init#backend-initialization
        """
        options: Dict[str, Any] = kwargs.copy()
        options.update(
            {
                TerraformCliOptions.backend_config: backend_config,
                TerraformCliOptions.reconfigure: reconfigure,
            }
        )
        options = self.utils.get_default_options(TerraformCommand.INIT, options)
        return self.run_command("terraform init", **options)

    def plan(
        self,
        detailed_exitcode: Type[FlaggedOption] = FlaggedOption,
        **kwargs: Dict[str, Any],
    ) -> RunCommandResult:
        """
        Implements `terraform plan` of terraform CLI.
        `terraform plan` creates an execution plan, which lets you preview the changes that Terraform plans to make to your infrastructure

        More information: https://www.terraform.io/cli/commands/plan

        detailed_exitcode:
            Returns a detailed exit code when the command exits
            More information: https://www.terraform.io/cli/commands/plan#other-options
        """
        options: Dict[str, Any] = kwargs.copy()
        options["detailed_exitcode"] = detailed_exitcode
        options = self.utils.get_default_options(TerraformCommand.PLAN, options)
        return self.run_command("terraform plan", **options)

    def run_command(
        self, command: str, capture_output: bool = True, **kwargs: Dict[str, Any]
    ) -> RunCommandResult:
        """
        Executes Terraform CLIs apply/destroy/init/plan
        """
        options: Dict[str, Any] = kwargs.copy()

        if capture_output:
            stderr = PIPE
            stdout = PIPE
        else:
            stderr = sys.stderr
            stdout = sys.stdout

        command_list = self.utils.get_command_list(command, **options)
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

        return RunCommandResult(return_code=ret_code, output=out, error=err)
