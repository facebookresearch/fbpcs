# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

from typing import Any, Dict, List, Optional


class TerraformUtils:
    def __init__(
        self,
        state_file_path: Optional[str] = None,
        terraform_variables: Optional[Dict[str, str]] = None,
        parallelism: Optional[str] = None,
        resource_targets: Optional[List[str]] = None,
        var_definition_file: Optional[str] = None,
    ) -> None:
        """
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
        self.state_file_path = state_file_path
        self.resource_targets: Optional[List[str]] = (
            [] if resource_targets is None else resource_targets
        )
        self.terraform_variables: Optional[Dict[str, str]] = (
            {} if terraform_variables is None else terraform_variables
        )
        self.parallelism = parallelism
        self.var_definition_file = var_definition_file

        """
        The -input=false option indicates that Terraform should not attempt to prompt for input,
        and instead expect all necessary values to be provided by either configuration files or the command line.
        https://learn.hashicorp.com/tutorials/terraform/automate-terraform
        """
        self.input = False

    def get_command_list(self, command: str, *args: Any, **kwargs: str) -> List[str]:
        """
        Converts command string to list and updates commands with terraform options provided through kwargs and args.
        """
        commands = command.split()
        for key, value in kwargs.items():
            # terraform CLI accepts options with "-" using "_" will results in error
            key = key.replace("_", "-")

            if isinstance(value, list):
                for inner_value in value:
                    commands.append(f"-{key}={inner_value}")
            elif isinstance(value, dict):
                if "backend-config" in key:
                    commands.extend(
                        [f"-backend-config {k}={v}" for k, v in value.items()]
                    )
                # TODO: read var in kwargs and update commands
            elif isinstance(value, bool):
                value = "true" if value else "false"
                commands.append(f"-{key}={value}")
            else:
                commands.append(f"-{key}={value}")

        # Add args to commands list
        commands.extend(args)
        return commands

    def get_default_options(self, input_options: Dict[str, Any]) -> Dict[str, Any]:
        """
        Returns the terraform configs needed to create terraform cli
        """
        return {
            "state": self.state_file_path,
            "target": self.resource_targets,
            "var": self.terraform_variables,
            "var_file": self.var_definition_file,
            "parallelism": self.parallelism,
            "input": self.input,
            **input_options,
        }
