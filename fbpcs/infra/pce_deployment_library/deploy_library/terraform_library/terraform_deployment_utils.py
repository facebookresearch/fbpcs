# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

from typing import Any, Dict, List, Optional

from fbpcs.infra.pce_deployment_library.deploy_library.models import (
    FlaggedOption,
    NOT_SUPPORTED_INIT_DEFAULT_OPTIONS,
    TerraformCliOptions,
    TerraformOptionFlag,
)


class TerraformDeploymentUtils:

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

        commands_list = command.split()

        type_to_func_dict = {
            type({}): self.add_dict_options,
            type([]): self.add_list_options,
            type(True): self.add_bool_options,
            type(TerraformOptionFlag): self.add_flagged_option,
        }

        for key, value in kwargs.items():
            # terraform CLI accepts options with "-" using "_" will results in error
            key = key.replace("_", "-")

            # pyre-fixme
            func = type_to_func_dict.get(type(value), self.add_other_options)

            # pyre-fixme
            commands_list.extend(func(key, value))

        # Add args to commands list
        commands_list.extend(args)
        return commands_list

    def get_default_options(
        self, terraform_command: str, input_options: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Returns the terraform configs needed to create terraform cli
        """

        return_dict: Dict[str, Any] = {
            TerraformCliOptions.state: self.state_file_path,
            TerraformCliOptions.target: self.resource_targets,
            TerraformCliOptions.var: self.terraform_variables,
            TerraformCliOptions.var_file: self.var_definition_file,
            TerraformCliOptions.parallelism: self.parallelism,
            TerraformCliOptions.terraform_input: self.input,
            **input_options,
        }

        if terraform_command == "init":
            for default_option in NOT_SUPPORTED_INIT_DEFAULT_OPTIONS:
                return_dict.pop(default_option, None)

        return return_dict

    def add_dict_options(self, key: str, value: Dict[str, Any]) -> List[str]:
        """
        Adds dict options in Terraform CLI:
        Eg: t = TerraformDeploymentUtils()
        options = {"backend_config": {"region": "us-west-2", "access_key":"fake_access_key"}}
        t.get_command_list("terraform apply")

        Returns:
        => ['terraform', 'apply', '-backend-config region=us-west-2', '-backend-config access_key=fake_access_key']
        """
        commands_list: List[str] = []
        if "backend-config" in key:
            commands_list.extend([f"-backend-config {k}={v}" for k, v in value.items()])
        elif "var" in key:
            commands_list.extend([f"-var {k}={v}" for k, v in value.items()])
        return commands_list

    def add_list_options(self, key: str, value: List[str]) -> List[str]:
        """
        Adds list options in Terraform CLI:
        Eg: t = TerraformDeploymentUtils()
        options = {"target": ["aws_s3_bucket_object.objects[2]", "aws_s3_bucket_object.objects[3]"]}
        t.get_command_list("terraform apply")

        Returns:
        => ['terraform', 'apply', '-target=aws_s3_bucket_object.objects[2]', '-target=aws_s3_bucket_object.objects[3]']
        """
        commands_list: List[str] = []
        for val in value:
            commands_list.append(f"-{key}={val}")
        return commands_list

    def add_bool_options(self, key: str, value: bool) -> List[str]:
        """
        Adds bool options in Terraform CLI:
        Eg: t = TerraformDeploymentUtils()
        options = {"input": False}
        t.get_command_list("terraform apply")

        Returns:
        => ['terraform', 'apply', '-input false']
        """
        commands_list: List[str] = []
        ret_value: str = "true" if value else "false"
        commands_list.append(f"-{key}={ret_value}")
        return commands_list

    def add_flagged_option(self, key: str, value: TerraformOptionFlag) -> List[str]:
        """
        Adds flag options in Terraform CLI:
        Eg: t = TerraformDeploymentUtils()
        options = {"reconfigure": FlaggedOption}
        t.get_command_list("terraform init")

        Returns:
        => ['terraform', 'init', '-reconfigure']
        """
        commands_list: List[str] = []
        if value == FlaggedOption:
            commands_list.append(f"-{key}")
        return commands_list

    def add_other_options(self, key: str, value: str) -> List[str]:
        """
        Adds default options in Terraform CLI:
        Eg: t = TerraformDeploymentUtils()
        options = {"target": "aws_s3_bucket_object.objects[2]"}
        t.get_command_list("terraform init")

        Returns:
        => ['terraform', 'init', '-target=aws_s3_bucket_object.objects[2]']
        """
        commands_list: List[str] = []
        if value is not None:
            commands_list.append(f"-{key}={value}")
        return commands_list
