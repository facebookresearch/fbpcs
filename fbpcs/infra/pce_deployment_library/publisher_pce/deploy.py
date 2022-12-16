# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

import logging

from typing import Any, Dict

from fbpcs.infra.pce_deployment_library.cloud_library.aws.aws import AWS
from fbpcs.infra.pce_deployment_library.deploy_library.models import (
    FlaggedOption,
    TerraformCommand,
)
from fbpcs.infra.pce_deployment_library.deploy_library.terraform_library.terraform_deployment import (
    TerraformDeployment,
)
from fbpcs.infra.pce_deployment_library.publisher_pce.publisher_pce_defaults import (
    TerraformDefaults,
)
from fbpcs.infra.pce_deployment_library.publisher_pce.publisher_pce_utils import (
    PublisherPceUtils,
)


class Deploy:
    """
    Class to store the arguments used for deployment and undeployment
    """

    def __init__(
        self,
        s3_bucket_name: str = None,
        s3_bucket_region: str = None,
        account_id: str = None,
        partner_account_id: str = None,
        aws_region: str = None,
        tag: str = None,
        vpc_cidr: str = None,
        partner_vpc_cidr: str = None,
        vpc_logging_enabled: bool = False,
        vpc_log_bucket_arn: str = None,
    ):
        self.s3_bucket_name = s3_bucket_name
        self.s3_bucket_region = s3_bucket_region
        self.account_id = account_id
        self.partner_account_id = partner_account_id
        self.aws_region = aws_region
        self.tag = tag
        self.vpc_cidr = vpc_cidr
        self.partner_vpc_cidr = partner_vpc_cidr
        self.vpc_logging_enabled = vpc_logging_enabled
        self.vpc_log_bucket_arn = vpc_log_bucket_arn

        self.tag_postfix = f"-{self.tag}"

        self.log: logging.Logger = logging.getLogger(__name__)

        self.aws = AWS(aws_region=self.aws_region)
        self.terraform = TerraformDeployment()
        self.publishe_pce_utils = PublisherPceUtils()

    def deploy_pce(self, bucket_version: bool = True) -> None:
        self.log.info(
            "########################Started AWS Infrastructure Deployment########################"
        )
        self.log.info("Creating S3 bucket...")
        self.aws.check_s3_buckets_exists(
            s3_bucket_name=self.s3_bucket_name, bucket_version=bucket_version
        )
        self.terraform.working_directory = TerraformDefaults.PCE_TERRAFORM_FILE_LOCATION

        self.log.info(
            f"Changing terraform working directory to {self.terraform.working_directory}"
        )
        backend_config = self._get_init_backend_config()

        self.log.info("Running terraform init...")
        terraform_init_log = self.terraform.terraform_init(
            backend_config=backend_config, reconfigure=FlaggedOption
        )
        self.log.info(
            f"Terraform init output is: {self.publishe_pce_utils.parse_command_output(TerraformCommand.INIT, terraform_init_log)}"
        )

        var_dict = self._get_var()
        if self.vpc_logging_enabled:
            opt_params = self._get_vpc_var()
            var_dict.update(opt_params)

        self.log.debug(f"Running terraform apply with vars: {var_dict}")
        terraform_create_log = self.terraform.create(var=var_dict)
        self.log.info(
            f"Terraform apply output is: {self.publishe_pce_utils.parse_command_output(TerraformCommand.APPLY, terraform_create_log)}"
        )

        self.log.info(
            "######################## PCE terraform output ########################"
        )
        terraform_output_result = self.terraform.terraform_output()
        self.log.info(
            f"Terraform output is: {self.publishe_pce_utils.parse_command_output(TerraformCommand.OUTPUT, terraform_output_result)}"
        )

        self.log.info(
            "########################Finished AWS Infrastructure Deployment########################"
        )

    def undeploy_pce(self) -> None:
        self.log.info("Start undeploying...")
        self.log.info(
            "########################Check tfstate files########################"
        )
        terraform_state_file = f"tfstate/pce{self.tag_postfix}.tfstate"
        self.aws.check_s3_object_exists(
            s3_bucket_name=self.s3_bucket_name, key_name=terraform_state_file
        )
        self.log.info("All tfstate files exist. Continue...")

        self.terraform.working_directory = TerraformDefaults.PCE_TERRAFORM_FILE_LOCATION

        backend_config = self._get_init_backend_config()

        self.log.info(
            "########################Delete PCE resources########################"
        )
        self.log.info("Running terraform init...")
        terraform_init_log = self.terraform.terraform_init(
            backend_config=backend_config, reconfigure=FlaggedOption
        )
        self.log.info(
            f"Terraform init output is: {self.publishe_pce_utils.parse_command_output(TerraformCommand.INIT, terraform_init_log)}"
        )

        var_dict = self._get_var()
        terraform_destroy_log = self.terraform.destroy(var=var_dict)
        self.log.info(
            f"Terraform init output is: {self.publishe_pce_utils.parse_command_output(TerraformCommand.DESTROY, terraform_destroy_log)}"
        )

    def _get_init_backend_config(self) -> Dict[str, Any]:
        return {
            "bucket": self.s3_bucket_name,
            "region": self.aws_region,
            "key": f"tfstate/pce{self.tag_postfix}.tfstate",
        }

    def _get_var(self, destroy: bool = False) -> Dict[str, Any]:
        return_dict = {
            "aws_region": self.aws_region,
            "tag_postfix": self.tag_postfix,
            "pce_id": self.tag,
        }

        if not destroy:
            return_dict.update(
                {
                    "vpc_cidr": self.vpc_cidr,
                    "otherparty_vpc_cidr": self.partner_vpc_cidr,
                }
            )

        return return_dict

    def _get_vpc_var(self) -> Dict[str, Any]:
        return {
            "vpc_logging": {
                "enabled": self.vpc_logging_enabled,
                "bucket_arn": self.vpc_log_bucket_arn,
            }
        }
