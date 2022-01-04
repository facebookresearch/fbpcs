# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

import json
import logging
import os
from string import Template

import boto3
from botocore.exceptions import ClientError
from botocore.exceptions import NoCredentialsError

from .policy_params import PolicyParams


class AwsDeploymentHelper:

    # policy_arn is fixed string. So defining it as a macro.
    POLICY_ARN = "arn:aws:iam::{}:policy/{}"

    def __init__(
        self,
        access_key: str = None,
        secret_key: str = None,
        account_id: str = None,
        region: str = None,
        log_path: str = "/tmp/pce_iam_user.log",
        log_level: logging = logging.INFO,
    ):
        self.access_key = access_key or os.environ.get("ACCESS_KEY")
        self.secret_key = secret_key or os.environ.get("SECRET_KEY")
        self.account_id = account_id or os.environ.get("ACCOUNT_ID")
        self.region = region or os.environ.get("AWS_REGION")
        self.log_path = log_path
        self.log_level = log_level

        self.log = None
        self.iam = None

        # setup logging
        logging.basicConfig(
            filename=self.log_path,
            level=self.log_level,
            format="[%(asctime)s][%(name)s][%(levelname)s] - %(message)s",
        )
        self.log = logging.getLogger(__name__)

        # create clients for iam and sts
        if self.access_key and self.secret_key:
            sts = boto3.client(
                "sts",
                aws_access_key_id=self.access_key,
                aws_secret_access_key=self.secret_key,
            )
            self.iam = boto3.client(
                "iam",
                aws_access_key_id=self.access_key,
                aws_secret_access_key=self.secret_key,
            )
        else:
            sts = boto3.client("sts")
            self.iam = boto3.client("iam")

        # verify if the account details are correct
        try:
            sts.get_caller_identity()
        except NoCredentialsError as error:
            self.log.error(
                f"""Error occured in validating access and secret keys of the aws account.
                Please verify if the correct access and secret key of root user are provided.
                Access and secret key can be passed using:
                1. cli.py options "--access_key" and "--secret_keys"
                2. Placing keys in ~/.aws/config
                3. Placing keys in ~/.aws/credentials
                4. As environment variables

                Please refer to: https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html

                Following is the error:
                {error}
                """
            )

        # get account id if not provided in cli
        if self.account_id is None:
            self.account_id = boto3.client("sts").get_caller_identity().get("Account")

        self.iam = boto3.client("iam")

    def create_user(self, user_name: str):
        self.log.info(f"Creating user {user_name}")
        try:
            self.iam.create_user(UserName=user_name)
            self.log.info(f"Created user with user name {user_name}")
        except ClientError as error:
            if error.response.get("Error", {}).get("Code") == "EntityAlreadyExists":
                self.log.error(
                    f"User {user_name} already exists.\n \
                    Please delete existing user or create user with different username"
                )
            else:
                self.log.error(
                    f"Unexpected error occured in creation of user {user_name}"
                )

    def delete_user(self, user_name: str):
        self.log.info(f"Deleting user {user_name}")
        try:
            self.iam.delete_user(UserName=user_name)
            self.log.info(f"Deleted user with user name {user_name}")
        except ClientError as error:
            if error.response.get("Error", {}).get("Code") == "NoSuchEntity":
                self.log.error(
                    f"Failed to delete user.\n \
                    user with username {user_name} doesn't exist."
                )
            else:
                self.log.error(
                    f"Unexpected error occured in deletion of user {user_name}"
                )

    def create_policy(
        self, policy_name: str, policy_params: PolicyParams, user_name: str = None
    ):
        self.log.info(f"Adding policy {policy_name}")

        # directly reading the json file from iam_policies folder
        # TODO: pass the policy to be added from cli.py when we need more granular control

        policy_json_data = self.read_json_file(
            file_name="iam_policies/fb_pc_iam_policy.json", policy_params=policy_params
        )

        try:
            self.iam.create_policy(
                PolicyName=policy_name, PolicyDocument=json.dumps(policy_json_data)
            )
            self.log.info(f"Created policy with policy name {policy_name}")
        except ClientError as error:
            if error.response.get("Error", {}).get("Code") == "EntityAlreadyExists":
                self.log.error(
                    "Policy already exits. Attaching exising policy to the user"
                )
            else:
                if user_name:
                    self.log.error(
                        f"Unexpected error occurred in policy {policy_name} creation for user {user_name}: {error}"
                    )
                else:
                    self.log.error(f"Unexpected error occurred in policy {policy_name}")

    def delete_policy(self, policy_name: str):
        self.log.info(f"Deleting policy {policy_name}")
        policy_arn = self.POLICY_ARN.format(self.account_id, policy_name)
        try:
            self.iam.delete_policy(PolicyArn=policy_arn)
            self.log.info(f"Deleted policy with policy name {policy_name}")
        except ClientError as error:
            if error.response.get("Error", {}).get("Code") == "NoSuchEntityException":
                self.log.error(f"Policy {policy_arn} doesn't exist")
            else:
                self.log.error(f"Unexpected error occurred in deleting policy: {error}")

    def attach_user_policy(self, policy_name: str, user_name: str):
        self.log.info(f"Attaching policy {policy_name} to user {user_name}")

        current_policies = self.list_policies()
        current_users = self.list_users()

        if policy_name not in current_policies:
            self.log.error(f"Policy {policy_name} is not present for this AWS account")
            self.log.error("Please check policy name or add a new policy")
            raise Exception(f"Policy {policy_name} not found")

        if user_name not in current_users:
            self.log.error(f"User {user_name} is not present for this AWS account")
            self.log.error("Please check user name or add a new user")
            raise Exception(f"User {user_name} not found")

        policy_arn = self.POLICY_ARN.format(self.account_id, policy_name)
        try:
            self.iam.attach_user_policy(UserName=user_name, PolicyArn=policy_arn)
            self.log.info(
                f"Attached policy with policy name {policy_name} to the user {user_name}"
            )
        except ClientError as error:
            self.log.error(
                f"Failed to attach the policy {policy_arn} for user {user_name}: {error}"
            )

    def detach_user_policy(self, policy_name: str, user_name: str):
        self.log.info(f"Detaching policy {policy_name} from the user {user_name}")

        current_policies = self.list_policies()
        current_users = self.list_users()

        if policy_name not in current_policies:
            self.log.error(f"Policy {policy_name} is not present for this AWS account")
            self.log.error("Please check policy name or add a new policy")
            raise Exception(f"Policy {policy_name} not found")

        if user_name not in current_users:
            self.log.error(f"User {user_name} is not present for this AWS account")
            self.log.error("Please check user name or add a new user")
            raise Exception(f"User {user_name} not found")

        policy_arn = self.POLICY_ARN.format(self.account_id, policy_name)
        try:
            self.iam.detach_user_policy(UserName=user_name, PolicyArn=policy_arn)
            self.log.info(
                f"Detached policy with policy name {policy_name} from the user {user_name}"
            )
        except ClientError as error:
            self.log.error(
                f"Failed to detach policy {policy_arn} from user {user_name}: {error}"
            )

    def list_policies(self):
        policy_name_list = []
        try:
            response = self.iam.list_policies().get("Policies", [])
            for policy_dict in response:
                policy_name_list.append(policy_dict["PolicyName"])
        except ClientError as error:
            self.log.error(f"Failed to list policies: {error}")
        return policy_name_list

    def list_users(self):
        user_name_list = []
        try:
            response = self.iam.list_users().get("Users", [])
            for users_dict in response:
                user_name_list.append(users_dict["UserName"])
        except ClientError as error:
            self.log.error(f"Failed to list users: {error}")
        return user_name_list

    def create_access_key(self, user_name: str):
        self.log.info(f"Creating access and secret keys for user {user_name}.")
        self.log.info("Access and secrect keys will not be printed in this log file.")
        try:
            response = self.iam.create_access_key(UserName=user_name)
            self.log.info(f"Creating access and secret key for the user {user_name}")
            access_key = response["AccessKey"]["AccessKeyId"]
            secret_key = response["AccessKey"]["SecretAccessKey"]
            print(
                """Printing access and secret keys. Please copy the text pasted as it won't be listed again"""
            )
            print(f"Access Key = {access_key}")
            print(f"Secret Key = {secret_key}")
            print(f"User = {user_name}")
        except ClientError as error:
            self.log.error(
                f"Error in generating access and secret for user {user_name}: {error}"
            )

    def delete_access_key(self, user_name: str, access_key: str):
        self.log.info(f"Deleting access and secret keys for user {user_name}.")
        try:
            self.iam.delete_access_key(UserName=user_name, AccessKeyId=access_key)
            self.log.info(f"Deleting access and secret key for the user {user_name}")
        except ClientError as error:
            self.log.error(f"Error in deleting access for user {user_name}: {error}")

    def list_access_keys(self, user_name: str):
        access_key_list = []
        try:
            response = self.iam.list_access_keys(UserName=user_name)
            for access_key in response["AccessKeyMetadata"]:
                access_key_list.append(access_key["AccessKeyId"])
        except ClientError as error:
            self.log.error(
                f"Error occured when listing access keys for the user {user_name}: {error}"
            )
        return access_key_list

    def read_json_file(
        self, file_name: str, policy_params: PolicyParams, read_mode: str = "r"
    ):

        # this can be replaced with a json file which is written in deploy.sh
        interpolation_data = {
            "REGION": self.region,
            "ACCOUNT_ID": self.account_id,
            "CLUSTER_NAME": policy_params.cluster_name,
            "DATA_BUCKET_NAME": policy_params.data_bucket_name,
            "CONFIG_BUCKET_NAME": policy_params.config_bucket_name,
            "DATA_INGESTION_KMS_KEY": policy_params.data_ingestion_kms_key,
            "ECS_TASK_EXECUTION_ROLE_NAME": policy_params.ecs_task_execution_role_name,
            "FIREHOSE_STREAM_NAME": policy_params.firehose_stream_name,
            "DATEBASE_NAME": policy_params.database_name,
        }

        file_path = os.path.join(os.path.dirname(__file__), file_name)
        with open(file_path, read_mode) as file_obj:
            content = "".join(file_obj.readlines())
            template = Template(content)
            json_data = json.loads(template.substitute(interpolation_data))
        return json_data

    def create_user_workflow(self, user_name: str):

        self.log.info(
            f"""Cli to create user is triggered. Following actions will be performed
        1. User {user_name} will be created
        2. Access and security keys for {user_name} will be created
        """
        )

        # create user
        self.create_user(user_name=user_name)

        # generate access and secret keys
        self.create_access_key(user_name=user_name)
        self.log.info("Creation operation completed.")

    def delete_user_workflow(self, user_name: str):
        self.log.info(
            f"""Cli to create user is triggered. Following actions will be performed
        1. User {user_name} will be deleted
        2. All access and security keys of {user_name} will be deleted
        """
        )

        # delete all the access keys for the user
        access_key_list = self.list_access_keys(user_name=user_name)
        for access_key in access_key_list:
            self.delete_access_key(user_name=user_name, access_key=access_key)

        # delete user
        self.delete_user(user_name=user_name)
        self.log.info("Deletion operation completed.")
