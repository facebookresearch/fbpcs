# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

import os


class AwsDeploymentHelper:
    def __init__(self, access_key: str, secret_key: str, account_id: str):
        self.access_key = access_key or os.environ.get("ACCESS_KEY")
        self.secret_key = secret_key or os.environ.get("SECRET_KEY")
        self.account_id = account_id or os.environ.get("ACCOUNT_ID")

        if not all([self.access_key, self.secret_key]):
            print("both access and secret keys are needed to perform further actions.")
            exit(0)

    def add_iam_user(self, user_name: str = None):
        if not user_name:
            print("user name is required to add the user")

        print(f"user_name inside create = {user_name}")

    def delete_iam_user(self, user_name: str = None):
        if not user_name:
            print("user name is required to add the user")

        print(f"user_name inside destroy = {user_name}")
