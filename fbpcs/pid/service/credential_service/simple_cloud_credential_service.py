#!/usr/bin/env python3
# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

from typing import Dict

from fbpcs.pid.service.credential_service.cloud_credential_service import CloudCredentialService


class SimpleCloudCredentialService(CloudCredentialService):
    def __init__(self, access_key_id: str, access_key_data: str):
        self.access_key_id = access_key_id
        self.access_key_data = access_key_data

    def get_creds(self) -> Dict[str, str]:
        return {
            "AWS_ACCESS_KEY_ID": self.access_key_id,
            "AWS_SECRET_ACCESS_KEY": self.access_key_data,
        }
