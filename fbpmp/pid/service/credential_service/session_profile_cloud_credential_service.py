#!/usr/bin/env python3

import ast
import os
import pathlib
import subprocess
import sys
from typing import Dict

from fbpmp.pid.service.credential_service.cloud_credential_service import CloudCredentialService


class SessionProfileCloudCredentialService(CloudCredentialService):
    def __init__(self, arn: str, session_name: str, profile: str):
        self.arn = arn
        self.session_name = session_name
        self.profile = profile

    def get_creds(self) -> Dict[str, str]:
        cmd = ['aws', 'sts', 'assume-role', '--role-arn', self.arn, '--role-session-name', self.session_name, '--profile', self.profile]
        operating_dir = pathlib.Path(os.getcwd())
        proc = subprocess.Popen(
            cmd, cwd=operating_dir, stdout=subprocess.PIPE, stderr=sys.stderr
        )
        out, err = proc.communicate()
        if proc.returncode != 0:
            raise Exception(f"Getting credentials with command {cmd} failed with return code {proc.returncode}")

        credentials = ast.literal_eval(out.decode("utf-8"))["Credentials"]
        creds = {
            "AWS_ACCESS_KEY_ID": credentials["AccessKeyId"],
            "AWS_SECRET_ACCESS_KEY": credentials["SecretAccessKey"],
        }

        if "SessionToken" in credentials:
            creds["AWS_SESSION_TOKEN"] = credentials["SessionToken"]

        return creds
