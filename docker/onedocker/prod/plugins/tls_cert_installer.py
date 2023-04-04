#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

import logging
import os
import sys

from fbpcp.service.secrets_manager_aws import AWSSecretsManagerService

# environment variables
SERVER_PRIVATE_KEY = "SERVER_PRIVATE_KEY"
SERVER_PRIVATE_KEY_REF = "SERVER_PRIVATE_KEY_REF"
SERVER_CERTIFICATE = "SERVER_CERTIFICATE"
ISSUER_CERTIFICATE = "ISSUER_CERTIFICATE"
SERVER_PRIVATE_KEY_PATH = "SERVER_PRIVATE_KEY_PATH"
SERVER_CERTIFICATE_PATH = "SERVER_CERTIFICATE_PATH"
ISSUER_CERTIFICATE_PATH = "ISSUER_CERTIFICATE_PATH"
HOME_DIR = "HOME"
HOSTALIASES = "HOSTALIASES"
IP_ADDRESS = "IP_ADDRESS"
SERVER_HOSTNAME = "SERVER_HOSTNAME"
REGION = "REGION"

# other constants
HOST_FILE_PATH = "/etc/hosts"
DEFAULT_REGION = "us-west-2"


def _get_env_var_if_set(env_var: str, default_val: str) -> str:
    val = os.getenv(env_var)
    return val if val else default_val


def _write_content_to_file(full_path: str, content: str) -> None:
    parent_path = "/".join(full_path.split("/")[:-1])
    os.makedirs(parent_path, exist_ok=True)
    with open(full_path, "w") as fw:
        fw.write(content)


def _get_secret(secret_id: str, region: str) -> str:
    secret_svc = AWSSecretsManagerService(region)
    return secret_svc.get_secret(secret_id).value


def main() -> None:
    logger = logging.getLogger()
    streamHandler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter("%(levelname)s:%(filename)s:%(message)s")
    logger.setLevel(logging.DEBUG)
    streamHandler.setFormatter(formatter)
    logger.addHandler(streamHandler)

    logging.info("Reading certificate content from environment variables...")
    server_certificate = _get_env_var_if_set(SERVER_CERTIFICATE, "")
    server_certificate_path = _get_env_var_if_set(SERVER_CERTIFICATE_PATH, "")
    issuer_certificate = _get_env_var_if_set(ISSUER_CERTIFICATE, "")
    issuer_certificate_path = _get_env_var_if_set(ISSUER_CERTIFICATE_PATH, "")
    private_key = _get_env_var_if_set(SERVER_PRIVATE_KEY, "")
    private_key_ref = _get_env_var_if_set(SERVER_PRIVATE_KEY_REF, "")
    private_key_path = _get_env_var_if_set(SERVER_PRIVATE_KEY_PATH, "")
    home_dir = _get_env_var_if_set(HOME_DIR, "")
    ip_address = _get_env_var_if_set(IP_ADDRESS, "")
    server_hostname = _get_env_var_if_set(SERVER_HOSTNAME, "")
    region = _get_env_var_if_set(REGION, DEFAULT_REGION)

    try:
        logging.info("Starting writing certificates...")
        if server_certificate_path and server_certificate:
            full_server_cert_path = os.path.join(home_dir, server_certificate_path)
            _write_content_to_file(full_server_cert_path, server_certificate)
            logging.info(f"Wrote server certificate to {full_server_cert_path}")

        if issuer_certificate_path and issuer_certificate:
            full_issuer_cert_path = os.path.join(home_dir, issuer_certificate_path)
            _write_content_to_file(full_issuer_cert_path, issuer_certificate)
            logging.info(f"Wrote issuer certificate to {full_issuer_cert_path}")

        if private_key_path:
            full_private_key_path = os.path.join(home_dir, private_key_path)
            if private_key_ref:
                secret = _get_secret(private_key_ref, region)
                _write_content_to_file(full_private_key_path, secret)
                logging.info(f"Wrote private_key to {full_private_key_path}")
            elif private_key:
                _write_content_to_file(full_private_key_path, private_key)
                logging.info(f"Wrote private_key to {full_private_key_path}")

        if ip_address and server_hostname:
            logging.info("Start setting up routing config in the host file.")
            # sudo permission to run this script is built into the dockerfile
            # with specified user group.
            os.system(
                f"sudo /home/onedocker/package/write_routing.sh {ip_address} {server_hostname}"
            )
            logging.info(
                f"Wrote IP address {ip_address} and host name {server_hostname} to {HOST_FILE_PATH}"
            )
        else:
            logging.info(
                "Routing not configured because at least one of ip_address and server_hostname is not specified."
            )
    except Exception as e:
        raise Exception(
            f"Caught an exception while executing the binary: {e.with_traceback(e.__traceback__)}"
        )


if __name__ == "__main__":
    main()
