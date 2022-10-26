#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

import typing

from fbpcs.pid.entity.pid_instance import PIDProtocol

from fbpcs.private_computation.entity.infra_config import PrivateComputationGameType

"""
43200 s = 12 hrs

We want to be conservative on this timeout just in case:
1) partner side is not able to connect in time. This is possible because it's a manual process
to run partner containers and humans can be slow;
2) during development, we add logic or complexity to the binaries running inside the containers
so that they take more than a few hours to run.
"""

DEFAULT_CONTAINER_TIMEOUT_IN_SEC = 43200
DEFAULT_SERVER_PORT_NUMBER = 15200
MAX_ROWS_PER_PID_CONTAINER = 10_000_000
TARGET_ROWS_PER_MPC_CONTAINER = 250_000
NUM_NEW_SHARDS_PER_FILE: int = round(
    MAX_ROWS_PER_PID_CONTAINER / TARGET_ROWS_PER_MPC_CONTAINER
)

DEFAULT_K_ANONYMITY_THRESHOLD_PL = 100
DEFAULT_K_ANONYMITY_THRESHOLD_PA = 0
DEFAULT_PID_PROTOCOL: PIDProtocol = PIDProtocol.UNION_PID
DEFAULT_HMAC_KEY: str = ""
DEFAULT_CONCURRENCY = 4
ATTRIBUTION_TEST_CONCURRENCY = 1
DEFAULT_PADDING_SIZE: typing.Dict[PrivateComputationGameType, typing.Optional[int]] = {
    PrivateComputationGameType.LIFT: 25,
    PrivateComputationGameType.ATTRIBUTION: 4,
    PrivateComputationGameType.PRIVATE_ID_DFCA: None,
}
DEFAULT_LOG_COST_TO_S3 = True
DEFAULT_SORT_STRATEGY = "sort"
DEFAULT_MULTIKEY_PROTOCOL_MAX_COLUMN_COUNT = 6
FBPCS_BUNDLE_ID = "FBPCS_BUNDLE_ID"

CA_CERT_PATH = "tls/ca_cert.pem"
SERVER_CERT_PATH = "tls/server_cert.pem"
PRIVATE_KEY_PATH = "tls/private_key.pem"

# TODO: pass number of rows per shard in arg instead of hardcoding
NUM_ROWS_PER_MPC_SHARD_PL = 1000000
NUM_ROWS_PER_MPC_SHARD_PA = 200000

# onedocker container env vars
SERVER_PRIVATE_KEY_ENV_VAR = "SERVER_PRIVATE_KEY"
SERVER_CERTIFICATE_ENV_VAR = "SERVER_CERTIFICATE"
CA_CERTIFICATE_ENV_VAR = "CA_CERTIFICATE"
SERVER_PRIVATE_KEY_PATH_ENV_VAR = "SERVER_PRIVATE_KEY_PATH"
SERVER_CERTIFICATE_PATH_ENV_VAR = "SERVER_CERTIFICATE_PATH"
CA_CERTIFICATE_PATH_ENV_VAR = "CA_CERTIFICATE_PATH"
