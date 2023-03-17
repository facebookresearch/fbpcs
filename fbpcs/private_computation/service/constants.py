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
# TODO: T142284889 Reduce this timeout back to 12 hours after we finishe the POC run in January and have a better idea for a solution.
DEFAULT_CONTAINER_TIMEOUT_IN_SEC = 86400
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
    PrivateComputationGameType.ANONYMIZER: None,
}
DEFAULT_LOG_COST_TO_S3 = True
DEFAULT_SORT_STRATEGY = "sort"
DEFAULT_MULTIKEY_PROTOCOL_MAX_COLUMN_COUNT = 6

# based on the the test we run, we decided to use 50 as a threshold.
# since DEFAULT_PADDING_SIZE is lower than 50, we may adjust it in the future.
DEFAULT_IDENTIFIER_FILTER_THRESH = 50
FBPCS_BUNDLE_ID = "FBPCS_BUNDLE_ID"

# RUN PID has separate timeout to accommodate for 20M rows capacity on SNMK
# According to the capacity test, 20M 6keys on both side would take 3.5 hrs total.
# We set default time out to 5 hrs as the buffer.
DEFAULT_RUN_PID_TIMEOUT_IN_SEC = 18000

# Since this stage is not sharded, larger inputs can cause it to exceed the default timeout
DEFAULT_AGGREGATE_TIMEOUT_IN_SEC = 7200

CA_CERT_PATH = "tls/ca_cert.pem"
SERVER_CERT_PATH = "tls/server_cert.pem"
PRIVATE_KEY_PATH = "tls/private_key.pem"

# TODO: pass number of rows per shard in arg instead of hardcoding
NUM_ROWS_PER_MPC_SHARD_PL = 1000000
NUM_ROWS_PER_MPC_SHARD_PA = 200000

# onedocker container env vars
SERVER_PRIVATE_KEY_ENV_VAR = "SERVER_PRIVATE_KEY"
SERVER_PRIVATE_KEY_REF_ENV_VAR = (
    "SERVER_PRIVATE_KEY_REF"  # a reference for accessing the Server Private Key
)
SERVER_PRIVATE_KEY_REGION_ENV_VAR = "REGION"  # the Server Private Key region
SERVER_CERTIFICATE_ENV_VAR = "SERVER_CERTIFICATE"
CA_CERTIFICATE_ENV_VAR = "ISSUER_CERTIFICATE"
SERVER_PRIVATE_KEY_PATH_ENV_VAR = "SERVER_PRIVATE_KEY_PATH"
SERVER_CERTIFICATE_PATH_ENV_VAR = "SERVER_CERTIFICATE_PATH"
CA_CERTIFICATE_PATH_ENV_VAR = "ISSUER_CERTIFICATE_PATH"
SERVER_HOSTNAME_ENV_VAR = "SERVER_HOSTNAME"
SERVER_IP_ADDRESS_ENV_VAR = "IP_ADDRESS"
