#!/usr/bin/env fbpython
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

from typing import Any, Dict

from fbpcs.private_computation.service.constants import PRIVATE_KEY_PATH

TLS_ARG_KEY_CA_CERT_PATH = "ca_cert_path"
TLS_ARG_KEY_SERVER_CERT_PATH = "server_cert_path"
TLS_ARG_KEY_PRIVATE_CERT_PATH = "private_key_path"


def get_tls_arguments(
    has_tls_feature: bool,
    server_certificate_path: str,
    ca_certificate_path: str,
) -> Dict[str, Any]:
    return {
        "use_tls": has_tls_feature,
        TLS_ARG_KEY_CA_CERT_PATH: ca_certificate_path if has_tls_feature else "",
        TLS_ARG_KEY_SERVER_CERT_PATH: (
            server_certificate_path if has_tls_feature else ""
        ),
        TLS_ARG_KEY_PRIVATE_CERT_PATH: PRIVATE_KEY_PATH if has_tls_feature else "",
    }
