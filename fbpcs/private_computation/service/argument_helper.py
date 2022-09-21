#!/usr/bin/env fbpython
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

from typing import Any, Dict

from fbpcs.private_computation.service.constants import (
    CA_CERT_PATH,
    PRIVATE_KEY_PATH,
    SERVER_CERT_PATH,
)


def get_tls_arguments(has_tls_feature: bool) -> Dict[str, Any]:
    return {
        "use_tls": has_tls_feature,
        "ca_cert_path": CA_CERT_PATH if has_tls_feature else "",
        "server_cert_path": SERVER_CERT_PATH if has_tls_feature else "",
        "private_key_path": PRIVATE_KEY_PATH if has_tls_feature else "",
    }
