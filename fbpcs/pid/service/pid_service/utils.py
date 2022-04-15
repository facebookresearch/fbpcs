#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict
from fbpcs.pid.entity.pid_instance import PIDProtocol
from fbpcs.private_computation.service.constants import (
    DEFAULT_MULTIKEY_PROTOCOL_MAX_COLUMN_COUNT,
)


def get_max_id_column_cnt(pid_protocol: PIDProtocol) -> int:
    if pid_protocol is PIDProtocol.MULTIKEY_PID:
        return DEFAULT_MULTIKEY_PROTOCOL_MAX_COLUMN_COUNT
    return 1
