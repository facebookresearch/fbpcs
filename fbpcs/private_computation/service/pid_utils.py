#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

from fbpcs.pid.entity.pid_instance import PIDProtocol
from fbpcs.private_computation.service.constants import (
    DEFAULT_MULTIKEY_PROTOCOL_MAX_COLUMN_COUNT,
    DEFAULT_PID_PROTOCOL,
)


def get_max_id_column_cnt(pid_protocol: PIDProtocol) -> int:
    if pid_protocol is PIDProtocol.UNION_PID_MULTIKEY:
        return DEFAULT_MULTIKEY_PROTOCOL_MAX_COLUMN_COUNT
    return 1


def get_pid_protocol_from_num_shards(
    num_pid_containers: int, multikey_enabled: bool
) -> PIDProtocol:
    if num_pid_containers == 1 and multikey_enabled:
        return PIDProtocol.UNION_PID_MULTIKEY
    return DEFAULT_PID_PROTOCOL


def pid_should_use_row_numbers(
    pid_use_row_numbers: bool, pid_protocol: PIDProtocol
) -> bool:
    if pid_use_row_numbers and pid_protocol is not PIDProtocol.UNION_PID_MULTIKEY:
        return True
    else:
        return False


def get_sharded_filepath(path: str, shard: int) -> str:
    """
    Although this function is incredibly simple, it's important that we
    centralize one definition for how sharded files should look. This will
    ensure that we remain consistent in how we "expect" sharded filepaths
    to be stored and will prevent any erroneous mistakes if one service
    gets changed in the future to change the filepath in the future. There
    are no software guarantees here, but it should hint to the developer
    that there's some special function to use to shard a filepath.
    """
    return f"{path}_{shard}"


def get_metrics_filepath(path: str, shard: int) -> str:
    """
    Although this function is incredibly simple, it's important that we
    centralize one definition for how sharded metrics files should look. This will
    ensure that we remain consistent in how we "expect" sharded metrics filepaths
    to be stored and will prevent any erroneous mistakes if one service
    gets changed in the future to change the filepath in the future. There
    are no software guarantees here, but it should hint to the developer
    that there's some special function to use to log PID metrics.
    """
    return get_sharded_filepath(path, shard) + "_metrics"  # noqa
