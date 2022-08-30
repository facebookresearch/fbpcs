#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict


from enum import Enum, IntEnum


class PIDRole(IntEnum):
    PUBLISHER = 0
    PARTNER = 1

    @classmethod
    def from_str(cls, s: str) -> "PIDRole":
        if s.upper() == "PUBLISHER":
            return cls.PUBLISHER
        elif s.upper() == "PARTNER":
            return cls.PARTNER
        else:
            raise ValueError(f"Unknown role: {s}")


class PIDStageStatus(Enum):
    UNKNOWN = "UNKNOWN"
    READY = "READY"
    STARTED = "STARTED"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


class PIDProtocol(IntEnum):
    UNION_PID = 0
    PS3I_M_TO_M = 1
    UNION_PID_MULTIKEY = 2
