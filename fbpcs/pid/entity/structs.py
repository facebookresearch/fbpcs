#!/usr/bin/env python3
# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

import dataclasses

from fbpcs.pid.entity.pid_instance import PIDRole


@dataclasses.dataclass(frozen=True)
class PIDPlayer(object):
    role: PIDRole
    hostname: str
    port: int

    @property
    def id(self) -> int:
        return int(self.role)

    @classmethod
    def me(cls, role: PIDRole, port: int) -> "PIDPlayer":
        hostname = "0.0.0.0"
        return cls(role, hostname, port)
