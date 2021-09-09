#!/usr/bin/env python3
# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

import dataclasses
from typing import Dict, List

from fbpcs.pid.entity.pid_instance import PIDProtocol, PIDRole
from fbpcs.pid.entity.pid_stages import UnionPIDStage


@dataclasses.dataclass(frozen=True)
class PIDExecutionFlowLookupKey(object):
    role: PIDRole
    protocol: PIDProtocol


@dataclasses.dataclass(frozen=True)
class PIDFlow(object):
    name: str
    base_flow: str
    extra_args: Dict[UnionPIDStage, List[str]] = dataclasses.field(default_factory=dict)
    flow: Dict[UnionPIDStage, List[UnionPIDStage]] = dataclasses.field(
        default_factory=dict
    )
