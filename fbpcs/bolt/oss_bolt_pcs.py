#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

from dataclasses import dataclass

from fbpcs.bolt.bolt_job import BoltCreateInstanceArgs
from fbpcs.bolt.bolt_runner import BoltClient


@dataclass
class BoltPCSCreateInstanceArgs(BoltCreateInstanceArgs):
    pass


class BoltPCSClient(BoltClient):
    pass
