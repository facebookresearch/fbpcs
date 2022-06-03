#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

from abc import ABC
from dataclasses import dataclass

from typing import Optional


@dataclass
class BoltCreateInstanceArgs(ABC):
    pass


@dataclass
class BoltPlayerArgs:
    create_instance_args: BoltCreateInstanceArgs
    expected_result_path: Optional[str] = None


@dataclass
class BoltJob:
    job_name: str
    publisher_bolt_args: BoltPlayerArgs
    partner_bolt_args: BoltPlayerArgs
