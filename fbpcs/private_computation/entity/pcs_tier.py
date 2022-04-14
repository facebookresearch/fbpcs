#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

from enum import Enum


class PCSTier(Enum):
    UNKNOWN = "unknown"
    RC = "rc"
    CANARY = "canary"
    PROD = "latest"

    @staticmethod
    def from_str(tier_str: str) -> "PCSTier":
        """maps str (possibly smc tier of deployed PCS thrift servers) to a PCSTier."""

        if tier_str in (
            "rc",
            "private_measurement.private_computation_service_rc",
        ):
            return PCSTier.RC
        elif tier_str in (
            "canary",
            "private_measurement.private_computation_service_canary",
        ):
            return PCSTier.CANARY
        elif tier_str in (
            "latest",
            "private_measurement.private_computation_service",
        ):
            return PCSTier.PROD
        else:
            return PCSTier.UNKNOWN
