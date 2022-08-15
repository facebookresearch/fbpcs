#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

import logging
from enum import Enum


class PCSFeature(Enum):

    BOLT_RUNNER = "bolt_runner"
    PCS_DUMMY = "pcs_dummy_feature"
    PRIVATE_LIFT_PCF2_RELEASE = "private_lift_pcf2_release"
    SHARD_COMBINER_PCF2_RELEASE = "shard_combiner_pcf2_release"
    UNKNOWN = "unknown"

    @staticmethod
    def from_str(feature_str: str) -> "PCSFeature":
        """maps str (possibly feature name defined in SV) to a PCSFeature."""
        feature_str = feature_str.casefold()
        try:
            return PCSFeature(feature_str)
        except ValueError:
            logging.warning(f"can't map {feature_str} to pre-defined PCSFeature")
            return PCSFeature.UNKNOWN
