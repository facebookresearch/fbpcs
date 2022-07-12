#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

import logging
from enum import Enum


class PCSFeature(Enum):
    UNKNOWN = "unknown"
    PCS_DUMMY = "pcs_dummy_feature"

    @staticmethod
    def from_str(feature_str: str) -> "PCSFeature":
        """maps str (possibly feature name defined in SV) to a PCSFeature."""
        feature_str = feature_str.casefold()
        if feature_str == PCSFeature.PCS_DUMMY.value:
            return PCSFeature.PCS_DUMMY
        else:
            logging.warning(f"can't map {feature_str} to pre-defined PCSFeature")
            return PCSFeature.UNKNOWN
