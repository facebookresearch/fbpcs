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
    PC_COORDINATED_RETRY = "private_computation_coordinated_retry"
    PRIVATE_LIFT_UNIFIED_DATA_PROCESS = "private_lift_unified_data_process"
    PCS_PRIVATE_LIFT_DECOUPLED_UDP = "pcs_private_lift_decoupled_udp"
    PRIVATE_ATTRIBUTION_MR_PID = "private_attribution_with_mr_pid"
    SHARD_COMBINER_PCF2_RELEASE = "shard_combiner_pcf2_release"
    NUM_MPC_CONTAINER_MUTATION = "num_mpc_container_mutation"
    PID_SNMK_LARGER_CONTAINER_TYPE = "pid_snmk_larger_container_type"
    PCF_TLS = "pcf_tls"
    PA_TIMESTAMP_VALIDATION = "pa_timestamp_validation"
    PL_TIMESTAMP_VALIDATION = "pl_timestamp_validation"
    PRE_VALIDATION_FILE_STREAM = "pre_validation_file_stream"
    PID_FILTER_LOW_QUALITY_IDENTIFIER_THRESH166 = (
        "pid_filter_low_quality_identifier_thresh166"
    )
    PUBLISHER_PC_PRE_VALIDATION = "publisher_pc_pre_validation"
    PARTNER_PC_PRE_VALIDATION = "partner_pc_pre_validation"
    PRIVATE_ATTRIBUTION_REFORMATTED_OUTPUT = "private_attribution_reformatted_output"
    PRIVATE_COMPUTATION_TRANSLATOR = "private_computation_translator"

    UNKNOWN = "unknown"

    @classmethod
    def _missing_(cls, value: object) -> "PCSFeature":
        """maps name/value in case insentitive to a PCSFeature."""
        for member in cls:
            if str(value).casefold() in [
                member.value.casefold(),
                member.name.casefold(),
            ]:
                return member

        return PCSFeature.UNKNOWN

    @staticmethod
    def from_str(feature_str: str) -> "PCSFeature":
        """maps str (possibly feature name defined in SV) to a PCSFeature."""
        # We intentionally do this length check outside of the try/except below.
        # It indicates that someone likely messed up a config. For example,
        # passing `"bolt_runner" => ["b", "o", "l", "t", "_", ...]` as features
        # because the config.yml had missing brackets `[]`, causing the string
        # to get interpreted as an interable of characters instead of a list of
        # strings with one element.
        if len(feature_str) <= 1:
            raise ValueError("Features of length <= 1 not supported. Check your config")

        feature_str = feature_str.casefold()
        try:
            return PCSFeature(feature_str)
        except ValueError:
            logging.warning(f"can't map {feature_str} to pre-defined PCSFeature")
            return PCSFeature.UNKNOWN
