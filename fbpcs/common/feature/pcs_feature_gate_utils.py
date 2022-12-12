#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict
from typing import Optional, Set, Type

from fbpcs.private_computation.entity.infra_config import PrivateComputationGameType
from fbpcs.private_computation.entity.pcs_feature import PCSFeature
from fbpcs.private_computation.stage_flows.private_computation_base_stage_flow import (
    PrivateComputationBaseStageFlow,
)
from fbpcs.private_computation.stage_flows.private_computation_mr_pid_pcf2_lift_stage_flow import (
    PrivateComputationMrPidPCF2LiftStageFlow,
)
from fbpcs.private_computation.stage_flows.private_computation_mr_pid_udp_pcf2_lift_stage_flow import (
    PrivateComputationMrPidUDPPCF2LiftStageFlow,
)
from fbpcs.private_computation.stage_flows.private_computation_mr_stage_flow import (
    PrivateComputationMRStageFlow,
)
from fbpcs.private_computation.stage_flows.private_computation_pcf2_lift_udp_stage_flow import (
    PrivateComputationPCF2LiftUDPStageFlow,
)
from fbpcs.private_computation.stage_flows.private_computation_pcf2_stage_flow import (
    PrivateComputationPCF2StageFlow,
)
from fbpcs.private_computation.stage_flows.private_computation_stage_flow import (
    PrivateComputationStageFlow,
)
from fbpcs.utils.optional import unwrap_or_default


def get_stage_flow(
    game_type: PrivateComputationGameType,
    pcs_feature_enums: Set[PCSFeature],
    stage_flow_cls: Optional[Type[PrivateComputationBaseStageFlow]] = None,
) -> Type[PrivateComputationBaseStageFlow]:
    selected_stage_flow_cls = unwrap_or_default(
        optional=stage_flow_cls,
        default=PrivateComputationPCF2StageFlow
        if game_type is PrivateComputationGameType.ATTRIBUTION
        else PrivateComputationStageFlow,
    )

    # warning, enabled feature gating will override stage flow, Please contact PSI team to have a similar adoption
    has_mr_pid = PCSFeature.PRIVATE_ATTRIBUTION_MR_PID in pcs_feature_enums
    has_udp = PCSFeature.PRIVATE_LIFT_UNIFIED_DATA_PROCESS in pcs_feature_enums

    if has_mr_pid and has_udp:
        selected_stage_flow_cls = (
            PrivateComputationMRStageFlow
            if game_type is PrivateComputationGameType.ATTRIBUTION
            else PrivateComputationMrPidUDPPCF2LiftStageFlow
        )
    elif has_mr_pid:
        selected_stage_flow_cls = (
            PrivateComputationMRStageFlow
            if game_type is PrivateComputationGameType.ATTRIBUTION
            else PrivateComputationMrPidPCF2LiftStageFlow
        )
    elif has_udp:
        selected_stage_flow_cls = (
            PrivateComputationPCF2LiftUDPStageFlow
            if game_type is PrivateComputationGameType.LIFT
            else selected_stage_flow_cls
        )
    return selected_stage_flow_cls
