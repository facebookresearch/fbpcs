#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict
from typing import Optional, Set, Type

# pyre-fixme[21]: Could not find module `fbpcs.private_computation.entity.infra_config`.
from fbpcs.private_computation.entity.infra_config import PrivateComputationGameType

# pyre-fixme[21]: Could not find module `fbpcs.private_computation.entity.pcs_feature`.
from fbpcs.private_computation.entity.pcs_feature import PCSFeature

# pyre-fixme[21]: Could not find module
#  `fbpcs.private_computation.stage_flows.private_computation_base_stage_flow`.
from fbpcs.private_computation.stage_flows.private_computation_base_stage_flow import (
    PrivateComputationBaseStageFlow,
)

# pyre-fixme[21]: Could not find module `fbpcs.private_computation.stage_flows.privat...
from fbpcs.private_computation.stage_flows.private_computation_mr_pid_pcf2_lift_stage_flow import (
    PrivateComputationMrPidPCF2LiftStageFlow,
)

# pyre-fixme[21]: Could not find module
#  `fbpcs.private_computation.stage_flows.private_computation_mr_stage_flow`.
from fbpcs.private_computation.stage_flows.private_computation_mr_stage_flow import (
    PrivateComputationMRStageFlow,
)

# pyre-fixme[21]: Could not find module
#  `fbpcs.private_computation.stage_flows.private_computation_pcf2_lift_udp_stage_flow`.
from fbpcs.private_computation.stage_flows.private_computation_pcf2_lift_udp_stage_flow import (
    PrivateComputationPCF2LiftUDPStageFlow,
)

# pyre-fixme[21]: Could not find module
#  `fbpcs.private_computation.stage_flows.private_computation_pcf2_stage_flow`.
from fbpcs.private_computation.stage_flows.private_computation_pcf2_stage_flow import (
    PrivateComputationPCF2StageFlow,
)

# pyre-fixme[21]: Could not find module
#  `fbpcs.private_computation.stage_flows.private_computation_stage_flow`.
from fbpcs.private_computation.stage_flows.private_computation_stage_flow import (
    PrivateComputationStageFlow,
)
from fbpcs.utils.optional import unwrap_or_default


def get_stage_flow(
    # pyre-fixme[11]: Annotation `PrivateComputationGameType` is not defined as a type.
    game_type: PrivateComputationGameType,
    # pyre-fixme[11]: Annotation `PCSFeature` is not defined as a type.
    pcs_feature_enums: Set[PCSFeature],
    # pyre-fixme[11]: Annotation `PrivateComputationBaseStageFlow` is not defined as
    #  a type.
    stage_flow_cls: Optional[Type[PrivateComputationBaseStageFlow]] = None,
) -> Type[PrivateComputationBaseStageFlow]:
    selected_stage_flow_cls = unwrap_or_default(
        optional=stage_flow_cls,
        default=(
            # pyre-fixme[16]: Module `private_computation` has no attribute
            #  `stage_flows`.
            PrivateComputationPCF2StageFlow
            # pyre-fixme[16]: Module `entity` has no attribute `infra_config`.
            if game_type is PrivateComputationGameType.ATTRIBUTION
            # pyre-fixme[16]: Module `private_computation` has no attribute
            #  `stage_flows`.
            else PrivateComputationStageFlow
        ),
    )

    # warning, enabled feature gating will override stage flow, Please contact PSI team to have a similar adoption
    if PCSFeature.PRIVATE_ATTRIBUTION_MR_PID in pcs_feature_enums:
        selected_stage_flow_cls = (
            # pyre-fixme[16]: Module `private_computation` has no attribute
            #  `stage_flows`.
            PrivateComputationMRStageFlow
            # pyre-fixme[16]: Module `entity` has no attribute `infra_config`.
            if game_type is PrivateComputationGameType.ATTRIBUTION
            # pyre-fixme[16]: Module `private_computation` has no attribute
            #  `stage_flows`.
            else PrivateComputationMrPidPCF2LiftStageFlow
        )
    if PCSFeature.PRIVATE_LIFT_UNIFIED_DATA_PROCESS in pcs_feature_enums:
        selected_stage_flow_cls = (
            # pyre-fixme[16]: Module `private_computation` has no attribute
            #  `stage_flows`.
            PrivateComputationPCF2LiftUDPStageFlow
            # pyre-fixme[16]: Module `entity` has no attribute `infra_config`.
            if game_type is PrivateComputationGameType.LIFT
            else selected_stage_flow_cls
        )
    return selected_stage_flow_cls
