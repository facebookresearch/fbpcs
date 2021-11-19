#!/usr/bin/env python3
# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

from dataclasses import dataclass
from typing import Dict, Optional, Union

from fbpcs.data_processing.lift_id_combiner.lift_id_spine_combiner_cpp import (
    CppLiftIdSpineCombinerService,
)
from fbpcs.data_processing.service.id_spine_combiner import IdSpineCombinerService
from fbpcs.onedocker_binary_names import OneDockerBinaryNames
from fbpcs.private_computation.entity.private_computation_instance import (
    PrivateComputationGameType,
)
from fbpcs.private_computation.repository.private_computation_game import (
    PRIVATE_COMPUTATION_GAME_CONFIG,
)


# TODO T100288161: create super class to extend from to avoid using Union
UnionedStageServices = Union[IdSpineCombinerService, CppLiftIdSpineCombinerService]

""" This is to get a mapping from onedocker_package_name to game name
{
    "private_attribution/compute":"attribution_compute",
    "private_lift/lift":"lift",
    ...
}
"""
BINARY_NAME_TO_GAME_NAME: Dict[str, str] = {
    v["onedocker_package_name"]: k for k, v in PRIVATE_COMPUTATION_GAME_CONFIG.items()
}


@dataclass
class StageData:
    binary_name: str
    game_name: Optional[str] = None
    service: Optional[UnionedStageServices] = None


@dataclass
class PrivateComputationServiceData:
    """
    This class groups data necessary to run each stage for all supported stages
    by the service. The service needs to provide the type of game (lift, attribution, etc.)
    because each game_type requires different data to run.

    Currently, this get function is directly used by PrivateComputationService.
    We plan to implement a PrivateComputationStageService which abstracts the
    business logic of each stage so that PrivateComputationService is not bloated with it.
    PrivateComputationStageService will be calling this function in the future to
    get data from each stage.
    """

    combiner_stage: StageData
    compute_stage: StageData

    LIFT_COMBINER_STAGE_DATA: StageData = StageData(
        binary_name=OneDockerBinaryNames.LIFT_ID_SPINE_COMBINER.value,
        game_name=None,
        service=CppLiftIdSpineCombinerService(),
    )

    LIFT_COMPUTE_STAGE_DATA: StageData = StageData(
        binary_name=OneDockerBinaryNames.LIFT_COMPUTE.value,
        game_name=BINARY_NAME_TO_GAME_NAME[OneDockerBinaryNames.LIFT_COMPUTE.value],
        service=None,
    )

    ATTRIBUTION_COMBINER_STAGE_DATA: StageData = StageData(
        binary_name=OneDockerBinaryNames.ATTRIBUTION_ID_SPINE_COMBINER.value,
        game_name=None,
        service=IdSpineCombinerService(),
    )

    ATTRIBUTION_COMPUTE_STAGE_DATA: StageData = StageData(
        binary_name=OneDockerBinaryNames.ATTRIBUTION_COMPUTE.value,
        game_name=BINARY_NAME_TO_GAME_NAME[
            OneDockerBinaryNames.ATTRIBUTION_COMPUTE.value
        ],
        service=None,
    )

    DECOUPLED_ATTRIBUTION_STAGE_DATA: StageData = StageData(
        binary_name=OneDockerBinaryNames.DECOUPLED_ATTRIBUTION.value,
        game_name=BINARY_NAME_TO_GAME_NAME[
            OneDockerBinaryNames.DECOUPLED_ATTRIBUTION.value
        ],
        service=None,
    )

    DECOUPLED_AGGREGATION_STAGE_DATA: StageData = StageData(
        binary_name=OneDockerBinaryNames.DECOUPLED_AGGREGATION.value,
        game_name=BINARY_NAME_TO_GAME_NAME[
            OneDockerBinaryNames.DECOUPLED_AGGREGATION.value
        ],
        service=None,
    )

    @classmethod
    def get(
        cls, game_type: PrivateComputationGameType
    ) -> "PrivateComputationServiceData":
        if game_type is PrivateComputationGameType.LIFT:
            return cls(
                combiner_stage=PrivateComputationServiceData.LIFT_COMBINER_STAGE_DATA,
                compute_stage=PrivateComputationServiceData.LIFT_COMPUTE_STAGE_DATA,
            )
        elif game_type is PrivateComputationGameType.ATTRIBUTION:
            return cls(
                combiner_stage=PrivateComputationServiceData.ATTRIBUTION_COMBINER_STAGE_DATA,
                compute_stage=PrivateComputationServiceData.ATTRIBUTION_COMPUTE_STAGE_DATA,
            )
        else:
            raise ValueError("Unknown game type")
