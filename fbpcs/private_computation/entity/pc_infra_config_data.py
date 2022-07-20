# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

from dataclasses import dataclass
from enum import Enum
from typing import Set


@dataclass(frozen=True)
class PrivateComputationInfraConfigData:
    cls_name: str
    args: Set[str]


class PrivateComputationInfraConfigInfo(Enum):

    CONTAINER_SERVICE = PrivateComputationInfraConfigData(
        "fbpcp.service.container_aws.AWSContainerService",
        {"region", "cluster", "subnets"},
    )

    PC_INSTANCE_REPO = PrivateComputationInfraConfigData(
        "fbpcs.private_computation.repository.private_computation_instance_local.LocalPrivateComputationInstanceRepository",
        {"base_dir"},
    )

    STORAGE_SERVICE = PrivateComputationInfraConfigData(
        "fbpcp.service.storage_s3.S3StorageService",
        {"region"},
    )

    PC_VALIDATOR_CONFIG = PrivateComputationInfraConfigData(
        "fbpcs.private_computation.entity.pc_validator_config.PCValidatorConfig",
        {"region"},
    )

    MPC_GAME_SERVICE = PrivateComputationInfraConfigData(
        "fbpcp.service.mpc_game.MPCGameService",
        set(),
    )

    PC_GAME_REPO = PrivateComputationInfraConfigData(
        "fbpcs.private_computation.repository.private_computation_game.PrivateComputationGameRepository",
        set(),
    )

    MPC_INSTANCE_REPO = PrivateComputationInfraConfigData(
        "fbpcs.common.repository.mpc_instance_local.LocalMPCInstanceRepository",
        {"base_dir"},
    )
