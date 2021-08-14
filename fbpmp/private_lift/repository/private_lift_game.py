#!/usr/bin/env python3
# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

from typing import List

from fbpcp.entity.mpc_game_config import MPCGameArgument, MPCGameConfig
from fbpcp.repository.mpc_game_repository import MPCGameRepository
from fbpmp.onedocker_binary_names import OneDockerBinaryNames


PL_GAME_CONFIG = {
    "lift": {
        "onedocker_package_name": OneDockerBinaryNames.LIFT_COMPUTE.value,
        "arguments": [
            {"name": "input_base_path", "required": True},
            {"name": "output_base_path", "required": True},
            {"name": "file_start_index", "required": False},
            {"name": "num_files", "required": True},
            {"name": "concurrency", "required": True},
        ],
    },
    "shard_aggregator": {
        "onedocker_package_name": OneDockerBinaryNames.SHARD_AGGREGATOR.value,
        "arguments": [
            {"name": "input_base_path", "required": True},
            {"name": "num_shards", "required": True},
            {"name": "output_path", "required": True},
            {"name": "metrics_format_type", "required": True},
            {"name": "first_shard_index", "required": False},
        ],
    },
}


class PrivateLiftGameRepository(MPCGameRepository):
    def __init__(self) -> None:
        self.pl_game_config = PL_GAME_CONFIG

    def get_game(self, name: str) -> MPCGameConfig:
        if name not in self.pl_game_config:
            raise ValueError(f"Game {name} is not supported.")

        game_config = self.pl_game_config[name]
        arguments: List[MPCGameArgument] = [
            MPCGameArgument(name=argument["name"], required=argument["required"])
            for argument in game_config["arguments"]
        ]

        return MPCGameConfig(
            game_name=name,
            onedocker_package_name=game_config["onedocker_package_name"],
            arguments=arguments,
        )
