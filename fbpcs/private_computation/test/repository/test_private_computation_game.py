#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

import unittest
from typing import List
from unittest.mock import patch

from fbpcp.entity.mpc_game_config import MPCGameArgument
from fbpcs.private_computation.repository.private_computation_game import (
    PrivateComputationGameRepository,
)


class TestPrivateComputationGameRepository(unittest.TestCase):
    @patch(
        "fbpcs.private_computation.repository.private_computation_game.PRIVATE_COMPUTATION_GAME_CONFIG",
        {
            "attribution_compute_dev": {
                "onedocker_package_name": "private_attribution/compute-dev",
                "arguments": [
                    {"name": "aggregators", "required": True},
                    {"name": "input_path", "required": True},
                    {"name": "output_path", "required": True},
                    {"name": "attribution_rules", "required": True},
                ],
            },
        },
    )
    def setUp(self) -> None:
        self.game_repository = PrivateComputationGameRepository()

    def test_get_game(self) -> None:
        game_config = self.game_repository.private_computation_game_config
        expected_game_name = "attribution_compute_dev"
        expected_onedocker_package_name = game_config[expected_game_name][
            "onedocker_package_name"
        ]
        attribution_game_config = self.game_repository.get_game(expected_game_name)

        expected_arguments: List[MPCGameArgument] = [
            MPCGameArgument(name=argument["name"], required=argument["required"])
            for argument in game_config[expected_game_name]["arguments"]
        ]
        self.assertEqual(attribution_game_config.game_name, expected_game_name)
        self.assertEqual(
            attribution_game_config.onedocker_package_name,
            expected_onedocker_package_name,
        )
        self.assertEqual(attribution_game_config.arguments, expected_arguments)

    def test_unsupported_game(self) -> None:
        unsupported_game_name = "unsupported game"
        with self.assertRaisesRegex(
            ValueError, f"Game {unsupported_game_name} is not supported."
        ):
            self.game_repository.get_game(unsupported_game_name)
