#!/usr/bin/env python3
# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

import unittest
from typing import List
from unittest.mock import patch

from fbpcp.entity.mpc_game_config import MPCGameArgument
from fbpmp.private_attribution.repository.private_attribution_game import (
    PrivateAttributionGameRepository,
)


class TestPrivateattributionGameRepository(unittest.TestCase):
    @patch(
        "fbpmp.private_attribution.repository.private_attribution_game.PA_GAME_CONFIG",
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
    def setUp(self):
        self.pa_game_repository = PrivateAttributionGameRepository()

    def test_get_game(self):
        pa_game_config = self.pa_game_repository.pa_game_config
        expected_game_name = "attribution_compute_dev"
        expected_onedocker_package_name = pa_game_config[expected_game_name][
            "onedocker_package_name"
        ]
        attribution_game_config = self.pa_game_repository.get_game(expected_game_name)

        expected_arguments: List[MPCGameArgument] = [
            MPCGameArgument(name=argument["name"], required=argument["required"])
            for argument in pa_game_config[expected_game_name]["arguments"]
        ]
        self.assertEqual(attribution_game_config.game_name, expected_game_name)
        self.assertEqual(
            attribution_game_config.onedocker_package_name,
            expected_onedocker_package_name,
        )
        self.assertEqual(attribution_game_config.arguments, expected_arguments)

    def test_unsupported_game(self):
        unsupported_game_name = "unsupported game"
        with self.assertRaisesRegex(
            ValueError, f"Game {unsupported_game_name} is not supported."
        ):
            self.pa_game_repository.get_game(unsupported_game_name)
