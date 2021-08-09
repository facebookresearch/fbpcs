#!/usr/bin/env python3
# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

import unittest
from typing import List
from unittest.mock import patch

from fbpcs.entity.mpc_game_config import MPCGameArgument
from fbpmp.private_lift.repository.private_lift_game import PrivateLiftGameRepository


class TestPrivateLiftGameRepository(unittest.TestCase):
    @patch(
        "fbpmp.private_lift.repository.private_lift_game.PL_GAME_CONFIG",
        {
            "test_game": {
                "onedocker_package_name": "test_onedocker_package_name",
                "arguments": [
                    {"name": "input_file", "required": True},
                    {"name": "output_file", "required": False},
                ],
            }
        },
    )
    def setUp(self):
        self.pl_game_repository = PrivateLiftGameRepository()

    def test_get_game(self):
        pl_game_config = self.pl_game_repository.pl_game_config
        expected_game_name = "test_game"
        expected_onedocker_package_name = pl_game_config[expected_game_name][
            "onedocker_package_name"
        ]
        lift_game_config = self.pl_game_repository.get_game(expected_game_name)

        expected_arguments: List[MPCGameArgument] = [
            MPCGameArgument(name=argument["name"], required=argument["required"])
            for argument in pl_game_config[expected_game_name]["arguments"]
        ]
        self.assertEqual(lift_game_config.game_name, expected_game_name)
        self.assertEqual(
            lift_game_config.onedocker_package_name, expected_onedocker_package_name
        )
        self.assertEqual(lift_game_config.arguments, expected_arguments)

    def test_unsupported_game(self):
        unsupported_game_name = "unsupported game"
        with self.assertRaisesRegex(
            ValueError, f"Game {unsupported_game_name} is not supported."
        ):
            self.pl_game_repository.get_game(unsupported_game_name)
