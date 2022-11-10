#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

import unittest
from typing import List
from unittest.mock import MagicMock, Mock

from fbpcp.entity.mpc_game_config import MPCGameArgument, MPCGameConfig
from fbpcp.entity.mpc_instance import MPCParty
from fbpcs.private_computation.service.mpc.mpc_game import MPCGameService


INPUT_DIRECTORY = "input_directory"
OUTPUT_DIRECTORY = "output_directory"
INPUT_PATH_1 = "input_path_1"
INPUT_PATH_2 = "input_path_2"
OUTPUT_PATH_1 = "out_path_1"
OUTPUT_PATH_2 = "out_path_2"
CONCURRENCY = 2
GAME_NAME = "test_game"
ONEDOCKER_PACKAGE_NAME = "onedocker_package/package_name"

GAME_CONFIG = {
    GAME_NAME: {
        "onedocker_package_name": ONEDOCKER_PACKAGE_NAME,
        "arguments": [
            {"name": "input_filenames", "required": True},
            {"name": "input_directory", "required": True},
            {"name": "output_filenames", "required": True},
            {"name": "output_directory", "required": True},
            {"name": "concurrency", "required": True},
        ],
    }
}


class TestMPCGameService(unittest.TestCase):
    def setUp(self):
        game_repository = Mock()
        self.mpc_game_svc = MPCGameService(game_repository)
        arguments: List[MPCGameArgument] = [
            MPCGameArgument(name=argument["name"], required=argument["required"])
            for argument in GAME_CONFIG[GAME_NAME]["arguments"]
        ]
        self.mpc_game_config = MPCGameConfig(
            game_name=GAME_NAME,
            onedocker_package_name=GAME_CONFIG[GAME_NAME]["onedocker_package_name"],
            arguments=arguments,
        )
        self.mpc_game_svc.mpc_game_repository.get_game = MagicMock(
            return_value=self.mpc_game_config
        )

    def test_prepare_args(self):
        self.assertEqual(
            self.mpc_game_svc._prepare_args(
                mpc_game_config=self.mpc_game_config,
                mpc_party=MPCParty.CLIENT,
                server_ip="192.0.2.0",
                port=12345,
                input_filenames=INPUT_PATH_1,
                input_directory=INPUT_DIRECTORY,
                output_filenames=OUTPUT_PATH_1,
                output_directory=OUTPUT_DIRECTORY,
                concurrency=CONCURRENCY,
            ),
            {
                "party": 2,
                "server_ip": "192.0.2.0",
                "port": 12345,
                "input_filenames": INPUT_PATH_1,
                "input_directory": INPUT_DIRECTORY,
                "output_filenames": OUTPUT_PATH_1,
                "output_directory": OUTPUT_DIRECTORY,
                "concurrency": CONCURRENCY,
            },
        )

    def test_prepare_args_with_optional_arg(self):
        self.assertEqual(
            self.mpc_game_svc._prepare_args(
                mpc_game_config=self.mpc_game_config,
                mpc_party=MPCParty.SERVER,
                input_filenames=INPUT_PATH_1,
                input_directory=INPUT_DIRECTORY,
                output_filenames=OUTPUT_PATH_1,
                output_directory=OUTPUT_DIRECTORY,
                concurrency=CONCURRENCY,
            ),
            {
                "party": 1,
                "input_filenames": INPUT_PATH_1,
                "input_directory": INPUT_DIRECTORY,
                "output_filenames": OUTPUT_PATH_1,
                "output_directory": OUTPUT_DIRECTORY,
                "concurrency": CONCURRENCY,
            },
        )

    def test_prepare_args_with_missing_arg(self):
        with self.assertRaisesRegex(
            ValueError, "Missing required argument input_filenames!"
        ):
            self.mpc_game_svc._prepare_args(
                mpc_game_config=self.mpc_game_config,
                mpc_party=MPCParty.SERVER,
                output_file=OUTPUT_PATH_1,
            )

    def test_build_cmd(self):
        self.assertEqual(
            self.mpc_game_svc._build_cmd(
                mpc_game_config=self.mpc_game_config,
                mpc_party=MPCParty.CLIENT,
                server_ip="192.0.2.0",
                port=12345,
                input_filenames=INPUT_PATH_1,
                input_directory=INPUT_DIRECTORY,
                output_filenames=OUTPUT_PATH_1,
                output_directory=OUTPUT_DIRECTORY,
                concurrency=CONCURRENCY,
            ),
            f"--party=2 --server_ip=192.0.2.0 --port=12345 --input_filenames={INPUT_PATH_1} "
            f"--input_directory={INPUT_DIRECTORY} --output_filenames={OUTPUT_PATH_1} "
            f"--output_directory={OUTPUT_DIRECTORY} --concurrency={CONCURRENCY}",
        )

    def test_build_onedocker_args(self):
        game_args = [
            {
                "input_filenames": INPUT_PATH_1,
                "input_directory": INPUT_DIRECTORY,
                "output_filenames": OUTPUT_PATH_1,
                "output_directory": OUTPUT_DIRECTORY,
                "concurrency": CONCURRENCY,
            },
            {
                "input_filenames": INPUT_PATH_2,
                "input_directory": INPUT_DIRECTORY,
                "output_filenames": OUTPUT_PATH_2,
                "output_directory": OUTPUT_DIRECTORY,
                "concurrency": CONCURRENCY,
            },
        ]

        expected_arguments = [
            (
                ONEDOCKER_PACKAGE_NAME,
                f"--party=1 --input_filenames={INPUT_PATH_1} --input_directory={INPUT_DIRECTORY} "
                f"--output_filenames={OUTPUT_PATH_1} --output_directory={OUTPUT_DIRECTORY} "
                f"--concurrency={CONCURRENCY}",
            ),
            (
                ONEDOCKER_PACKAGE_NAME,
                f"--party=1 --input_filenames={INPUT_PATH_2} --input_directory={INPUT_DIRECTORY} "
                f"--output_filenames={OUTPUT_PATH_2} --output_directory={OUTPUT_DIRECTORY} "
                f"--concurrency={CONCURRENCY}",
            ),
        ]

        for i in range(len(game_args)):
            expected_argument = expected_arguments[i]
            game_arg = game_args[i]
            self.assertEqual(
                self.mpc_game_svc.build_onedocker_args(
                    game_name="game",
                    mpc_party=MPCParty.SERVER,
                    **game_arg,
                ),
                expected_argument,
            )
