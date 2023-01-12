#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

from unittest import IsolatedAsyncioTestCase
from unittest.mock import MagicMock, patch

from fbpcs.private_computation.service.mpc.entity.mpc_instance import MPCParty
from fbpcs.private_computation.service.mpc.mpc import MPCService


TEST_GAME_NAME = "lift"
TEST_MPC_ROLE = MPCParty.SERVER
TEST_INPUT_ARGS = "test_input_file"
TEST_OUTPUT_ARGS = "test_output_file"
TEST_CONCURRENCY_ARGS = 1
TEST_INPUT_DIRECTORY = "TEST_INPUT_DIRECTORY/"
TEST_OUTPUT_DIRECTORY = "TEST_OUTPUT_DIRECTORY/"
GAME_ARGS = [
    {
        "input_filenames": TEST_INPUT_ARGS,
        "input_directory": TEST_INPUT_DIRECTORY,
        "output_filenames": TEST_OUTPUT_ARGS,
        "output_directory": TEST_OUTPUT_DIRECTORY,
        "concurrency": TEST_CONCURRENCY_ARGS,
    }
]


class TestMPCService(IsolatedAsyncioTestCase):
    def setUp(self):
        cspatcher = patch("fbpcp.service.container.ContainerService")
        irpatcher = patch(
            "fbpcs.private_computation.service.mpc.repository.mpc_instance.MPCInstanceRepository"
        )
        gspatcher = patch(
            "fbpcs.private_computation.service.mpc.mpc_game.MPCGameService"
        )
        container_svc = cspatcher.start()
        instance_repository = irpatcher.start()
        mpc_game_svc = gspatcher.start()
        for patcher in (cspatcher, irpatcher, gspatcher):
            self.addCleanup(patcher.stop)
        self.mpc_service = MPCService(
            container_svc,
            instance_repository,
            "test_task_definition",
            mpc_game_svc,
        )

    def test_convert_cmd_args_list(self) -> None:
        # Prep
        built_onedocker_args = ("private_lift/lift", "test one docker arguments")
        self.mpc_service.mpc_game_svc.build_onedocker_args = MagicMock(
            return_value=built_onedocker_args
        )
        # Ack
        binary_name, cmd_args_list = self.mpc_service.convert_cmd_args_list(
            game_name=TEST_GAME_NAME,
            game_args=GAME_ARGS,
            mpc_party=TEST_MPC_ROLE,
        )
        # Asserts
        self.assertEqual(binary_name, built_onedocker_args[0])
        self.assertEqual(cmd_args_list, [built_onedocker_args[1]])
        self.mpc_service.mpc_game_svc.build_onedocker_args.assert_called_once_with(
            game_name=TEST_GAME_NAME,
            mpc_party=TEST_MPC_ROLE,
            server_ip=None,
            input_filenames=TEST_INPUT_ARGS,
            input_directory=TEST_INPUT_DIRECTORY,
            output_filenames=TEST_OUTPUT_ARGS,
            output_directory=TEST_OUTPUT_DIRECTORY,
            concurrency=TEST_CONCURRENCY_ARGS,
        )
