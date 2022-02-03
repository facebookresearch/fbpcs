#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

import copy
import unittest
from pathlib import Path
from unittest.mock import mock_open, MagicMock, patch

from fbpcp.entity.mpc_instance import MPCInstanceStatus, MPCParty
from fbpcs.common.entity.pcs_mpc_instance import PCSMPCInstance
from fbpcs.common.repository.instance_local import LocalInstanceRepository

TEST_BASE_DIR = Path("./")
TEST_INSTANCE_ID = "test-instance-id"
TEST_GAME_NAME = "lift"
TEST_MPC_PARTY = MPCParty.SERVER
TEST_NUM_WORKERS = 1
TEST_SERVER_IPS = ["192.0.2.0"]
TEST_GAME_ARGS = [{}]
ERROR_MSG_ALREADY_EXISTS = f"{TEST_INSTANCE_ID} already exists"
ERROR_MSG_NOT_EXISTS = f"{TEST_INSTANCE_ID} does not exist"


class TestLocalInstanceRepository(unittest.TestCase):
    def setUp(self):
        self.mpc_instance = PCSMPCInstance.create_instance(
            instance_id=TEST_INSTANCE_ID,
            game_name=TEST_GAME_NAME,
            mpc_party=TEST_MPC_PARTY,
            num_workers=TEST_NUM_WORKERS,
            server_ips=TEST_SERVER_IPS,
            status=MPCInstanceStatus.CREATED,
            game_args=TEST_GAME_ARGS,
        )
        self.local_instance_repo = LocalInstanceRepository(TEST_BASE_DIR)

    def test_create_existing_instance(self):
        self.local_instance_repo._exist = MagicMock(return_value=True)
        with self.assertRaisesRegex(RuntimeError, ERROR_MSG_ALREADY_EXISTS):
            self.local_instance_repo.create(self.mpc_instance)

    @patch("builtins.open")
    def test_create_non_existing_instance(self, mock_open):
        self.local_instance_repo._exist = MagicMock(return_value=False)
        path = TEST_BASE_DIR.joinpath(TEST_INSTANCE_ID)
        self.assertIsNone(self.local_instance_repo.create(self.mpc_instance))
        mock_open.assert_called_with(path, "w")

    def test_read_non_existing_instance(self):
        self.local_instance_repo._exist = MagicMock(return_value=False)
        with self.assertRaisesRegex(RuntimeError, ERROR_MSG_NOT_EXISTS):
            self.local_instance_repo.read(TEST_INSTANCE_ID)

    def test_read_existing_instance(self):
        self.local_instance_repo._exist = MagicMock(return_value=True)
        data = self.mpc_instance.dumps_schema()
        path = TEST_BASE_DIR.joinpath(TEST_INSTANCE_ID)
        with patch("builtins.open", mock_open(read_data=data)) as mock_file:
            self.assertEqual(open(path).read().strip(), data)
            mpc_instance = PCSMPCInstance.loads_schema(
                self.local_instance_repo.read(TEST_INSTANCE_ID)
            )
            self.assertEqual(self.mpc_instance, mpc_instance)
            mock_file.assert_called_with(path, "r")

    def test_update_non_existing_instance(self):
        self.local_instance_repo._exist = MagicMock(return_value=False)
        with self.assertRaisesRegex(RuntimeError, ERROR_MSG_NOT_EXISTS):
            self.local_instance_repo.update(self.mpc_instance)

    @patch("builtins.open")
    def test_update_existing_instance(self, mock_open):
        self.local_instance_repo._exist = MagicMock(return_value=True)
        new_mpc_instance = copy.deepcopy(self.mpc_instance)
        new_mpc_instance.game_name = "aggregator"
        path = TEST_BASE_DIR.joinpath(TEST_INSTANCE_ID)
        self.assertIsNone(self.local_instance_repo.update(new_mpc_instance))
        mock_open.assert_called_with(path, "w")

    def test_exists(self):
        self.assertFalse(self.local_instance_repo._exist(TEST_INSTANCE_ID))
        with patch.object(Path, "exists"):
            self.assertTrue(self.local_instance_repo._exist(TEST_INSTANCE_ID))

    def test_delete_non_existing_instance(self):
        self.local_instance_repo._exist = MagicMock(return_value=False)
        with self.assertRaisesRegex(RuntimeError, ERROR_MSG_NOT_EXISTS):
            self.local_instance_repo.delete(TEST_INSTANCE_ID)

    def test_delete_existing_instance(self):
        with patch.object(Path, "joinpath") as mock_join:
            with patch.object(Path, "unlink") as mock_unlink:
                mock_unlink.return_value = None
                self.assertIsNone(self.local_instance_repo.delete(TEST_INSTANCE_ID))
                mock_join.assert_called_with(TEST_INSTANCE_ID)
