#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe

import copy
import unittest
from pathlib import Path
from unittest.mock import MagicMock, mock_open, patch

from fbpcs.common.entity.stage_state_instance import StageStateInstance
from fbpcs.common.repository.instance_local import LocalInstanceRepository

TEST_BASE_DIR = Path("./")
TEST_INSTANCE_ID = "test-instance-id"
ERROR_MSG_ALREADY_EXISTS = f"{TEST_INSTANCE_ID} already exists"
ERROR_MSG_NOT_EXISTS = f"{TEST_INSTANCE_ID} does not exist"


class TestLocalInstanceRepository(unittest.TestCase):
    def setUp(self):
        self.instance = StageStateInstance(
            instance_id=TEST_INSTANCE_ID, stage_name="compute"
        )
        self.local_instance_repo = LocalInstanceRepository(TEST_BASE_DIR)

    def test_create_existing_instance(self):
        self.local_instance_repo._exist = MagicMock(return_value=True)
        with self.assertRaisesRegex(RuntimeError, ERROR_MSG_ALREADY_EXISTS):
            self.local_instance_repo.create(self.instance)

    @patch("builtins.open")
    def test_create_non_existing_instance(self, mock_open):
        self.local_instance_repo._exist = MagicMock(return_value=False)
        path = TEST_BASE_DIR.joinpath(TEST_INSTANCE_ID)
        self.assertIsNone(self.local_instance_repo.create(self.instance))
        mock_open.assert_called_with(path, "w")

    def test_read_non_existing_instance(self):
        self.local_instance_repo._exist = MagicMock(return_value=False)
        with self.assertRaisesRegex(RuntimeError, ERROR_MSG_NOT_EXISTS):
            self.local_instance_repo.read(TEST_INSTANCE_ID)

    def test_read_existing_instance(self):
        self.local_instance_repo._exist = MagicMock(return_value=True)
        data = self.instance.dumps_schema()
        path = TEST_BASE_DIR.joinpath(TEST_INSTANCE_ID)
        with patch("builtins.open", mock_open(read_data=data)) as mock_file:
            self.assertEqual(open(path).read().strip(), data)
            instance = StageStateInstance.loads_schema(
                self.local_instance_repo.read(TEST_INSTANCE_ID)
            )
            self.assertEqual(self.instance, instance)
            mock_file.assert_called_with(path, "r")

    def test_update_non_existing_instance(self):
        self.local_instance_repo._exist = MagicMock(return_value=False)
        with self.assertRaisesRegex(RuntimeError, ERROR_MSG_NOT_EXISTS):
            self.local_instance_repo.update(self.instance)

    @patch("builtins.open")
    def test_update_existing_instance(self, mock_open):
        self.local_instance_repo._exist = MagicMock(return_value=True)
        new_instance = copy.deepcopy(self.instance)
        new_instance.stage_name = "aggregate"
        path = TEST_BASE_DIR.joinpath(TEST_INSTANCE_ID)
        self.assertIsNone(self.local_instance_repo.update(new_instance))
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
