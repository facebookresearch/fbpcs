#!/usr/bin/env python3
# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

import unittest
import uuid
from unittest.mock import MagicMock

from fbpcp.entity.mpc_instance import MPCInstance, MPCInstanceStatus, MPCParty
from fbpcp.service.storage_s3 import S3StorageService
from fbpmp.common.repository.instance_s3 import S3InstanceRepository


class TestS3InstanceRepository(unittest.TestCase):
    TEST_BASE_DIR = "./"
    TEST_INSTANCE_ID = str(uuid.uuid4())
    TEST_GAME_NAME = "lift"
    TEST_MPC_PARTY = MPCParty.SERVER
    TEST_NUM_WORKERS = 1
    TEST_SERVER_IPS = ["192.0.2.0", "192.0.2.1"]
    TEST_GAME_ARGS = [{}]
    ERROR_MSG_ALREADY_EXISTS = f"{TEST_INSTANCE_ID} already exists"
    ERROR_MSG_NOT_EXISTS = f"{TEST_INSTANCE_ID} does not exist"

    def setUp(self):
        storage_svc = S3StorageService("us-west-1")
        self.s3_storage_repo = S3InstanceRepository(storage_svc, self.TEST_BASE_DIR)
        self.mpc_instance = MPCInstance.create_instance(
            instance_id=self.TEST_INSTANCE_ID,
            game_name=self.TEST_GAME_NAME,
            mpc_party=self.TEST_MPC_PARTY,
            num_workers=self.TEST_NUM_WORKERS,
            server_ips=self.TEST_SERVER_IPS,
            status=MPCInstanceStatus.CREATED,
            game_args=self.TEST_GAME_ARGS,
        )

    def test_create_non_existing_instance(self):
        self.s3_storage_repo._exist = MagicMock(return_value=False)
        self.s3_storage_repo.s3_storage_svc.write = MagicMock(return_value=None)
        self.s3_storage_repo.create(self.mpc_instance)
        self.s3_storage_repo.s3_storage_svc.write.assert_called()

    def test_create_existing_instance(self):
        self.s3_storage_repo._exist = MagicMock(
            side_effect=RuntimeError(self.ERROR_MSG_ALREADY_EXISTS)
        )
        with self.assertRaisesRegex(RuntimeError, self.ERROR_MSG_ALREADY_EXISTS):
            self.s3_storage_repo.create(self.mpc_instance)

    def test_read_non_existing_instance(self):
        self.s3_storage_repo._exist = MagicMock(
            side_effect=RuntimeError(self.ERROR_MSG_NOT_EXISTS)
        )
        with self.assertRaisesRegex(RuntimeError, self.ERROR_MSG_NOT_EXISTS):
            self.s3_storage_repo.read(self.TEST_INSTANCE_ID)

    def test_read_existing_instance(self):
        self.s3_storage_repo._exist = MagicMock(return_value=True)
        self.s3_storage_repo.s3_storage_svc.read = MagicMock(
            return_value=self.mpc_instance.dumps_schema()
        )
        instance = MPCInstance.loads_schema(
            self.s3_storage_repo.read(self.mpc_instance)
        )
        self.assertEqual(self.mpc_instance, instance)

    def test_update_non_existing_instance(self):
        self.s3_storage_repo._exist = MagicMock(
            side_effect=RuntimeError(self.ERROR_MSG_NOT_EXISTS)
        )
        with self.assertRaisesRegex(RuntimeError, self.ERROR_MSG_NOT_EXISTS):
            self.s3_storage_repo.update(self.mpc_instance)

    def test_update_existing_instance(self):
        self.s3_storage_repo._exist = MagicMock(return_value=True)
        self.s3_storage_repo.s3_storage_svc.write = MagicMock(return_value=None)
        self.s3_storage_repo.update(self.mpc_instance)
        self.s3_storage_repo.s3_storage_svc.write.assert_called()

    def test_delete_non_existing_instance(self):
        self.s3_storage_repo._exist = MagicMock(
            side_effect=RuntimeError(self.ERROR_MSG_NOT_EXISTS)
        )
        with self.assertRaisesRegex(RuntimeError, self.ERROR_MSG_NOT_EXISTS):
            self.s3_storage_repo.delete(self.TEST_INSTANCE_ID)

    def test_delete_existing_instance(self):
        self.s3_storage_repo._exist = MagicMock(return_value=True)
        self.s3_storage_repo.s3_storage_svc.delete = MagicMock(return_value=None)
        self.s3_storage_repo.delete(self.TEST_INSTANCE_ID)
        self.s3_storage_repo.s3_storage_svc.delete.assert_called()

    def test_exists(self):
        self.s3_storage_repo.s3_storage_svc.file_exists = MagicMock(return_value=True)
        self.assertTrue(self.s3_storage_repo._exist(self.TEST_INSTANCE_ID))
