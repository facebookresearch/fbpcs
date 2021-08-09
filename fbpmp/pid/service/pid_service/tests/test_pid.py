#!/usr/bin/env python3
# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

import unittest
from unittest.mock import MagicMock, patch

from fbpcs.service.onedocker import OneDockerService
from fbpcs.service.storage_s3 import S3StorageService
from fbpmp.pcf.tests.async_utils import to_sync
from fbpmp.pid.entity.pid_instance import PIDInstance, PIDProtocol, PIDRole
from fbpmp.pid.service.pid_service.pid import PIDService
from fbpmp.pid.service.pid_service.pid_dispatcher import PIDDispatcher


TEST_INSTANCE_ID = "123"
TEST_PROTOCOL = PIDProtocol.UNION_PID
TEST_PID_ROLE = PIDRole.PUBLISHER
TEST_PID_CONFIG = {"pid": "config"}
TEST_NUM_SHARDS = 4
TEST_INPUT_PATH = "in"
TEST_OUTPUT_PATH = "out"
TEST_DATA_PATH = "data"
TEST_SPINE_PATH = "spine"
TEST_IS_VALIDATING = False
TEST_HMAC_KEY = "CoXbp7BOEvAN9L1CB2DAORHHr3hB7wE7tpxMYm07tc0="


class TestPIDService(unittest.TestCase):
    @patch("fbpmp.onedocker_binary_config.OneDockerBinaryConfig", spec="OneDockerBinaryConfig")
    @patch("fbpcs.service.storage_s3.S3StorageService", spec=S3StorageService)
    @patch("fbpcs.service.onedocker.OneDockerService", spec=OneDockerService)
    @patch("fbpmp.pid.repository.pid_instance.PIDInstanceRepository")
    def setUp(
        self,
        mock_instance_repo,
        mock_onedocker_service,
        mock_s3_storage_service,
        mock_onedocker_binary_config,
    ):
        self.pid_service = PIDService(
            mock_onedocker_service,
            mock_s3_storage_service,
            mock_instance_repo,
            mock_onedocker_binary_config,
        )

    def test_create_instance(self):
        self.pid_service.create_instance(
            instance_id=TEST_INSTANCE_ID,
            protocol=TEST_PROTOCOL,
            pid_role=TEST_PID_ROLE,
            num_shards=TEST_NUM_SHARDS,
            input_path=TEST_INPUT_PATH,
            output_path=TEST_OUTPUT_PATH,
            data_path=TEST_DATA_PATH,
            spine_path=TEST_SPINE_PATH,
            hmac_key=TEST_HMAC_KEY,
        )
        # check that the right parameters are used when creating pid instance
        self.pid_service.instance_repository.create.assert_called()
        create_call_params = self.pid_service.instance_repository.create.call_args[0][0]
        self.assertEqual(TEST_INSTANCE_ID, create_call_params.instance_id)
        self.assertEqual(TEST_PROTOCOL, create_call_params.protocol)
        self.assertEqual(TEST_PID_ROLE, create_call_params.pid_role)
        self.assertEqual(TEST_NUM_SHARDS, create_call_params.num_shards)
        self.assertEqual(TEST_INPUT_PATH, create_call_params.input_path)
        self.assertEqual(TEST_OUTPUT_PATH, create_call_params.output_path)
        self.assertEqual(TEST_DATA_PATH, create_call_params.data_path)
        self.assertEqual(TEST_SPINE_PATH, create_call_params.spine_path)
        self.assertEqual(TEST_HMAC_KEY, create_call_params.hmac_key)

    @to_sync
    async def test_run_instance(self):
        with patch.object(PIDDispatcher, "__init__") as mock_init, patch.object(
            PIDDispatcher, "build_stages"
        ) as mock_build_stages, patch.object(PIDDispatcher, "run_all") as mock_run_all:
            # add the line below to avoid "TypeError: __init__() should return None, not 'MagicMock'""
            mock_init.return_value = None
            await self.pid_service.run_instance(
                instance_id=TEST_INSTANCE_ID,
                pid_config=TEST_PID_CONFIG,
            )
            mock_init.assert_called_once()
            init_call_params = mock_init.call_args[1]
            self.assertEqual(TEST_INSTANCE_ID, init_call_params["instance_id"])
            mock_build_stages.assert_called_once()
            mock_run_all.assert_called_once()

    def test_get_instance(self):
        sample_pid_instance = self._get_sample_pid_instance()
        self.pid_service.instance_repository.read = MagicMock(
            return_value=sample_pid_instance
        )

        self.assertEqual(
            sample_pid_instance, self.pid_service.get_instance(TEST_INSTANCE_ID)
        )

    def _get_sample_pid_instance(self) -> PIDInstance:
        return PIDInstance(
            instance_id=TEST_INSTANCE_ID,
            protocol=TEST_PROTOCOL,
            pid_role=TEST_PID_ROLE,
            num_shards=TEST_NUM_SHARDS,
            is_validating=TEST_IS_VALIDATING,
            input_path=TEST_INPUT_PATH,
            output_path=TEST_OUTPUT_PATH,
            data_path=TEST_DATA_PATH,
            spine_path=TEST_SPINE_PATH,
        )
