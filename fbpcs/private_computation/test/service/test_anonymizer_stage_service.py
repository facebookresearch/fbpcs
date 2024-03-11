#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe

from collections import defaultdict

from unittest import IsolatedAsyncioTestCase
from unittest.mock import Mock, patch

from fbpcs.onedocker_binary_config import OneDockerBinaryConfig
from fbpcs.private_computation.service.anonymizer_stage_service import (
    AnonymizerStageService,
)


class TestAnonymizerStageService(IsolatedAsyncioTestCase):
    @patch("fbpcp.service.onedocker.OneDockerService")
    def setUp(self, onedocker_service: Mock) -> None:
        onedocker_binary_config_map = defaultdict(
            lambda: OneDockerBinaryConfig(
                tmp_directory="/test_tmp_directory/",
                binary_version="latest",
                repository_path="test_path/",
            )
        )
        self.stage_svc = AnonymizerStageService(
            onedocker_service, onedocker_binary_config_map
        )

    async def test_anonymizer(self) -> None:
        # Once the AnonymizerStageService is implemented, this test case
        # can be implemented
        pass
