#!/usr/bin/env fbpython
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict


import unittest
from unittest.mock import MagicMock

from fbpcp.entity.container_instance import ContainerInstance

from fbpcs.common.entity.stage_state_instance import StageStateInstance
from fbpcs.private_computation.entity.infra_config import InfraConfig

from fbpcs.private_computation.entity.private_computation_instance import (
    PrivateComputationInstance,
)
from fbpcs.private_computation.entity.private_computation_status import (
    PrivateComputationInstanceStatus,
)
from fbpcs.private_computation.entity.product_config import ProductConfig
from fbpcs.private_computation.stage_flows.private_computation_pcf2_lift_stage_flow import (
    PrivateComputationPCF2LiftStageFlow,
)


class TestPrivateComputationInstance(unittest.TestCase):
    def setUp(self) -> None:
        super().setUp()
        self.infra_config = MagicMock(spec=InfraConfig)
        self.product_config = MagicMock(spec=ProductConfig)
        self.instance = PrivateComputationInstance(
            infra_config=self.infra_config,
            product_config=self.product_config,
        )
        self.current_stage = PrivateComputationPCF2LiftStageFlow.PCF2_LIFT
        self.infra_config.stage_flow = self.current_stage
        self.infra_config.status = (
            PrivateComputationInstanceStatus.PCF2_LIFT_INITIALIZED
        )
        self.mock_instance = MagicMock(spec=StageStateInstance)
        self.infra_config.instances = [self.mock_instance]
        self.mock_instance.stage_name = self.current_stage.name
        self.mock_container = MagicMock(spec=ContainerInstance)
        self.mock_instance.containers = [self.mock_container]

    def test_get_existing_containers_for_retry(self) -> None:
        with self.subTest("No retries"):
            self.infra_config.retry_counter = 0
            self.assertIsNone(self.instance.get_existing_containers_for_retry())

        self.infra_config.retry_counter = 1

        with self.subTest("Previous stage name matches"):
            containers = self.instance.get_existing_containers_for_retry()
            self.assertIsNotNone(containers)
            self.assertEquals(1, len(containers))
            self.assertEquals(self.mock_container, containers[0])

        with self.subTest("Previous stage name does not match."):
            self.mock_instance.stage_name = "RESHARD"
            self.assertIsNone(self.instance.get_existing_containers_for_retry())
