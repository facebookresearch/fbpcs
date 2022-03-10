#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

import unittest

from fbpcp.entity.container_instance import ContainerInstance, ContainerInstanceStatus
from fbpcs.common.entity.stage_state_instance import (
    StageStateInstance,
    StageStateInstanceStatus,
)


class TestStageStateInstance(unittest.TestCase):
    def setUp(self):
        self.stage_state_instance = StageStateInstance(
            instance_id="stage_state_instance",
            stage_name="test_stage",
            status=StageStateInstanceStatus.COMPLETED,
            containers=[
                ContainerInstance(
                    instance_id="test_container_instance_1",
                    ip_address="192.0.2.4",
                    status=ContainerInstanceStatus.COMPLETED,
                ),
                ContainerInstance(
                    instance_id="test_container_instance_2",
                    ip_address="192.0.2.5",
                    status=ContainerInstanceStatus.COMPLETED,
                ),
            ],
            start_time=1646642432,
            end_time=1646642432 + 5,
        )

    def test_server_ips(self) -> None:
        self.assertEqual(len(self.stage_state_instance.containers), 2)
        self.assertEqual(
            self.stage_state_instance.server_ips, ["192.0.2.4", "192.0.2.5"]
        )

    def test_elapsed_time(self) -> None:
        self.assertEqual(self.stage_state_instance.elapsed_time, 5)
