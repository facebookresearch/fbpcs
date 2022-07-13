#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

import unittest
from unittest.mock import MagicMock, patch

from fbpcp.entity.container_instance import ContainerInstance, ContainerInstanceStatus
from fbpcs.common.entity.stage_state_instance import (
    StageStateInstance,
    StageStateInstanceStatus,
)


class TestStageStateInstance(unittest.TestCase):
    def setUp(self) -> None:
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
            creation_ts=1646642432,
            end_ts=1646642432 + 5,
        )

    def test_server_ips(self) -> None:
        self.assertEqual(len(self.stage_state_instance.containers), 2)
        self.assertEqual(
            self.stage_state_instance.server_ips, ["192.0.2.4", "192.0.2.5"]
        )

    def test_elapsed_time(self) -> None:
        self.assertEqual(self.stage_state_instance.elapsed_time, 5)

    @patch("fbpcp.service.onedocker.OneDockerService")
    def test_stop_containers(self, mock_onedocker_svc) -> None:
        for container_stoppable in (True, False):
            with self.subTest(
                "Subtest with container_stoppable: {container_stoppable}",
                container_stoppable=container_stoppable,
            ):

                mock_onedocker_svc.reset_mock()
                if container_stoppable:
                    mock_onedocker_svc.stop_containers = MagicMock(
                        return_value=[None, None]
                    )
                    self.stage_state_instance.stop_containers(mock_onedocker_svc)
                else:
                    mock_onedocker_svc.stop_containers = MagicMock(
                        return_value=[None, "Oops"]
                    )
                    with self.assertRaises(RuntimeError):
                        self.stage_state_instance.stop_containers(mock_onedocker_svc)

                mock_onedocker_svc.stop_containers.assert_called_with(
                    ["test_container_instance_1", "test_container_instance_2"]
                )
