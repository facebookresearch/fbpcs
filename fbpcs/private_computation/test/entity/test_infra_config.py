# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe

import unittest
from unittest.mock import MagicMock, patch

from fbpcs.private_computation.entity.infra_config import (
    InfraConfig,
    post_update_status,
    PrivateComputationGameType,
    PrivateComputationRole,
    StatusUpdate,
)
from fbpcs.private_computation.entity.pcs_feature import PCSFeature
from fbpcs.private_computation.entity.private_computation_instance import (
    PrivateComputationInstanceStatus,
)


class TestInfraConfig(unittest.TestCase):
    def test_stage_flow(self) -> None:
        pass

    def test_is_stage_flow_completed(self) -> None:
        pass

    def test_is_tls_enabled_when_flag_missing(self):
        # Arrange
        config = self._create_config(PrivateComputationGameType.LIFT, set())

        # Act
        is_tls_enabled = config.is_tls_enabled

        # Assert
        self.assertFalse(is_tls_enabled)

    def test_is_tls_enabled_when_not_supported(self):
        # Arrange
        config = self._create_config(
            PrivateComputationGameType.ATTRIBUTION, {PCSFeature.PCF_TLS}
        )

        # Act
        is_tls_enabled = config.is_tls_enabled

        # Assert
        self.assertFalse(is_tls_enabled)

    @patch("fbpcs.private_computation.entity.infra_config.InfraConfig")
    def test_is_tls_enabled_when_supported_and_flag_present(self, mock_infra_config):
        # Arrange
        config = self._create_config(
            PrivateComputationGameType.LIFT, {PCSFeature.PCF_TLS}
        )

        # Act
        is_tls_enabled = config.is_tls_enabled

        # Assert
        self.assertTrue(is_tls_enabled)

    def _create_config(self, game_type, pcs_features):
        return InfraConfig(
            instance_id="test-instance-id",
            role=PrivateComputationRole.PARTNER,
            status=PrivateComputationInstanceStatus.CREATED,
            status_update_ts=1,
            instances=[],
            game_type=game_type,
            num_pid_containers=1,
            num_mpc_containers=1,
            num_files_per_mpc_container=1,
            status_updates=[],
            pcs_features=pcs_features,
        )


class TestInfraConfigFreeFunctions(unittest.TestCase):
    @patch("time.time", return_value=444)
    def test_post_update_status_stage_flow_incomplete(
        self, mock_time: MagicMock
    ) -> None:
        # Arrange
        config = InfraConfig(
            instance_id="test_instance_123",
            role=PrivateComputationRole.PARTNER,
            status=PrivateComputationInstanceStatus.ID_MATCHING_STARTED,
            status_update_ts=123,
            instances=[],
            game_type=PrivateComputationGameType.ATTRIBUTION,
            num_pid_containers=10,
            num_mpc_containers=20,
            num_files_per_mpc_container=100,
            status_updates=[],
        )
        original_end_ts = config.end_ts
        expected_status_updates = [
            StatusUpdate(
                status=config.status, status_update_ts=444, status_update_ts_delta=0
            )
        ]

        # Act
        post_update_status(config)

        # Assert
        self.assertEqual(expected_status_updates, config.status_updates)
        # Check that the end_ts wasn't updated
        self.assertEqual(original_end_ts, config.end_ts)

    @patch("time.time", return_value=555)
    def test_post_update_status_stage_flow_complete(self, mock_time: MagicMock) -> None:
        # Arrange
        config = InfraConfig(
            instance_id="test_instance_123",
            role=PrivateComputationRole.PARTNER,
            status=PrivateComputationInstanceStatus.POST_PROCESSING_HANDLERS_COMPLETED,
            status_update_ts=123,
            instances=[],
            game_type=PrivateComputationGameType.ATTRIBUTION,
            num_pid_containers=10,
            num_mpc_containers=20,
            num_files_per_mpc_container=100,
            status_updates=[],
        )
        expected_status_updates = [
            StatusUpdate(
                status=config.status, status_update_ts=555, status_update_ts_delta=0
            )
        ]

        # Act
        post_update_status(config)

        # Assert
        self.assertEqual(expected_status_updates, config.status_updates)
        # Check that the end_ts was updated
        self.assertEqual(555, config.end_ts)

    def test_append_status_updates(self) -> None:
        pass

    def test_raise_containers_error(self) -> None:
        pass

    def test_not_valid_containers(self) -> None:
        pass
