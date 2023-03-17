#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.


import unittest
from collections import defaultdict
from typing import Optional, Set
from unittest import mock
from unittest.mock import patch

from fbpcp.entity.container_instance import ContainerInstance, ContainerInstanceStatus

from fbpcp.service.onedocker import OneDockerService
from fbpcs.bolt.bolt_client import BoltState

from fbpcs.bolt.oss_bolt_pcs import BoltPCSClient, BoltPCSCreateInstanceArgs
from fbpcs.common.entity.stage_state_instance import (
    StageStateInstance,
    StageStateInstanceStatus,
)
from fbpcs.onedocker_binary_config import OneDockerBinaryConfig
from fbpcs.onedocker_service_config import OneDockerServiceConfig
from fbpcs.private_computation.entity.infra_config import (
    InfraConfig,
    PrivateComputationGameType,
    PrivateComputationRole,
)
from fbpcs.private_computation.entity.pc_validator_config import PCValidatorConfig
from fbpcs.private_computation.entity.pcs_feature import PCSFeature
from fbpcs.private_computation.entity.private_computation_instance import (
    PrivateComputationInstance,
)

from fbpcs.private_computation.entity.private_computation_status import (
    PrivateComputationInstanceStatus,
)
from fbpcs.private_computation.entity.product_config import (
    AggregationType,
    AnonymizerConfig,
    AttributionConfig,
    AttributionRule,
    CommonProductConfig,
    LiftConfig,
    PrivateIdDfcaConfig,
    ProductConfig,
)
from fbpcs.private_computation.service.errors import (
    PrivateComputationServiceValidationError,
)

from fbpcs.private_computation.service.mpc.mpc import MPCService
from fbpcs.private_computation.service.private_computation import (
    NUM_NEW_SHARDS_PER_FILE,
    PrivateComputationService,
)
from fbpcs.private_computation.stage_flows.private_computation_stage_flow import (
    PrivateComputationStageFlow,
)


class TestBoltPCSClient(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        container_svc_patcher = patch("fbpcp.service.container_aws.AWSContainerService")
        storage_svc_patcher = patch("fbpcp.service.storage_s3.S3StorageService")
        private_computation_instance_repo_patcher = patch(
            "fbpcs.private_computation.repository.private_computation_instance_local.LocalPrivateComputationInstanceRepository"
        )
        mpc_game_svc_patcher = patch(
            "fbpcs.private_computation.service.mpc.mpc_game.MPCGameService"
        )
        container_svc = container_svc_patcher.start()
        storage_svc = storage_svc_patcher.start()
        private_computation_instance_repository = (
            private_computation_instance_repo_patcher.start()
        )
        mpc_game_svc = mpc_game_svc_patcher.start()

        for patcher in (
            container_svc_patcher,
            storage_svc_patcher,
            private_computation_instance_repo_patcher,
            mpc_game_svc_patcher,
        ):
            self.addCleanup(patcher.stop)

        self.onedocker_service_config = OneDockerServiceConfig(
            task_definition="test_task_definition",
        )

        self.onedocker_binary_config_map = defaultdict(
            lambda: OneDockerBinaryConfig(
                tmp_directory="/test_tmp_directory/",
                binary_version="latest",
                repository_path="test_path/",
            )
        )

        self.onedocker_service = OneDockerService(
            container_svc, self.onedocker_service_config.task_definition
        )

        self.mpc_service = MPCService(
            container_svc=container_svc,
            task_definition="test_task_definition",
            mpc_game_svc=mpc_game_svc,
        )

        self.pc_validator_config = PCValidatorConfig(
            region="us-west-2",
        )

        self.private_computation_service = PrivateComputationService(
            instance_repository=private_computation_instance_repository,
            storage_svc=storage_svc,
            mpc_svc=self.mpc_service,
            onedocker_svc=self.onedocker_service,
            onedocker_binary_config_map=self.onedocker_binary_config_map,
            pc_validator_config=self.pc_validator_config,
        )

        self.bolt_pcs_client = BoltPCSClient(self.private_computation_service)

        self.test_instance_id = "test_id"
        self.test_role = PrivateComputationRole.PUBLISHER
        self.test_game_type = PrivateComputationGameType.LIFT
        self.test_input_path = "in_path"
        self.test_output_path = "out_path"
        self.test_num_containers = 2
        self.test_concurrency = 1
        self.test_hmac_key = "CoXbp7BOEvAN9L1CB2DAORHHr3hB7wE7tpxMYm07tc0="

        self.bolt_instance_args = BoltPCSCreateInstanceArgs(
            instance_id=self.test_instance_id,
            role=self.test_role,
            game_type=self.test_game_type,
            input_path=self.test_input_path,
            output_dir=self.test_output_path,
            num_pid_containers=self.test_num_containers,
            num_mpc_containers=self.test_num_containers,
            concurrency=self.test_concurrency,
            num_files_per_mpc_container=NUM_NEW_SHARDS_PER_FILE,
            hmac_key=self.test_hmac_key,
            pcs_features=[PCSFeature.PCS_DUMMY.value],
            log_cost_bucket="test_log_cost",
        )

    async def test_create_instance(self) -> None:
        return_id = await self.bolt_pcs_client.create_instance(self.bolt_instance_args)
        self.assertEqual(return_id, self.test_instance_id)

    @mock.patch(
        "fbpcs.private_computation.service.private_computation.PrivateComputationService.get_instance"
    )
    async def test_has_feature(self, mock_get_instance) -> None:
        # mock pc get_instancwe to return a pc instance with specific test status, instances and features.
        mock_get_instance.return_value = self._get_test_instance()
        for pcs_feature, expected_result in [
            (PCSFeature.PCS_DUMMY, True),
            (PCSFeature.PRIVATE_LIFT_PCF2_RELEASE, False),
        ]:
            with self.subTest(
                pcs_feature=pcs_feature,
                expected_result=expected_result,
            ):
                is_feature_enabled = await self.bolt_pcs_client.has_feature(
                    self.test_instance_id, pcs_feature
                )
                self.assertEqual(is_feature_enabled, expected_result)

    @mock.patch(
        "fbpcs.private_computation.service.private_computation.PrivateComputationService.update_instance"
    )
    async def test_update_instance(self, mock_update) -> None:
        # mock pc update_instance to return a pc instance with specific test status and instances
        mock_update.return_value = self._get_test_instance()
        return_state = await self.bolt_pcs_client.update_instance(
            instance_id=self.test_instance_id,
        )
        self.assertEqual(
            return_state.pc_instance_status, PrivateComputationInstanceStatus.CREATED
        )

        self.assertEqual(["10.0.10.242"], return_state.server_ips)
        self.assertEqual(None, return_state.issuer_certificate)
        self.assertEqual(None, return_state.server_hostnames)

    @mock.patch(
        "fbpcs.private_computation.service.private_computation.PrivateComputationService.update_instance"
    )
    async def test_update_instance_tls(self, mock_update) -> None:
        # mock pc update_instance to return a pc instance with specific test status and instances
        mock_update.return_value = self._get_test_instance(
            features={PCSFeature.PCF_TLS}
        )
        return_state = await self.bolt_pcs_client.update_instance(
            instance_id=self.test_instance_id,
        )
        self.assertEqual(
            return_state.pc_instance_status, PrivateComputationInstanceStatus.CREATED
        )

        self.assertEqual(["10.0.10.242"], return_state.server_ips)
        self.assertEqual("test_cert", return_state.issuer_certificate)
        self.assertEqual(["domain.test"], return_state.server_hostnames)

    @mock.patch(
        "fbpcs.private_computation.service.private_computation.PrivateComputationService.validate_metrics"
    )
    async def test_validate_results(self, mock_validate) -> None:
        # Confirm that validate_results returns False when an exception is raised
        mock_validate.side_effect = PrivateComputationServiceValidationError()
        result = await self.bolt_pcs_client.validate_results(
            self.test_instance_id, expected_result_path="test/path"
        )
        self.assertFalse(result)

        # Confirm that validate_results returns True when private_computation.validate_metrics runs successfully
        mock_validate.side_effect = None
        mock_validate.return_value = None
        result = await self.bolt_pcs_client.validate_results(
            self.test_instance_id, expected_result_path="test/path"
        )
        self.assertTrue(result)

    async def test_get_valid_stage(self) -> None:
        stage = PrivateComputationStageFlow.ID_MATCH
        for status, expected_stage in [
            # pyre-fixme: Undefined attribute [16]: Optional type has no attribute `completed_status`.
            (stage.previous_stage.completed_status, stage),
            (stage.started_status, stage),
            (stage.completed_status, stage.next_stage),
            (stage.failed_status, stage),
            (PrivateComputationStageFlow.get_last_stage().completed_status, None),
        ]:
            with self.subTest(status=status, expected_stage=expected_stage):
                self.bolt_pcs_client.update_instance = mock.AsyncMock(
                    return_value=BoltState(status)
                )
                valid_stage = await self.bolt_pcs_client.get_valid_stage(
                    "test_id", PrivateComputationStageFlow
                )
                self.assertEqual(valid_stage, expected_stage)

    @mock.patch("fbpcs.bolt.bolt_job.BoltCreateInstanceArgs")
    @mock.patch("fbpcs.bolt.bolt_client.BoltState")
    async def test_is_existing_instance(self, mock_state, mock_instance_args) -> None:
        self.bolt_pcs_client.update_instance = mock.AsyncMock(
            side_effect=[mock_state, Exception()]
        )
        for expected_result in (True, False):
            with self.subTest(expected_result=expected_result):
                actual_result = await self.bolt_pcs_client.is_existing_instance(
                    instance_args=mock_instance_args
                )
                self.assertEqual(actual_result, expected_result)

    async def test_get_or_create_instance(self) -> None:
        for exists in (True, False):
            with self.subTest(exists=exists):
                self.bolt_pcs_client.is_existing_instance = mock.AsyncMock(
                    return_value=exists
                )

                expected_result = self.bolt_instance_args.instance_id
                actual_result = await self.bolt_pcs_client.get_or_create_instance(
                    self.bolt_instance_args
                )
                self.assertEqual(expected_result, actual_result)

    def _get_test_instance(
        self, features: Optional[Set[PCSFeature]] = None
    ) -> PrivateComputationInstance:
        stage_state_instance = StageStateInstance(
            instance_id="stage_state_instance",
            stage_name="test_stage",
            status=StageStateInstanceStatus.COMPLETED,
            containers=[
                ContainerInstance(
                    instance_id="test_container_instance",
                    ip_address="10.0.10.242",
                    status=ContainerInstanceStatus.COMPLETED,
                )
            ],
            server_uris=["domain.test"],
        )
        if not features:
            features = {PCSFeature.PCS_DUMMY}

        infra_config: InfraConfig = InfraConfig(
            instance_id=self.test_instance_id,
            role=self.test_role,
            status=PrivateComputationInstanceStatus.CREATED,
            status_update_ts=0,
            instances=[stage_state_instance],
            game_type=self.test_game_type,
            num_pid_containers=self.test_num_containers,
            num_mpc_containers=self.test_num_containers,
            num_files_per_mpc_container=NUM_NEW_SHARDS_PER_FILE,
            status_updates=[],
            pcs_features=features,
            ca_certificate="test_cert",
        )
        common: CommonProductConfig = CommonProductConfig(
            input_path=self.test_input_path,
            output_dir=self.test_output_path,
        )
        product_config: ProductConfig
        if self.test_game_type is PrivateComputationGameType.ATTRIBUTION:
            product_config = AttributionConfig(
                common=common,
                attribution_rule=AttributionRule.LAST_CLICK_1D,
                aggregation_type=AggregationType.MEASUREMENT,
            )
        elif self.test_game_type is PrivateComputationGameType.LIFT:
            product_config = LiftConfig(common=common)
        elif self.test_game_type is PrivateComputationGameType.PRIVATE_ID_DFCA:
            product_config = PrivateIdDfcaConfig(common=common)
        elif self.test_game_type is PrivateComputationGameType.ANONYMIZER:
            product_config = AnonymizerConfig(common=common)
        test_instance = PrivateComputationInstance(
            infra_config=infra_config,
            product_config=product_config,
        )
        return test_instance

    async def test_should_invoke(self) -> None:
        stage = PrivateComputationStageFlow.ID_MATCH
        for status, expected_result in (
            (PrivateComputationInstanceStatus.PID_PREPARE_COMPLETED, True),
            (PrivateComputationInstanceStatus.ID_MATCHING_STARTED, False),
            (
                PrivateComputationInstanceStatus.ID_MATCHING_INITIALIZED,
                False,
            ),
            (PrivateComputationInstanceStatus.ID_MATCHING_FAILED, True),
            (PrivateComputationInstanceStatus.ID_MATCHING_COMPLETED, False),
            (PrivateComputationInstanceStatus.PROCESSING_REQUEST, False),
            (PrivateComputationInstanceStatus.ID_SPINE_COMBINER_FAILED, False),
        ):
            with self.subTest(status=status, expected_result=expected_result):
                self.bolt_pcs_client.update_instance = mock.AsyncMock(
                    return_value=BoltState(status)
                )
                actual_result = await self.bolt_pcs_client.should_invoke_stage(
                    "instance_id", stage
                )
                self.assertEqual(expected_result, actual_result)

    @mock.patch(
        "fbpcs.private_computation.service.private_computation.PrivateComputationService"
    )
    async def test_run_stage_with_stage(self, MockPrivateComputationService) -> None:
        # Arrange
        expected_instance_id = self.test_instance_id
        expected_stage = PrivateComputationStageFlow.ID_MATCH
        expected_server_ips = ["127.0.0.1", "127.0.0.2"]
        expected_ca_certificate = "test_cert"
        expected_server_hostnames = ["node0.test", "node1.test"]

        pl_instance = mock.MagicMock()
        mock_pcs = MockPrivateComputationService()
        mock_pcs.run_stage_async = mock.AsyncMock(return_value=pl_instance)
        client = BoltPCSClient(mock_pcs, None)

        # Act
        await client.run_stage(
            expected_instance_id,
            stage=expected_stage,
            server_ips=expected_server_ips,
            ca_certificate=expected_ca_certificate,
            server_hostnames=expected_server_hostnames,
        )

        # Assert
        mock_pcs.run_stage_async.assert_awaited_once_with(
            instance_id=expected_instance_id,
            stage=expected_stage,
            server_ips=expected_server_ips,
            ca_certificate=expected_ca_certificate,
            server_hostnames=expected_server_hostnames,
        )

    @mock.patch(
        "fbpcs.private_computation.service.private_computation.PrivateComputationService"
    )
    async def test_run_stage_without_stage(self, MockPrivateComputationService) -> None:
        # Arrange
        expected_instance_id = self.test_instance_id
        expected_server_ips = ["127.0.0.1", "127.0.0.2"]
        expected_ca_certificate = "test_cert"
        expected_server_hostnames = ["node0.test", "node1.test"]

        pl_instance = mock.MagicMock()
        mock_pcs = MockPrivateComputationService()
        mock_pcs.run_next_async = mock.AsyncMock(return_value=pl_instance)
        client = BoltPCSClient(mock_pcs, None)

        # Act
        await client.run_stage(
            expected_instance_id,
            server_ips=expected_server_ips,
            ca_certificate=expected_ca_certificate,
            server_hostnames=expected_server_hostnames,
        )

        # Assert
        mock_pcs.run_next_async.assert_awaited_once_with(
            instance_id=expected_instance_id,
            server_ips=expected_server_ips,
            ca_certificate=expected_ca_certificate,
            server_hostnames=expected_server_hostnames,
        )
