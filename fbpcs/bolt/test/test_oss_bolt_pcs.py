#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.


import unittest
from collections import defaultdict
from unittest import mock
from unittest.mock import patch

from fbpcp.service.mpc import MPCService

from fbpcp.service.onedocker import OneDockerService

from fbpcs.bolt.oss_bolt_pcs import BoltPCSClient, BoltPCSCreateInstanceArgs
from fbpcs.onedocker_binary_config import OneDockerBinaryConfig
from fbpcs.onedocker_service_config import OneDockerServiceConfig
from fbpcs.pid.entity.pid_instance import PIDInstance, PIDInstanceStatus, PIDRole
from fbpcs.pid.service.pid_service.pid import PIDService
from fbpcs.private_computation.entity.infra_config import (
    InfraConfig,
    PrivateComputationGameType,
)
from fbpcs.private_computation.entity.pc_validator_config import PCValidatorConfig
from fbpcs.private_computation.entity.private_computation_instance import (
    PrivateComputationInstance,
    PrivateComputationRole,
)
from fbpcs.private_computation.entity.private_computation_status import (
    PrivateComputationInstanceStatus,
)
from fbpcs.private_computation.entity.product_config import (
    AggregationType,
    AttributionConfig,
    AttributionRule,
    CommonProductConfig,
    LiftConfig,
    ProductConfig,
)
from fbpcs.private_computation.service.constants import (
    DEFAULT_PID_PROTOCOL,
    NUM_NEW_SHARDS_PER_FILE,
)
from fbpcs.private_computation.service.errors import (
    PrivateComputationServiceValidationError,
)
from fbpcs.private_computation.service.private_computation import (
    PrivateComputationService,
)


class TestBoltPCSClient(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        container_svc_patcher = patch("fbpcp.service.container_aws.AWSContainerService")
        storage_svc_patcher = patch("fbpcp.service.storage_s3.S3StorageService")
        mpc_instance_repo_patcher = patch(
            "fbpcs.common.repository.mpc_instance_local.LocalMPCInstanceRepository"
        )
        pid_instance_repo_patcher = patch(
            "fbpcs.pid.repository.pid_instance_local.LocalPIDInstanceRepository"
        )
        private_computation_instance_repo_patcher = patch(
            "fbpcs.private_computation.repository.private_computation_instance_local.LocalPrivateComputationInstanceRepository"
        )
        mpc_game_svc_patcher = patch("fbpcp.service.mpc_game.MPCGameService")
        container_svc = container_svc_patcher.start()
        storage_svc = storage_svc_patcher.start()
        mpc_instance_repository = mpc_instance_repo_patcher.start()
        pid_instance_repository = pid_instance_repo_patcher.start()
        private_computation_instance_repository = (
            private_computation_instance_repo_patcher.start()
        )
        mpc_game_svc = mpc_game_svc_patcher.start()

        for patcher in (
            container_svc_patcher,
            storage_svc_patcher,
            mpc_instance_repo_patcher,
            pid_instance_repo_patcher,
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
            instance_repository=mpc_instance_repository,
            task_definition="test_task_definition",
            mpc_game_svc=mpc_game_svc,
        )

        self.pid_service = PIDService(
            instance_repository=pid_instance_repository,
            storage_svc=storage_svc,
            onedocker_svc=self.onedocker_service,
            onedocker_binary_config_map=self.onedocker_binary_config_map,
        )

        self.pc_validator_config = PCValidatorConfig(
            region="us-west-2",
        )

        self.private_computation_service = PrivateComputationService(
            instance_repository=private_computation_instance_repository,
            storage_svc=storage_svc,
            mpc_svc=self.mpc_service,
            pid_svc=self.pid_service,
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

    async def test_create_instance(self) -> None:
        bolt_instance_args = BoltPCSCreateInstanceArgs(
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
        )
        return_id = await self.bolt_pcs_client.create_instance(bolt_instance_args)
        self.assertEqual(return_id, self.test_instance_id)

    @mock.patch(
        "fbpcs.private_computation.service.private_computation.PrivateComputationService.update_instance"
    )
    async def test_update_instance(self, mock_update) -> None:
        # mock pc update_instance to return a pc instance with specific test status and instances
        test_pid_id = self.test_instance_id
        test_pid_role = PIDRole.PUBLISHER
        test_input_path = "pid_in"
        test_output_path = "pid_out"
        # create one PID instance to be put into PrivateComputationInstance
        pid_instance = PIDInstance(
            instance_id=test_pid_id,
            protocol=DEFAULT_PID_PROTOCOL,
            pid_role=test_pid_role,
            num_shards=self.test_num_containers,
            input_path=test_input_path,
            output_path=test_output_path,
            status=PIDInstanceStatus.STARTED,
            server_ips=["10.0.10.242"],
        )
        infra_config: InfraConfig = InfraConfig(
            instance_id=self.test_instance_id,
            role=self.test_role,
            status=PrivateComputationInstanceStatus.CREATED,
            status_update_ts=0,
            instances=[pid_instance],
            game_type=self.test_game_type,
            num_pid_containers=self.test_num_containers,
            num_mpc_containers=self.test_num_containers,
            num_files_per_mpc_container=NUM_NEW_SHARDS_PER_FILE,
        )
        common_product_config: CommonProductConfig = CommonProductConfig()
        product_config: ProductConfig
        if self.test_game_type is PrivateComputationGameType.ATTRIBUTION:
            product_config = AttributionConfig(
                common_product_config=common_product_config,
                attribution_rule=AttributionRule.LAST_CLICK_1D,
                aggregation_type=AggregationType.MEASUREMENT,
            )
        elif self.test_game_type is PrivateComputationGameType.LIFT:
            product_config = LiftConfig(common_product_config=common_product_config)
        test_instance = PrivateComputationInstance(
            infra_config=infra_config,
            product_config=product_config,
            input_path=self.test_input_path,
            output_dir=self.test_output_path,
        )
        mock_update.return_value = test_instance
        return_state = await self.bolt_pcs_client.update_instance(
            instance_id=self.test_instance_id,
        )
        self.assertEqual(
            return_state.pc_instance_status, PrivateComputationInstanceStatus.CREATED
        )

        self.assertEqual(["10.0.10.242"], return_state.server_ips)

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
