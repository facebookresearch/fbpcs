#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

import itertools
from typing import List, Optional
from unittest import IsolatedAsyncioTestCase
from unittest.mock import patch

from fbpcp.entity.container_instance import ContainerInstance, ContainerInstanceStatus
from fbpcs.common.entity.stage_state_instance import StageStateInstance
from fbpcs.onedocker_binary_config import OneDockerBinaryConfig

from fbpcs.pcf.tests.async_utils import AsyncMock, to_sync
from fbpcs.private_computation.entity.infra_config import InfraConfig
from fbpcs.private_computation.entity.private_computation_instance import (
    PrivateComputationGameType,
    PrivateComputationInstance,
    PrivateComputationInstanceStatus,
    PrivateComputationRole,
)

from fbpcs.private_computation.service.pid_shard_stage_service import (
    PIDShardStageService,
)

from libfb.py.testutil import data_provider, MagicMock


class TestPIDShardStageService(IsolatedAsyncioTestCase):
    @patch("fbpcp.service.storage.StorageService")
    @patch("fbpcp.service.onedocker.OneDockerService")
    def setUp(self, mock_onedocker_svc, mock_storage_svc) -> None:
        self.mock_onedocker_svc = mock_onedocker_svc
        self.mock_storage_svc = mock_storage_svc
        self.onedocker_binary_config = OneDockerBinaryConfig(
            tmp_directory="/tmp",
            binary_version="latest",
            repository_path="test_path/",
        )
        self.onedocker_binary_config = self.onedocker_binary_config
        self.binary_name = "data_processing/sharder_hashed_for_pid"
        self.onedocker_binary_config_map = {
            self.binary_name: self.onedocker_binary_config
        }
        self.input_path = "in"
        self.output_path = "out"
        self.pc_instance_id = "test_instance_123"
        self.container_timeout = 789
        self.test_hmac_key = "CoXbp7BOEvAN9L1CB2DAORHHr3hB7wE7tpxMYm07tc0="

    @data_provider(
        lambda: (
            itertools.product(
                [PrivateComputationRole.PUBLISHER, PrivateComputationRole.PARTNER],
                [1, 2],
                [True, False],
            )
        )
    )
    @to_sync
    async def test_pid_shard_stage_service(
        self,
        pc_role: PrivateComputationRole,
        test_num_containers: int,
        has_hmac_key: bool,
    ) -> None:
        hamc_key_expected = self.test_hmac_key if has_hmac_key else None
        pc_instance = self.create_sample_pc_instance(
            pc_role, test_num_containers, hamc_key_expected
        )
        stage_svc = PIDShardStageService(
            storage_svc=self.mock_storage_svc,
            onedocker_svc=self.mock_onedocker_svc,
            onedocker_binary_config_map=self.onedocker_binary_config_map,
            container_timeout=self.container_timeout,
        )
        containers = [
            self.create_container_instance() for _ in range(test_num_containers)
        ]
        self.mock_onedocker_svc.start_containers = MagicMock(return_value=containers)
        self.mock_onedocker_svc.wait_for_pending_containers = AsyncMock(
            return_value=containers
        )
        updated_pc_instance = await stage_svc.run_async(pc_instance=pc_instance)
        env_vars = {
            "ONEDOCKER_REPOSITORY_PATH": self.onedocker_binary_config.repository_path
        }
        args_ls_expect = self.get_args_expect(
            pc_role, test_num_containers, has_hmac_key
        )
        # test the start_containers is called with expected parameters
        self.mock_onedocker_svc.start_containers.assert_called_with(
            package_name=self.binary_name,
            version=self.onedocker_binary_config.binary_version,
            cmd_args_list=args_ls_expect,
            timeout=self.container_timeout,
            env_vars=env_vars,
        )
        # test the return value is as expected
        self.assertEqual(
            len(updated_pc_instance.infra_config.instances),
            1,
            "Failed to add the StageStageInstance into pc_instance",
        )
        stage_state_expect = StageStateInstance(
            pc_instance.infra_config.instance_id,
            pc_instance.current_stage.name,
            containers=containers,
        )
        stage_state_actual = updated_pc_instance.infra_config.instances[0]
        self.assertEqual(
            stage_state_actual,
            stage_state_expect,
            "Appended StageStageInstance is not as expected",
        )

    def create_sample_pc_instance(
        self,
        pc_role: PrivateComputationRole,
        test_num_containers: int,
        hmac_key: Optional[str],
    ) -> PrivateComputationInstance:
        infra_config: InfraConfig = InfraConfig(
            instance_id=self.pc_instance_id,
            role=pc_role,
            status=PrivateComputationInstanceStatus.PID_SHARD_COMPLETED,
            status_update_ts=1600000000,
            instances=[],
            game_type=PrivateComputationGameType.LIFT,
            num_pid_containers=test_num_containers,
            num_mpc_containers=test_num_containers,
            num_files_per_mpc_container=test_num_containers,
        )
        return PrivateComputationInstance(
            infra_config,
            input_path=self.input_path,
            output_dir=self.output_path,
            hmac_key=hmac_key,
        )

    def create_container_instance(self) -> ContainerInstance:
        return ContainerInstance(
            instance_id="test_container_instance_123",
            ip_address="127.0.0.1",
            status=ContainerInstanceStatus.COMPLETED,
        )

    def get_args_expect(
        self,
        pc_role: PrivateComputationRole,
        num_containers: int,
        has_hmac_key: bool,
    ) -> List[str]:
        if pc_role is PrivateComputationRole.PUBLISHER:
            args = f"--input_filename=in --output_base_path=out/test_instance_123_out_dir/pid_stage/out.csv_publisher_sharded --file_start_index=0 --num_output_files={num_containers} --tmp_directory=/tmp"
        else:
            args = f"--input_filename=in --output_base_path=out/test_instance_123_out_dir/pid_stage/out.csv_advertiser_sharded --file_start_index=0 --num_output_files={num_containers} --tmp_directory=/tmp"
        if has_hmac_key:
            args += f" --hmac_base64_key={self.test_hmac_key}"
        return [args]
